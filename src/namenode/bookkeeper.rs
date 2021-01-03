use crate::block::Block;
use crate::config::Config;
use crate::error::{Result, UdfsError};
use crate::namenode::block_map::BlockMap;
use crate::namenode::creates_in_progress::CreatesInProgress;
use crate::namenode::datanode_info::DataNodeInfo;
use crate::namenode::dfs_state::DfsState;
use crate::namenode::editlog::{EditLog, EditOperation};

use std::collections::{HashMap, HashSet};
use std::sync::{atomic, Mutex, RwLock};
use std::time::{Duration, Instant};

use rand::seq::SliceRandom;
use rand::thread_rng;

use tokio::time;

use tracing::info;

pub(crate) struct BookKeeper {
    // logs all changes, so that state can be recovered after a restart
    edit_logger: tokio::sync::Mutex<EditLog>,
    // keeps track of the latest heartbeats
    heartbeats: Mutex<HashMap<String, Instant>>,
    // set of alive datanode addresses
    alive_datanodes: Mutex<HashMap<String, DataNodeInfo>>,
    dfs_state: RwLock<DfsState>,
    heartbeat_decomission: Duration,
    heartbeat_check_interval: Duration,
    // from config
    block_replication: u64,
    block_size: u64,

    // keep track which files are about to created
    creates_in_progress: RwLock<CreatesInProgress>,
    // map datanode address to its blocks
    datanode_to_blocks: Mutex<HashMap<String, HashSet<Block>>>,
    // map block to datanode addresses
    block_to_datanodes: RwLock<BlockMap>,
    // TODO: Last generated block id does not survive. Make it persistent.
    block_id_generator: atomic::AtomicU64,
}

impl BookKeeper {
    pub(crate) fn new(config: &Config) -> Result<Self> {
        let edit_logger = tokio::sync::Mutex::new(EditLog::open(&config)?);
        let heartbeats = Mutex::new(HashMap::new());
        let alive_datanodes = Mutex::new(HashMap::new());
        let dfs_state = RwLock::new(DfsState::new());
        let creates_in_progress = RwLock::new(CreatesInProgress::new());
        let heartbeat_decomission = Duration::from_millis(config.namenode.heartbeat_decomission);
        let heartbeat_check_interval = Duration::from_millis(config.namenode.heartbeat_check);
        let datanode_to_blocks = Mutex::new(HashMap::new());
        let block_to_datanodes = RwLock::new(BlockMap::new());

        Ok(Self {
            edit_logger,
            heartbeats,
            alive_datanodes,
            dfs_state,
            heartbeat_decomission,
            heartbeat_check_interval,
            block_replication: config.dfs.replication,
            block_size: config.dfs.block_size,
            creates_in_progress,
            datanode_to_blocks,
            block_to_datanodes,
            block_id_generator: atomic::AtomicU64::new(1),
        })
    }

    pub(crate) async fn restore(&self) -> Result<()> {
        let mut edit_logger = self.edit_logger.lock().await;
        let ops = edit_logger.restore().await?;
        for op in ops {
            match op {
                EditOperation::Mkdir(path) => self.non_logging_mkdir(&path)?,
                EditOperation::AddFile(filename, blocks) => {
                    self.non_logging_finish_file(&filename, &blocks)?
                }
            };
        }
        Ok(())
    }

    pub(crate) async fn run(&self) {
        let heartbeat_monitor = HeartbeatMonitor {
            bookkeeper: &self,
            heartbeat_check_interval: self.heartbeat_check_interval,
        };
        heartbeat_monitor.run().await;
    }

    pub(crate) fn receive_heartbeat(&self, datanode_address: String, available: u64, used: u64) {
        let datanode_info = DataNodeInfo::new(&datanode_address, available, used);
        let mut heartbeats = self.heartbeats.lock().unwrap();
        let mut alive_datanodes = self.alive_datanodes.lock().unwrap();
        if heartbeats
            .insert(datanode_address.clone(), Instant::now())
            .is_none()
        {
            info!("Received new heartbeat from: {}", datanode_address);
            let mut datanode_to_blocks = self.datanode_to_blocks.lock().unwrap();
            datanode_to_blocks.insert(datanode_address.clone(), HashSet::new());
        }
        alive_datanodes.insert(datanode_address, datanode_info);
    }

    pub(crate) fn nodes_report(&self) -> Vec<DataNodeInfo> {
        let alive_datanodes = self.alive_datanodes.lock().unwrap();
        alive_datanodes.values().cloned().collect()
    }

    pub(crate) fn ls(&self, path: &str) -> Result<Vec<String>> {
        let dfs_state = self.dfs_state.read().unwrap();
        Ok(dfs_state
            .ls(path)?
            .iter()
            .map(|s| String::from(*s))
            .collect())
    }

    pub(crate) async fn mkdir(&self, path: &str) -> Result<()> {
        self.non_logging_mkdir(path)?;
        self.log_operation(EditOperation::Mkdir(path.to_owned()))
            .await;
        Ok(())
    }

    fn non_logging_mkdir(&self, path: &str) -> Result<()> {
        let mut dfs_state = self.dfs_state.write().unwrap();
        dfs_state.mkdir(path)
    }

    pub(crate) fn open_file(&self, path: &str) -> Result<Vec<(Block, Vec<String>)>> {
        let dfs_state = self.dfs_state.read().unwrap();
        let block_to_datanodes = self.block_to_datanodes.read().unwrap();
        let file_blocks = dfs_state.open_file(path)?;

        Ok(file_blocks
            .iter()
            .map(|block| {
                let datanodes = block_to_datanodes
                    .get_nodes(block.id)
                    .expect("If file with block exists, then this block should be replicated")
                    .map(|s| s.to_owned())
                    .collect();
                (*block, datanodes)
            })
            .collect())
    }

    pub(crate) fn start_file_create(
        &self,
        path: &str,
    ) -> Result<Option<(Block, Vec<DataNodeInfo>)>> {
        let dfs_state = self.dfs_state.read().unwrap();
        dfs_state.check_file_creation(path)?;
        self.creates_in_progress
            .write()
            .unwrap()
            .add_file(path.to_owned())?;

        let mut target_nodes = HashSet::new();
        let mut available_nodes = self
            .alive_datanodes
            .lock()
            .unwrap()
            .values()
            .cloned()
            .collect::<Vec<_>>();
        available_nodes.shuffle(&mut thread_rng());

        for node_info in available_nodes {
            if node_info.available() > self.block_size {
                target_nodes.insert(node_info);
            }
            if target_nodes.len() as u64 >= self.block_replication {
                let block_id = self.next_block_id();
                let block = Block::new(block_id, 0);
                let mut blocks = HashSet::new();
                blocks.insert(block);
                self.creates_in_progress
                    .write()
                    .unwrap()
                    .add_block(path, block_id)?;
                return Ok(Some((block, target_nodes.into_iter().collect())));
            }
        }

        Ok(None)
    }

    pub(crate) fn abort_file_create(&self, path: &str) -> Result<()> {
        let mut creates_in_progress = self.creates_in_progress.write().unwrap();
        creates_in_progress.remove_file(path)?;
        Ok(())
    }

    pub(crate) async fn finish_file_create(&self, path: &str) -> Result<()> {
        let blocks = self.internal_finish_file_create(path)?;
        self.non_logging_finish_file(path, blocks.as_slice())?;
        self.log_operation(EditOperation::AddFile(path.to_owned(), blocks))
            .await;

        Ok(())
    }

    fn internal_finish_file_create(&self, path: &str) -> Result<Vec<Block>> {
        let creates_in_progress = self.creates_in_progress.read().unwrap();
        let block_ids = creates_in_progress.block_ids(path)?;
        for block_id in block_ids {
            let replication_count = creates_in_progress.replication_count(*block_id);
            if replication_count < self.block_replication {
                return Err(UdfsError::WaitingForReplication(format!(
                    "Block {} has been replicated only {} times, but {} replications are required",
                    block_id, replication_count, self.block_replication,
                )));
            }
        }

        let block_to_datanodes = self.block_to_datanodes.read().unwrap();
        let blocks = block_ids
            .iter()
            .map(|id| {
                block_to_datanodes
                    .stored_block(*id)
                    .expect("Block should exist")
            })
            .collect();
        Ok(blocks)
    }

    fn non_logging_finish_file(&self, path: &str, blocks: &[Block]) -> Result<()> {
        let mut dfs_state = self.dfs_state.write().unwrap();
        dfs_state.create_file(path, blocks)?;
        let mut block_map = self.block_to_datanodes.write().unwrap();
        for block in blocks {
            block_map.add_block(*block);
        }
        Ok(())
    }

    pub(crate) fn start_another_block(
        &self,
        path: &str,
    ) -> Result<Option<(Block, Vec<DataNodeInfo>)>> {
        self.check_all_blocks_replicated(path)?;

        let mut target_nodes = HashSet::new();
        let mut available_nodes = self
            .alive_datanodes
            .lock()
            .unwrap()
            .values()
            .cloned()
            .collect::<Vec<_>>();
        available_nodes.shuffle(&mut thread_rng());

        for node_info in available_nodes {
            if node_info.available() > self.block_size {
                target_nodes.insert(node_info);
            }
            if target_nodes.len() as u64 >= self.block_replication {
                let block_id = self.next_block_id();
                let block = Block::new(block_id, 0);
                self.creates_in_progress
                    .write()
                    .unwrap()
                    .add_block(path, block_id)?;
                return Ok(Some((block, target_nodes.into_iter().collect())));
            }
        }

        Ok(None)
    }

    fn check_all_blocks_replicated(&self, path: &str) -> Result<()> {
        let creates_in_progress = self.creates_in_progress.read().unwrap();
        let block_ids = creates_in_progress.block_ids(path)?;
        for block_id in block_ids {
            let replication_count = self
                .creates_in_progress
                .read()
                .unwrap()
                .replication_count(*block_id);
            if replication_count < self.block_replication {
                return Err(UdfsError::WaitingForReplication(format!(
                    "Block {} has been replicated only {} times, but {} replications are required",
                    block_id, replication_count, self.block_replication,
                )));
            }
        }
        Ok(())
    }

    pub(crate) fn block_received(&self, block: &Block, datanode_address: &str) -> Result<()> {
        let mut datanode_to_blocks = self.datanode_to_blocks.lock().unwrap();
        let mut block_to_datanodes = self.block_to_datanodes.write().unwrap();

        let newly_reported = block_to_datanodes.add_node(*block, datanode_address.to_owned());
        if newly_reported {
            let mut creates_in_progress = self.creates_in_progress.write().unwrap();
            creates_in_progress.increment_replication(block.id);
        }

        let blocks = datanode_to_blocks
            .get_mut(datanode_address)
            .ok_or_else(|| {
                UdfsError::FSError(format!(
                    "Received that node '{}' has this block, but 
            this datanode has not been registered yet.",
                    datanode_address
                ))
            })?;
        blocks.insert(*block);

        Ok(())
    }

    pub(crate) fn abort_block(&self, path: &str, block: &Block) -> Result<()> {
        let mut creates_in_progress = self.creates_in_progress.write().unwrap();
        creates_in_progress.remove_block(path, block.id)?;
        Ok(())
    }

    async fn log_operation(&self, op: EditOperation) {
        let mut edit_logger = self.edit_logger.lock().await;
        edit_logger.log_operation(&op).await;
    }

    fn check_heartbeat(&self) {
        let mut heartbeats = self.heartbeats.lock().unwrap();
        let mut alive_datanodes = self.alive_datanodes.lock().unwrap();

        heartbeats.retain(|address, last_heartbeat| {
            if last_heartbeat.elapsed() > self.heartbeat_decomission {
                info!("Removing {} from activate datanodes", address);
                alive_datanodes.remove(address);
                false
            } else {
                true
            }
        });
    }

    fn next_block_id(&self) -> u64 {
        let block_to_datanodes = self.block_to_datanodes.read().unwrap();
        let creates_in_progress = self.creates_in_progress.read().unwrap();
        loop {
            let block_id = self
                .block_id_generator
                .fetch_add(1, atomic::Ordering::Relaxed);
            if block_to_datanodes.contains_block(block_id)
                || creates_in_progress.contains_block(block_id)
            {
                continue;
            } else {
                return block_id;
            }
        }
    }
}

struct HeartbeatMonitor<'a> {
    bookkeeper: &'a BookKeeper,
    heartbeat_check_interval: Duration,
}

impl<'a> HeartbeatMonitor<'a> {
    async fn run(&self) {
        let mut heartbeat_check_interval = time::interval(self.heartbeat_check_interval);

        loop {
            heartbeat_check_interval.tick().await;
            self.bookkeeper.check_heartbeat();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::path::PathBuf;

    use tempdir::TempDir;

    const BLOCK_SIZE: u64 = 10;
    const DFS_REPLICATION: u64 = 2;

    #[test]
    fn test_start_file_create() {
        let tempdir = TempDir::new("bookkeeper-test").unwrap();
        let conf = get_config(tempdir.path());
        let mut alive_datanodes = HashMap::new();
        alive_datanodes.insert(
            "foo:42".to_owned(),
            DataNodeInfo::new("foo:42", BLOCK_SIZE * 10, 0),
        );
        alive_datanodes.insert(
            "bar:42".to_owned(),
            DataNodeInfo::new("bar:42", BLOCK_SIZE * 10, 0),
        );
        alive_datanodes.insert(
            "baz:42".to_owned(),
            DataNodeInfo::new("baz:42", BLOCK_SIZE * 10, 0),
        );

        let mut bookkeeper = BookKeeper::new(&conf).unwrap();
        bookkeeper.alive_datanodes = Mutex::new(alive_datanodes);

        let block_with_targets = bookkeeper
            .start_file_create("/foo.txt")
            .expect("File creation should start without any problems");
        let (_, targets) =
            block_with_targets.expect("There are enough alive datanodes with free space");

        assert_eq!(targets.len() as u64, DFS_REPLICATION);
    }

    fn get_config(logdir: impl Into<PathBuf>) -> Config {
        let mut config = Config::default();
        config.dfs.replication = DFS_REPLICATION;
        config.dfs.block_size = BLOCK_SIZE;
        config.namenode.name_dir = logdir.into();
        config
    }
}
