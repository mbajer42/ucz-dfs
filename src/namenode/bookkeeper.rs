use crate::block::Block;
use crate::config::Config;
use crate::error::{Result, UdfsError};
use crate::namenode::datanode_info::DataNodeInfo;
use crate::namenode::dfs_state::DfsState;

use std::collections::{hash_map::Entry, HashMap, HashSet};
use std::sync::{atomic, Mutex, RwLock};
use std::time::{Duration, Instant};

use rand::seq::SliceRandom;
use rand::thread_rng;

use tokio::time;

use tracing::info;

pub(crate) struct BookKeeper {
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

    // TODO: The following 3 maps have the problem that block length might be never get updated.

    // keep track which files are about to created
    creates_in_progress: Mutex<HashMap<String, HashSet<Block>>>,
    // map datanode address to its blocks
    datanode_to_blocks: Mutex<HashMap<String, HashSet<Block>>>,
    // map block to datanode addresses
    block_to_datanodes: Mutex<HashMap<Block, HashSet<String>>>,
    // TODO: Last generated block id does not survive. Make it persistent.
    block_id_generator: atomic::AtomicU64,
}

impl BookKeeper {
    pub(crate) fn new(config: &Config) -> Self {
        let heartbeats = Mutex::new(HashMap::new());
        let alive_datanodes = Mutex::new(HashMap::new());
        let dfs_state = RwLock::new(DfsState::new());
        let heartbeat_decomission = Duration::from_millis(config.namenode.heartbeat_decomission);
        let heartbeat_check_interval = Duration::from_millis(config.namenode.heartbeat_check);
        let creates_in_progress = Mutex::new(HashMap::new());
        let datanode_to_blocks = Mutex::new(HashMap::new());
        let block_to_datanodes = Mutex::new(HashMap::new());

        Self {
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
        }
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

    pub(crate) fn mkdir(&self, path: &str) -> Result<()> {
        let mut dfs_state = self.dfs_state.write().unwrap();
        dfs_state.mkdir(path)
    }

    pub(crate) fn open_file(&self, path: &str) -> Result<Vec<(Block, Vec<String>)>> {
        let dfs_state = self.dfs_state.read().unwrap();
        let block_to_datanodes = self.block_to_datanodes.lock().unwrap();
        let file_blocks = dfs_state.open_file(path)?;

        Ok(file_blocks
            .iter()
            .map(|block| {
                let datanodes = block_to_datanodes
                    .get(block)
                    .expect("If file with block exists, then this block should be replicated")
                    .iter()
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
        let mut creates_in_progress = self.creates_in_progress.lock().unwrap();
        dfs_state.check_file_creation(path)?;

        if creates_in_progress.contains_key(path) {
            return Err(UdfsError::FSError(format!(
                "'{}': File creation already in progress",
                path
            )));
        }

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
            if target_nodes.len() as u64 > self.block_replication {
                let block_id = self.next_block_id();
                let block = Block::new(block_id, 0);
                let mut blocks = HashSet::new();
                blocks.insert(block);
                creates_in_progress.insert(path.to_owned(), blocks);
                return Ok(Some((block, target_nodes.into_iter().collect())));
            }
        }

        Ok(None)
    }

    pub(crate) fn abort_file_create(&self, path: &str) {
        let mut creates_in_progress = self.creates_in_progress.lock().unwrap();
        creates_in_progress.remove(path);
    }

    pub(crate) fn finish_file_create(&self, path: &str) -> Result<()> {
        let mut creates_in_progress = self.creates_in_progress.lock().unwrap();
        let blocks = creates_in_progress.get_mut(path).ok_or_else(|| {
            UdfsError::FSError(format!("'{}': File creation not in progress", path))
        })?;

        let block_to_datanodes = self.block_to_datanodes.lock().unwrap();
        let mut blocks = blocks.iter().copied().collect::<Vec<_>>();
        // Blocks with lower id were generated earlier
        blocks.sort_by_key(|block| block.id);

        let is_block_replicated =
            |datanodes: &HashSet<String>| (datanodes.len() as u64) >= self.block_replication;

        for block in blocks.iter() {
            if !block_to_datanodes
                .get(block)
                .map_or(false, is_block_replicated)
            {
                return Err(UdfsError::FSError(
                    "Not all blocks have been written yet".to_owned(),
                ));
            }
        }

        let mut dfs_state = self.dfs_state.write().unwrap();
        dfs_state.create_file(path, &blocks)?;

        Ok(())
    }

    pub(crate) fn start_another_block(
        &self,
        path: &str,
    ) -> Result<Option<(Block, Vec<DataNodeInfo>)>> {
        let mut creates_in_progress = self.creates_in_progress.lock().unwrap();
        let block_to_datanodes = self.block_to_datanodes.lock().unwrap();
        let blocks = creates_in_progress.get_mut(path).ok_or_else(|| {
            UdfsError::FSError(format!("'{}': File creation has not started yet", path))
        })?;

        let is_block_replicated =
            |datanodes: &HashSet<String>| (datanodes.len() as u64) >= self.block_replication;

        for block in blocks.iter() {
            if !block_to_datanodes
                .get(block)
                .map_or(false, is_block_replicated)
            {
                return Err(UdfsError::FSError(
                    "Not all previous blocks have been written yet".to_owned(),
                ));
            }
        }

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
            if target_nodes.len() as u64 > self.block_replication {
                let block_id = self.next_block_id();
                let block = Block::new(block_id, 0);
                blocks.insert(block);
                return Ok(Some((block, target_nodes.into_iter().collect())));
            }
        }

        Ok(None)
    }

    pub(crate) fn block_received(
        &self,
        block: &Block,
        datanode_address: &str,
        path: &str,
    ) -> Result<()> {
        let mut datanode_to_blocks = self.datanode_to_blocks.lock().unwrap();
        let mut block_to_datanodes = self.block_to_datanodes.lock().unwrap();
        let mut creates_in_progress = self.creates_in_progress.lock().unwrap();

        // update blocks in creates_in_progress to new block length
        let blocks = creates_in_progress.get_mut(path).ok_or_else(|| {
            UdfsError::FSError(format!(
                "Block does not seem to be part of the file {}",
                path
            ))
        })?;
        blocks.replace(*block);

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

        let datanodes = match block_to_datanodes.entry(*block) {
            Entry::Vacant(entry) => entry.insert(HashSet::new()),
            Entry::Occupied(entry) => entry.into_mut(),
        };
        datanodes.insert(datanode_address.to_owned());

        Ok(())
    }

    pub(crate) fn abort_block(&self, path: &str, block: &Block) -> Result<()> {
        let mut creates_in_progress = self.creates_in_progress.lock().unwrap();
        let blocks = creates_in_progress.get_mut(path).ok_or_else(|| {
            UdfsError::FSError(format!("'{}': File creation is not in progress", path))
        })?;

        if blocks.remove(block) {
            Ok(())
        } else {
            Err(UdfsError::FSError(format!(
                "Block with id '{}' was not in progress for file '{}'",
                block.id, path
            )))
        }
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
        self.block_id_generator
            .fetch_add(1, atomic::Ordering::Relaxed)
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
