use crate::block::Block;
use crate::config::Config;
use crate::datanode::disk_statistics::DiskStatistics;
use crate::error::{Result, UdfsError};

use std::collections::HashSet;
use std::ops::{Deref, DerefMut};
use std::path::PathBuf;
use std::sync::Mutex;

use tokio::fs;
use tokio::fs::File;

pub(crate) struct DataNodeStorage {
    disk_statistics: Mutex<DiskStatistics>,
    // keep track which blocks are about to created
    creates_in_progress: Mutex<HashSet<Block>>,
    // Directory of block files
    block_directory: PathBuf,
}

impl DataNodeStorage {
    pub(crate) fn new(config: &Config) -> Result<Self> {
        let disk_statistics = Mutex::new(DiskStatistics::new(config)?);
        let block_directory = config.datanode.data_dir.clone();
        Ok(Self {
            disk_statistics,
            creates_in_progress: Mutex::new(HashSet::new()),
            block_directory,
        })
    }

    pub(crate) fn used(&self) -> Result<u64> {
        self.disk_statistics.lock().unwrap().used()
    }

    pub(crate) fn available(&self) -> Result<u64> {
        self.disk_statistics.lock().unwrap().available()
    }

    pub(crate) fn get_blockfile(&self, block: &Block) -> Result<File> {
        let path = self.block_directory.join(block.filename());
        let file = std::fs::File::open(path)?;
        Ok(File::from_std(file))
    }

    pub(crate) async fn start_block_creation(&self, block: &Block) -> Result<File> {
        if self.block_exists(block) {
            return Err(UdfsError::IOError(format!(
                "Block {:?} already exists",
                block
            )));
        }

        self.add_new_in_progress_block(block)?;
        let block_path = self.block_directory.join(block.filename() + ".tmp");
        let file = match File::create(block_path).await {
            Ok(file) => file,
            Err(e) => {
                let mut creates_in_progress = self.creates_in_progress.lock().unwrap();
                creates_in_progress.deref_mut().remove(block);
                return Err(e.into());
            }
        };

        Ok(file)
    }

    pub(crate) async fn abort_block_creation(&self, block: &Block) -> Result<()> {
        self.remove_in_progress_block(block)
    }

    pub(crate) async fn finish_block_creation(&self, block: &Block) -> Result<()> {
        self.remove_in_progress_block(block)?;
        let filename = self.block_directory.join(block.filename());
        let temp_filename = self
            .block_directory
            .join(format!("{}.tmp", block.filename()));
        fs::rename(temp_filename, filename).await?;

        Ok(())
    }

    fn add_new_in_progress_block(&self, block: &Block) -> Result<()> {
        let mut creates_in_progress = self.creates_in_progress.lock().unwrap();
        if creates_in_progress.deref().contains(block) {
            return Err(UdfsError::IOError(format!(
                "A creation of block {:?} is already in progress",
                block
            )));
        }
        creates_in_progress.deref_mut().insert(*block);
        Ok(())
    }

    fn remove_in_progress_block(&self, block: &Block) -> Result<()> {
        let mut creates_in_progress = self.creates_in_progress.lock().unwrap();
        if !creates_in_progress.deref().contains(block) {
            return Err(UdfsError::IOError(format!(
                "Creation of block {:?} has not started",
                block
            )));
        }
        creates_in_progress.deref_mut().remove(block);
        Ok(())
    }

    fn block_exists(&self, block: &Block) -> bool {
        self.block_directory.join(block.filename()).exists()
    }
}
