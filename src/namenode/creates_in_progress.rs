use crate::error::{Result, UdfsError};

use std::collections::HashMap;

/// Keeps track of all files and blocks which are in progress.
pub(crate) struct CreatesInProgress {
    // filename to block ids
    filename_to_blocks: HashMap<String, Vec<u64>>,
    // block id to replication count
    block_to_replication_count: HashMap<u64, u64>,
}

impl CreatesInProgress {
    pub(crate) fn new() -> Self {
        Self {
            filename_to_blocks: HashMap::new(),
            block_to_replication_count: HashMap::new(),
        }
    }

    pub(crate) fn block_ids(&self, filename: &str) -> Result<&[u64]> {
        let blocks = self.filename_to_blocks.get(filename);
        if let Some(blocks) = blocks {
            Ok(blocks)
        } else {
            Err(UdfsError::FSError(format!(
                "'{}': File creation has not started yet",
                filename
            )))
        }
    }

    pub(crate) fn replication_count(&self, block_id: u64) -> u64 {
        *self.block_to_replication_count.get(&block_id).unwrap_or(&0)
    }

    pub(crate) fn increment_replication(&mut self, block_id: u64) {
        let prev_count = *self.block_to_replication_count.get(&block_id).unwrap_or(&0);
        self.block_to_replication_count
            .insert(block_id, prev_count + 1);
    }

    pub(crate) fn add_file(&mut self, filename: String) -> Result<()> {
        if self.filename_to_blocks.contains_key(&filename) {
            return Err(UdfsError::FSError(format!(
                "'{}': File creation already in progress",
                filename
            )));
        }
        self.filename_to_blocks.insert(filename, Vec::new());
        Ok(())
    }

    pub(crate) fn add_block(&mut self, filename: &str, block_id: u64) -> Result<()> {
        let blocks = self.filename_to_blocks.get_mut(filename);
        if let Some(blocks) = blocks {
            blocks.push(block_id);
            Ok(())
        } else {
            Err(UdfsError::FSError(format!(
                "'{}': File creation has not started yet",
                filename
            )))
        }
    }

    pub(crate) fn remove_file(&mut self, filename: &str) -> Result<()> {
        let blocks = self.filename_to_blocks.remove(filename);
        if let Some(blocks) = blocks {
            for block_id in blocks {
                self.block_to_replication_count.remove(&block_id);
            }
            Ok(())
        } else {
            Err(UdfsError::FSError(format!(
                "'{}': File creation has not started yet",
                filename
            )))
        }
    }

    pub(crate) fn remove_block(&mut self, filename: &str, block_id: u64) -> Result<()> {
        let blocks = self.filename_to_blocks.get_mut(filename);
        if let Some(blocks) = blocks {
            blocks.retain(|&id| id != block_id);
            Ok(())
        } else {
            Err(UdfsError::FSError(format!(
                "'{}': File creation has not started yet",
                filename
            )))
        }
    }
}
