use crate::block::Block;
use crate::error::{Result, UdfsError};

use std::collections::hash_set::Iter;
use std::collections::{HashMap, HashSet};

struct BlockInfo {
    block: Block,
    datanodes: HashSet<String>,
}

pub(crate) struct BlockMap {
    id_to_info: HashMap<u64, BlockInfo>,
}

impl BlockMap {
    pub(crate) fn new() -> Self {
        Self {
            id_to_info: HashMap::new(),
        }
    }

    pub(crate) fn contains_block(&self, block_id: u64) -> bool {
        self.id_to_info.contains_key(&block_id)
    }

    pub(crate) fn add_block(&mut self, block: Block) {
        self.id_to_info
            .entry(block.id)
            .or_insert_with(|| BlockInfo {
                block,
                datanodes: HashSet::new(),
            });
    }

    pub(crate) fn stored_block(&self, block_id: u64) -> Result<Block> {
        if let Some(block_info) = self.id_to_info.get(&block_id) {
            Ok(block_info.block)
        } else {
            Err(UdfsError::FSError(format!(
                "No block with id {} found",
                block_id
            )))
        }
    }

    /// Reports that a block is replicated on a given datanode.
    /// If this is a newly reported node for this block, return true, else false
    pub(crate) fn add_node(&mut self, block: Block, datanode_address: String) -> bool {
        if let Some(block_info) = self.id_to_info.get_mut(&block.id) {
            block_info.datanodes.insert(datanode_address)
        } else {
            let mut nodes = HashSet::new();
            nodes.insert(datanode_address);
            let block_info = BlockInfo {
                block,
                datanodes: nodes,
            };
            self.id_to_info.insert(block.id, block_info);
            true
        }
    }

    pub(crate) fn get_nodes(&self, block_id: u64) -> Result<Iter<'_, String>> {
        if let Some(block_info) = self.id_to_info.get(&block_id) {
            Ok(block_info.datanodes.iter())
        } else {
            Err(UdfsError::FSError(format!(
                "No block with id {} found",
                block_id
            )))
        }
    }
}
