use crate::proto;

use std::hash::{Hash, Hasher};

#[derive(Copy, Clone, Debug)]
pub struct Block {
    pub id: u64,
    pub len: u64,
}

impl Block {
    pub fn new(id: u64, len: u64) -> Self {
        Self { id, len }
    }

    pub fn filename(&self) -> String {
        format!("block_{}", self.id)
    }
}

impl PartialEq for Block {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for Block {}

impl Hash for Block {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

impl From<proto::Block> for Block {
    fn from(block: proto::Block) -> Self {
        let proto::Block { id, len } = block;
        Self { id, len }
    }
}
