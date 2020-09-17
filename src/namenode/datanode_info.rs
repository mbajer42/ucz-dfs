#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct DataNodeInfo {
    address: String,
    available: u64,
    used: u64,
}

impl DataNodeInfo {
    pub fn new(address: impl Into<String>, available: u64, used: u64) -> Self {
        Self {
            address: address.into(),
            available,
            used,
        }
    }

    pub fn address(&self) -> &str {
        &self.address
    }

    pub fn available(&self) -> u64 {
        self.available
    }

    pub fn used(&self) -> u64 {
        self.used
    }
}
