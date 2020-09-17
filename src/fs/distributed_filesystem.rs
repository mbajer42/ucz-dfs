use crate::fs::dfs_client::DfsClient;
use crate::error::Result;
use crate::proto;

pub struct DistributedFileSystem<'a> {
    dfs_client: DfsClient<'a>,
}

impl<'a> DistributedFileSystem<'a> {
    pub fn new(namenode_rpc_address: &'a str) -> Self {
        let dfs_client = DfsClient::new(namenode_rpc_address);
        Self { dfs_client }
    }

    pub async fn nodes_report(&self) -> Result<Vec<proto::DataNodeInfo>> {
        self.dfs_client.nodes_report().await
    }

    pub async fn mkdir(&self, path: impl Into<String>) -> Result<()> {
        self.dfs_client.mkdir(path).await
    }

    pub async fn ls(&self, path: impl Into<String>) -> Result<Vec<String>> {
        self.dfs_client.ls(path).await
    }
}
