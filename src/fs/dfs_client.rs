use crate::error::{Result, UdfsError};
use crate::proto;
use crate::proto::client_protocol_client::ClientProtocolClient;

use tonic::transport::Channel;

pub struct DfsClient<'a> {
    namenode_rpc_address: &'a str,
}

impl<'a> DfsClient<'a> {
    pub fn new(namenode_rpc_address: &'a str) -> Self {
        Self {
            namenode_rpc_address,
        }
    }

    async fn get_client(&self) -> Result<ClientProtocolClient<Channel>> {
        match ClientProtocolClient::connect(String::from(self.namenode_rpc_address)).await {
            Ok(client) => Ok(client),
            Err(err) => {
                return Err(UdfsError::RPCError(format!(
                    "Could not connect to namenode: {}",
                    err
                )))
            }
        }
    }

    pub async fn nodes_report(&self) -> Result<Vec<proto::DataNodeInfo>> {
        let mut client = self.get_client().await?;
        let response = client.nodes_report(proto::EmptyMessage {}).await?;
        let proto::DataNodeInfoResponse { datanodes } = response.into_inner();
        Ok(datanodes)
    }

    pub async fn mkdir(&self, path: impl Into<String>) -> Result<()> {
        let mut client = self.get_client().await?;
        client
            .mkdir(proto::MkdirRequest { path: path.into() })
            .await?;
        Ok(())
    }

    pub async fn ls(&self, path: impl Into<String>) -> Result<Vec<String>> {
        let mut client = self.get_client().await?;
        let response = client.ls(proto::LsRequest { path: path.into() }).await?;
        let proto::LsResponse { files } = response.into_inner();
        Ok(files)
    }
}
