use crate::config::Config;
use crate::error::{Result, UdfsError};
use crate::io::{DfsReader, DfsWriter};
use crate::proto;
use crate::proto::client_protocol_client::ClientProtocolClient;

use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader, BufWriter};

use tonic::transport::Channel;

pub struct DfsClient<'a> {
    namenode_rpc_address: &'a str,
    config: &'a Config,
}

impl<'a> DfsClient<'a> {
    pub fn new(namenode_rpc_address: &'a str, config: &'a Config) -> Self {
        Self {
            namenode_rpc_address,
            config,
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

    pub async fn put(&self, src: &str, dst: impl Into<String>) -> Result<()> {
        let mut reader = BufReader::new(File::open(src).await?);
        let mut writer = DfsWriter::create(dst, self.namenode_rpc_address, self.config).await?;

        let mut buf = vec![0; 128];
        loop {
            let read = reader.read(&mut buf).await?;
            if read == 0 {
                break;
            }
            writer.write(&buf[..read]).await?;
        }
        writer.flush().await?;
        writer.shutdown().await?;

        Ok(())
    }

    pub async fn get(&self, src: &str, dst: &str) -> Result<()> {
        let mut reader = DfsReader::open(self.namenode_rpc_address, src).await?;
        let mut writer = BufWriter::new(File::create(dst).await?);

        let mut buf = vec![0; 128];
        loop {
            let read = reader.read(&mut buf).await?;
            if read == 0 {
                break;
            }
            writer.write_all(&buf[..read]).await?;
        }
        writer.flush().await?;
        writer.shutdown().await?;

        Ok(())
    }
}
