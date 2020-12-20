use crate::config::Config;
use crate::error::{Result, UdfsError};
use crate::proto;
use crate::proto::client_protocol_client::ClientProtocolClient;
use crate::utils::proto_utils;

use prost::Message;

use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufStream, SeekFrom};
use tokio::net::TcpStream;

use tonic::transport::Channel;

async fn new_backup_file() -> Result<BufStream<File>> {
    let backup_dir = std::env::temp_dir();
    let filename = format!("tmp_{}", rand::random::<u64>());
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(backup_dir.join(filename))
        .await?;
    Ok(BufStream::new(file))
}

pub struct DfsWriter {
    namenode_client: ClientProtocolClient<Channel>,
    block_size: u64,
    packet_size: u64,
    path: String,
    file_started: bool,
    bytes_written_to_block: u64,
    backup_buffer: BufStream<File>,
}

// TODO It would be nice to implement AsyncWrite trait
// instead of these write/flush/shutdown methods
impl DfsWriter {
    pub async fn create(
        path: impl Into<String>,
        namenode_rpc_address: &str,
        config: &Config,
    ) -> Result<Self> {
        let client = match ClientProtocolClient::connect(String::from(namenode_rpc_address)).await {
            Ok(client) => client,
            Err(err) => {
                return Err(UdfsError::RPCError(format!(
                    "Could not connect to namenode: {}",
                    err
                )))
            }
        };

        Ok(Self {
            namenode_client: client,
            block_size: config.dfs.block_size,
            packet_size: config.dfs.packet_size,
            path: path.into(),
            file_started: false,
            bytes_written_to_block: 0,
            backup_buffer: new_backup_file().await?,
        })
    }

    pub async fn write(&mut self, buf: &[u8]) -> Result<()> {
        let start_pos = if self.bytes_written_to_block + buf.len() as u64 >= self.block_size {
            let bytes_to_fill_block = self.block_size - self.bytes_written_to_block;
            let buf = &buf[..(bytes_to_fill_block as usize)];
            self.backup_buffer.write_all(&buf).await?;
            self.bytes_written_to_block += bytes_to_fill_block;
            self.flush().await?;

            bytes_to_fill_block
        } else {
            0
        };

        self.backup_buffer
            .write_all(&buf[(start_pos as usize)..])
            .await?;
        self.bytes_written_to_block += buf.len() as u64 - start_pos;
        Ok(())
    }

    pub async fn flush(&mut self) -> Result<()> {
        self.backup_buffer.flush().await?;

        if self.bytes_written_to_block >= self.block_size {
            self.write_block().await?;
        }

        Ok(())
    }

    pub async fn shutdown(&mut self) -> Result<()> {
        if self.bytes_written_to_block > 0 {
            self.backup_buffer.flush().await?;
            self.write_block().await?;
        }

        self.namenode_client
            .finish_file_create(proto::CreateFileRequest {
                path: self.path.clone(),
            })
            .await?;

        Ok(())
    }

    async fn next_block(&mut self) -> Result<(proto::Block, Vec<proto::DataNodeInfo>)> {
        let proto::BlockWithTargets { block, targets } = if self.file_started {
            self.namenode_client
                .add_block(proto::AddBlockRequest {
                    path: self.path.clone(),
                })
                .await?
                .into_inner()
        } else {
            self.namenode_client
                .start_file_create(proto::CreateFileRequest {
                    path: self.path.clone(),
                })
                .await?
                .into_inner()
        };
        self.file_started = true;
        let block = block.into();

        Ok((block, targets))
    }

    async fn write_block(&mut self) -> Result<()> {
        let (block, targets) = self.next_block().await?;

        let mut datanode = TcpStream::connect(&targets[0].address).await?;
        let mut buffer = vec![];
        let op = proto::Operation {
            op: proto::operation::OpCode::WriteBlock as i32,
        };
        op.encode_length_delimited(&mut buffer)?;
        datanode.write_all(&buffer).await?;
        buffer.clear();

        let write_op = proto::WriteBlockOperation {
            block,
            targets: targets.iter().map(|info| info.address.clone()).collect(),
        };
        write_op.encode_length_delimited(&mut buffer)?;
        datanode.write_all(&buffer).await?;
        buffer.clear();

        let mut remaining_to_send = self.bytes_written_to_block;
        self.backup_buffer
            .get_mut()
            .seek(SeekFrom::Start(0u64))
            .await?;
        while remaining_to_send > 0 {
            let packet_size = std::cmp::min(remaining_to_send, self.packet_size);
            remaining_to_send -= packet_size;
            let packet = proto::Packet {
                size: packet_size,
                last: remaining_to_send == 0,
            };
            packet.encode_length_delimited(&mut buffer)?;
            datanode.write_all(&buffer).await?;
            buffer.clear();
            buffer.resize_with(packet_size as usize, u8::default);
            self.backup_buffer.read_exact(&mut buffer).await?;
            datanode.write_all(&buffer).await?;
            buffer.clear();
        }
        datanode.flush().await?;

        let proto::WriteBlockResponse {
            success,
            block,
            locations,
        } = proto_utils::parse_message(&mut datanode).await?;

        if success {
            self.namenode_client
                .finish_block_write(proto::FinishBlockWriteRequest {
                    block,
                    locations,
                    path: self.path.clone(),
                })
                .await?;
        } else {
            return Err(UdfsError::FSError(
                "Replicating block was not successful".to_owned(),
            ));
        }

        //self.backup_buffer.get_mut().set_len(0).await?;
        self.backup_buffer
            .get_mut()
            .seek(SeekFrom::Start(0u64))
            .await?;
        self.bytes_written_to_block = 0;

        Ok(())
    }
}
