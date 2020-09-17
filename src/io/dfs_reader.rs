use crate::block::Block;
use crate::error::{Result, UdfsError};
use crate::utils::proto_utils;
use crate::proto;
use crate::proto::client_protocol_client::ClientProtocolClient;

use std::io::Read;

use prost::Message;

use tokio::io::{AsyncReadExt, AsyncWriteExt, BufStream};
use tokio::net::TcpStream;

pub struct DfsReader {
    blocks_with_locations: Vec<(Block, Vec<String>)>,
    // current location of blocks_with_locations
    block_index: usize,
    total_file_size: u64,
    // current position in the file
    current_file_pos: u64,
    current_reader: BufStream<TcpStream>,
    current_block_size: u64,
    current_block_pos: u64,
    current_packet_size: u64,
    current_packet_pos: u64,
    // buffers the last read packet
    buffer: Vec<u8>,
}

impl DfsReader {
    pub async fn open(namenode_rpc_address: &str, path: impl Into<String>) -> Result<Self> {
        let mut client =
            match ClientProtocolClient::connect(String::from(namenode_rpc_address)).await {
                Ok(client) => client,
                Err(err) => {
                    return Err(UdfsError::RPCError(format!(
                        "Could not connect to namenode: {}",
                        err
                    )))
                }
            };

        let open_file_request = proto::OpenFileRequest { path: path.into() };
        let proto::OpenFileResponse { blocks } =
            client.open_file(open_file_request).await?.into_inner();
        let blocks_with_locations = blocks
            .into_iter()
            .map(|block_with_locations| {
                let proto::BlockWithLocations { block, locations } = block_with_locations;
                let block = block
                    .ok_or_else(|| {
                        UdfsError::RPCError("Expected to receive a block from namenode".to_owned())
                    })?
                    .into();
                Ok((block, locations))
            })
            .collect::<Result<Vec<(Block, Vec<String>)>>>()?;

        let total_file_size = blocks_with_locations
            .iter()
            .fold(0, |current_size, (block, _)| current_size + block.len);

        // this kind of sucks, since it's very similar to next_block
        let current_block = blocks_with_locations[0].0;
        let mut current_reader =
            BufStream::new(TcpStream::connect(&blocks_with_locations[0].1[0]).await?);

        let mut buffer = vec![];
        let op = proto::Operation {
            op: proto::operation::OpCode::ReadBlock as i32,
        };
        op.encode_length_delimited(&mut buffer)?;
        current_reader.write_all(&buffer).await?;
        buffer.clear();

        let read_op = proto::ReadBlockOperation {
            block: Some(proto::Block {
                id: current_block.id,
                len: current_block.len,
            }),
        };
        read_op.encode_length_delimited(&mut buffer)?;
        current_reader.write_all(&buffer).await?;
        current_reader.flush().await?;

        Ok(Self {
            blocks_with_locations,
            block_index: 0,
            total_file_size,
            current_file_pos: 0,
            current_block_size: current_block.len,
            current_packet_size: 0,
            current_block_pos: 0,
            current_packet_pos: 0,
            current_reader,
            buffer: Vec::new(),
        })
    }

    //TODO Would be nice to implement AsyncRead instead
    pub async fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        if self.current_file_pos == self.total_file_size {
            return Ok(0);
        } else if self.current_block_pos == self.current_block_size {
            self.next_block().await?;
        } else if self.current_packet_pos == self.current_packet_size {
            self.next_packet().await?;
        }

        let buffer_start_pos = self.current_packet_pos as usize;
        let bytes_read = Read::read(&mut (&self.buffer[buffer_start_pos..]), buf)? as u64;
        self.current_file_pos += bytes_read;
        self.current_block_pos += bytes_read;
        self.current_packet_pos += bytes_read;

        Ok(bytes_read as usize)
    }

    pub async fn next_block(&mut self) -> Result<()> {
        self.block_index += 1;
        let current_block = self.blocks_with_locations[self.block_index].0;

        self.current_block_size = current_block.len;
        self.current_block_pos = 0;

        let mut current_reader = BufStream::new(
            TcpStream::connect(&self.blocks_with_locations[self.block_index].1[0]).await?,
        );
        self.buffer.clear();

        let op = proto::Operation {
            op: proto::operation::OpCode::ReadBlock as i32,
        };
        op.encode_length_delimited(&mut self.buffer)?;
        current_reader.write_all(&self.buffer).await?;
        self.buffer.clear();

        let read_op = proto::ReadBlockOperation {
            block: Some(proto::Block {
                id: current_block.id,
                len: current_block.len,
            }),
        };
        read_op.encode_length_delimited(&mut self.buffer)?;

        current_reader.write_all(&self.buffer).await?;
        current_reader.flush().await?;

        self.current_reader = current_reader;
        self.next_packet().await?;

        Ok(())
    }

    pub async fn next_packet(&mut self) -> Result<()> {
        let proto::Packet { size, last: _ } =
            proto_utils::parse_message(&mut self.current_reader).await?;
        self.current_packet_pos = 0;
        self.current_packet_size = size;
        self.buffer.clear();
        self.buffer.resize_with(size as usize, u8::default);
        self.current_reader.read_exact(&mut self.buffer).await?;
        Ok(())
    }
}
