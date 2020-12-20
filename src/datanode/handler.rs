use crate::block::Block;
use crate::datanode::datanode_storage::DataNodeStorage;
use crate::error::Result;
use crate::proto;
use crate::proto::{
    operation::OpCode, Operation, Packet, ReadBlockOperation, WriteBlockOperation,
    WriteBlockResponse,
};
use crate::utils::proto_utils::parse_message;

use std::sync::Arc;

use prost::Message;

use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader, BufStream, BufWriter};
use tokio::net::TcpStream;

use tracing::debug;

pub(crate) struct DataTransferHandler {
    addr: String,
    socket: BufStream<TcpStream>,
    storage: Arc<DataNodeStorage>,
    packet_size: u64,
}

impl DataTransferHandler {
    pub(crate) fn new(
        addr: String,
        socket: TcpStream,
        storage: Arc<DataNodeStorage>,
        packet_size: u64,
    ) -> Self {
        Self {
            addr,
            socket: BufStream::new(socket),
            storage,
            packet_size,
        }
    }

    pub(crate) async fn handle(&mut self) -> Result<()> {
        match self.socket.get_ref().peer_addr() {
            Ok(peer_addr) => {
                debug!("Received request form {:?}", peer_addr);
            }
            Err(e) => debug!("Could not retrieve peer address, reason: {:?}", e),
        };
        let Operation { op } = parse_message(&mut self.socket).await?;

        match op {
            op if op == OpCode::WriteBlock as i32 => self.handle_write().await?,
            op if op == OpCode::ReadBlock as i32 => self.handle_read().await?,
            _ => unreachable!(),
        };

        Ok(())
    }

    async fn handle_write(&mut self) -> Result<()> {
        let WriteBlockOperation { block, targets } = parse_message(&mut self.socket).await?;
        let block = block.into();
        let block_file = self.storage.start_block_creation(&block).await?;

        match self.write_block(block_file, &block, &targets[1..]).await {
            Ok((response_block, locations)) => {
                self.storage.finish_block_creation(&block).await?;
                let response = WriteBlockResponse {
                    success: true,
                    block: response_block,
                    locations,
                };
                let mut buffer = vec![];
                response.encode_length_delimited(&mut buffer)?;
                self.socket.write_all(&buffer).await?;
                self.socket.flush().await?;
                Ok(())
            }
            Err(e) => {
                self.storage.abort_block_creation(&block).await?;
                Err(e)
            }
        }
    }

    async fn handle_read(&mut self) -> Result<()> {
        let ReadBlockOperation { block } = parse_message(&mut self.socket).await?;
        let block = block.into();
        let mut buffer = vec![];
        let mut blockfile = BufReader::new(self.storage.get_blockfile(&block)?);
        let mut remaining_to_send = blockfile.get_ref().metadata().await?.len();

        while remaining_to_send > 0 {
            let packet_size = std::cmp::min(remaining_to_send, self.packet_size);
            remaining_to_send -= packet_size;
            let packet = Packet {
                size: packet_size,
                last: remaining_to_send == 0,
            };
            packet.encode_length_delimited(&mut buffer)?;
            self.socket.write_all(&buffer).await?;
            buffer.clear();

            buffer.resize_with(packet_size as usize, u8::default);
            blockfile.read_exact(&mut buffer).await?;
            self.socket.write_all(&buffer).await?;
        }

        self.socket.flush().await?;
        Ok(())
    }

    async fn write_block(
        &mut self,
        block_file: File,
        block: &Block,
        targets: &[String],
    ) -> Result<(proto::Block, Vec<String>)> {
        let mut block_file = BufWriter::new(block_file);

        let mut buffer = vec![];
        let mut next_node = if !targets.is_empty() {
            let address = &targets[0];
            let mut stream = BufStream::new(TcpStream::connect(address).await?);

            proto::Operation {
                op: OpCode::WriteBlock as i32,
            }
            .encode_length_delimited(&mut buffer)?;
            stream.write_all(&buffer).await?;
            buffer.clear();

            WriteBlockOperation {
                block: proto::Block {
                    id: block.id,
                    len: block.len,
                },
                targets: targets.into(),
            }
            .encode_length_delimited(&mut buffer)?;
            stream.write_all(&buffer).await?;
            buffer.clear();

            Some(stream)
        } else {
            None
        };

        let mut total_block_length: u64 = 0;
        loop {
            let Packet { size, last } = parse_message(&mut self.socket).await?;
            buffer.resize_with(size as usize, u8::default);
            total_block_length += self.socket.read_exact(&mut buffer).await? as u64;
            block_file.write_all(&buffer).await?;

            if let Some(ref mut stream) = next_node {
                let packet = Packet { size, last };
                let mut message_buffer = vec![];
                packet.encode_length_delimited(&mut message_buffer)?;
                stream.write_all(&message_buffer).await?;
                stream.write_all(&buffer).await?;
            }

            buffer.clear();
            if last {
                break;
            }
        }

        if let Some(ref mut stream) = next_node {
            stream.flush().await?;
        }

        block_file.flush().await?;

        let mut successful_locations = vec![self.addr.to_owned()];

        if let Some(ref mut stream) = next_node {
            let WriteBlockResponse {
                success,
                block: _,
                locations,
            } = parse_message(stream).await?;
            if !success {
                debug!("Unexpected failure from {}", targets[0]);
            }
            successful_locations.extend_from_slice(&locations);
        }

        let block = proto::Block {
            id: block.id,
            len: total_block_length,
        };

        Ok((block, successful_locations))
    }
}
