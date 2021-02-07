use crate::config::Config;
use crate::datanode::datanode_storage::DataNodeStorage;
use crate::datanode::handler::DataTransferHandler;
use crate::error::{Result, UdfsError};
use crate::proto;
use crate::proto::node_protocol_client::NodeProtocolClient;
use crate::proto::HeartbeatMessage;

use std::future::Future;
use std::iter::Iterator;
use std::sync::Arc;
use std::time::Duration;

use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast;
use tokio::sync::mpsc;
use tokio::time;

use tonic::transport::Channel;

use tracing::{error, info};

/// DataNode is responsible for storing the actual data.
/// It regularly sends heartbeats to the [`crate::namenode::NameNode`] to signal that it is still alive.
/// It is also responsible for accepting read and write requests from clients.
///
/// A DataNode needs to be started by calling the [`run`](`DataNode::run`) method.
pub struct DataNode<'a> {
    addr: &'a str,
    namenode_rpc_address: &'a str,
    packet_size: u64,
    storage: Arc<DataNodeStorage>,
}

impl<'a> DataNode<'a> {
    /// Creates a new DataNode.
    pub fn new(addr: &'a str, config: &'a Config) -> Result<Self> {
        let storage = Arc::new(DataNodeStorage::new(&config)?);
        Ok(Self {
            addr,
            namenode_rpc_address: &config.datanode.namenode_rpc_address,
            packet_size: config.dfs.packet_size,
            storage,
        })
    }

    async fn get_namenode_client(&self) -> Result<NodeProtocolClient<Channel>> {
        match NodeProtocolClient::connect(String::from(self.namenode_rpc_address)).await {
            Ok(client) => Ok(client),
            Err(err) => {
                return Err(UdfsError::RPCError(format!(
                    "Could not connect to namenode: {}",
                    err
                )))
            }
        }
    }

    /// Binds a TCP Stream on which it accepts client requests and runs
    /// a background thread to regularly catch-up with the `NameNode`.
    /// Runs until it resolves a value from `shutdown_signal`.
    pub async fn run(&mut self, shutdown_signal: impl Future) -> Result<()> {
        let (shutdown_tx, shutdown_rx) = broadcast::channel(1);
        let (block_tx, block_rx) = mpsc::channel(8);

        let data_server = self.run_data_server(block_tx, shutdown_rx);
        let service = self.run_service(block_rx, shutdown_tx.subscribe());

        tokio::select! {
            data_res = data_server => {
                if let Err(err) = data_res {
                    error!("DataNode data server failed!");
                    return Err(UdfsError::RPCError(err.to_string()));
                }
            },
            service_res = service => {
                if let Err(err) = service_res {
                    error!("DataNode Service failed!");
                    return Err(UdfsError::RPCError(err.to_string()));
                }
            },
            _ = shutdown_signal => {
                info!("Shutting down datanode");
            }
        };

        Ok(())
    }

    /// Runs the data server. It is responsible for datatransfer handling.
    async fn run_data_server(
        &self,
        block_tx: mpsc::Sender<proto::Block>,
        mut shutdown_signal: broadcast::Receiver<()>,
    ) -> Result<()> {
        let listener = TcpListener::bind(self.addr).await?;

        loop {
            tokio::select! {
                _ = shutdown_signal.recv() => {
                    return Ok(())
                },
                incoming = listener.accept() => {
                    let (socket, _) = incoming?;
                    let block_sender = block_tx.clone();
                    self.handle_incoming_request(socket, block_sender);
                }
            };
        }
    }

    /// Send regular messages to the namenode (heartbeats, block reports, etc.)
    async fn run_service(
        &self,
        mut block_rx: mpsc::Receiver<proto::Block>,
        mut shutdown_signal: broadcast::Receiver<()>,
    ) -> Result<()> {
        // TODO Should be in the config
        let mut heartbeat_interval = time::interval(Duration::from_millis(3000));

        loop {
            tokio::select! {
                _ = shutdown_signal.recv() => {
                    return Ok(())
                },
                block = block_rx.recv() => {
                    match self.handle_received_block(block).await {
                        Ok(_) => (),
                        Err(e) => {
                            error!("Could not inform namenode of received block: {}", e)
                        }
                    }
                },
                _ = heartbeat_interval.tick() => {
                    match self.send_heartbeat().await {
                        Ok(_) => (),
                        Err(e) => {
                            error!("Could not send heartbeat: {}", e)
                        }
                    }
                }
            };
        }
    }

    fn handle_incoming_request(&self, socket: TcpStream, block_sender: mpsc::Sender<proto::Block>) {
        let storage = Arc::clone(&self.storage);
        let packet_size = self.packet_size;

        tokio::spawn(async move {
            let mut handler = DataTransferHandler::new(socket, storage, packet_size, block_sender);
            match handler.handle().await {
                Ok(()) => (),
                Err(e) => error!(
                    "An error occured while handling data server request: {:?}",
                    e
                ),
            }
        });
    }

    async fn handle_received_block(&self, block: Option<proto::Block>) -> Result<()> {
        if let Some(block) = block {
            info!("Stored new block {:?}", block);
            let message = proto::BlockReceivedRequest {
                address: self.addr.to_string(),
                block,
            };
            let mut client = self.get_namenode_client().await?;
            tokio::spawn(async move {
                let request = tonic::Request::new(message);
                match client.block_received(request).await {
                    Ok(_) => (),
                    Err(e) => error!("Could not inform namenode of received block: {}", e),
                }
            });
        }
        Ok(())
    }

    async fn send_heartbeat(&self) -> Result<()> {
        let message = HeartbeatMessage {
            address: self.addr.to_string(),
            available: self.storage.available()?,
            used: self.storage.used()?,
        };
        let mut client = self.get_namenode_client().await?;
        tokio::spawn(async move {
            let request = tonic::Request::new(message);
            match client.heartbeat(request).await {
                Ok(_) => (),
                Err(e) => {
                    error!("Problem sending heartbeat: {}", e);
                }
            }
        });

        Ok(())
    }
}
