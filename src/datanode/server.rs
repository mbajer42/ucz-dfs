use crate::config::Config;
use crate::datanode::datanode_storage::DataNodeStorage;
use crate::datanode::handler::DataTransferHandler;
use crate::error::{Result, UdfsError};
use crate::proto::node_protocol_client::NodeProtocolClient;
use crate::proto::HeartbeatMessage;

use std::future::Future;
use std::iter::Iterator;
use std::sync::Arc;
use std::time::Duration;

use tokio::net::TcpListener;
use tokio::sync::broadcast;
use tokio::time;

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

    /// Binds a TCP Stream on which it accepts client requests and runs
    /// a background thread to regularly catch-up with the `NameNode`.
    /// Runs until it resolves a value from `shutdown_signal`.
    pub async fn run(&mut self, shutdown_signal: impl Future) -> Result<()> {
        let (tx, rx1) = broadcast::channel(1);
        let rx2 = tx.subscribe();

        let data_server = self.run_data_server(rx1);
        let service = self.run_service(rx2);

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
    async fn run_data_server(&self, mut shutdown_signal: broadcast::Receiver<()>) -> Result<()> {
        let mut listener = TcpListener::bind(self.addr).await?;

        loop {
            tokio::select! {
                _ = shutdown_signal.recv() => {
                    return Ok(())
                },
                incoming = listener.accept() => {
                    let (socket, _) = incoming?;
                    let storage = Arc::clone(&self.storage);
                    let addr = self.addr.to_owned();
                    let packet_size = self.packet_size;

                    tokio::spawn(async move {
                        let mut handler = DataTransferHandler::new(addr, socket, storage, packet_size);
                        match handler.handle().await {
                            Ok(()) => (),
                            Err(e) => error!(
                                "An error occured while handling data server request: {:?}",
                                e
                            ),
                        }
                    });
                }
            };
        }
    }

    /// Send regular messages to the namenode (heartbeats, block reports, etc.)
    async fn run_service(&self, mut shutdown_signal: broadcast::Receiver<()>) -> Result<()> {
        // TODO Should be in the config
        let mut heartbeat_interval = time::interval(Duration::from_millis(3000));

        loop {
            tokio::select! {
                _ = shutdown_signal.recv() => {
                    return Ok(())
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

    async fn send_heartbeat(&self) -> Result<()> {
        let namenode_rpc_address = self.namenode_rpc_address.to_owned();
        let message = HeartbeatMessage {
            address: self.addr.to_string(),
            available: self.storage.available()?,
            used: self.storage.used()?,
        };
        tokio::spawn(async {
            let mut client = match NodeProtocolClient::connect(namenode_rpc_address).await {
                Ok(client) => client,
                Err(err) => {
                    error!("Could not connect to namenode: {}", err);
                    return;
                }
            };

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
