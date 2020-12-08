use crate::config::Config;
use crate::error::{Result, UdfsError};
use crate::namenode::bookkeeper::BookKeeper;
use crate::proto;
use crate::proto::client_protocol_server::{ClientProtocol, ClientProtocolServer};
use crate::proto::node_protocol_server::{NodeProtocol, NodeProtocolServer};

use std::future::Future;
use std::sync::Arc;

use tonic::transport::Server;
use tonic::{Request, Response, Status};

use tracing::{debug, info};

pub struct NameNode<'a> {
    config: &'a Config,
    bookkeeper: Arc<BookKeeper>,
}

impl<'a> NameNode<'a> {
    pub fn new(config: &'a Config) -> Result<Self> {
        let bookkeeper = BookKeeper::new(config)?;
        Ok(Self {
            config,
            bookkeeper: Arc::new(bookkeeper),
        })
    }

    /// Restores a previous state of a namenode, if there is any
    pub async fn restore(&self) -> Result<()> {
        self.bookkeeper.restore().await
    }

    pub async fn run(&self, shutdown_signal: impl Future) -> Result<()> {
        let rpc_bind_host = self.config.namenode.rpc_bind_host.parse()?;
        let address = std::net::SocketAddr::new(rpc_bind_host, self.config.namenode.rpc_port);

        let rpc_service = Server::builder()
            .add_service(NodeProtocolServer::new(NodeProtocolService::new(
                Arc::clone(&self.bookkeeper),
            )))
            .add_service(ClientProtocolServer::new(ClientProtocolService::new(
                Arc::clone(&self.bookkeeper),
            )))
            .serve(address);

        let running_bookkeeper = self.bookkeeper.run();

        tokio::select! {
            res = rpc_service => {
                if let Err(err) = res {
                    return Err(UdfsError::RPCError(err.to_string()));
                }
            },
            _ = running_bookkeeper => {},
            _ = shutdown_signal => {
                info!("Shutting down namenode");
            }
        };

        Ok(())
    }
}

struct NodeProtocolService {
    bookkeeper: Arc<BookKeeper>,
}

impl NodeProtocolService {
    fn new(bookkeeper: Arc<BookKeeper>) -> Self {
        Self { bookkeeper }
    }
}

#[tonic::async_trait]
impl NodeProtocol for NodeProtocolService {
    async fn heartbeat(
        &self,
        request: Request<proto::HeartbeatMessage>,
    ) -> std::result::Result<Response<proto::EmptyMessage>, Status> {
        let message = request.into_inner();

        debug!("Received heartbeat: {:?}", message);

        let proto::HeartbeatMessage {
            address,
            available,
            used,
        } = message;
        self.bookkeeper.receive_heartbeat(address, available, used);
        Ok(Response::new(proto::EmptyMessage {}))
    }
}

struct ClientProtocolService {
    bookkeeper: Arc<BookKeeper>,
}

impl ClientProtocolService {
    fn new(bookkeeper: Arc<BookKeeper>) -> Self {
        Self { bookkeeper }
    }
}

#[tonic::async_trait]
impl ClientProtocol for ClientProtocolService {
    async fn nodes_report(
        &self,
        _: Request<proto::EmptyMessage>,
    ) -> std::result::Result<Response<proto::DataNodeInfoResponse>, Status> {
        let datanodes = self.bookkeeper.nodes_report();
        let datanodes = datanodes
            .into_iter()
            .map(|info| proto::DataNodeInfo {
                address: info.address().to_owned(),
                available: info.available(),
                used: info.used(),
            })
            .collect();
        Ok(Response::new(proto::DataNodeInfoResponse { datanodes }))
    }

    async fn mkdir(
        &self,
        request: Request<proto::MkdirRequest>,
    ) -> std::result::Result<Response<proto::EmptyMessage>, Status> {
        let proto::MkdirRequest { path } = request.into_inner();

        match self.bookkeeper.mkdir(&path).await {
            Ok(()) => Ok(Response::new(proto::EmptyMessage {})),
            Err(err) => Err(Status::invalid_argument(err.to_string())),
        }
    }

    async fn ls(
        &self,
        request: Request<proto::LsRequest>,
    ) -> std::result::Result<Response<proto::LsResponse>, Status> {
        let proto::LsRequest { path } = request.into_inner();

        let files = self.bookkeeper.ls(&path);

        match files {
            Ok(files) => Ok(Response::new(proto::LsResponse { files })),
            Err(err) => Err(Status::invalid_argument(err.to_string())),
        }
    }

    async fn open_file(
        &self,
        request: Request<proto::OpenFileRequest>,
    ) -> std::result::Result<Response<proto::OpenFileResponse>, tonic::Status> {
        let proto::OpenFileRequest { path } = request.into_inner();
        let blocks_with_locations = self.bookkeeper.open_file(&path);

        match blocks_with_locations {
            Ok(blocks_with_locations) => {
                let blocks = blocks_with_locations
                    .into_iter()
                    .map(|(block, locations)| {
                        let block = Some(proto::Block {
                            id: block.id,
                            len: block.len,
                        });
                        proto::BlockWithLocations { block, locations }
                    })
                    .collect();
                Ok(Response::new(proto::OpenFileResponse { blocks }))
            }
            Err(err) => Err(Status::invalid_argument(err.to_string())),
        }
    }

    async fn start_file_create(
        &self,
        request: Request<proto::CreateFileRequest>,
    ) -> std::result::Result<Response<proto::BlockWithTargets>, Status> {
        let proto::CreateFileRequest { path } = request.into_inner();

        let resp = self.bookkeeper.start_file_create(&path);

        match resp {
            Ok(Some((block, targets))) => {
                let block = proto::Block {
                    id: block.id,
                    len: block.len,
                };
                let targets = targets
                    .into_iter()
                    .map(|info| proto::DataNodeInfo {
                        address: info.address().to_owned(),
                        available: info.available(),
                        used: info.used(),
                    })
                    .collect();
                Ok(Response::new(proto::BlockWithTargets {
                    block: Some(block),
                    targets,
                }))
            }
            Ok(None) => Err(Status::failed_precondition(
                "Cannot create file, not enough avaialable datanodes with free space",
            )),
            Err(err) => Err(Status::invalid_argument(err.to_string())),
        }
    }

    async fn finish_file_create(
        &self,
        request: Request<proto::CreateFileRequest>,
    ) -> std::result::Result<Response<proto::EmptyMessage>, Status> {
        let proto::CreateFileRequest { path } = request.into_inner();
        match self.bookkeeper.finish_file_create(&path) {
            Ok(()) => Ok(Response::new(proto::EmptyMessage {})),
            Err(err) => Err(Status::invalid_argument(err.to_string())),
        }
    }

    async fn abort_file_create(
        &self,
        request: Request<proto::CreateFileRequest>,
    ) -> std::result::Result<Response<proto::EmptyMessage>, Status> {
        let proto::CreateFileRequest { path } = request.into_inner();
        self.bookkeeper.abort_file_create(&path);
        Ok(Response::new(proto::EmptyMessage {}))
    }

    async fn add_block(
        &self,
        request: Request<proto::AddBlockRequest>,
    ) -> std::result::Result<Response<proto::BlockWithTargets>, Status> {
        let proto::AddBlockRequest { path } = request.into_inner();

        let resp = self.bookkeeper.start_another_block(&path);

        match resp {
            Ok(Some((block, targets))) => {
                let block = proto::Block {
                    id: block.id,
                    len: block.len,
                };
                let targets = targets
                    .into_iter()
                    .map(|info| proto::DataNodeInfo {
                        address: info.address().to_owned(),
                        available: info.available(),
                        used: info.used(),
                    })
                    .collect();
                Ok(Response::new(proto::BlockWithTargets {
                    block: Some(block),
                    targets,
                }))
            }
            Ok(None) => Err(Status::failed_precondition(
                "Cannot create another block, not enough avaialable datanodes with free space",
            )),
            Err(err) => Err(Status::invalid_argument(err.to_string())),
        }
    }

    async fn finish_block_write(
        &self,
        request: Request<proto::FinishBlockWriteRequest>,
    ) -> std::result::Result<Response<proto::EmptyMessage>, Status> {
        let proto::FinishBlockWriteRequest {
            block,
            locations,
            path,
        } = request.into_inner();
        let block = if let Some(block) = block {
            block.into()
        } else {
            return Err(Status::invalid_argument("Expected block to be non-empty"));
        };

        for location in locations {
            match self.bookkeeper.block_received(&block, &location, &path) {
                Ok(()) => (),
                Err(err) => return Err(Status::invalid_argument(err.to_string())),
            };
        }

        Ok(Response::new(proto::EmptyMessage {}))
    }

    async fn abort_block_write(
        &self,
        request: Request<proto::AbortBlockWriteRequest>,
    ) -> std::result::Result<Response<proto::EmptyMessage>, Status> {
        let proto::AbortBlockWriteRequest { block, path } = request.into_inner();
        let block = if let Some(block) = block {
            block.into()
        } else {
            return Err(Status::invalid_argument("Expected block to be non-empty"));
        };

        match self.bookkeeper.abort_block(&path, &block) {
            Ok(()) => Ok(Response::new(proto::EmptyMessage {})),
            Err(err) => Err(Status::invalid_argument(err.to_string())),
        }
    }
}
