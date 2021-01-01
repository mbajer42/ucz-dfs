use udfs::block::Block;
use udfs::config;
use udfs::config::Config;
use udfs::datanode::DataNode;
use udfs::error::Result;
use udfs::proto;
use udfs::utils::proto_utils;

use prost::Message;

use tempdir::TempDir;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::oneshot;
use tokio::time::{delay_for, Duration};

static HEARTBEAT_INTERVAL: u64 = 500;

static PAYLOAD_TO_SAFE: &str = "To every differentiable symmetry generated \
        by local actions there corresponds a conserved current.";

#[tokio::test]
async fn datanodes_handle_write_requests() -> Result<()> {
    let datanode_addresses = vec!["127.0.0.1:42001", "127.0.0.1:42002", "127.0.0.1:42003"];
    let datanodes = datanode_addresses
        .iter()
        .map(|addr| {
            let port = addr.split(':').last().unwrap().parse::<u16>().unwrap();

            let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
            let (temp_datadir, config) = datanode_config(port);
            spawn_datanode(shutdown_rx, addr, config);

            (temp_datadir, shutdown_tx)
        })
        .collect::<Vec<_>>();

    // allow datanodes to spawn
    delay_for(Duration::from_millis(100)).await;
    let mut client = TcpStream::connect("127.0.0.1:42001").await?;

    let mut buffer = vec![];

    let op = proto::Operation {
        op: proto::operation::OpCode::WriteBlock as i32,
    };
    op.encode_length_delimited(&mut buffer)?;
    client.write_all(&buffer).await?;
    buffer.clear();

    let write_operation = proto::WriteBlockOperation {
        block: proto::Block { id: 42, len: 0 },
        targets: datanode_addresses
            .iter()
            .map(|addr| String::from(*addr))
            .collect(),
    };
    write_operation.encode_length_delimited(&mut buffer)?;
    client.write_all(&buffer).await?;
    buffer.clear();

    for (packet, bytes) in packets() {
        packet.encode_length_delimited(&mut buffer)?;
        buffer.extend_from_slice(bytes);
        client.write_all(&buffer).await?;
        buffer.clear();
    }
    client.flush().await?;

    let proto::WriteBlockResponse { success } = proto_utils::parse_message(&mut client).await?;
    assert!(success, "Write should end successfully");

    let op = proto::Operation {
        op: proto::operation::OpCode::ReadBlock as i32,
    };
    let read_operation = proto::ReadBlockOperation {
        block: proto::Block {
            id: 42,
            len: PAYLOAD_TO_SAFE.bytes().len() as u64,
        },
    };
    for datanode_addr in datanode_addresses {
        let mut client = TcpStream::connect(datanode_addr).await?;
        op.encode_length_delimited(&mut buffer)?;
        read_operation.encode_length_delimited(&mut buffer)?;
        client.write_all(&buffer).await?;
        client.flush().await?;

        let mut all_data_received = false;
        let mut complete_response = vec![];
        while !all_data_received {
            let proto::Packet { size, last } = proto_utils::parse_message(&mut client).await?;
            all_data_received = last;
            buffer.resize_with(size as usize, u8::default);
            client.read_exact(&mut buffer).await?;
            complete_response.extend_from_slice(&buffer);
            buffer.clear();
        }
        let response = String::from_utf8(complete_response).expect("Should be a valid string");
        assert_eq!(response, PAYLOAD_TO_SAFE);
    }

    let block = Block {
        id: 42,
        len: PAYLOAD_TO_SAFE.bytes().len() as u64,
    };
    for (temp_datadir, shutdown_tx) in datanodes {
        assert_block_written(&temp_datadir, &block);
        shutdown_tx.send(()).unwrap();
    }

    Ok(())
}

fn assert_block_written(dir: &TempDir, block: &Block) {
    let block_path = dir.path().to_path_buf().join(block.filename());
    let block_content = std::fs::read(block_path).expect("Should be able to open block file");
    let block_content = String::from_utf8(block_content).expect("Should be a valid string");
    assert_eq!(block_content, PAYLOAD_TO_SAFE);
}

fn packets() -> Vec<(proto::Packet, &'static [u8])> {
    let mut packets = PAYLOAD_TO_SAFE
        .as_bytes()
        .chunks(5)
        .map(|chunk| {
            let packet = proto::Packet {
                size: chunk.len() as u64,
                last: false,
            };
            (packet, chunk)
        })
        .collect::<Vec<_>>();
    let length = packets.len();
    packets[length - 1].0.last = true;

    packets
}

fn spawn_datanode(shutdown_signal: oneshot::Receiver<()>, addr: &'static str, config: Config) {
    tokio::spawn(async move {
        let mut datanode = DataNode::new(addr, &config).unwrap();
        datanode.run(shutdown_signal).await.unwrap();
    });
}

fn datanode_config(port: u16) -> (TempDir, Config) {
    let datadir = TempDir::new("udfs-test").expect("Should create a temporary directory");
    let config = Config {
        namenode: config::NameNode::default(),
        datanode: config::DataNode {
            namenode_rpc_address: String::from("http://localhost:8888"),
            rpc_port: port,
            heartbeat_interval: HEARTBEAT_INTERVAL,
            data_dir: datadir.path().into(),
            disk_statistics_interval: 3000,
        },
        filesystem: config::FileSystem::default(),
        dfs: config::Dfs::default(),
    };
    (datadir, config)
}
