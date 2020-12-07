use udfs::config;
use udfs::config::Config;
use udfs::datanode::DataNode;
use udfs::error::Result;
use udfs::io::DfsReader;
use udfs::io::DfsWriter;
use udfs::namenode::NameNode;

use std::path::PathBuf;

use tempdir::TempDir;

use tokio::sync::oneshot;
use tokio::time::{delay_for, Duration};

static HEARTBEAT_CHECK_INTERVAL: u64 = 50;
static HEARTBEAT_INTERVAL: u64 = 50;
static HEARTBEAT_TIMEOUT: u64 = 2000;

static REPLICATION: u64 = 2;

static PAYLOAD_TO_SAFE: &str = "To every differentiable symmetry generated \
        by local actions there corresponds a conserved current.";

#[tokio::test]
async fn test_read_write() -> Result<()> {
    let (namenode_shutdown_tx, namenode_shutdown_rx) = oneshot::channel::<()>();
    tokio::spawn(async move {
        let namenode_log_dir = TempDir::new("test").unwrap();
        let config = namenode_config(namenode_log_dir.path());
        let namenode = NameNode::new(&config).unwrap();
        namenode.run(namenode_shutdown_rx).await.unwrap();
    });

    // allow namenode to spawn
    delay_for(Duration::from_millis(100)).await;
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

    let config = namenode_config("/tmp");
    let mut writer = DfsWriter::create("/noether.txt", "http://127.0.0.1:42000", &config).await?;
    for chunk in PAYLOAD_TO_SAFE.as_bytes().chunks(4) {
        writer.write(chunk).await?;
    }
    writer.shutdown().await?;

    let mut reader = DfsReader::open("http://127.0.0.1:42000", "/noether.txt").await?;
    let mut complete_file = vec![];
    let mut buffer = vec![0; 10];
    loop {
        let bytes_read = reader.read(&mut buffer).await?;
        if bytes_read == 0 {
            break;
        }
        complete_file.extend_from_slice(&buffer[..bytes_read]);
    }
    let response = String::from_utf8(complete_file).expect("Should be a valid string");
    assert_eq!(response, PAYLOAD_TO_SAFE);

    for (_, shutdown_tx) in datanodes {
        shutdown_tx.send(()).unwrap();
    }
    namenode_shutdown_tx.send(()).unwrap();

    Ok(())
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
            namenode_rpc_address: String::from("http://localhost:42000"),
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

fn namenode_config(log_dir: impl Into<PathBuf>) -> Config {
    Config {
        namenode: config::NameNode {
            rpc_bind_host: String::from("0.0.0.0"),
            rpc_port: 42000,
            heartbeat_check: HEARTBEAT_CHECK_INTERVAL,
            heartbeat_decomission: HEARTBEAT_TIMEOUT,
            name_dir: log_dir.into(),
        },
        datanode: config::DataNode::default(),
        filesystem: config::FileSystem::default(),
        dfs: config::Dfs {
            packet_size: 3,
            block_size: 5,
            replication: REPLICATION,
        },
    }
}
