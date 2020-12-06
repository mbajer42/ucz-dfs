use udfs::config;
use udfs::config::Config;
use udfs::datanode::DataNode;
use udfs::fs::DistributedFileSystem;
use udfs::namenode::NameNode;

use std::path::PathBuf;

use tokio::sync::oneshot;
use tokio::time::{delay_for, Duration};

static HEARTBEAT_CHECK_INTERVAL: u64 = 100;
static HEARTBEAT_INTERVAL: u64 = 500;
static HEARTBEAT_TIMEOUT: u64 = 2000;

#[tokio::test]
async fn namenode_keeps_track_of_datanodes() {
    let dfs_config = namenode_config();
    let dfs = DistributedFileSystem::new("http://localhost:42000", &dfs_config);

    let (namenode_shutdown_tx, namenode_shutdown_rx) = oneshot::channel::<()>();
    tokio::spawn(async move {
        let namenode_conf = namenode_config();
        let namenode = NameNode::new(&namenode_conf);
        namenode.run(namenode_shutdown_rx).await.unwrap();
    });

    let nodes_report = dfs.nodes_report().await.unwrap();
    assert_eq!(nodes_report.is_empty(), true);

    let (datanode1_shutdown_tx, datanode1_shutdown_rx) = oneshot::channel::<()>();
    spawn_datanode(datanode1_shutdown_rx, "127.0.0.1:42001", 42001);

    let (datanode2_shutdown_tx, datanode2_shutdown_rx) = oneshot::channel::<()>();
    spawn_datanode(datanode2_shutdown_rx, "127.0.0.1:42002", 42002);

    let (datanode3_shutdown_tx, datanode2_shutdown_rx) = oneshot::channel::<()>();
    spawn_datanode(datanode2_shutdown_rx, "127.0.0.1:42003", 42003);

    // Allow datanodes to get registered
    delay_for(Duration::from_millis(1000)).await;

    let nodes_report = dfs.nodes_report().await.unwrap();
    let expected_addresses = vec!["127.0.0.1:42001", "127.0.0.1:42002", "127.0.0.1:42003"];
    assert_alive_datanodes(nodes_report, expected_addresses);

    // Shutdown datanode2 and wait for heartbeat timeout
    datanode2_shutdown_tx.send(()).unwrap();
    delay_for(Duration::from_millis(5000)).await;

    let nodes_report = dfs.nodes_report().await.unwrap();
    let expected_addresses = vec!["127.0.0.1:42001", "127.0.0.1:42003"];
    assert_alive_datanodes(nodes_report, expected_addresses);

    // Spawn new datanodes, wait until is registered
    let (datanode2_shutdown_tx, datanode2_shutdown_rx) = oneshot::channel::<()>();
    spawn_datanode(datanode2_shutdown_rx, "127.0.0.1:42002", 42002);
    delay_for(Duration::from_millis(1000)).await;

    let nodes_report = dfs.nodes_report().await.unwrap();
    let expected_addresses = vec!["127.0.0.1:42001", "127.0.0.1:42002", "127.0.0.1:42003"];
    assert_alive_datanodes(nodes_report, expected_addresses);

    namenode_shutdown_tx.send(()).unwrap();
    datanode1_shutdown_tx.send(()).unwrap();
    datanode2_shutdown_tx.send(()).unwrap();
    datanode3_shutdown_tx.send(()).unwrap();
}

fn assert_alive_datanodes(nodes_report: Vec<udfs::proto::DataNodeInfo>, expected: Vec<&str>) {
    assert_eq!(nodes_report.len(), expected.len());

    for addr in &expected {
        assert_eq!(
            nodes_report
                .iter()
                .map(|node_info| node_info.address.as_str())
                .any(|x| &x == addr),
            true
        );
    }
}

fn spawn_datanode(shutdown_signal: oneshot::Receiver<()>, addr: &'static str, port: u16) {
    tokio::spawn(async move {
        let datanode_conf = datanode_config(port);
        let mut datanode = DataNode::new(addr, &datanode_conf).unwrap();
        datanode.run(shutdown_signal).await.unwrap();
    });
}

fn datanode_config(port: u16) -> Config {
    Config {
        namenode: config::NameNode::default(),
        datanode: config::DataNode {
            namenode_rpc_address: String::from("http://localhost:42000"),
            rpc_port: port,
            heartbeat_interval: HEARTBEAT_INTERVAL,
            data_dir: PathBuf::from("/tmp"),
            disk_statistics_interval: 3000,
        },
        filesystem: config::FileSystem::default(),
        dfs: config::Dfs::default(),
    }
}

fn namenode_config() -> Config {
    Config {
        namenode: config::NameNode {
            rpc_bind_host: String::from("0.0.0.0"),
            rpc_port: 42000,
            heartbeat_check: HEARTBEAT_CHECK_INTERVAL,
            heartbeat_decomission: HEARTBEAT_TIMEOUT,
        },
        datanode: config::DataNode::default(),
        filesystem: config::FileSystem::default(),
        dfs: config::Dfs::default(),
    }
}
