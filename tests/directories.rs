use udfs::config;
use udfs::config::Config;
use udfs::fs::DistributedFileSystem;
use udfs::error::Result;
use udfs::namenode::NameNode;

use tokio::sync::oneshot;

static HEARTBEAT_CHECK_INTERVAL: u64 = 100;
static HEARTBEAT_TIMEOUT: u64 = 2000;

#[tokio::test]
async fn namenode_keeps_track_of_datanodes() -> Result<()> {
    let dfs = DistributedFileSystem::new("http://localhost:42000");

    let (namenode_shutdown_tx, namenode_shutdown_rx) = oneshot::channel::<()>();
    tokio::spawn(async move {
        let namenode_conf = namenode_config();
        let namenode = NameNode::new(&namenode_conf);
        namenode.run(namenode_shutdown_rx).await.unwrap();
    });

    let files = dfs.ls("/").await?;
    assert!(files.is_empty());

    let directories = ["/foo", "/bar", "/baz"];
    for directory in &directories {
        assert!(dfs.mkdir(*directory).await.is_ok());
    }

    let mut files = dfs.ls("/").await?;
    files.sort();
    let expected = vec!["bar", "baz", "foo"];
    assert_eq!(files, expected);

    namenode_shutdown_tx.send(()).unwrap();

    Ok(())
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
