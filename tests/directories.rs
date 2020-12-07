use udfs::config;
use udfs::config::Config;
use udfs::error::Result;
use udfs::fs::DistributedFileSystem;
use udfs::namenode::NameNode;

use std::path::PathBuf;

use tempdir::TempDir;

use tokio::sync::oneshot;

static HEARTBEAT_CHECK_INTERVAL: u64 = 100;
static HEARTBEAT_TIMEOUT: u64 = 2000;

#[tokio::test]
async fn namenode_keeps_track_of_datanodes() -> Result<()> {
    let config = namenode_config("/tmp");
    let dfs = DistributedFileSystem::new("http://localhost:42000", &config);

    let (namenode_shutdown_tx, namenode_shutdown_rx) = oneshot::channel::<()>();
    tokio::spawn(async move {
        let log_dir = TempDir::new("test").unwrap();
        let namenode_conf = namenode_config(log_dir.path());
        let namenode = NameNode::new(&namenode_conf).unwrap();
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

fn namenode_config(name_dir: impl Into<PathBuf>) -> Config {
    Config {
        namenode: config::NameNode {
            rpc_bind_host: String::from("0.0.0.0"),
            rpc_port: 42000,
            heartbeat_check: HEARTBEAT_CHECK_INTERVAL,
            heartbeat_decomission: HEARTBEAT_TIMEOUT,
            name_dir: name_dir.into(),
        },
        datanode: config::DataNode::default(),
        filesystem: config::FileSystem::default(),
        dfs: config::Dfs::default(),
    }
}
