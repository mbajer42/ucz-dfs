use crate::error::{Result, UdfsError};

use std::path::{Path, PathBuf};

use serde::Deserialize;

static CONFIG_FILE_ENV_KEY: &str = "UDFS_CONFIG_FILE";

#[derive(Debug, Deserialize)]
pub struct Config {
    pub namenode: NameNode,
    pub datanode: DataNode,
    pub filesystem: FileSystem,
    pub dfs: Dfs,
}

#[derive(Debug, Deserialize)]
#[serde(default)]
pub struct NameNode {
    pub rpc_bind_host: String,
    pub rpc_port: u16,
    pub heartbeat_check: u64,
    pub heartbeat_decomission: u64,
    pub name_dir: PathBuf,
}

#[derive(Debug, Deserialize)]
#[serde(default)]
pub struct DataNode {
    pub namenode_rpc_address: String,
    pub rpc_port: u16,
    pub heartbeat_interval: u64,
    pub disk_statistics_interval: u64,
    pub data_dir: PathBuf,
}

#[derive(Debug, Deserialize)]
#[serde(default)]
pub struct Dfs {
    pub replication: u64,
    pub block_size: u64,
    pub packet_size: u64,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
pub enum FileSystem {
    #[serde(rename(deserialize = "local"))]
    Local {},
    #[serde(rename(deserialize = "udfs"))]
    Udfs { namenode_rpc_address: String },
}

impl Config {
    pub fn load_from_file() -> Result<Self> {
        let path = std::env::var(CONFIG_FILE_ENV_KEY).map_err(|_| {
            UdfsError::ConfigError(format!(
                "Could not read {} environment variable.",
                CONFIG_FILE_ENV_KEY
            ))
        })?;
        let path = Path::new(&path);

        if !path.exists() {
            return Err(UdfsError::ConfigError(format!(
                "{} does not exist.",
                path.display()
            )));
        }
        if !path.is_file() {
            return Err(UdfsError::ConfigError(format!(
                "{} is not a file.",
                path.display()
            )));
        }

        let config = std::fs::read_to_string(path)?;
        let config: Self = toml::de::from_str(&config)?;

        Ok(config)
    }
}

impl std::default::Default for Config {
    fn default() -> Self {
        Self {
            namenode: NameNode::default(),
            datanode: DataNode::default(),
            filesystem: FileSystem::default(),
            dfs: Dfs::default(),
        }
    }
}

impl std::default::Default for NameNode {
    fn default() -> Self {
        let mut namedir = std::env::temp_dir();
        namedir.push("ucz-dfs");
        namedir.push("namenode");

        Self {
            rpc_bind_host: String::from("0.0.0.0"),
            rpc_port: 42000,
            heartbeat_check: 1000,
            heartbeat_decomission: 30_000,
            name_dir: namedir,
        }
    }
}

impl std::default::Default for DataNode {
    fn default() -> Self {
        Self {
            namenode_rpc_address: String::from("http://localhost:42000"),
            rpc_port: 42001,
            heartbeat_interval: 3000,
            disk_statistics_interval: 3000,
            data_dir: PathBuf::from("/tmp/ucz-dfs/datanode"),
        }
    }
}

impl std::default::Default for Dfs {
    fn default() -> Self {
        Self {
            replication: 3,
            block_size: 64 * 1024 * 1024,
            packet_size: 64 * 1024,
        }
    }
}

impl std::default::Default for FileSystem {
    fn default() -> Self {
        Self::Udfs {
            namenode_rpc_address: String::from("http://localhost:42000"),
        }
    }
}
