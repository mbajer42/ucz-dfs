use udfs::config::{Config, FileSystem};
use udfs::error::{Result, UdfsError};
use udfs::fs::DistributedFileSystem;

use std::process::exit;

use clap::clap_app;

#[tokio::main]
async fn main() -> Result<()> {
    let config = Config::load_from_file().unwrap();

    let namenode_rpc_address = match config.filesystem {
        FileSystem::Udfs {
            ref namenode_rpc_address,
        } => namenode_rpc_address,
        _ => unimplemented!(),
    };

    let dfs = DistributedFileSystem::new(&namenode_rpc_address, &config);

    let matches = clap_app![udfs =>
        (@subcommand report =>
            (about: "Reports basic filesystem information.")
        )
        (@subcommand ls =>
            (about: "Lists the content of a given directory.")
            (@arg path: "The complete path to the directory.")
        )
        (@subcommand mkdir =>
            (about: "Creates a new directory (equivalent to `mkdir -p` on Unix systems).")
            (@arg path: "The directory to create.")
        )
        (@subcommand put =>
            (about: "Uploads a local file from `src` to remote `dst`")
            (@arg src: "Path to local file")
            (@arg dst: "Path to remote file")
        )
        (@subcommand get =>
            (about: "Downloads a remote file `src` to local destination `dst`")
            (@arg src: "Path to remote file")
            (@arg dst: "Path to local file")
        )
    ]
    .get_matches();

    match matches.subcommand() {
        ("report", _) => {
            let datanodes = dfs.nodes_report().await?;
            println!("Datanodes:");
            for datanode in datanodes {
                let used_percentage = 100.0 * (datanode.used as f64)
                    / (datanode.available as f64 + datanode.used as f64);
                println!("\tAddress: {}", datanode.address);
                println!("\tAvailable storage (kB): {}", datanode.available);
                println!(
                    "\tUsed storage (kB): {} ({:.2}%)",
                    datanode.used, used_percentage
                );
                println!();
            }
        }
        ("ls", Some(path)) => {
            let path = path
                .value_of("path")
                .ok_or_else(|| UdfsError::ArgMissingError("Path required".to_owned()))?;
            let mut files = dfs.ls(path).await?;
            files.sort();
            for file in files {
                println!("{}", file);
            }
        }
        ("mkdir", Some(path)) => {
            let path = path
                .value_of("path")
                .ok_or_else(|| UdfsError::ArgMissingError("Path required".to_owned()))?;
            dfs.mkdir(path).await?;
        }
        ("put", Some(args)) => {
            let src = args
                .value_of("src")
                .ok_or_else(|| UdfsError::ArgMissingError("Source required".to_owned()))?;
            let dst = args
                .value_of("dst")
                .ok_or_else(|| UdfsError::ArgMissingError("Destination required".to_owned()))?;
            dfs.put(src, dst).await?;
        }
        ("get", Some(args)) => {
            let src = args
                .value_of("src")
                .ok_or_else(|| UdfsError::ArgMissingError("Source required".to_owned()))?;
            let dst = args
                .value_of("dst")
                .ok_or_else(|| UdfsError::ArgMissingError("Destination required".to_owned()))?;
            dfs.get(src, dst).await?;
        }
        (subcommand, _) => {
            eprintln!("Unrecognized command: '{}'", subcommand);
            exit(1);
        }
    }

    Ok(())
}
