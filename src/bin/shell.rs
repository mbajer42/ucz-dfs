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
            namenode_rpc_address,
        } => namenode_rpc_address,
        _ => unimplemented!(),
    };

    let dfs = DistributedFileSystem::new(&namenode_rpc_address);

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
    ]
    .get_matches();

    match matches.subcommand() {
        ("report", _) => {
            let datanodes = dfs.nodes_report().await?;
            println!("Datanodes:");
            for datanode in datanodes {
                let used_percentage = 100.0 * (datanode.used as f64) / (datanode.available as f64);
                println!("\tAddress: {}", datanode.address);
                println!("\tAvailable storage (kB): {}", datanode.available);
                println!(
                    "\tUsed storage (kB): {} ({:.2}%)",
                    datanode.used, used_percentage
                );
                println!(
                    "\tRemaining storage (kB): {}",
                    datanode.available - datanode.used
                )
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
        (subcommand, _) => {
            eprintln!("Unrecognized command: {}", subcommand);
            exit(1);
        }
    }

    Ok(())
}
