use udfs::config::Config;
use udfs::datanode::DataNode;
use udfs::error::Result;

use clap::clap_app;

use tokio::signal;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let config = Config::load_from_file()?;
    let matches = clap_app![udfs =>
        (@arg address: +required "Bind host:port for this datanode")
    ]
    .get_matches();

    let address = matches
        .value_of("address")
        .expect("Address for datanode is required");
    let mut datanode = DataNode::new(address, &config)?;
    datanode.run(signal::ctrl_c()).await?;

    Ok(())
}
