use udfs::config::Config;
use udfs::datanode::DataNode;
use udfs::error::Result;

use tokio::signal;



#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let config = Config::load_from_file()?;
    let mut datanode = DataNode::new("0.0.0.0:42001", &config)?;
    datanode.run(signal::ctrl_c()).await?;

    Ok(())
}
