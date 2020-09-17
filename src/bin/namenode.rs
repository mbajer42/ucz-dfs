use udfs::config::Config;
use udfs::namenode::NameNode;

use tokio::signal;



#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();
    let config = Config::load_from_file().unwrap();
    let namenode = NameNode::new(&config);
    namenode.run(signal::ctrl_c()).await.unwrap();
}
