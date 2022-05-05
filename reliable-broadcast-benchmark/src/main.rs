use anyhow::Result;
use clap::Parser;
use output_config::Cli;

mod utils;

#[async_std::main]
async fn main() -> Result<()> {
    pretty_env_logger::init();
    let opts = Cli::parse();
    utils::run(&opts).await?;
    Ok(())
}
