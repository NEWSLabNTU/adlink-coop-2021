use anyhow::Result;
use clap::Parser;
use output_config::Cli;

mod exp;

#[async_std::main]
async fn main() -> Result<()> {
    pretty_env_logger::init();
    let opts = Cli::parse();
    exp::run(&opts).await?;
    Ok(())
}
