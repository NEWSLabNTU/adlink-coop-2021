use anyhow::Result;
use clap::Parser;
use itertools::iproduct;
use output_config::Cli;
use std::time::Duration;

mod utils;
use utils::Experiment;

const PAYLOAD_SIZE: usize = 4096;

#[async_std::main]
async fn main() -> Result<()> {
    pretty_env_logger::init();

    let opts = Cli::parse();
    let warmup = Duration::from_millis(1000);
    let timeout = Duration::from_millis(5000) + warmup;

    for (n_peers, payload_size) in iproduct!(2usize..=10, (1..=16).map(|x| x * PAYLOAD_SIZE)) {
        let exp = Experiment {
            n_peers,
            payload_size,
            warmup,
            timeout,
        };

        utils::run(&opts).await?;
    }

    Ok(())
}
