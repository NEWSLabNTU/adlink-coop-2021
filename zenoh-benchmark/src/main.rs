use anyhow::Result;
use std::time::Duration;
use itertools::iproduct;
use structopt::StructOpt;

mod utils;
use utils::Experiment;

#[derive(StructOpt)]
struct Opt {
    #[structopt(long)]
    n_peers: usize,
}

const PAYLOAD_SIZE: usize = 4096;

#[async_std::main]
async fn main() ->Result<()> {
    // let Opt { n_peers } = Opt::from_args();

    pretty_env_logger::init();
    let warmup = Duration::from_millis(1000);
    let timeout = Duration::from_millis(5000) + warmup;

    for (n_peers, payload_size) in iproduct!(
        2usize..=10,
        (1..=16).map(|x| x * PAYLOAD_SIZE)
    ) {
        let exp = Experiment {
            n_peers,
            payload_size,
            warmup,
            timeout,
        };
        exp.run().await.expect("faield");
    }

    Ok(())
}
