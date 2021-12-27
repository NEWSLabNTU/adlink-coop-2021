use anyhow::Error;
use anyhow::Result;
use collected::SumVal;
use futures::future;
use futures::stream::StreamExt as _;
use itertools::iproduct;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;
use zenoh as zn;

const KEY: &str = "/key";

#[async_std::main]
async fn main() {
    pretty_env_logger::init();

    let n_peers_iter = 2usize..=10;
    let payload_size_iter = (8..=(8 * 256)).step_by(8);

    for (n_peers, payload_size) in iproduct!(n_peers_iter, payload_size_iter) {
        run_test(n_peers, payload_size).await;
    }
}

async fn run_test(n_peers: usize, payload_size: usize) {
    eprintln!("# n={} s={}", n_peers, payload_size);

    // spawn a worker for each peer
    let workers = (0..n_peers).map(|_| {
        async_std::task::spawn(async move {
            // start zenoh session
            let zenoh = Arc::new(zn::Zenoh::new(zn::net::config::default()).await?);

            // run producer and consumer simultanously
            // note that producer and consumer share a common Zenoh session
            let producer_future = producer(zenoh.clone(), payload_size);
            let consumer_future = consumer(zenoh.clone(), payload_size);

            let instant = Instant::now();
            let ((), n_received) = futures::try_join!(producer_future, consumer_future)?;
            let elapsed = instant.elapsed();

            // close zenoh session
            let zenoh = Arc::try_unwrap(zenoh).map_err(|_| ()).unwrap();
            zenoh.close().await?;

            Result::<_, Error>::Ok((n_received, elapsed))
        })
    });

    // wait for all peers to complete
    let result = future::try_join_all(workers).await;

    // print report
    match result {
        Ok(vec) => {
            let (total_received, total_elapsed): (SumVal<usize>, SumVal<Duration>) =
                vec.into_iter().unzip();
            let total_expect = n_peers.pow(2);
            let recv_rate = total_received.into_inner() as f64 / total_expect as f64;
            let mean_elapsed = total_elapsed.into_inner().div_f64(n_peers as f64);
            eprintln!("loss rate {:.2}%", recv_rate * 100.0);
            eprintln!("mean elapsed time {:?}", mean_elapsed);
        }
        Err(err) => {
            eprintln!("error: {:?}", err);
        }
    }
}

async fn producer(zenoh: Arc<zn::Zenoh>, payload_size: usize) -> Result<()> {
    let workspace = zenoh.workspace(None).await?;
    workspace
        .put(&KEY.try_into()?, vec![0u8; payload_size].into())
        .await?;

    Ok(())
}

async fn consumer(zenoh: Arc<zn::Zenoh>, n_peers: usize) -> Result<usize> {
    let timeout = Duration::from_millis(5000);

    let workspace = zenoh.workspace(None).await?;
    let stream = workspace.subscribe(&KEY.try_into()?).await?;

    let n_received = stream
        .take(n_peers)
        .take_until({
            async move {
                async_std::task::sleep(timeout).await;
            }
        })
        .filter(|change| future::ready(change.kind == zn::ChangeKind::Put))
        .count()
        .await;

    Ok(n_received)
}
