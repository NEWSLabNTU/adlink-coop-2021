use anyhow::{anyhow, ensure, Result};
use async_std::task::{sleep, spawn};
// use std::sync::Arc;
use collected::SumVal;
use futures::{future::try_join_all, try_join, StreamExt};
use output_config::{Cli, TestResult};
use serde::{Deserialize, Serialize};
use std::time::{Duration, Instant};
use zenoh as zn;

const KEY: &str = "/key";

type Error = Box<dyn std::error::Error + Sync + Send + 'static>;

pub async fn run(config: &Cli) -> Result<TestResult> {
    ensure!(!config.pub_sub_separate, "pub_sub_separate must be false");

    let init_time = Duration::from_millis(config.init_time);
    let n_peers = config.total_put_number;
    let round_timeout = Duration::from_millis(config.round_timeout);
    let payload_size = config.payload_size;

    let workers = (0..n_peers).map(|_| {
        spawn(async move {
            let session = zn::open(zn::config::Config::default()).await?;
            let producer_fut = producer(&session, payload_size, init_time);
            let consumer_fut = consumer(&session, n_peers, round_timeout);
            let instant = Instant::now();
            let ((), num_received) =
                try_join!(producer_fut, consumer_fut).expect("Failed on try_join");
            let elapsed = instant.elapsed();
            session.close().await?;
            Result::<_, Error>::Ok((num_received, elapsed))
        })
    });

    if let Some(results) = try_join_all(workers).await.ok() {
        let (total_received, total_elapsed): (SumVal<usize>, SumVal<Duration>) =
            results.into_iter().unzip();
        let exp_log = ExpLog {
            receive_rate: total_received.into_inner() as f64 / n_peers.pow(2) as f64,
            average_time: total_elapsed.into_inner().as_secs_f64() / n_peers as f64,
        };
    }

    Ok(TestResult {
        config: config.clone(),
        total_sub_returned: todo!(),
        total_receive_rate: todo!(),
        per_peer_result: todo!(),
    })
}

async fn producer(session: &zn::Session, payload_size: usize, warmup: Duration) -> Result<()> {
    sleep(warmup).await;
    session
        .put(KEY, vec![0u8; payload_size])
        .await
        .map_err(|err| anyhow!("{}", err))?;
    Ok(())
}

async fn consumer(session: &zn::Session, n_peers: usize, timeout: Duration) -> Result<usize> {
    let mut sub = session
        .subscribe(KEY)
        .await
        .map_err(|err| anyhow!("{}", err))?;
    let stream = sub.receiver();

    let num_received = stream
        .take(n_peers)
        .take_until({
            async move {
                sleep(timeout).await;
            }
        })
        .filter(|change| futures::future::ready(change.kind == zn::prelude::SampleKind::Put))
        .count()
        .await;
    Ok(num_received)
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ExpLog {
    // pub config: Experiment,
    pub receive_rate: f64,
    pub average_time: f64,
}
