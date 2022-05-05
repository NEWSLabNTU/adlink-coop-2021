use anyhow::{anyhow, ensure, Result};
use async_std::task::{sleep, spawn};
// use std::sync::Arc;
use crate::Opts;
use collected::SumVal;
use futures::{future::try_join_all, try_join, StreamExt};
use output_config::{Cli, TestResult};
use reliable_broadcast as rb;
use serde::{Deserialize, Serialize};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use zenoh as zn;

const KEY: &str = "/key";

type Error = Box<dyn std::error::Error + Sync + Send + 'static>;

pub async fn run(config: &Opts) -> Result<TestResult> {
    ensure!(
        !config.cli.pub_sub_separate,
        "pub_sub_separate must be false"
    );

    let init_time = Duration::from_millis(config.cli.init_time);
    let n_peers = config.cli.total_put_number;
    let round_timeout = Duration::from_millis(config.cli.round_timeout);
    let payload_size = config.cli.payload_size;
    let echo_interval = Duration::from_millis(config.echo_interval);

    let session = zn::open(zn::config::Config::default())
        .await
        .map_err(|err| anyhow!("{}", err))?;
    let session = Arc::new(session);

    let (sender, stream) = rb::Config {
        max_rounds: config.max_rounds,
        extra_rounds: config.extra_rounds,
        round_timeout,
        echo_interval,
        congestion_control: config.congestion_control,
        reliability: config.reliability,
        sub_mode: config.sub_mode,
    }
    .build::<(), _>(session.clone(), KEY)
    .await
    .map_err(|err| anyhow!("{}", err))?;

    let instant = Instant::now();
    let elapsed = instant.elapsed();
    let session = Arc::try_unwrap(session).expect("please report bug");
    session.close().await.map_err(|err| anyhow!("{}", err))?;

    Ok(TestResult {
        config: config.cli.clone(),
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
