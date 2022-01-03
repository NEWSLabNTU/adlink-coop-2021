// use std::sync::Arc;
use futures::StreamExt;
use zenoh as zn;
use std::time::{Duration, Instant};
use collected::SumVal;
use futures::{try_join, future::try_join_all};
use serde::{Deserialize, Serialize};
use std::io::Write;

const KEY: &str = "/key";

type Result<T, E = Error> = std::result::Result<T, E>;
type Error = Box<dyn std::error::Error + Sync + Send + 'static>;

async fn producer(session: &zn::Session, payload_size: usize, warmup: Duration) -> Result<()> {
    async_std::task::sleep(warmup).await;
    session.put(KEY, vec![0u8; payload_size]).await?;
    Ok(())
}

async fn consumer(session: &zn::Session, n_peers: usize, timeout: Duration) -> Result<usize> {
    let mut sub = session.subscribe(KEY).await?;
    let stream = sub.receiver();

    let num_received = stream
        .take(n_peers)
        .take_until({async move {
            async_std::task::sleep(timeout).await;
        }})
        .filter(|change| futures::future::ready(change.kind == zn::prelude::SampleKind::Put))
        .count().await;
    Ok(num_received)
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
pub struct Experiment {
    pub n_peers: usize,
    pub payload_size: usize,
    pub warmup: Duration,
    pub timeout: Duration,
}

impl std::fmt::Display for Experiment {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}-{}", self.n_peers, self.payload_size)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ExpLog {
    pub config: Experiment,
    pub receive_rate: f64,
    pub average_time: f64,
}

impl Experiment {
    pub async fn run(self) -> Result<()> {
        let workers = (0..self.n_peers).map(|_| {
            async_std::task::spawn(async move {
                let session = zn::open(zn::config::Config::default()).await?;
                let producer_fut = producer(&session, self.payload_size, self.warmup);
                let consumer_fut = consumer(&session, self.n_peers, self.timeout);
                let instant = Instant::now();
                let ((), num_received) = try_join!(producer_fut, consumer_fut).expect("Failed on try_join");
                let elapsed = instant.elapsed();
                session.close().await?;
                Result::<_, Error>::Ok((num_received, elapsed))
            })
        });

        match try_join_all(workers).await {
            Ok(results) => {
                let (total_received, total_elapsed): (SumVal<usize>, SumVal<Duration>) = results.into_iter().unzip();
                let exp_log = ExpLog {
                    config: self.clone(),
                    receive_rate: total_received.into_inner() as f64 / self.n_peers.pow(2) as f64,
                    average_time: total_elapsed.into_inner().as_secs_f64() / self.n_peers as f64,
                };
                let mut file = std::fs::File::create(format!("{}.json", &self))?;
                writeln!(&mut file, "{}", serde_json::to_string_pretty(&exp_log)?)?;
                Ok(())
            }
            Err(_) => {
                Ok(())
            }
        }
    }
}

