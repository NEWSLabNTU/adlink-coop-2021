use anyhow::Error;
use anyhow::Result;
use collected::SumVal;
use futures::future;
use futures::stream::StreamExt as _;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;
use zenoh as zn;

#[async_std::main]
async fn main() {
    const KEY: &str = "/key";

    for n_peers in 2..=10 {
        for payload_size in (8..=(8 * 256)).step_by(8) {
            eprintln!("# n={} s={}", n_peers, payload_size);

            let workers = (0..n_peers).map(|_| {
                async_std::task::spawn(async move {
                    let zenoh = Arc::new(zn::Zenoh::new(zn::net::config::default()).await?);

                    let producer_task = {
                        let zenoh = zenoh.clone();

                        async move {
                            let workspace = zenoh.workspace(None).await?;
                            workspace
                                .put(&KEY.try_into()?, vec![0u8; payload_size].into())
                                .await?;

                            Result::<_, Error>::Ok(())
                        }
                    };

                    let consumer_task = {
                        let zenoh = zenoh.clone();

                        async move {
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

                            Result::<_, Error>::Ok(n_received)
                        }
                    };

                    let instant = Instant::now();
                    let ((), n_received) = futures::try_join!(producer_task, consumer_task)?;
                    let elapsed = instant.elapsed();

                    let zenoh = Arc::try_unwrap(zenoh).map_err(|_| ()).unwrap();
                    zenoh.close().await?;

                    Result::<_, Error>::Ok((n_received, elapsed))
                })
            });

            let result = future::try_join_all(workers).await;

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
    }
}
