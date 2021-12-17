use zenoh_consensus::{common::*, reliable_broadcast, utils};

use json5;

#[async_std::main]
async fn main() {
    pretty_env_logger::init();

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct TestConfig {
        num_peers: usize,
        local_peer_id_start: usize,
        local_peer_id_end: usize,
        num_msgs: usize,
        zenoh_dir: String,
        recv_timeout_ms: usize,
        round_timeout_ms: usize,
        max_rounds: usize,
        extra_rounds: usize,
        remote_peer_locator: String,
        initial_delay: usize,
    }

    let TestConfig {
        num_peers,
        local_peer_id_start,
        local_peer_id_end,
        num_msgs,
        zenoh_dir,
        recv_timeout_ms,
        round_timeout_ms,
        max_rounds,
        extra_rounds,
        remote_peer_locator,
        initial_delay,
    } = {
        let text = fs::read_to_string(
            Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("tests")
                .join("reliable_broadcast_test.json5"),
        )
        .unwrap();
        json5::from_str(&text).unwrap()
    };
    let zenoh_dir = &zenoh_dir;
    let remote_peer_locator = &remote_peer_locator;
    eprintln!("{:?}", Instant::now());
    let until = Instant::now()
        + Duration::from_millis(
            (round_timeout_ms * (max_rounds + extra_rounds + 10)
                + 120 * num_peers
                + 100 * num_msgs
                + initial_delay) as u64,
        );

    let start_until = Instant::now() + Duration::from_millis((800 + 120 * num_peers) as u64); // wait till all peers are ready

    let futures = (local_peer_id_start..local_peer_id_end).map(|peer_index| async move {
        let mut config = zenoh::ConfigProperties::default();
        config.insert(zenoh::net::config::ZN_ADD_TIMESTAMP_KEY, "true".to_string());
        config.insert(
            zenoh::net::config::ZN_PEER_KEY,
            remote_peer_locator.to_string(),
        );
        let zenoh = Arc::new(Zenoh::new(config).await?);

        let name = format!("peer_{}", peer_index);
        let path = zenoh::path(&zenoh_dir);

        let (tx, mut rx) = reliable_broadcast::new(
            zenoh,
            path,
            &name,
            reliable_broadcast::Config {
                max_rounds,
                extra_rounds,
                recv_timeout: Duration::from_millis(recv_timeout_ms as u64),
                round_timeout: Duration::from_millis(round_timeout_ms as u64),
            },
        )?;

        let producer = {
            let name = name.clone();
            async move {
                let mut rng = rand::thread_rng();
                async_std::task::sleep(start_until - Instant::now()).await;

                for seq in 0..num_msgs {
                    let mut rand_jitter: u8 = rng.gen();
                    rand_jitter = rand_jitter % 50;
                    async_std::task::sleep(Duration::from_millis(100 + rand_jitter as u64)).await;

                    let data: u8 = rng.gen();
                    eprintln!("{} sends seq={}, data={}", name, seq, data);
                    tx.send(data).await?;
                }

                Fallible::Ok(())
            }
        };
        let consumer = async move {
            let mut cnt = 0;
            for _ in 0..(num_peers * num_msgs) {
                let msg;
                let result = utils::timeout_until(until, rx.recv()).await;
                match result {
                    Ok(result) => {
                        msg = result?.unwrap();
                    }
                    Err(_) => {
                        continue;
                    }
                }
                if msg.data != None {
                    eprintln!(
                        "{} accepted sender={}, seq={}, data={}",
                        name,
                        msg.sender,
                        msg.seq,
                        msg.data.unwrap()
                    );
                } else {
                    eprintln!("{} timeout in sender={}, seq={}", name, msg.sender, msg.seq);
                }
                cnt += 1;
            }
            if cnt != num_peers {
                eprintln!("{} lost {} broadcast messages.", name, (num_peers - cnt));
            }

            Fallible::Ok(())
        };

        eprintln!("fut = {:?}", Instant::now());
        futures::try_join!(producer, consumer).unwrap();
        Fallible::Ok(())
    });
    futures::future::try_join_all(futures).await.unwrap();
}
