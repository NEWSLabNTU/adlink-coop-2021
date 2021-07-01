use crate::{
    common::*,
    utils::{self, ValueExt},
    zenoh_sender::ZenohSender,
};
use tokio_stream::wrappers::ReceiverStream;

use message::*;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Config {
    pub max_rounds: usize,
    pub recv_timeout: Duration,
    pub round_timeout: Duration,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            max_rounds: 3,
            recv_timeout: Duration::from_millis(100),
            round_timeout: Duration::from_millis(210),
        }
    }
}

pub fn new<T>(
    zenoh: Arc<Zenoh>,
    path: impl Borrow<zenoh::Path>,
    id: impl AsRef<str>,
    config: Config,
) -> Result<(Sender<T>, Receiver<Msg<T>>)>
where
    T: 'static + Send + Sync + Serialize + DeserializeOwned,
{
    ensure!(config.max_rounds > 1);
    ensure!(config.recv_timeout > Duration::ZERO);
    ensure!(config.round_timeout > Duration::ZERO);
    ensure!(config.round_timeout > config.recv_timeout * 2);

    let id = id.as_ref();
    let path = path.borrow();

    debug!("reliable broadcast created, id={}, path={}", id, path);

    let send_key = zenoh::path(format!("{}/{}", path, id));
    let recv_selector = zenoh::selector(format!("{}/**", path));

    let peers = Arc::new(DashSet::new());
    let contexts = Arc::new(DashMap::new());
    let zenoh_tx = ZenohSender::new(zenoh.clone(), send_key.clone());

    let hlc = Arc::new(HLC::default());

    let (job_tx, job_rx) = mpsc::channel(2);
    let (output_tx, output_rx) = mpsc::channel(2);

    let recv_worker = tokio::spawn(recv_worker::<T>(
        id.to_string(),
        zenoh.clone(),
        config.max_rounds,
        config.recv_timeout,
        config.round_timeout,
        send_key.clone(),
        recv_selector.clone(),
        job_tx,
        zenoh_tx.clone(),
        peers.clone(),
        contexts.clone(),
        hlc.clone(),
    ))
    .map(|result: Result<Result<()>, _>| Fallible::Ok(result??));

    let await_worker = tokio::spawn(await_worker::<T>(job_rx, output_tx))
        .map(|result: Result<Result<()>, _>| Fallible::Ok(result??));

    let worker_stream = futures::future::try_join(recv_worker, await_worker)
        .into_stream()
        .map(|result: Result<((), ())>| result.map(|_| None));
    let output_stream = ReceiverStream::new(output_rx).map(|msg| Ok(Some(msg)));
    let stream = futures::stream::select(worker_stream, output_stream)
        .filter_map(|result: Result<Option<_>>| async move { result.transpose() })
        .boxed();

    let tx = Sender {
        id: id.to_string(),
        seq_counter: AtomicUsize::new(0),
        // zenoh,
        // send_key,
        // recv_selector,
        zenoh_tx,
        // peers,
        // contexts,
        // recv_timeout: config.recv_timeout,
        hlc,
    };
    let rx = Receiver { stream };

    Ok((tx, rx))
}

#[derive(Derivative)]
#[derivative(Debug)]
pub struct Sender<T>
where
    T: 'static + Send + Sync + Serialize + DeserializeOwned,
{
    id: String,
    seq_counter: AtomicUsize,
    #[derivative(Debug = "ignore")]
    // zenoh: Arc<Zenoh>,
    // send_key: zenoh::Path,
    // recv_selector: zenoh::Selector,
    zenoh_tx: ZenohSender<Message<T>>,
    #[derivative(Debug = "ignore")]
    // peers: Arc<DashSet<String>>,
    // recv_timeout: Duration,
    // contexts: Arc<DashMap<(String, usize), Context<T>>>,
    #[derivative(Debug = "ignore")]
    hlc: Arc<HLC>,
}

impl<T> Sender<T>
where
    T: 'static + Send + Sync + Serialize + DeserializeOwned,
{
    pub async fn send(&self, data: T) -> Result<()> {
        let seq = self.seq_counter.fetch_add(1, Ordering::SeqCst);
        self.zenoh_tx
            .send(
                Broadcast {
                    seq,
                    data,
                    timestamp: self.hlc.new_timestamp(),
                }
                .into(),
            )
            .await?;
        Ok(())
    }
}

#[derive(Derivative)]
#[derivative(Debug)]
pub struct Receiver<T>
where
    T: 'static + Send + Sync + Serialize + DeserializeOwned,
{
    #[derivative(Debug = "ignore")]
    stream: Pin<Box<dyn Send + Stream<Item = Result<T>>>>,
}

impl<T> Receiver<T>
where
    T: 'static + Send + Sync + Serialize + DeserializeOwned,
{
    pub async fn recv(&mut self) -> Result<Option<T>> {
        self.stream.next().await.transpose()
    }
}

async fn recv_worker<T>(
    id: String,
    zenoh: Arc<Zenoh>,
    max_rounds: usize,
    recv_timeout: Duration,
    round_timeout: Duration,
    _send_key: zenoh::Path,
    recv_selector: zenoh::Selector,
    job_tx: mpsc::Sender<Pin<Box<dyn Send + Future<Output = Result<Option<Msg<T>>>>>>>,
    zenoh_tx: ZenohSender<Message<T>>,
    peers: Arc<DashSet<String>>,
    contexts: Arc<DashMap<(String, usize), Context<T>>>,
    hlc: Arc<HLC>,
) -> Result<()>
where
    T: 'static + Send + Sync + Serialize + DeserializeOwned,
{
    debug!("{} sends present", id);
    zenoh_tx
        .send(
            Present {
                timestamp: hlc.new_timestamp(),
            }
            .into(),
        )
        .await?;

    let workspace = zenoh.workspace(None).await?;
    let mut change_stream = workspace.subscribe(&recv_selector).await?;

    debug!("{} is listening", id);

    while let Some(change) = change_stream.next().await {
        // let when = Instant::now();
        let peer_name = change.path.last_segment();

        match change.kind {
            zenoh::ChangeKind::Put => {
                peers.insert(peer_name.to_string());

                let value = change.value.unwrap();
                let msg: Message<T> = match value.deserialize_to() {
                    Ok(msg) => msg,
                    Err(err) => {
                        warn!("invalid message from {}: {:?}", peer_name, err);
                        continue;
                    }
                };

                match msg {
                    Message::Broadcast(Broadcast {
                        seq,
                        data,
                        timestamp: _,
                    }) => {
                        debug!("{} -> {}: broadcast, seq={}", peer_name, id, seq);

                        let key = (peer_name.to_string(), seq);
                        let entry = contexts.entry(key);

                        match entry {
                            dashmap::mapref::entry::Entry::Occupied(_) => {
                                warn!(
                                    "{} received duplicated broadcast from {} (seq={})",
                                    id, peer_name, seq
                                );
                            }
                            dashmap::mapref::entry::Entry::Vacant(entry) => {
                                let (echo_count_tx, echo_count_rx) = watch::channel(0);
                                // let zenoh_tx = ZenohSender::new(zenoh.clone(), send_key.clone());

                                // save future to some place and await it
                                let future = tokio::spawn(coordinate_worker(
                                    id.clone(),
                                    seq,
                                    peer_name.to_string(),
                                    max_rounds,
                                    recv_timeout,
                                    round_timeout,
                                    peers.clone(),
                                    hlc.clone(),
                                    zenoh_tx.clone(),
                                    echo_count_rx,
                                    data,
                                ))
                                .map(|result| Fallible::Ok(result??))
                                .boxed();

                                let result = job_tx.send(future).await;
                                if result.is_err() {
                                    break;
                                }

                                let context = Context {
                                    finished: false,
                                    echo_count_tx,
                                    _phantom: PhantomData,
                                };

                                entry.insert(context);
                            }
                        }
                    }
                    Message::Present(Present { timestamp: _ }) => {
                        debug!("{} -> {}: present", peer_name, id);
                    }
                    Message::Echo(Echo {
                        seq,
                        sender,
                        timestamp: _,
                        round: _,
                    }) => {
                        debug!(
                            "{} -> {}: echo, seq={}, sender={}",
                            peer_name, id, seq, sender,
                        );

                        let key = (sender.clone(), seq);
                        let mut context = match contexts.get_mut(&key) {
                            Some(context) => context,
                            None => {
                                warn!(
                                    "{} received echo from {} (sender={}, seq={}), but broadcast was not received",
                                    id, peer_name, sender, seq
                                );
                                debug!(
                                    "{} -> {}: received echo (sender={}, seq={}), but broadcast was not received",
                                    peer_name, id, sender, seq
                                );
                                continue;
                            }
                        };

                        if !context.finished {
                            let new_echo_count = *context.echo_count_tx.borrow() + 1;
                            let result = context.echo_count_tx.send(new_echo_count);

                            if result.is_err() {
                                context.finished = true;
                            }
                        }
                    }
                }
            }
            zenoh::ChangeKind::Patch => {}
            zenoh::ChangeKind::Delete => {
                // if peer_name == &*id {
                //     break;
                // }
            }
        }
    }

    Ok(())
}

async fn coordinate_worker<T>(
    id: String,
    seq: usize,
    peer_name: String,
    max_rounds: usize,
    recv_timeout: Duration,
    round_timeout: Duration,
    peers: Arc<DashSet<String>>,
    hlc: Arc<HLC>,
    zenoh_tx: ZenohSender<Message<T>>,
    mut echo_count_rx: watch::Receiver<usize>,
    data: T,
) -> Result<Option<Msg<T>>>
where
    T: 'static + Send + Sync + Serialize + DeserializeOwned,
{
    // TODO: determine start time from timestamp in broadcast message
    let init_time = Instant::now();

    {
        let until = init_time + round_timeout;
        tokio::time::sleep_until(until.into()).await;
    }

    zenoh_tx
        .send(
            Echo {
                seq,
                sender: peer_name.clone(),
                timestamp: hlc.new_timestamp(),
                round: 0,
            }
            .into(),
        )
        .await?;

    let mut accepted = false;
    let mut last_round = 0;

    'round_loop: for round in 1..max_rounds {
        last_round = round;

        let round_start_time = init_time + round_timeout * round as u32;
        tokio::time::sleep_until(round_start_time.into()).await;

        debug!(
            "{} start round {}, sender={}, seq={}",
            id, round, peer_name, seq
        );
        let until = round_start_time + recv_timeout;

        // in 1st phase, collect echos until echo_count >= 1/3 nv
        'first_phase: loop {
            let result = utils::timeout_until(until, echo_count_rx.changed()).await;
            match result {
                Ok(result) => {
                    if result.is_err() {
                        todo!();
                    };

                    let echo_count = *echo_count_rx.borrow();
                    let num_peers = peers.len();
                    debug!(
                        "{} in 1st phase: sender={}, seq={}, {} echos, {} peers",
                        id, peer_name, seq, echo_count, num_peers
                    );

                    if echo_count * 3 >= num_peers {
                        break 'first_phase;
                    }
                }
                Err(_) => {
                    debug!(
                        "{} timeout in 1st phase: sender={}, seq={}",
                        id, peer_name, seq
                    );
                    continue 'round_loop;
                }
            }
        }

        // broadcast echo
        zenoh_tx
            .send(
                Echo {
                    seq,
                    sender: peer_name.clone(),
                    timestamp: hlc.new_timestamp(),
                    round,
                }
                .into(),
            )
            .await?;

        // in 2nd phase, collect echos until echo_ount >= 2/3 nv
        let until = round_start_time + recv_timeout * 2;
        loop {
            let result = utils::timeout_until(until, echo_count_rx.changed()).await;
            match result {
                Ok(result) => {
                    if result.is_err() {
                        todo!();
                    }

                    let echo_count = *echo_count_rx.borrow();
                    let num_peers = peers.len();
                    debug!(
                        "{} in 2nd phase: sender={}, seq={}, {} echos, {} peers",
                        id, peer_name, seq, echo_count, num_peers
                    );

                    if echo_count * 3 >= num_peers * 2 {
                        accepted = true;
                        break 'round_loop;
                    }
                }
                Err(_) => {
                    debug!(
                        "{} timeout in 1st phase: sender={}, seq={}",
                        id, peer_name, seq
                    );
                    continue 'round_loop;
                }
            }
        }
    }

    // if accepting early, unconditionally send echo in each round
    if accepted {
        for round in (last_round + 1)..max_rounds {
            let round_start_time = init_time + round_timeout * round as u32;
            tokio::time::sleep_until(round_start_time.into()).await;
            zenoh_tx
                .send(
                    Echo {
                        seq,
                        sender: peer_name.clone(),
                        round,
                        timestamp: hlc.new_timestamp(),
                    }
                    .into(),
                )
                .await?;
        }
    }

    if accepted {
        debug!("{} accepts, sender={}, seq={}", id, peer_name, seq);
        Ok(Some(Msg {
            data: Some(data),
            sender: peer_name,
            seq,
        }))
    } else {
        debug!("{} rejects, sender={}, seq={}", id, peer_name, seq);
        Ok(Some(Msg {
            data: None,
            sender: peer_name,
            seq,
        }))
    }
}

async fn await_worker<T>(
    mut job_rx: mpsc::Receiver<Pin<Box<dyn Send + Future<Output = Result<Option<Msg<T>>>>>>>,
    output_tx: mpsc::Sender<Msg<T>>,
) -> Result<()> {
    let mut futures = vec![];

    loop {
        if futures.is_empty() {
            let job = match job_rx.recv().await {
                Some(job) => job,
                None => break,
            };
            futures.push(job);
        } else {
            tokio::select! {
                job = job_rx.recv() => {
                    match job {
                        Some(job) => {
                            futures.push(job);
                        }
                        None => break,
                    }
                }
                (result, index, _futures) = futures::future::select_all(&mut futures) => {
                    futures.remove(index);
                    let output = result?;
                    if let Some(output) = output {
                        let result = output_tx.send(output).await;
                        if result.is_err() {
                            break;
                        }
                    }
                }
            }
        }
    }

    futures::future::try_join_all(futures).await?;
    Ok(())
}

async fn repeating_send<T>(
    zenoh: Arc<Zenoh>,
    key: zenoh::Path,
    data: T,
    send_timeout: Duration,
    until: Instant,
) -> Result<()>
where
    T: 'static + Clone + Send + Serialize,
{
    fn add_jitter(time: Instant) -> Instant {
        const JITTER_MILLIS: isize = 5;
        let mut rng = rand::thread_rng();
        let jitter = rng.gen_range(-JITTER_MILLIS..=JITTER_MILLIS);

        if jitter >= 0 {
            time + Duration::from_millis(jitter as u64)
        } else {
            time - Duration::from_millis(-jitter as u64)
        }
    }

    tokio::spawn(async move {
        let init_time = Instant::now();

        // 1st round
        {
            let send_until = add_jitter(init_time);
            tokio::time::sleep_until(send_until.into()).await;
            let msg = zenoh::Value::serialize_from(&data)?;
            let workspace = zenoh.workspace(None).await?;
            workspace.put(&key, msg).await?;
        }

        let mut send_until = add_jitter(init_time + send_timeout);
        let mut round = 1;

        loop {
            tokio::select! {
                _ = tokio::time::sleep_until(send_until.into()) => {
                    let msg = zenoh::Value::serialize_from(&data)?;
                    let workspace = zenoh.workspace(None).await?;
                    workspace.put(&key, msg).await?;

                    round += 1;
                    send_until = add_jitter(init_time + send_timeout * round);

                }
                _ = tokio::time::sleep_until(until.into()) => {
                    break;
                }
            }
        }

        Fallible::Ok(())
    })
    .await??;

    Ok(())
}

#[derive(Derivative)]
#[derivative(Debug)]
struct Context<T> {
    finished: bool,
    echo_count_tx: watch::Sender<usize>,
    _phantom: PhantomData<T>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Msg<T> {
    pub sender: String,
    pub seq: usize,
    pub data: Option<T>,
}

mod message {
    use super::*;

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub enum Message<T> {
        Broadcast(Broadcast<T>),
        Present(Present),
        Echo(Echo),
    }

    impl<T> Message<T> {
        pub fn timestamp(&self) -> &uhlc::Timestamp {
            match self {
                Self::Broadcast(msg) => &msg.timestamp,
                Self::Present(msg) => &msg.timestamp,
                Self::Echo(msg) => &msg.timestamp,
            }
        }
    }

    impl<T> From<Broadcast<T>> for Message<T> {
        fn from(from: Broadcast<T>) -> Self {
            Self::Broadcast(from)
        }
    }

    impl<T> From<Present> for Message<T> {
        fn from(from: Present) -> Self {
            Self::Present(from)
        }
    }

    impl<T> From<Echo> for Message<T> {
        fn from(from: Echo) -> Self {
            Self::Echo(from)
        }
    }

    #[derive(Debug, Clone, PartialEq, Eq, Derivative, Serialize, Deserialize)]
    #[derivative(Hash)]
    pub struct Broadcast<T> {
        pub seq: usize,
        #[serde(with = "utils::serde_uhlc_timestamp")]
        #[derivative(Hash(hash_with = "utils::hash_uhlc_timestamp"))]
        pub timestamp: uhlc::Timestamp,
        pub data: T,
    }

    #[derive(Debug, Clone, PartialEq, Eq, Derivative, Serialize, Deserialize)]
    #[derivative(Hash)]
    pub struct Echo {
        pub seq: usize,
        pub sender: String,
        pub round: usize,
        #[serde(with = "utils::serde_uhlc_timestamp")]
        #[derivative(Hash(hash_with = "utils::hash_uhlc_timestamp"))]
        pub timestamp: uhlc::Timestamp,
    }

    #[derive(Debug, Clone, PartialEq, Eq, Derivative, Serialize, Deserialize)]
    #[derivative(Hash)]
    pub struct Present {
        #[serde(with = "utils::serde_uhlc_timestamp")]
        #[derivative(Hash(hash_with = "utils::hash_uhlc_timestamp"))]
        pub timestamp: uhlc::Timestamp,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[async_std::test]
    async fn reliable_broadcast_test() -> Result<()> {
        pretty_env_logger::init();

        #[derive(Debug, Clone, Serialize, Deserialize)]
        struct TestConfig {
            num_peers: usize,
            num_msgs: usize,
            zenoh_dir: String,
        }

        let TestConfig {
            num_peers,
            num_msgs,
            zenoh_dir,
        } = {
            let text = fs::read_to_string(
                Path::new(env!("CARGO_MANIFEST_DIR"))
                    .join("tests")
                    .join("reliable_broadcast_test.json5"),
            )?;
            json5::from_str(&text)?
        };
        let zenoh_dir = &zenoh_dir;

        let futures = (0..num_peers).map(|peer_index| async move {
            let mut config = zenoh::ConfigProperties::default();
            config.insert(zenoh::net::config::ZN_ADD_TIMESTAMP_KEY, "true".to_string());
            let zenoh = Arc::new(Zenoh::new(config).await?);

            let name = format!("peer_{}", peer_index);
            let path = zenoh::path(&zenoh_dir);

            let (tx, mut rx) = super::new(zenoh, path, &name, Default::default())?;

            let producer = {
                let name = name.clone();
                async move {
                    let mut rng = rand::thread_rng();

                    for seq in 0..num_msgs {
                        async_std::task::sleep(Duration::from_millis(100)).await;

                        let data: u8 = rng.gen();
                        eprintln!("{} sends seq={}, data={}", name, seq, data);
                        tx.send(data).await?;
                    }

                    Fallible::Ok(())
                }
            };
            let consumer = async move {
                let mut cnt = 0;
                let until = Instant::now() + Duration::from_secs(10);
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
                    // let msg = rx.recv().await?.unwrap();
                    if msg.data != None {
                        eprintln!(
                            "{} received sender={}, seq={}, data={}",
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

            futures::try_join!(producer, consumer)?;
            Fallible::Ok(())
        });

        futures::future::try_join_all(futures).await?;

        Ok(())
    }
}
