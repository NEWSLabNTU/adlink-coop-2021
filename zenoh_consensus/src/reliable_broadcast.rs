use crate::{
    common::*,
    utils::{self, ValueExt},
    zenoh_sender::ZenohSender,
};
use tokio_stream::wrappers::ReceiverStream;

use message::*;

const REPEATING_SEND_PERIOD: Duration = Duration::from_millis(40);

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
            recv_timeout: Duration::from_millis(50),
            round_timeout: Duration::from_millis(110),
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
        zenoh,
        send_key,
        // recv_selector,
        zenoh_tx,
        // peers,
        // contexts,
        // recv_timeout: config.recv_timeout,
        round_timeout: config.round_timeout,
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
    zenoh: Arc<Zenoh>,
    send_key: zenoh::Path,
    // recv_selector: zenoh::Selector,
    zenoh_tx: ZenohSender<Message<T>>,
    #[derivative(Debug = "ignore")]
    // peers: Arc<DashSet<String>>,
    round_timeout: Duration,
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
        let msg: Message<_> = Broadcast {
            seq,
            data,
            timestamp: self.hlc.new_timestamp(),
        }
        .into();

        let until = Instant::now() + self.round_timeout;
        repeating_send(
            self.zenoh.clone(),
            self.send_key.clone(),
            REPEATING_SEND_PERIOD,
            None,
            until,
            msg,
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
    send_key: zenoh::Path,
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

    let mut pending_echos = HashMap::new();

    'stream_loop: while let Some(change) = change_stream.next().await {
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
                        let entry = contexts.entry(key.clone());

                        match entry {
                            dashmap::mapref::entry::Entry::Occupied(_) => {
                                // ignore duplicated broadcast
                            }
                            dashmap::mapref::entry::Entry::Vacant(entry) => {
                                let (worker_tx, worker_rx) = mpsc::channel(8);

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
                                    zenoh.clone(),
                                    send_key.clone(),
                                    worker_rx,
                                    data,
                                ))
                                .map(|result| Fallible::Ok(result??))
                                .boxed();

                                let result = job_tx.send(future).await;
                                if result.is_err() {
                                    break 'stream_loop;
                                }

                                // flush out pending echos if any
                                for msg in pending_echos.remove(&key).into_iter().flatten() {
                                    let result = worker_tx.send(msg).await;
                                    if result.is_err() {
                                        break 'stream_loop;
                                    }
                                }

                                let context = Context {
                                    finished: false,
                                    worker_tx,
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
                        timestamp,
                        round,
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

                                // save the echo message
                                // TODO: clear pending echos periodically
                                let key = (sender, seq);
                                match pending_echos.entry(key) {
                                    hash_map::Entry::Occupied(entry) => entry.into_mut(),
                                    hash_map::Entry::Vacant(entry) => entry.insert(vec![]),
                                }
                                .push(EchoNotify {
                                    peer: peer_name.to_owned(),
                                    round,
                                    timestamp,
                                });

                                continue;
                            }
                        };

                        if !context.finished {
                            let msg = EchoNotify {
                                peer: peer_name.to_owned(),
                                round,
                                timestamp,
                            };
                            let result = context.worker_tx.send(msg).await;

                            if result.is_err() {
                                context.finished = true;
                            }
                        }
                    }
                }
            }
            zenoh::ChangeKind::Patch => {}
            zenoh::ChangeKind::Delete => {
                // TODO: decrease peer count
            }
        }
    }

    Ok(())
}

async fn coordinate_worker<T>(
    id: String,
    seq: usize,
    sender: String,
    max_rounds: usize,
    recv_timeout: Duration,
    round_timeout: Duration,
    peers: Arc<DashSet<String>>,
    hlc: Arc<HLC>,
    zenoh: Arc<Zenoh>,
    send_key: zenoh::Path,
    mut worker_rx: mpsc::Receiver<EchoNotify>,
    data: T,
) -> Result<Option<Msg<T>>>
where
    T: 'static + Send + Sync + Serialize + DeserializeOwned,
{
    // TODO: determine start time from timestamp in broadcast message
    let init_time = Instant::now();

    // wait until broadcasting finishes (round 1 in paper)
    {
        let until = init_time + round_timeout;
        tokio::time::sleep_until(until.into()).await;
    }

    let mut sending_futures = HashMap::new();
    let mut pending_echos: Vec<EchoNotify> = vec![];
    let mut accepted = false;
    let mut last_round = 0;

    // in round 0 (round 2 in paper), send echo unconditionally
    {
        let msg: Message<T> = Echo {
            seq,
            sender: sender.clone(),
            timestamp: hlc.new_timestamp(),
            round: 0,
        }
        .into();
        let until = init_time + round_timeout * 2;
        let future = tokio::spawn(repeating_send(
            zenoh.clone(),
            send_key.clone(),
            REPEATING_SEND_PERIOD,
            None,
            until,
            msg,
        ))
        .map(|result| Fallible::Ok(result??));
        sending_futures.insert(0, future.boxed());
    };

    'round_loop: for round in 1..max_rounds {
        last_round = round;

        // wait until round starting time
        let round_start_time = init_time + round_timeout * round as u32;
        tokio::time::sleep_until(round_start_time.into()).await;

        debug!(
            "{} start round {}, sender={}, seq={}",
            id, round, sender, seq
        );

        // join repeating sending worker in last 2 round
        if round >= 2 {
            sending_futures.remove(&(round - 2)).unwrap().await?;
        }

        let mut echo_peer_set = HashSet::new();

        // process pending echos
        pending_echos.drain(..).for_each(|echo| {
            assert!(echo.round == round - 1);
            echo_peer_set.insert(echo.peer);
        });

        // in 1st phase, collect echos until echo_count >= 1/3 nv
        let until = round_start_time + recv_timeout;
        'first_phase: loop {
            let result = utils::timeout_until(until, worker_rx.recv()).await;
            match result {
                Ok(Some(echo)) => {
                    if round - 1 != echo.round {
                        if round == echo.round {
                            pending_echos.push(echo);
                        }
                        continue 'first_phase;
                    }

                    echo_peer_set.insert(echo.peer);

                    let echo_count = echo_peer_set.len();
                    let num_peers = peers.len();
                    debug!(
                        "{} in 1st phase: sender={}, seq={}, {} echos, {} peers",
                        id, sender, seq, echo_count, num_peers
                    );

                    if echo_count * 3 >= num_peers {
                        break 'first_phase;
                    }
                }
                Ok(None) => {
                    todo!();
                }
                Err(_) => {
                    debug!(
                        "{} timeout in 1st phase: sender={}, seq={}",
                        id, sender, seq
                    );
                    continue 'round_loop;
                }
            }
        }

        // send echo
        {
            let msg: Message<T> = Echo {
                seq,
                sender: sender.clone(),
                timestamp: hlc.new_timestamp(),
                round,
            }
            .into();
            let start = init_time + round_timeout * (round + 1) as u32;
            let until = init_time + round_timeout * (round + 2) as u32;
            let future = tokio::spawn(repeating_send(
                zenoh.clone(),
                send_key.clone(),
                REPEATING_SEND_PERIOD,
                Some(start),
                until,
                msg,
            ))
            .map(|result| Fallible::Ok(result??));
            sending_futures.insert(round, future.boxed());
        };

        // in 2nd phase, collect echos until echo_ount >= 2/3 nv
        let until = round_start_time + recv_timeout * 2;
        'second_phase: loop {
            let result = utils::timeout_until(until, worker_rx.recv()).await;
            match result {
                Ok(Some(echo)) => {
                    if round - 1 != echo.round {
                        if round == echo.round {
                            pending_echos.push(echo);
                        }
                        continue 'second_phase;
                    }
                    echo_peer_set.insert(echo.peer);

                    let echo_count = echo_peer_set.len();
                    let num_peers = peers.len();
                    debug!(
                        "{} in 2nd phase: sender={}, seq={}, {} echos, {} peers",
                        id, sender, seq, echo_count, num_peers
                    );

                    if echo_count * 3 >= num_peers * 2 {
                        accepted = true;
                        break 'round_loop;
                    }
                }
                Ok(None) => {
                    todo!();
                }
                Err(_) => {
                    debug!(
                        "{} timeout in 1st phase: sender={}, seq={}",
                        id, sender, seq
                    );
                    continue 'round_loop;
                }
            }
        }
    }

    // if accepting early, unconditionally send echo in each round
    if accepted {
        for round in (last_round + 1)..max_rounds {
            // wait until the round starting time
            let round_start_time = init_time + round_timeout * round as u32;
            tokio::time::sleep_until(round_start_time.into()).await;

            // join sending worker in last 2 round
            if round >= 2 {
                sending_futures.remove(&(round - 2)).unwrap().await?;
            }

            let msg: Message<T> = Echo {
                seq,
                sender: sender.clone(),
                timestamp: hlc.new_timestamp(),
                round,
            }
            .into();
            let start = init_time + round_timeout * (round + 1) as u32;
            let until = init_time + round_timeout * (round + 2) as u32;
            let future = tokio::spawn(repeating_send(
                zenoh.clone(),
                send_key.clone(),
                REPEATING_SEND_PERIOD,
                Some(start),
                until,
                msg,
            ))
            .map(|result| Fallible::Ok(result??));
            sending_futures.insert(round, future.boxed());
        }
    }

    if accepted {
        debug!("{} accepts, sender={}, seq={}", id, sender, seq);
        Ok(Some(Msg {
            data: Some(data),
            sender,
            seq,
        }))
    } else {
        debug!("{} rejects, sender={}, seq={}", id, sender, seq);
        Ok(Some(Msg {
            data: None,
            sender,
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
    send_timeout: Duration,
    start: Option<Instant>,
    until: Instant,
    data: T,
) -> Result<()>
where
    T: 'static + Send + Serialize,
{
    fn add_jitter(time: Instant) -> Instant {
        const JITTER_MICROS: isize = 3000;
        let mut rng = rand::thread_rng();
        let jitter = rng.gen_range(-JITTER_MICROS..=JITTER_MICROS);

        if jitter >= 0 {
            time + Duration::from_micros(jitter as u64)
        } else {
            time - Duration::from_micros(-jitter as u64)
        }
    }

    if let Some(start) = start {
        ensure!(start < until);
        tokio::time::sleep_until(start.into()).await;
    }

    let init_time = Instant::now();

    // 1st round
    {
        let send_until = add_jitter(init_time);
        tokio::time::sleep_until(send_until.into()).await;
        let msg = zenoh::Value::serialize_from(&data)?;
        let workspace = zenoh.workspace(None).await?;
        workspace.put(&key, msg).await?;
    }

    for round in 1.. {
        let send_until = add_jitter(init_time + send_timeout * round);

        tokio::select! {
            _ = tokio::time::sleep_until(send_until.into()) => {
                let msg = zenoh::Value::serialize_from(&data)?;
                let workspace = zenoh.workspace(None).await?;
                workspace.put(&key, msg).await?;

            }
            _ = tokio::time::sleep_until(until.into()) => {
                break;
            }
        }
    }

    Ok(())
}

#[derive(Derivative)]
#[derivative(Debug)]
struct Context<T> {
    finished: bool,
    worker_tx: mpsc::Sender<EchoNotify>,
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

    #[derive(Debug, Clone)]
    pub struct EchoNotify {
        pub peer: String,
        pub round: usize,
        pub timestamp: uhlc::Timestamp,
    }

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
            recv_timeout_ms: usize,
            round_timeout_ms: usize,
            max_rounds: usize,
        }

        let TestConfig {
            num_peers,
            num_msgs,
            zenoh_dir,
            recv_timeout_ms,
            round_timeout_ms,
            max_rounds,
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

            let (tx, mut rx) = super::new(
                zenoh,
                path,
                &name,
                super::Config {
                    recv_timeout: Duration::from_millis(recv_timeout_ms as u64),
                    round_timeout: Duration::from_millis(round_timeout_ms as u64),
                    max_rounds,
                },
            )?;

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
