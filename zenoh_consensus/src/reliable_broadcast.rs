use crate::{
    common::*,
    utils::{self, ValueExt},
    zenoh_sender::ZenohSender,
};
use tokio_stream::wrappers::ReceiverStream;

use message::*;

/// The `send_timeout` for the repeating echo sender.
const REPEATING_SEND_PERIOD: Duration = Duration::from_millis(15);
/// The jitter added for sending repeating echo messages in microseconds.
const JITTER_MICROS: u64 = 5000;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
/// Defines the structure of the config file.
pub struct Config {
    /// The maximum number of rounds to run the reliable broadcast.
    pub max_rounds: usize,
    /// The number of extra rounds to send echo(m,s). It will not exceed the `max_rounds`
    pub extra_rounds: usize,
    /// The timeout for receiving first 1/3Nv echo messages.
    pub recv_timeout: Duration,
    /// The timeout for each round. Must be larger than 2 * `recv_timeout`.
    pub round_timeout: Duration,
}

impl Default for Config {
    /// Function that returns a default configuration.
    fn default() -> Self {
        Self {
            max_rounds: 3,
            extra_rounds: 2,
            recv_timeout: Duration::from_millis(50),
            round_timeout: Duration::from_millis(110),
        }
    }
}

/// Creates a new reliable broadcast instance
///
/// Returns a Sender handle and a Receiver handle to send and receive messages.
/// * `zenoh`: A zenoh instance wrapped in Arc (Atomic Reference Counter).
/// * `path`: A Borrow typed [zenoh::Path] that indicates the workspace where reliable broadcast happens.
/// * `id`: A str that identifies the participant of reliable broadcast.
/// * `config`: The config file for the reliable broadcast. It should be the same for all participants of the same reliable broadcast.
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

    let (job_tx, job_rx) = mpsc::channel(10);
    let (output_tx, output_rx) = mpsc::channel(10);

    let recv_worker = tokio::spawn(recv_worker::<T>(
        id.to_string(),
        zenoh.clone(),
        config.max_rounds,
        config.extra_rounds,
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
/// A Sender for reliable broadcast.
pub struct Sender<T>
where
    T: 'static + Send + Sync + Serialize + DeserializeOwned,
{
    /// The id of the sender peer.
    id: String,
    /// The sequence number of the message.
    seq_counter: AtomicUsize,
    #[derivative(Debug = "ignore")]
    /// The zenoh instance for sending and receiving messages.
    zenoh: Arc<Zenoh>,
    /// The [zenoh::Path] to send the message to.
    send_key: zenoh::Path,
    // recv_selector: zenoh::Selector,
    /// The abstract type of a Zenoh Message sender to help send messages.
    zenoh_tx: ZenohSender<Message<T>>,
    // #[derivative(Debug = "ignore")]
    // peers: Arc<DashSet<String>>,
    /// The timeout for each round of reliable broadcast.
    round_timeout: Duration,
    // contexts: Arc<DashMap<(String, usize), Context<T>>>,
    #[derivative(Debug = "ignore")]
    /// The [HLC] instance for timestamp creation.
    hlc: Arc<HLC>,
}

impl<T> Sender<T>
where
    T: 'static + Send + Sync + Serialize + DeserializeOwned,
{
    /// A function that sends the message.
    ///
    /// It automatically tag the message with a random sequence number and a timestamp.
    /// * `data`: The message content to be sent.
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
/// A receiver for reliable broadcast.
pub struct Receiver<T>
where
    T: 'static + Send + Sync + Serialize + DeserializeOwned,
{
    #[derivative(Debug = "ignore")]
    /// A stream of received messages.
    stream: Pin<Box<dyn Send + Stream<Item = Result<T>>>>,
}

impl<T> Receiver<T>
where
    T: 'static + Send + Sync + Serialize + DeserializeOwned,
{
    /// A function to receive messages.
    ///
    /// Returns a Result of Option of message data. If there is no message, it will be None.
    pub async fn recv(&mut self) -> Result<Option<T>> {
        self.stream.next().await.transpose()
    }
}

/// A worker that helps to deal with message *(m, s)* and *present* and counts N<sub>v</sub> for the peer.
///
/// It creates a [coordinate_worker] to listen for *echo(m, s)* if received a *(m, s)* type message.
/// * `id`: The ID of the receiver.
/// * `zenoh`: The zenoh instance for sending and receiving messages.
/// * `max_rounds`: The maximum number of rounds in a reliable broadcast.
/// * `extra_rounds`: The number of extra rounds to send echo(m,s). It will not exceed the `max_rounds`.
/// * `recv_timeout`: The timeout for receiving first 1/3Nv echo messages.
/// * `round_timeout`: The timeout for each round. Must be larger than 2 * `recv_timeout`.
/// * `send_key`: The [zenoh::Path] to send the message to.
/// * `recv_selector`: The [zenoh::Selector] indicating where to receive the messages from.
///  `job_tx`: The transmitter to send [coordinate_worker] futures to a job polling worker named [await_worker].
/// * `zenoh_tx`: The zenoh sending handle to send messages.
/// * `peers`: The set of peers which the current peer has heard at least one message from.
/// * `contexts`: A set of contexts. Each context deal with each peer.
/// * `hlc`: A [HLC] instance for timestamp creation.
async fn recv_worker<T>(
    id: String,
    zenoh: Arc<Zenoh>,
    max_rounds: usize,
    extra_rounds: usize,
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
    // let mut cnt = 0;
    let workspace = zenoh.workspace(None).await?;
    let mut change_stream = workspace.subscribe(&recv_selector).await?;

    debug!("{} is listening", id);

    zenoh_tx
        .send(
            Present {
                timestamp: hlc.new_timestamp(),
            }
            .into(),
        )
        .await?;
    debug!("{} sends present", id);

    // eprintln!("recv_worker = {:?}", Instant::now());

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
                                let (worker_tx, worker_rx) = mpsc::channel(100000);
                                // save future to some place and await it
                                let future = tokio::spawn(coordinate_worker(
                                    id.clone(),
                                    seq,
                                    peer_name.to_string(),
                                    max_rounds,
                                    extra_rounds,
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
                                    timestamp,
                                });

                                continue;
                            }
                        };

                        if !context.finished {
                            let msg = EchoNotify {
                                peer: peer_name.to_owned(),
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

/// A worker that is spawned to deal with *echo(m, s)* typed messages with a *(m, s)* typed message is received by [recv_worker].
///
/// Returns a message if the *(m, s)* message is accepted.
/// * `id`: The ID of the [coordinate_worker] owner.
/// * `seq`: The sequence number of the receiving message.
/// * `sender`: The ID of the sender of the message *(m, s)*.
/// * `max_rounds`: The maximum number of rounds in a reliable broadcast.
/// * `extra_rounds`: The number of extra rounds to send echo(m,s). It will not exceed the `max_rounds`.
/// * `recv_timeout`: The timeout for receiving first 1/3Nv echo messages.
/// * `round_timeout`: The timeout for each round. Must be larger than 2 * `recv_timeout`.
/// * `peers`: The set of peers which the current peer has heard at least one message from.
/// * `hlc`: A [HLC] instance for timestamp creation.
/// * `zenoh`: The zenoh instance for sending and receiving messages.
/// * `send_key`: The [zenoh::Path] to send the message to.
/// * `worker_rx`: The receiver to receive all *echo(m, s)* messages once it is received by [recv_worker].
/// * `data`: The message data in *(m, s)* received by [recv_worker].
async fn coordinate_worker<T>(
    id: String,
    seq: usize,
    sender: String,
    max_rounds: usize,
    extra_rounds: usize,
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
    //sends an echo
    let msg: Message<T> = Echo {
        seq,
        sender: sender.clone(),
        timestamp: hlc.new_timestamp(),
    }
    .into();
    let msg_echo = zenoh::Value::serialize_from(&msg)?;
    let workspace = zenoh.workspace(None).await?;
    workspace.put(&send_key, msg_echo).await?;

    // let mut sending_futures = HashMap::new();
    let mut pending_echos: Vec<EchoNotify> = vec![];
    let mut accepted = false;
    let mut last_round = 0;
    let mut send_echo_flag = true;
    let mut repeating_echo_future = None;
    let mut echo_peer_set = HashSet::new();

    'round_loop: for round in 1..max_rounds {
        last_round = round;

        // wait until round starting time
        let round_start_time = init_time + round_timeout * round as u32;
        tokio::time::sleep_until(round_start_time.into()).await;

        debug!(
            "{} start round {}, sender={}, seq={}",
            id, round, sender, seq
        );

        // join echo future
        if let Some(future) = repeating_echo_future {
            future.await?;
            repeating_echo_future = None;
        }

        // send echo
        if send_echo_flag {
            let msg: Message<T> = Echo {
                seq,
                sender: sender.clone(),
                timestamp: hlc.new_timestamp(),
            }
            .into();
            let until = init_time + round_timeout * (round + 1) as u32;
            let future = tokio::spawn(repeating_send(
                zenoh.clone(),
                send_key.clone(),
                REPEATING_SEND_PERIOD,
                None, // start
                until,
                msg,
            ))
            .map(|result| Fallible::Ok(result??));
            // sending_futures.insert(round, future.boxed());
            repeating_echo_future = Some(future);
        }
        send_echo_flag = false;

        // process pending echos
        pending_echos.drain(..).for_each(|echo| {
            echo_peer_set.insert(echo.peer);
        });

        // in 1st phase, collect echos until echo_count >= 1/3 nv
        let until = round_start_time + recv_timeout;
        'first_phase: loop {
            let result = utils::timeout_until(until, worker_rx.recv()).await;
            match result {
                Ok(Some(echo)) => {
                    echo_peer_set.insert(echo.peer);

                    let echo_count = echo_peer_set.len();
                    let num_peers = peers.len();
                    debug!(
                        "{} in 1st phase: sender={}, seq={}, {} echos, {} peers",
                        id, sender, seq, echo_count, num_peers
                    );

                    if num_peers >= 4 && echo_count * 3 >= num_peers {
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

        // mark to send echo in the next round
        send_echo_flag = true;

        // in 2nd phase, collect echos until echo_ount >= 2/3 nv
        let until = round_start_time + recv_timeout * 2;
        loop {
            let result = utils::timeout_until(until, worker_rx.recv()).await;
            match result {
                Ok(Some(echo)) => {
                    echo_peer_set.insert(echo.peer);

                    let echo_count = echo_peer_set.len();
                    let num_peers = peers.len();
                    debug!(
                        "{} in 2nd phase: sender={}, seq={}, {} echos, {} peers",
                        id, sender, seq, echo_count, num_peers
                    );

                    if num_peers >= 4 && echo_count * 3 >= num_peers * 2 {
                        accepted = true;
                        debug!(
                            "{} accepted msg in round {}: sender={}, seq={}",
                            id, round, sender, seq
                        );
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

    // join repeating echo future
    // if let Some(future) = repeating_echo_future {
    //     future.await?;
    // }

    // if accepting early, unconditionally send echo in each round
    if accepted {
        for round in (last_round + 1)..min(last_round + extra_rounds + 1, max_rounds) {
            // wait until the round starting time
            let round_start_time = init_time + round_timeout * round as u32;
            tokio::time::sleep_until(round_start_time.into()).await;

            let msg: Message<T> = Echo {
                seq,
                sender: sender.clone(),
                timestamp: hlc.new_timestamp(),
            }
            .into();
            let until = init_time + round_timeout * (round + 1) as u32;
            tokio::spawn(repeating_send(
                zenoh.clone(),
                send_key.clone(),
                REPEATING_SEND_PERIOD,
                None,
                until,
                msg,
            ))
            .await??;
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

/// A worker that actually spawns the [coordinate_worker] when received the future from [recv_worker] and sends the accepted messages from the [coordinate_worker] to the [Receiver] instance.
/// * `job_rx`: The receiver to receive `coordinator_worker` futures from [recv_worker].
/// * `output_tx`: The transmitter to send the accepted messages to the [Receiver].
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

/// A worker that repeatedly sends *echo(m, s)* messages.
/// * `zenoh`: The zenoh instance for sending and receiving messages.
/// * `key`: The [zenoh::Path] to send the message to.
/// * `send_timeout`: The timeout for sending each *echo(m, s)*.
/// * `start`: (Optional) A starting time to send the first repeating echo messages. If not specified, it will start as soon as possible.
/// * `until`: The end time for the worker.
/// * `data`: The data of the message *(m, s)* that need to be echoed.
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
        let mut rng = rand::thread_rng();
        let jitter = rng.gen_range(0..=JITTER_MICROS);
        time + Duration::from_micros(jitter)
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
/// The structure to store the context of the dealing progress of each message (m, s) for each peer.
struct Context<T> {
    /// The flag indicating if the context has been finished dealing with or not.
    finished: bool,
    /// The transmitter that sends *echo(m, s)* to [coordinate_worker] if [recv_worker] received one.
    worker_tx: mpsc::Sender<EchoNotify>,
    /// (Currently not used).
    _phantom: PhantomData<T>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
/// The structure that defines the message *(m, s)*.
pub struct Msg<T> {
    /// The ID of the sender.
    pub sender: String,
    /// The sequence number of the message.
    pub seq: usize,
    /// The data that is sent in the message.
    pub data: Option<T>,
}

/// A module that performs operations related to messages.
pub mod message {
    use super::*;

    #[derive(Debug, Clone)]
    /// A structure that defines the echo notification.
    pub struct EchoNotify {
        /// The ID of the peer sending the *echo(m, s)* message.
        pub peer: String,
        /// The timestamp of the message *echo(m, s)*.
        pub timestamp: uhlc::Timestamp,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    /// The enumeration of all types of messages used in reliable broadcast.
    pub enum Message<T> {
        /// The message type of *(m, s)*.
        Broadcast(Broadcast<T>),
        /// The message type of *present*.
        Present(Present),
        /// The message type of *echo(m, s)*
        Echo(Echo),
    }

    impl<T> Message<T> {
        /// A function that obtains the timestamp of the messages.
        /// Returns a reference to [uhlc::Timestamp].
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
    /// The structure for the message type *(m, s)*.
    pub struct Broadcast<T> {
        /// The sequence number of the message.
        pub seq: usize,
        #[serde(with = "utils::serde_uhlc_timestamp")]
        #[derivative(Hash(hash_with = "utils::hash_uhlc_timestamp"))]
        /// The timestamp of the message.
        pub timestamp: uhlc::Timestamp,
        /// The data to send in the message.
        pub data: T,
    }

    #[derive(Debug, Clone, PartialEq, Eq, Derivative, Serialize, Deserialize)]
    #[derivative(Hash)]
    /// The structure for the message type *echo(m, s)*
    pub struct Echo {
        /// The sequence number of the message.
        pub seq: usize,
        /// The ID of the message sender of (m, s).
        pub sender: String,
        #[serde(with = "utils::serde_uhlc_timestamp")]
        #[derivative(Hash(hash_with = "utils::hash_uhlc_timestamp"))]
        /// The timestamp of the message *echo(m, s)*
        pub timestamp: uhlc::Timestamp,
    }

    #[derive(Debug, Clone, PartialEq, Eq, Derivative, Serialize, Deserialize)]
    #[derivative(Hash)]
    /// The structure for the message type *present*.
    pub struct Present {
        #[serde(with = "utils::serde_uhlc_timestamp")]
        #[derivative(Hash(hash_with = "utils::hash_uhlc_timestamp"))]
        /// The timestamp of the message.
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
            extra_rounds: usize,
        }

        let TestConfig {
            num_peers,
            num_msgs,
            zenoh_dir,
            recv_timeout_ms,
            round_timeout_ms,
            max_rounds,
            extra_rounds,
        } = {
            let text = fs::read_to_string(
                Path::new(env!("CARGO_MANIFEST_DIR"))
                    .join("tests")
                    .join("reliable_broadcast_test.json5"),
            )?;
            json5::from_str(&text)?
        };
        let zenoh_dir = &zenoh_dir;
        eprintln!("{:?}", Instant::now());
        let until = Instant::now()
            + Duration::from_millis(
                (round_timeout_ms * (max_rounds + extra_rounds + 10)
                    + 120 * num_peers
                    + 100 * num_msgs
                    + 800) as u64,
            );

        let start_until = Instant::now() + Duration::from_millis((800 + 120 * num_peers) as u64); // wait till all peers are ready

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
                        async_std::task::sleep(Duration::from_millis(100 + rand_jitter as u64))
                            .await;

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
            futures::try_join!(producer, consumer)?;
            Fallible::Ok(())
        });
        futures::future::try_join_all(futures).await?;

        Ok(())
    }
}
