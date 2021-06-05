use crate::{
    common::*,
    utils::{self, ValueExt},
    zenoh_sender::ZenohSender,
};
use tokio_stream::wrappers::ReceiverStream;

pub fn new<T>(
    zenoh: Arc<Zenoh>,
    path: impl Borrow<zenoh::Path>,
    id: impl AsRef<str>,
    max_rounds: usize,
    timeout: Duration,
) -> (Sender<T>, Receiver<Msg<T>>)
where
    T: 'static + Send + Sync + Serialize + DeserializeOwned,
{
    let id = id.as_ref();
    let path = path.borrow();

    debug!("reliable broadcast created, id={}, path={}", id, path);

    let send_key = zenoh::path(format!("{}/{}", path, id));
    let recv_selector = zenoh::selector(format!("{}/**", path));

    let peers = Arc::new(DashSet::new());
    let contexts = Arc::new(DashMap::new());
    let zenoh_tx = ZenohSender::new(zenoh.clone(), send_key.clone());

    let (job_tx, job_rx) = mpsc::channel(2);
    let (output_tx, output_rx) = mpsc::channel(2);

    let recv_worker = tokio::spawn(recv_worker::<T>(
        id.to_string(),
        zenoh.clone(),
        max_rounds,
        timeout,
        send_key.clone(),
        recv_selector.clone(),
        job_tx,
        zenoh_tx.clone(),
        peers.clone(),
        contexts.clone(),
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
        recv_selector,
        zenoh_tx,
        peers,
        contexts,
        timeout,
    };
    let rx = Receiver { stream };

    (tx, rx)
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
    recv_selector: zenoh::Selector,
    zenoh_tx: ZenohSender<Message<T>>,
    #[derivative(Debug = "ignore")]
    peers: Arc<DashSet<String>>,
    timeout: Duration,
    contexts: Arc<DashMap<(String, usize), Context<T>>>,
}

impl<T> Sender<T>
where
    T: 'static + Send + Sync + Serialize + DeserializeOwned,
{
    pub async fn send(&self, data: T) -> Result<()> {
        let seq = self.seq_counter.fetch_add(1, Ordering::SeqCst);
        self.zenoh_tx.send(Message::Msg { seq, data }).await?;
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
    timeout: Duration,
    _send_key: zenoh::Path,
    recv_selector: zenoh::Selector,
    job_tx: mpsc::Sender<Pin<Box<dyn Send + Future<Output = Result<Option<Msg<T>>>>>>>,
    zenoh_tx: ZenohSender<Message<T>>,
    peers: Arc<DashSet<String>>,
    contexts: Arc<DashMap<(String, usize), Context<T>>>,
) -> Result<()>
where
    T: 'static + Send + Sync + Serialize + DeserializeOwned,
{
    debug!("{} sends present", id);
    zenoh_tx.send(Message::Present).await?;

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
                    Message::Msg { seq, data } => {
                        debug!("{} -> {}: msg, seq={}", peer_name, id, seq);

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
                                    timeout,
                                    peers.clone(),
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
                    Message::Present => {
                        debug!("{} -> {}: present", peer_name, id);
                    }
                    Message::Echo { seq, sender } => {
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
    timeout: Duration,
    peers: Arc<DashSet<String>>,
    zenoh_tx: ZenohSender<Message<T>>,
    mut echo_count_rx: watch::Receiver<usize>,
    data: T,
) -> Result<Option<Msg<T>>>
where
    T: 'static + Send + Sync + Serialize + DeserializeOwned,
{
    zenoh_tx
        .send(Message::Echo {
            seq,
            sender: peer_name.clone(),
        })
        .await?;

    let mut accepted = false;

    'round_loop: for round in 0..max_rounds {
        debug!("{} start round {}", peer_name, round);

        let until = Instant::now() + timeout;
        loop {
            let result = utils::timeout_until(until, echo_count_rx.changed()).await;
            match result {
                Ok(result) => {
                    result?;
                }
                Err(_) => {
                    debug!(
                        "{} timeout in 1st phase: sender={}, seq={}",
                        id, peer_name, seq
                    );
                    continue 'round_loop;
                }
            }

            let echo_count = *echo_count_rx.borrow();
            let num_peers = peers.len();
            debug!(
                "{} in 1st phase: sender={}, seq={}, {} echos, {} peers",
                id, peer_name, seq, echo_count, num_peers
            );

            if echo_count * 3 >= num_peers {
                break;
            }
        }

        zenoh_tx
            .send(Message::Echo {
                seq,
                sender: peer_name.clone(),
            })
            .await?;

        let until = Instant::now() + timeout;
        loop {
            let result = utils::timeout_until(until, echo_count_rx.changed()).await;
            match result {
                Ok(result) => {
                    result?;
                }
                Err(_) => {
                    debug!(
                        "{} timeout in 1st phase: sender={}, seq={}",
                        id, peer_name, seq
                    );
                    continue 'round_loop;
                }
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
    }

    if accepted {
        debug!("{} accepts, sender={}, seq={}", id, peer_name, seq);
        Ok(Some(Msg {
            data,
            sender: peer_name,
            seq,
        }))
    } else {
        debug!("{} rejects, sender={}, seq={}", id, peer_name, seq);
        Ok(None)
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
    pub data: T,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum Message<T> {
    Msg { seq: usize, data: T },
    Present,
    Echo { seq: usize, sender: String },
}

// mod pubsub {
//     use super::*;

//     #[derive(Derivative)]
//     #[derivative(Debug)]
//     pub struct Sender<T> {
//         #[derivative(Debug = "ignore")]
//         tx: watch::Sender<Cell<Option<T>>>,
//     }

//     impl<T> Sender<T> {
//         pub fn send(&self, msg: T) -> Result<()> {
//             self.tx
//                 .send(Cell::new(Some(msg)))
//                 .map_err(|_| format_err!("send failed"))?;
//             Ok(())
//         }
//     }

//     #[derive(Derivative)]
//     #[derivative(Debug)]
//     pub struct Receiver<T> {
//         #[derivative(Debug = "ignore")]
//         rx: watch::Receiver<Cell<Option<T>>>,
//     }

//     impl<T> Receiver<T> {
//         pub async fn recv(&self) -> Option<T> {
//             let mut rx = self.rx.clone();
//             loop {
//                 if rx.changed().await.is_err() {
//                     return None;
//                 }

//                 let msg = match rx.borrow().take().take() {
//                     Some(msg) => msg,
//                     None => continue,
//                 };
//                 return Some(msg);
//             }
//         }
//     }

//     pub fn channel<T>() -> (Sender<T>, Receiver<T>) {
//         let (tx, rx) = watch::channel(Cell::new(None));
//         (Sender { tx }, Receiver { rx })
//     }

//     #[cfg(test)]
//     mod tests {
//         use super::*;

//         #[async_std::test]
//         async fn pubsub_test() -> Result<()> {
//             let (tx, rx) = super::channel();

//             let producer = async move {
//                 for index in 0..5 {
//                     tx.send(index).unwrap();
//                     async_std::task::sleep(Duration::from_millis(10)).await;
//                 }
//             };

//             let consumer = async move {
//                 for index in 0..5 {
//                     let value = rx.recv().await.unwrap();
//                     assert_eq!(index, value);
//                 }
//                 assert!(rx.recv().await.is_none());
//             };

//             futures::join!(producer, consumer);

//             Ok(())
//         }
//     }
// }

#[cfg(test)]
mod tests {
    use super::*;

    #[async_std::test]
    async fn reliable_broadcast_test() -> Result<()> {
        pretty_env_logger::init();

        const NUM_PEERS: usize = 4;
        const NUM_MSGS: usize = 1;
        const BASE_DIR: &str = "reliable_broadcast";

        let futures = (0..NUM_PEERS).map(|peer_index| async move {
            let mut config = zenoh::ConfigProperties::default();
            config.insert(zenoh::net::config::ZN_ADD_TIMESTAMP_KEY, "true".to_string());
            let zenoh = Arc::new(Zenoh::new(config).await?);

            let name = format!("peer_{}", peer_index);
            let path = zenoh::path(BASE_DIR);

            let (tx, mut rx) = super::new(zenoh, path, &name, 4, Duration::from_millis(100));

            let producer = {
                let name = name.clone();
                async move {
                    let mut rng = rand::thread_rng();

                    for seq in 0..NUM_MSGS {
                        async_std::task::sleep(Duration::from_millis(100)).await;

                        let data: u8 = rng.gen();
                        eprintln!("{} sends seq={}, data={}", name, seq, data);
                        tx.send(data).await?;
                    }

                    Fallible::Ok(())
                }
            };

            let consumer = async move {
                for _ in 0..(NUM_PEERS * NUM_MSGS) {
                    let msg = rx.recv().await?.unwrap();
                    eprintln!(
                        "{} received sender={}, seq={}, data={}",
                        name, msg.sender, msg.seq, msg.data
                    );
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
