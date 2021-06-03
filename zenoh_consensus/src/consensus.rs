use crate::{
    common::*,
    utils::{NTP64Ext, ValueExt},
};

#[derive(Derivative)]
#[derivative(Debug)]
pub struct Consensus {
    state: Arc<State>,
}

#[derive(Derivative)]
#[derivative(Debug)]
struct State {
    id: String,
    #[derivative(Debug = "ignore")]
    zenoh: Arc<Zenoh>,
    send_key: zenoh::Path,
    recv_selector: zenoh::Selector,
    #[derivative(Debug = "ignore")]
    peers: DashSet<String>,
    timeout: Duration,
    contexts: DashMap<(String, usize), Context>,
}

impl Consensus {
    pub async fn new(
        zenoh: Arc<Zenoh>,
        path: impl Borrow<zenoh::Path>,
        id: impl AsRef<str>,
        timeout: Duration,
    ) -> Result<Self> {
        let id = id.as_ref();
        let path = path.borrow();

        let send_key = zenoh::path(format!("{}/{}", path, id));
        let recv_selector = zenoh::selector(format!("{}/*", path));

        let state = Arc::new(State {
            id: id.to_string(),
            zenoh,
            send_key,
            recv_selector,
            peers: DashSet::new(),
            timeout,
            contexts: DashMap::new(),
        });

        Ok(Self { state })
    }
}

impl State {
    fn num_peers(&self) -> usize {
        self.peers.len()
    }

    // pub async fn broadcast(self: Arc<Self>, msg: T) -> Result<()>
    // {
    //     self.send(Message::Msg { msg})
    // }

    async fn send<T>(self: Arc<Self>, msg: Message<T>) -> Result<()>
    where
        T: Serialize,
    {
        let msg = Arc::new(zenoh::Value::serialize_from(&msg)?);
        let workspace = self.zenoh.workspace(None).await?;
        workspace.put(&self.send_key, (*msg).to_owned()).await?;
        Ok(())
    }

    // async fn recv<T>(self: Arc<Self>) -> Result<Option<ReceivedMessage<T>>> {
    //     let since = Instant::now();

    //     loop {
    //         let msg = match self.rx.recv().await {
    //             Some(msg) => msg?,
    //             None => return Ok(None),
    //         };

    //         if msg.when >= since {
    //             return Ok(Some(msg));
    //         }
    //     }
    // }

    async fn recv_worker<T>(self: Arc<Self>) -> Result<()>
    where
        T: 'static + Send + Serialize + DeserializeOwned,
    {
        let workspace = self.zenoh.workspace(None).await?;
        let mut change_stream = workspace.subscribe(&self.recv_selector).await?;

        while let Some(change) = change_stream.next().await {
            let when = Instant::now();
            let peer_name = change.path.last_segment();

            match change.kind {
                zenoh::ChangeKind::Put => {
                    self.peers.insert(peer_name.to_string());

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
                            let key = (peer_name.to_string(), seq);
                            self.contexts
                                .entry(key)
                                .and_modify(|_context| {
                                    warn!("received duplicated message {}/{}", peer_name, seq);
                                })
                                .or_insert_with(|| {
                                    let (tx, rx) = watch::channel(0);

                                    // save future to some place and await it
                                    let future = tokio::spawn(worker(
                                        seq,
                                        peer_name.to_string(),
                                        rx,
                                        self.clone(),
                                        data,
                                    ))
                                    .map(|result| Fallible::Ok(result??))
                                    .boxed();

                                    let context = Context {
                                        echo_count: tx,
                                        // future,
                                    };

                                    context
                                });
                        }
                        Message::Present => {}
                        Message::Echo { seq, sender } => {
                            let key = (peer_name.to_string(), seq);
                            let context = match self.contexts.get(&key) {
                                Some(context) => context,
                                None => {
                                    warn!("received echo {}/{} from {}, but broadcast was not received", sender, seq, peer_name);
                                    continue;
                                }
                            };

                            let new_echo_count = *context.echo_count.borrow() + 1;
                            context.echo_count.send(new_echo_count).unwrap();
                        }
                    }
                }
                zenoh::ChangeKind::Patch => {
                    break;
                }
                zenoh::ChangeKind::Delete => {
                    if peer_name == self.id {
                        break;
                    }
                }
            }
        }

        Ok(())
    }
}

async fn worker<T>(
    seq: usize,
    peer_name: String,
    mut echo_count_rx: watch::Receiver<usize>,
    state: Arc<State>,
    data: T,
) -> Result<T>
where
    T: Serialize,
{
    state
        .clone()
        .send::<T>(Message::Echo {
            seq,
            sender: peer_name.clone(),
        })
        .await?;

    for _round in 0.. {
        loop {
            echo_count_rx.changed().await?;
            let echo_count = *echo_count_rx.borrow();
            let num_peers = state.num_peers();

            if echo_count * 3 >= num_peers {
                break;
            }
        }

        state
            .clone()
            .send::<T>(Message::Echo {
                seq,
                sender: peer_name.clone(),
            })
            .await?;

        loop {
            echo_count_rx.changed().await?;
            let echo_count = *echo_count_rx.borrow();
            let num_peers = state.num_peers();

            if echo_count * 3 >= num_peers * 2 {
                break;
            }
        }
    }

    Ok(data)
}

#[derive(Derivative)]
#[derivative(Debug)]
struct Context {
    echo_count: watch::Sender<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum Message<T> {
    Msg { seq: usize, data: T },
    Present,
    Echo { seq: usize, sender: String },
}

mod pubsub {

    use super::*;

    #[derive(Derivative)]
    #[derivative(Debug)]
    pub struct Sender<T> {
        #[derivative(Debug = "ignore")]
        tx: watch::Sender<Cell<Option<T>>>,
    }

    impl<T> Sender<T> {
        pub fn send(&self, msg: T) -> Result<()> {
            self.tx
                .send(Cell::new(Some(msg)))
                .map_err(|_| format_err!("send failed"))?;
            Ok(())
        }
    }

    #[derive(Derivative)]
    #[derivative(Debug)]
    pub struct Receiver<T> {
        #[derivative(Debug = "ignore")]
        rx: watch::Receiver<Cell<Option<T>>>,
    }

    impl<T> Receiver<T> {
        pub async fn recv(&self) -> Option<T> {
            let mut rx = self.rx.clone();
            loop {
                if rx.changed().await.is_err() {
                    return None;
                }

                let msg = match rx.borrow().take().take() {
                    Some(msg) => msg,
                    None => continue,
                };
                return Some(msg);
            }
        }
    }

    pub fn channel<T>() -> (Sender<T>, Receiver<T>) {
        let (tx, rx) = watch::channel(Cell::new(None));
        (Sender { tx }, Receiver { rx })
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[async_std::test]
        async fn pubsub_test() -> Result<()> {
            let (tx, rx) = super::channel();

            let producer = async move {
                for index in 0..5 {
                    tx.send(index).unwrap();
                    async_std::task::sleep(Duration::from_millis(10)).await;
                }
            };

            let consumer = async move {
                for index in 0..5 {
                    let value = rx.recv().await.unwrap();
                    assert_eq!(index, value);
                }
                assert!(rx.recv().await.is_none());
            };

            futures::join!(producer, consumer);

            Ok(())
        }
    }
}
