use super::{message::*, ConsensusError, Event};
use crate::{common::*, utils::ValueExt as _};
use async_std::{sync::RwLock, task::JoinHandle};
use rand::rngs::OsRng;
use zenoh as zn;

type Error = Box<dyn StdError + Send + Sync + 'static>;

pub(crate) struct State<T>
where
    T: 'static + Serialize + DeserializeOwned + Send + Sync,
{
    pub(crate) zenoh: Arc<zn::Zenoh>,
    pub(crate) my_id: Uuid,
    pub(crate) seq_number: AtomicUsize,
    pub(crate) key: zn::Path,
    pub(crate) active_peers: DashSet<Uuid>,
    pub(crate) echo_requests: RwLock<DashSet<BroadcastId>>,
    pub(crate) contexts: DashMap<BroadcastId, BroadcastContext>,
    pub(crate) pending_echos: DashMap<BroadcastId, Arc<DashSet<Uuid>>>,
    pub(crate) commit_tx: flume::Sender<Event<T>>,
    /// The maximum number of rounds to run the reliable broadcast.
    pub(crate) max_rounds: usize,
    /// The number of extra rounds to send echo(m,s). It will not exceed the `max_rounds`
    pub(crate) extra_rounds: usize,
    /// The timeout for each round. Must be larger than 2 * `recv_timeout`.
    pub(crate) round_timeout: Duration,
    pub(crate) echo_interval: Duration,
    // pub(crate) sub_mode: SubMode,
    // pub(crate) reliability: Reliability,
    // pub(crate) congestion_control: CongestionControl,
}

impl<T> State<T>
where
    T: 'static + Serialize + DeserializeOwned + Send + Sync,
{
    /// Schedule a future task to publish an echo.
    async fn request_sending_echo(self: Arc<Self>, broadcast_id: BroadcastId) {
        self.echo_requests.read().await.insert(broadcast_id);
    }

    /// Publish a broadcast.
    pub async fn broadcast(self: Arc<Self>, data: T) -> Result<(), Error> {
        let seq = self.seq_number.fetch_add(1, SeqCst);

        let msg: Message<T> = Broadcast {
            from: self.my_id,
            seq,
            data,
        }
        .into();
        let value = zenoh::Value::serialize_from(&msg)?;
        let workspace = self.zenoh.workspace(None).await?;
        workspace.put(&self.key, value).await?;

        Ok(())
    }

    /// Process an input broadcast.
    fn handle_broadcast(self: Arc<Self>, msg: Broadcast<T>) {
        // TODO: check timestamp
        // let peer_id = sample.source_info.source_id.unwrap();
        // let seq = sample.source_info.source_sn.unwrap();

        let broadcast_id = msg.broadcast_id();
        self.active_peers.insert(broadcast_id.broadcaster);
        debug!(
            "{} -> {}: broadcast, seq={}",
            broadcast_id.broadcaster, self.my_id, broadcast_id.seq
        );

        use dashmap::mapref::entry::Entry::*;
        match self.contexts.entry(broadcast_id) {
            Occupied(_) => {
                debug!(
                    "ignore duplicated broadcast for broadcast_id {}",
                    broadcast_id
                );
            }
            Vacant(entry) => {
                // remove related pending echos
                let acked_peers =
                    if let Some((_, acked_peers)) = self.pending_echos.remove(&broadcast_id) {
                        acked_peers
                    } else {
                        Arc::new(DashSet::new())
                    };

                let task = async_std::task::spawn(self.clone().run_broadcast_worker(
                    broadcast_id,
                    acked_peers.clone(),
                    msg.data,
                ));

                let context = BroadcastContext {
                    acked: acked_peers,
                    task,
                };

                entry.insert(context);
            }
        }
    }

    /// Process an input present message.
    fn handle_present(&self, msg: Present) {
        let Present { from: sender } = msg;
        // TODO: check timestamp
        // let peer_id = sample.source_info.source_id.unwrap();

        debug!("{} -> {}: present", sender, self.my_id);
        self.active_peers.insert(sender);
    }

    /// Process an input echo.
    fn handle_echo(&self, msg: Echo) {
        // TODO: check timestamp
        // let peer_id = sample.source_info.source_id.unwrap();

        let sender = msg.from;
        self.active_peers.insert(sender);

        msg.broadcast_ids.into_iter().for_each(|broadcast_id| {
            debug!(
                "{} -> {}: echo, broadcast={}",
                sender, self.my_id, broadcast_id,
            );

            match self.contexts.get(&broadcast_id) {
                Some(context) => {
                    // save the echoing peer id to corr. broadcast
                    context.acked.insert(sender);
                }
                None => {
                    info!(
                        "{} received echo from {} for broadcast_id {}, \
                 but broadcast was not received",
                        self.my_id, sender, broadcast_id
                    );

                    // save the echo message
                    use dashmap::mapref::entry::Entry::*;

                    let acked_peers = match self.pending_echos.entry(broadcast_id) {
                        Occupied(entry) => entry.into_ref(),
                        Vacant(entry) => entry.insert(Arc::new(DashSet::new())),
                    };
                    acked_peers.insert(sender);
                }
            }
        });
    }

    /// Start a worker that consumes input messages and handle each message accordingly.
    pub fn run_receiving_worker(
        self: Arc<Self>,
    ) -> (
        impl Future<Output = ()>,
        impl Future<Output = Result<(), Error>>,
    ) {
        let (ready_tx, ready_rx) = oneshot::channel();
        let ready_future = ready_rx.map(|result| result.unwrap());

        let future = async_std::task::spawn(async move {
            let workspace = self.zenoh.workspace(None).await?;
            let stream = workspace.subscribe(&(&self.key).into()).await?;

            // tell that subscription is ready
            ready_tx.send(());

            stream
                .filter_map(|change| async move {
                    if change.kind != zn::ChangeKind::Put {
                        return None;
                    }

                    let msg: Message<T> = match change.value?.deserialize_to() {
                        Ok(msg) => msg,
                        Err(err) => {
                            debug!("unable to decode message: {:?}", err);
                            return None;
                        }
                    };

                    Some(msg)
                })
                .map(Result::<_, Error>::Ok)
                .try_for_each_concurrent(8, {
                    let me = self.clone();

                    move |msg| {
                        let me = me.clone();

                        async move {
                            match msg {
                                Message::Broadcast(msg) => me.handle_broadcast(msg),
                                Message::Present(msg) => me.handle_present(msg),
                                Message::Echo(msg) => me.handle_echo(msg),
                            }
                            Ok(())
                        }
                    }
                })
                .await?;

            Result::<_, Error>::Ok(())
        });

        (ready_future, future)
    }

    /// Start a worker that periodically publishes batched echos.
    pub async fn run_echo_worker(self: Arc<Self>) -> Result<(), Error> {
        async_std::task::spawn(async move {
            async_std::stream::interval(self.echo_interval)
                .map(Ok)
                .try_for_each(|()| {
                    let me = self.clone();

                    async move {
                        let echo_requests = {
                            let mut echo_requests = me.echo_requests.write().await;

                            // if no echo request is scheduled, skip this time
                            if echo_requests.is_empty() {
                                return Ok(());
                            }

                            mem::take(&mut *echo_requests)
                        };
                        let broadcast_ids: Vec<_> = echo_requests.into_iter().collect();

                        debug!(
                            "{} sends an echo with {} broadcast ids",
                            me.my_id,
                            broadcast_ids.len()
                        );

                        let msg: Message<T> = Echo {
                            from: me.my_id.clone(),
                            broadcast_ids,
                        }
                        .into();
                        let value = zn::Value::serialize_from(&msg)?;

                        // let jitter = Duration::from_millis(OsRng.gen_range(0..=10));
                        // async_std::task::sleep(jitter).await;

                        let workspace = me.zenoh.workspace(None).await?;
                        workspace.put(&me.key, value).await?;
                        Result::<(), Error>::Ok(())
                    }
                })
                .await?;

            Result::<(), Error>::Ok(())
        })
        .await?;
        Ok(())
    }

    /// Start a worker for a received broadcast.
    async fn run_broadcast_worker(
        self: Arc<Self>,
        broadcast_id: BroadcastId,
        acked_peers: Arc<DashSet<Uuid>>,
        data: T,
    ) {
        async_std::task::spawn(async move {
            // TODO: determine start time from timestamp in broadcast message
            let mut interval = async_std::stream::interval(self.round_timeout);

            // send echo
            self.clone().request_sending_echo(broadcast_id).await;

            let tuple = (&mut interval)
                .take(self.max_rounds)
                .enumerate()
                .filter_map(|(round, ())| {
                    let me = self.clone();
                    let acked_peers = acked_peers.clone();

                    async move {
                        debug!(
                            "{} finishes round {} for broadcast_id {}",
                            me.my_id, round, broadcast_id
                        );

                        let num_peers = me.active_peers.len();
                        let num_echos = acked_peers.len();

                        if num_peers >= 4 {
                            // case: n_echos >= 2/3 n_peers
                            if num_echos * 3 >= num_peers * 2 {
                                Some((round, Ok(())))
                            }
                            // case: n_echos >= 1/3 n_peers
                            else if num_echos * 3 >= num_peers {
                                // send echo and try again
                                me.request_sending_echo(broadcast_id.clone()).await;
                                None
                            }
                            // case: n_echos < 1/3 n_peers
                            else {
                                Some((round, Err(ConsensusError::InsufficientEchos)))
                            }
                        }
                        // case: n_peers < 4
                        else {
                            Some((round, Err(ConsensusError::InsufficientPeers)))
                        }
                    }
                })
                .boxed()
                .next()
                .await;

            match tuple {
                // accepted before max_roudns
                Some((last_round, Ok(()))) => {
                    debug!(
                        "{} accepts a msg in round {} for broadcast_id {}",
                        self.my_id, last_round, broadcast_id
                    );

                    // trigger event
                    let event = Event {
                        result: Ok(data),
                        broadcast_id,
                    };
                    let _ = self.commit_tx.send_async(event).await;

                    // unconditionally send echo for more extra rounds
                    let extra_rounds = min(self.extra_rounds, self.max_rounds - last_round - 1);

                    interval
                        .take(extra_rounds)
                        .enumerate()
                        .for_each(|(round, ())| {
                            let me = self.clone();

                            async move {
                                debug!(
                                    "{} runs extra round {} for broadcast_id {}",
                                    me.my_id,
                                    round + last_round + 1,
                                    broadcast_id
                                );
                                me.request_sending_echo(broadcast_id).await;
                            }
                        })
                        .await;
                }
                // error before max_roudns
                Some((_, Err(err))) => {
                    debug!(
                        "{} rejects the msg for broadcast_id {}",
                        self.my_id, broadcast_id
                    );

                    let event = Event {
                        result: Err(err),
                        broadcast_id,
                    };
                    let _ = self.commit_tx.send_async(event).await;
                }
                // not accepted when reaching max_rounds
                None => {
                    debug!(
                        "{} rejects the msg for broadcast_id {}",
                        self.my_id, broadcast_id
                    );

                    let event = Event {
                        result: Err(ConsensusError::ConsensusLost),
                        broadcast_id,
                    };
                    let _ = self.commit_tx.send_async(event).await;
                }
            }
        })
        .await
    }
}

/// The context for a broadcast.
pub struct BroadcastContext {
    /// The set of peers that replies echos.
    pub acked: Arc<DashSet<Uuid>>,
    /// The task handle to the broadcast worker.
    pub task: JoinHandle<()>,
}
