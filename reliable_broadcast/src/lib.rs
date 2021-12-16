use async_std::{sync::RwLock, task::JoinHandle};
use dashmap::{DashMap, DashSet};
use derivative::Derivative;
use futures::{
    future::{self, FutureExt as _, TryFutureExt as _},
    stream::{self, Stream, StreamExt as _, TryStreamExt as _},
};
use guard::guard;
use log::{debug, info};
use serde::{
    de::{DeserializeOwned, Error as _},
    Deserialize, Deserializer, Serialize, Serializer,
};
use std::{
    cmp,
    error::Error as StdError,
    fmt,
    fmt::Display,
    hash::{Hash, Hasher},
    mem,
    ops::Deref,
    str::FromStr,
    sync::Arc,
    time::Duration,
};
use zenoh as zn;
use zenoh::{prelude::*, publication::CongestionControl};

type Error = Box<dyn StdError + Send + Sync + 'static>;

use session_ext::*;

mod session_ext {
    use super::*;

    #[derive(Debug, Clone)]
    pub struct SessionExt(Arc<zn::Session>);

    impl SessionExt {
        pub fn new(session: Arc<zn::Session>) -> Self {
            Self(session)
        }

        // pub async fn receiver<'a>(&self, key: impl Into<KeyExpr<'a>>) -> zn::Result<SampleReceiver> {
        //     let receiver = self.0.subscribe(key).await?.receiver().clone();
        //     Ok(receiver)
        // }

        // pub fn inner(&self) -> Arc<zn::Session> {
        //     self.0.clone()
        // }

        // pub fn into_inner(self) -> Arc<zn::Session> {
        //     self.0
        // }

        // pub async fn sender<'a>(&self, key: impl Into<KeyExpr<'a>>) -> zn::Result<Sender> {
        //     let key = key.into();
        //     self.declare_publication(&key).await?;

        //     Ok(Sender {
        //         session: self.0.clone(),
        //         key: key.to_owned(),
        //     })
        // }
    }

    impl Deref for SessionExt {
        type Target = zn::Session;

        fn deref(&self) -> &Self::Target {
            &*self.0
        }
    }

    // pub struct Sender {
    //     pub(crate) session: Arc<zn::Session>,
    //     pub(crate) key: KeyExpr<'static>,
    // }

    // impl Sender {
    //     pub async fn send(&self, value: impl Into<Value>) -> zn::Result<()> {
    //         self.session.put(&self.key, value).await
    //     }

    //     pub fn into_sink(self) -> impl Sink<Value, Error = Error> {
    //         sink::unfold(self, |sender, value| async move {
    //             let sender = sender.send(value).await.map(|()| sender)?;
    //             Ok(sender)
    //         })
    //     }
    // }
}

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
    pub echo_period: Duration,
}

impl Config {
    pub async fn build<'a, T, K>(
        &self,
        session: Arc<zn::Session>,
        key: K,
    ) -> (
        ReliableBroadcastSender<T>,
        impl Stream<Item = Result<T, Error>> + Send,
    )
    where
        T: 'static + Serialize + DeserializeOwned + Send + Sync,
        K: Into<KeyExpr<'a>>,
    {
        let key = key.into().to_owned();
        let (commit_tx, commit_rx) = flume::unbounded();

        let state = Arc::new(State::<T> {
            key,
            my_id: session.id().await,
            active_peers: DashSet::new(),
            echo_requests: RwLock::new(DashSet::new()),
            contexts: DashMap::new(),
            pending_echos: DashSet::new(),
            session: SessionExt::new(session),
            max_rounds: self.max_rounds,
            extra_rounds: self.extra_rounds,
            // recv_timeout: self.recv_timeout,
            round_timeout: self.round_timeout,
            echo_period: self.echo_period,
            commit_tx,
        });
        let receiving_worker = state.clone().run_receiving_worker();
        let echo_worker = state.clone().run_echo_worker();

        let sender = ReliableBroadcastSender {
            state: state.clone(),
        };
        let stream = commit_rx
            .into_stream()
            .map(Result::<_, Error>::Ok)
            .try_filter_map(move |commit| {
                let state = state.clone();

                async move {
                    let Commit { data, broadcast_id } = commit;
                    state.contexts.remove(&broadcast_id).unwrap().1.task.await;
                    Ok(data)
                }
            });

        let stream = stream::select(
            future::try_join(receiving_worker, echo_worker)
                .map_ok(|_| None)
                .into_stream(),
            stream.map_ok(Some),
        )
        .try_filter_map(|data| async move { Ok(data) });

        (sender, stream)
    }
}

#[derive(Clone)]
pub struct ReliableBroadcastSender<T>
where
    T: 'static + Serialize + DeserializeOwned + Send + Sync,
{
    state: Arc<State<T>>,
}

impl<T> ReliableBroadcastSender<T>
where
    T: 'static + Serialize + DeserializeOwned + Send + Sync,
{
    pub async fn send(&self, data: T) -> Result<(), Error> {
        self.state.clone().broadcast(data).await?;
        Ok(())
    }
}

pub struct State<T>
where
    T: 'static + Serialize + DeserializeOwned + Send + Sync,
{
    session: SessionExt,
    my_id: String,
    key: KeyExpr<'static>,
    active_peers: DashSet<PeerId>,
    echo_requests: RwLock<DashSet<BroadcastId>>,
    contexts: DashMap<BroadcastId, BroadcastContext>,
    pending_echos: DashSet<BroadcastId>,
    /// The maximum number of rounds to run the reliable broadcast.
    max_rounds: usize,
    /// The number of extra rounds to send echo(m,s). It will not exceed the `max_rounds`
    extra_rounds: usize,
    // /// The timeout for receiving first 1/3Nv echo messages.
    // recv_timeout: Duration,
    /// The timeout for each round. Must be larger than 2 * `recv_timeout`.
    round_timeout: Duration,
    echo_period: Duration,
    commit_tx: flume::Sender<Commit<T>>,
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
    async fn broadcast(self: Arc<Self>, data: T) -> Result<(), Error> {
        let msg: Message<T> = Broadcast { data }.into();
        let value: Value = serde_json::to_value(&msg)?.into();
        self.session
            .put(&self.key, value)
            .congestion_control(CongestionControl::Drop)
            .kind(SampleKind::Put)
            .encoding(Encoding::TEXT_PLAIN)
            .await?;
        Ok(())
    }

    /// Process an input broadcast.
    fn handle_broadcast(self: Arc<Self>, sample: Sample, msg: Broadcast<T>) {
        let Broadcast { data } = msg;
        let peer_id = sample.source_info.source_id.unwrap();
        let seq = sample.source_info.source_sn.unwrap();
        let broadcast_id = BroadcastId { peer_id, seq };
        self.active_peers.insert(peer_id);
        debug!("{} -> {}: broadcast, seq={}", peer_id, self.my_id, seq);

        use dashmap::mapref::entry::Entry::*;
        match self.contexts.entry(broadcast_id) {
            Occupied(_) => {
                debug!(
                    "ignore duplicated broadcast for broadcast_id {}",
                    broadcast_id
                );
            }
            Vacant(entry) => {
                let acked_peers = Arc::new(DashSet::new());
                let task = async_std::task::spawn(self.clone().run_broadcast_worker(
                    broadcast_id,
                    acked_peers.clone(),
                    data,
                ));

                let context = BroadcastContext {
                    acked: acked_peers,
                    task,
                };

                // remove related pending echos
                if self.pending_echos.remove(&broadcast_id).is_some() {
                    context.acked.insert(peer_id);
                }

                entry.insert(context);
            }
        }
    }

    /// Process an input present message.
    fn handle_present(&self, sample: Sample, msg: Present) {
        // TODO: check timestamp
        let peer_id = sample.source_info.source_id.unwrap();
        debug!("{} -> {}: present", peer_id, self.my_id);
        self.active_peers.insert(peer_id);
    }

    /// Process an input echo.
    fn handle_echo(&self, sample: Sample, msg: Echo) {
        let peer_id = sample.source_info.source_id.unwrap();
        self.active_peers.insert(peer_id);

        msg.broadcast_ids.into_iter().for_each(|broadcast_id| {
            debug!(
                "{} -> {}: echo, broadcast={}",
                peer_id, self.my_id, broadcast_id,
            );

            match self.contexts.get(&broadcast_id) {
                Some(context) => {
                    // save the echoing peer id to corr. broadcast
                    context.acked.insert(peer_id);
                }
                None => {
                    info!(
                        "{} received echo from {} for broadcast_id {}, \
                 but broadcast was not received",
                        self.my_id, peer_id, broadcast_id
                    );

                    // save the echo message
                    self.pending_echos.insert(broadcast_id);
                }
            }
        });
    }

    /// Start a worker that consumes input messages and handle each message accordingly.
    async fn run_receiving_worker(self: Arc<Self>) -> Result<(), Error> {
        let me = self.clone();
        let subscriber_builder = me.session.subscribe(&self.key);
        let mut subscriber = subscriber_builder.best_effort().pull_mode().await?;
        let receiver = subscriber.receiver().clone();
        let future = receiver
            .filter_map(|sample| async move {
                if sample.kind != SampleKind::Put {
                    return None;
                }

                guard!(let Some(value) = sample.value.as_json() else {
                    // TODO: warning
                    return None;
                });

                let value: Message<T> = match serde_json::from_value(value) {
                    Ok(value) => value,
                    Err(_err) => {
                        // TODO: warning
                        return None;
                    }
                };

                Some((sample, value))
            })
            .map(Result::<_, Error>::Ok)
            .try_for_each_concurrent(8, move |(sample, msg)| {
                let me = self.clone();

                async move {
                    match msg {
                        Message::Broadcast(msg) => me.handle_broadcast(sample, msg),
                        Message::Present(msg) => me.handle_present(sample, msg),
                        Message::Echo(msg) => me.handle_echo(sample, msg),
                    }
                    Ok(())
                }
            });

        async_std::task::spawn(future).await?;

        Ok(())
    }

    /// Start a worker that periodically publishes batched echos.
    async fn run_echo_worker(self: Arc<Self>) -> Result<(), Error> {
        async_std::task::spawn(async move {
            async_std::stream::interval(self.echo_period)
                .map(Ok)
                .try_for_each(|()| {
                    let me = self.clone();

                    async move {
                        let echo_requests = {
                            let mut echo_requests = me.echo_requests.write().await;
                            mem::take(&mut *echo_requests)
                        };
                        let broadcast_ids: Vec<_> = echo_requests.into_iter().collect();
                        let msg: Message<T> = Echo { broadcast_ids }.into();
                        let value: Value = serde_json::to_value(&msg)?.into();
                        me.session
                            .put(&me.key, value)
                            .congestion_control(CongestionControl::Drop)
                            .kind(SampleKind::Put)
                            .encoding(Encoding::TEXT_PLAIN)
                            .await?;
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
        acked_peers: Arc<DashSet<PeerId>>,
        data: T,
    ) {
        async_std::task::spawn(async move {
            // TODO: determine start time from timestamp in broadcast message
            let mut interval = async_std::stream::interval(self.round_timeout);

            // send echo
            self.clone().request_sending_echo(broadcast_id).await;

            let (last_round, accepted) = (&mut interval)
                .take(self.max_rounds)
                .enumerate()
                .then(|(round, ())| {
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
                                debug!(
                                    "{} accepted a msg in round {} for broadcast_id {}",
                                    me.my_id, round, broadcast_id
                                );
                                (true, round, true)
                            }
                            // case: n_echos >= 1/3 n_peers
                            else if num_echos * 3 >= num_peers {
                                // send echo and try again
                                me.request_sending_echo(broadcast_id).await;
                                (false, round, false)
                            }
                            // case: n_echos < 1/3 n_peers
                            else {
                                (true, round, false)
                            }
                        }
                        // case: n_peers < 4
                        else {
                            (true, round, false)
                        }
                    }
                })
                .scan(false, |fused, (finished, round, accepted)| {
                    let output = if *fused {
                        None
                    } else if finished {
                        *fused = true;
                        Some((round, accepted))
                    } else {
                        Some((round, accepted))
                    };
                    async move { output }
                })
                .fold((0, false), |_, (round, accepted)| async move {
                    (round, accepted)
                })
                .await;

            // if accepting early, unconditionally send echo for more extra rounds
            if accepted {
                let extra_rounds = cmp::min(last_round + self.extra_rounds + 1, self.max_rounds)
                    - (last_round + 1);

                interval
                    .take(extra_rounds)
                    .enumerate()
                    .for_each(|(round, ())| {
                        let me = self.clone();

                        async move {
                            debug!(
                                "{} finishes round {} for broadcast_id {}",
                                me.my_id,
                                round + last_round,
                                broadcast_id
                            );
                            me.request_sending_echo(broadcast_id).await;
                        }
                    })
                    .await;
            }

            let commit = if accepted {
                debug!(
                    "{} accepts the msg for broadcast_id {}",
                    self.my_id, broadcast_id
                );
                Commit {
                    data: Some(data),
                    broadcast_id,
                }
            } else {
                debug!(
                    "{} rejects the msg for broadcast_id {}",
                    self.my_id, broadcast_id
                );
                Commit {
                    data: Some(data),
                    broadcast_id,
                }
            };

            let _ = self.commit_tx.send_async(commit).await;
        })
        .await;
    }
}

/// The identifier for a broadcast.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct BroadcastId {
    /// The ID of the message sender of (m, s).
    #[serde(with = "serde_peer_id")]
    pub peer_id: PeerId,
    /// The sequence number of the message.
    pub seq: ZInt,
}

impl Display for BroadcastId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "{}/{}", self.peer_id, self.seq)
    }
}

/// The context for a broadcast.
pub struct BroadcastContext {
    /// The set of peers that replies echos.
    acked: Arc<DashSet<PeerId>>,
    /// The task handle to the broadcast worker.
    task: JoinHandle<()>,
}

pub struct Commit<T> {
    pub data: Option<T>,
    pub broadcast_id: BroadcastId,
}

use message::*;

/// A module that performs operations related to messages.
pub mod message {
    use super::*;

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
        /// The data to send in the message.
        pub data: T,
    }

    #[derive(Debug, Clone, PartialEq, Eq, Derivative, Serialize, Deserialize)]
    #[derivative(Hash)]
    /// The structure for the message type *echo(m, s)*
    pub struct Echo {
        pub broadcast_ids: Vec<BroadcastId>,
    }

    #[derive(Debug, Clone, PartialEq, Eq, Derivative, Serialize, Deserialize)]
    #[derivative(Hash)]
    /// The structure for the message type *present*.
    pub struct Present {}
}

pub fn hash_uhlc_timestamp<H>(timestamp: &uhlc::Timestamp, state: &mut H)
where
    H: Hasher,
{
    timestamp.get_id().hash(state);
    hash_uhlc_ntp64(timestamp.get_time(), state);
}

pub mod serde_uhlc_timestamp {
    use super::*;

    pub fn serialize<S>(timestamp: &uhlc::Timestamp, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        format!("{}", timestamp).serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<uhlc::Timestamp, D::Error>
    where
        D: Deserializer<'de>,
    {
        let text = String::deserialize(deserializer)?;
        let timestamp = uhlc::Timestamp::from_str(&text)
            .map_err(|err| D::Error::custom(format!("invalid timestamp '{}': {:?}", text, err)))?;
        Ok(timestamp)
    }
}

pub mod serde_peer_id {
    use super::*;

    pub fn serialize<S>(id: &PeerId, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        format!("{}", id).serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<PeerId, D::Error>
    where
        D: Deserializer<'de>,
    {
        let text = String::deserialize(deserializer)?;
        let timestamp = PeerId::from_str(&text)
            .map_err(|err| D::Error::custom(format!("invalid peer ID '{}': {:?}", text, err)))?;
        Ok(timestamp)
    }
}

pub fn hash_uhlc_ntp64<H>(timestamp: &uhlc::NTP64, state: &mut H)
where
    H: Hasher,
{
    timestamp.as_u64().hash(state);
}
