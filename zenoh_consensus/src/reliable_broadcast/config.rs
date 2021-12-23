use super::{sender::Sender, state::State, stream::Event};
use crate::common::*;
use async_std::sync::RwLock;

type Error = Box<dyn StdError + Send + Sync + 'static>;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
/// Defines the structure of the config file.
pub struct Config {
    /// The maximum number of rounds to run the reliable broadcast.
    pub max_rounds: usize,
    /// The number of extra rounds to send echo(m,s). It will not exceed the `max_rounds`
    pub extra_rounds: usize,
    /// The timeout for each round.
    pub round_timeout: Duration,
    /// The interval that publishes echo messages.
    pub echo_interval: Duration,
    pub sub_mode: SubMode,
    pub reliability: Reliability,
    pub congestion_control: CongestionControl,
}

impl Config {
    pub async fn build<'a, T, K>(
        &self,
        zenoh: Arc<zn::Zenoh>,
        key: K,
    ) -> Result<
        (
            Sender<T>,
            impl Stream<Item = Result<Event<T>, Error>> + Send,
        ),
        Error,
    >
    where
        T: 'static + Serialize + DeserializeOwned + Send + Sync,
        K: TryInto<zn::Path, Error = zn::ZError>,
    {
        // sanity check
        if !(self.echo_interval < self.round_timeout) {
            return Err(anyhow!("echo_interval must be less than round_timeout").into());
        }
        if !(self.extra_rounds < self.max_rounds) {
            return Err(anyhow!("extra_rounds must be less than max_rounds").into());
        }

        let key = key.try_into()?;
        let (commit_tx, commit_rx) = flume::unbounded();

        let state = Arc::new(State::<T> {
            key,
            my_id: zenoh.session().id().await.parse()?,
            seq_number: AtomicUsize::new(0),
            active_peers: DashSet::new(),
            echo_requests: RwLock::new(DashSet::new()),
            contexts: DashMap::new(),
            pending_echos: DashMap::new(),
            zenoh,
            max_rounds: self.max_rounds,
            extra_rounds: self.extra_rounds,
            round_timeout: self.round_timeout,
            echo_interval: self.echo_interval,
            commit_tx,
            // congestion_control: self.congestion_control.into(),
            // sub_mode: self.sub_mode.into(),
            // reliability: self.reliability.into(),
        });
        let (receiving_worker_ready, receiving_worker) = state.clone().run_receiving_worker();
        let echo_worker = state.clone().run_echo_worker();

        let sender = Sender {
            state: state.clone(),
        };
        let stream = {
            let stream = commit_rx.into_stream().then(move |event| {
                let state = state.clone();

                async move {
                    state
                        .contexts
                        .remove(&event.broadcast_id)
                        .unwrap()
                        .1
                        .task
                        .await;
                    event
                }
            });

            let stream = stream::select(
                future::try_join(receiving_worker, echo_worker)
                    .map_ok(|_| None)
                    .into_stream(),
                stream.map(|event| Ok(Some(event))),
            )
            .try_filter_map(|data| async move { Ok(data) });
            stream
        };

        receiving_worker_ready.await;

        Ok((sender, stream))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CongestionControl {
    Block,
    Drop,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SubMode {
    Push,
    Pull,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Reliability {
    BestEffort,
    Reliable,
}

impl From<CongestionControl> for zn::net::CongestionControl {
    fn from(from: CongestionControl) -> Self {
        type F = CongestionControl;

        match from {
            F::Block => Self::Block,
            F::Drop => Self::Drop,
        }
    }
}

impl From<SubMode> for zn::net::SubMode {
    fn from(from: SubMode) -> Self {
        type F = SubMode;

        match from {
            F::Push => Self::Push,
            F::Pull => Self::Pull,
        }
    }
}

impl From<Reliability> for zn::net::Reliability {
    fn from(from: Reliability) -> Self {
        type F = Reliability;

        match from {
            F::BestEffort => Self::BestEffort,
            F::Reliable => Self::Reliable,
        }
    }
}
