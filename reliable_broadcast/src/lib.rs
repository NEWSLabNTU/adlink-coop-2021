mod common;
mod message;
mod state;
mod utils;

use crate::{common::*, message::*, state::*};


pub use config::*;
mod config {
    use super::*;

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
}

pub use sender::*;
mod sender {
    use super::*;

    #[derive(Clone)]
    pub struct ReliableBroadcastSender<T>
    where
        T: 'static + Serialize + DeserializeOwned + Send + Sync,
    {
        pub(super) state: Arc<State<T>>,
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
}
