use crate::{common::*, utils::ValueExt};

pub struct NPCounter {
    id: usize,
    key: zenoh::Path,
    state: Arc<Mutex<State>>,
    zenoh: Arc<Zenoh>,
}

impl NPCounter {
    pub async fn new(
        zenoh: Arc<Zenoh>,
        path: impl Borrow<zenoh::Path>,
        id: usize,
        num_peers: usize,
    ) -> Result<Self> {
        ensure!(num_peers > 0, "num_peers must be nonzero");
        ensure!(id < num_peers, "id must be in range [0, num_peers)");

        let path = path.borrow();
        let dir = Selector::try_from(path.as_str().to_owned() + "/*").unwrap();
        let key = zenoh::Path::try_from(format!("{}/{}", path, id)).unwrap();
        let state = Arc::new(Mutex::new(State::new(num_peers)));

        async_std::task::spawn({
            let state = state.clone();
            let zenoh = zenoh.clone();

            async move {
                let workspace = zenoh.workspace(None).await?;

                workspace
                    .subscribe(&dir)
                    .await?
                    .map(Fallible::Ok)
                    .try_for_each(|change| {
                        let state = state.clone();

                        async move {
                            let peer_id: usize = change.path.last_segment().parse().unwrap();

                            // skip self update
                            if peer_id == id {
                                return Ok(());
                            }
                            eprintln!("peer {} received update from peer {}", id, peer_id);

                            // merge state from peer with ours
                            let peer_state: State = change.value.unwrap().deserialize_to()?;
                            state.lock().unwrap().merge_assign(&peer_state).unwrap();

                            Fallible::Ok(())
                        }
                    })
                    .await?;

                Fallible::Ok(())
            }
        });

        Ok(NPCounter {
            id,
            key,
            state,
            zenoh,
        })
    }

    pub async fn increase(&self, count: usize) {
        let Self { id, ref state, .. } = *self;
        (*state.lock().unwrap()).pos_count[id] += count;
    }

    pub async fn decrease(&self, count: usize) {
        let Self { id, ref state, .. } = *self;
        (*state.lock().unwrap()).neg_count[id] += count;
    }

    pub async fn get(&self) -> isize {
        let Self {
            id: _, ref state, ..
        } = *self;
        let state = state.lock().unwrap();
        let sum_pos: usize = state.pos_count.iter().sum();
        let sum_neg: usize = state.neg_count.iter().sum();
        (sum_pos as isize) - (sum_neg as isize)
    }

    pub async fn publish(&self) -> Result<()> {
        let Self {
            key, state, zenoh, ..
        } = self;

        let value = Value::serialize_from(&*state.lock().unwrap())?;
        zenoh.workspace(None).await?.put(key, value).await?;

        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct State {
    pos_count: Vec<usize>,
    neg_count: Vec<usize>,
}

impl State {
    pub fn new(size: usize) -> Self {
        Self {
            pos_count: vec![0; size],
            neg_count: vec![0; size],
        }
    }

    pub fn merge(&self, other: &Self) -> Result<Self> {
        ensure!(self.pos_count.len() == other.pos_count.len());
        ensure!(self.neg_count.len() == other.neg_count.len());

        let pos_count: Vec<_> = self
            .pos_count
            .iter()
            .zip(other.pos_count.iter())
            .map(|(&lhs, &rhs)| lhs.max(rhs))
            .collect();

        let neg_count: Vec<_> = self
            .neg_count
            .iter()
            .zip(other.neg_count.iter())
            .map(|(&lhs, &rhs)| lhs.max(rhs))
            .collect();

        Ok(Self {
            pos_count,
            neg_count,
        })
    }

    pub fn merge_assign(&mut self, other: &Self) -> Result<()> {
        *self = self.merge(other)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[async_std::test]
    async fn np_counter_test() -> Result<()> {
        let num_peers = 2;
        let prefix = Arc::new(zenoh::Path::try_from("/np_counter/").unwrap());

        let peer_futures = (0..num_peers).map(|id| {
            let prefix = prefix.clone();

            async move {
                let zenoh = Arc::new(Zenoh::new(Default::default()).await?);
                eprintln!("peer {} started zenoh", id);

                let np_counter = NPCounter::new(zenoh.clone(), prefix, id, num_peers).await?;
                eprintln!("peer {} joined np counter", id);
                async_std::task::sleep(Duration::from_millis(
                    (1000 * (id + 1)).try_into().unwrap(),
                ))
                .await;
                np_counter.increase(id * 10 + 1).await;
                eprintln!("peer {} increased {}", id, id * 10 + 1);
                np_counter.publish().await.expect("could not publish");
                async_std::task::sleep(Duration::from_millis(1000)).await;
                let mut cnt_value = np_counter.get().await;
                eprintln!("peer {} has value of {}", id, cnt_value);

                np_counter.decrease((id + 1) * 2).await;
                eprintln!("peer {} decreased {}", id, (id + 1) * 2);
                np_counter.publish().await.expect("could not publish");
                async_std::task::sleep(Duration::from_millis(1000)).await;

                cnt_value = np_counter.get().await;
                eprintln!("peer {} has value of {}", id, cnt_value);

                Fallible::Ok(())
            }
        });

        futures::future::try_join_all(peer_futures).await?;

        Ok(())
    }
}
