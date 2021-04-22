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
        let dir = Selector::try_from(path.as_str()).unwrap();
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

                            // merge state from peer with ours
                            let peer_state: State = change.value.unwrap().deserialize_to()?;
                            *state.lock().unwrap() = peer_state;

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
        let Self { id, ref state, .. } = *self;
        let state = state.lock().unwrap();
        (state.pos_count[id] as isize) - (state.pos_count[id] as isize)
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
