use crate::{common::*, utils::ValueExt};

pub struct NPCounterHasmap {
    id: usize,
    key: zenoh::Path,
    state: Arc<Mutex<State>>,
    zenoh: Arc<Zenoh>,
}

impl NPCounterHasmap {
    pub async fn new(zenoh: Arc<Zenoh>, path: impl Borrow<zenoh::Path>, id: usize) -> Result<Self> {
        let path = path.borrow();
        let dir = Selector::try_from(path.as_str().to_owned() + "/*").unwrap();
        let key = zenoh::Path::try_from(format!("{}/{}", path, id)).unwrap();
        let state = Arc::new(Mutex::new(State::new(id)));

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

        Ok(NPCounterHasmap {
            id,
            key,
            state,
            zenoh,
        })
    }

    pub async fn increase(&self, count: usize) {
        let Self { id, ref state, .. } = *self;
        let mut state = state.lock().unwrap();
        let origin_value = *state.pos_count.get(&id).unwrap();
        state.pos_count.insert(id, origin_value + count);
    }

    pub async fn decrease(&self, count: usize) {
        let Self { id, ref state, .. } = *self;
        let mut state = state.lock().unwrap();
        let origin_value = *state.neg_count.get(&id).unwrap();
        state.neg_count.insert(id, origin_value + count);
    }

    pub async fn get(&self) -> isize {
        let Self { id:_, ref state, .. } = *self;
        let state = state.lock().unwrap();
        let mut sum_pos: usize = 0;
        let mut sum_neg: usize = 0;
        for (_k_pos, v_pos) in state.pos_count.iter() {
            sum_pos += v_pos;
        }
        for (_k_neg, v_neg) in state.neg_count.iter() {
            sum_neg += v_neg;
        }
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
    pos_count: HashMap<usize, usize>,
    neg_count: HashMap<usize, usize>,
}

impl State {
    pub fn new(id: usize) -> Self {
        let mut tmp_pos_count: HashMap<usize, usize> = HashMap::new();
        let mut tmp_neg_count: HashMap<usize, usize> = HashMap::new();
        tmp_pos_count.insert(id, 0);
        tmp_neg_count.insert(id, 0);
        Self {
            pos_count: tmp_pos_count,
            neg_count: tmp_neg_count,
        }
    }

    pub fn merge(&self, other: &Self) -> Result<Self> {
        let mut pos_count = self.pos_count.clone();
        let mut neg_count = self.neg_count.clone();

        for (k, v) in other.pos_count.iter() {
            if pos_count.contains_key(&k) {
                if pos_count.get(k).unwrap() < v {
                    pos_count.insert(*k, *v);
                }
            } else {
                pos_count.insert(*k, *v);
            }
        }

        for (k, v) in other.neg_count.iter() {
            if neg_count.contains_key(k) {
                if neg_count.get(k).unwrap() < v {
                    neg_count.insert(*k, *v);
                }
            } else {
                neg_count.insert(*k, *v);
            }
        }

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
#[cfg(test)]
mod tests {
    use super::*;

    #[async_std::test]
    async fn np_counter_hashmap_test() -> Result<()> {
        let num_peers = 2;
        let prefix = Arc::new(zenoh::Path::try_from("/np_counter/").unwrap());

        let peer_futures = (0..num_peers).map(|id| {
            let prefix = prefix.clone();

            async move {
                let zenoh = Arc::new(Zenoh::new(Default::default()).await?);
                eprintln!("peer {} started zenoh", id);

                let np_counter = NPCounterHasmap::new(zenoh.clone(), prefix, id).await?;
                eprintln!("peer {} joined np counter", id);
                async_std::task::sleep(Duration::from_millis(
                    (1000 * (id + 1)).try_into().unwrap(),
                ))
                .await;
                np_counter.increase(id * 10 + 1).await;
                eprintln!("peer {} increased {}", id, id * 10 + 1);
                np_counter.publish().await;
                async_std::task::sleep(Duration::from_millis(1000)).await;
                let mut cnt_value = np_counter.get().await;
                eprintln!("peer {} has value of {}", id, cnt_value);

                np_counter.decrease((id + 1) * 2).await;
                eprintln!("peer {} decreased {}", id, (id + 1) * 2);
                np_counter.publish().await;
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
