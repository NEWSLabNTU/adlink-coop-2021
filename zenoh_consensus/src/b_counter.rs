use crate::{common::*, utils::ValueExt};

pub struct BCounter {
    id: usize,
    key: zenoh::Path,
    state: Arc<Mutex<State>>,
    zenoh: Arc<Zenoh>,
}

impl BCounter {
    pub async fn new(
        zenoh: Arc<Zenoh>,
        path: impl Borrow<zenoh::Path>,
        id: usize,
        num_peers: usize,
    ) -> Result<Self> {
        todo!();
    }

    pub async fn increase(&self, count: usize) {
        todo!();
    }

    pub async fn decrease(&self, count: usize) {
        todo!();
    }

    pub async fn get(&self) -> isize {
        todo!();
    }

    pub async fn publish(&self) -> Result<()> {
        todo!();
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct State {
    pos_count: Vec<usize>,
    neg_count: Vec<usize>,
    transfer: Vec<Vec<usize>>,
}

impl State {
    pub fn new(size: usize) -> Self {
        Self {
            pos_count: vec![0; size],
            neg_count: vec![0; size],
            transfer: vec![vec![0; size]; size],
        }
    }

    pub fn merge(&self, other: &Self) -> Result<Self> {
        todo!();
    }

    pub fn merge_assign(&mut self, other: &Self) -> Result<()> {
        *self = self.merge(other)?;
        Ok(())
    }
}
