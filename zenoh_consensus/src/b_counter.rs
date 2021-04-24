use async_std::sync::MutexGuard;

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
    ) -> Result<Self> {
        let path = path.borrow();
        let dir = Selector::try_from(path.as_str().to_owned()+"/*").unwrap();
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
                            state.lock().unwrap().merge_assign(&peer_state) ;
                            Fallible::Ok(())
                        }
                    })
                    .await?;

                Fallible::Ok(())
            }
        });

        Ok(BCounter {
            id,
            key,
            state,
            zenoh,
        })
    }

    fn get_quota(id: usize, state: &std::sync::MutexGuard<State>) -> isize{
        let origin_neg_value = *(state.neg_count.get(&id).unwrap());
        let origin_pos_value = *(state.pos_count.get(&id).unwrap());
        let mut quota = origin_pos_value as isize - origin_neg_value as isize;
        for (key, value) in state.transfer.iter(){
            // key: (sender, receiver)
            if id == key.0{
                quota -= *value as isize;
            }
        }
        return quota;
    }

    pub async fn increase(&self, count: usize) {
        let Self { id, ref state, .. } = *self;
        let mut state = state.lock().unwrap();
        let origin_value = *state.pos_count.get(&id).unwrap();
        state.pos_count.insert(id, origin_value+count);
    }

    pub async fn decrease(&self, count: usize) {
        let Self { id, ref state, .. } = *self;
        let mut state = state.lock().unwrap();
        let origin_neg_value = *state.neg_count.get(&id).unwrap();
        let origin_pos_value = *state.pos_count.get(&id).unwrap();
        let self_quota = BCounter::get_quota(id, &state);
        let mut to_borrow = (count as isize) - self_quota;
        if to_borrow <= 0{
            //No need to borrow from others
            state.neg_count.insert(id, origin_neg_value+count);
        }
        else{
            state.neg_count.insert(id, origin_neg_value+max(self_quota as usize, 0));
            //Borrow from known peers until sufficient or no more to borrow
            let old_state = state.clone();
            for (key, value) in old_state.pos_count.iter(){
                if *key != id {
                    //Other peers
                    let quota = BCounter::get_quota(*key, &state);
                    if quota >= to_borrow{
                        //sufficient with single borrow
                        state.transfer.insert((*key, id), (quota - to_borrow) as usize);
                        break;
                    }
                    else{
                        //update to_borrow and continue borrow
                        state.transfer.insert((*key, id), (quota) as usize);
                        to_borrow -= quota;
                    }
                }
            }
        }
        
    }

    pub async fn get(&self) -> isize {
        let Self { id, ref state, .. } = *self;
        let state = state.lock().unwrap();
        let mut sum_quota: isize = 0;
        
        for (k_pos, _v_pos) in state.pos_count.iter(){
            sum_quota += BCounter::get_quota(*k_pos, &state);
        }
        sum_quota
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
    transfer: HashMap<(usize, usize), usize>,
}

impl State {
    pub fn new(id: usize) -> Self {
        let mut tmp_pos_count: HashMap<usize, usize> = HashMap::new();
        let mut tmp_neg_count: HashMap<usize, usize> = HashMap::new();
        let tmp_transfer: HashMap<(usize, usize), usize> = HashMap::new();
        tmp_pos_count.insert(id, 0);
        tmp_neg_count.insert(id, 0);
        Self {
            pos_count: tmp_pos_count,
            neg_count: tmp_neg_count,
            transfer: tmp_transfer,
        }
    }

    pub fn merge(&self, other: &Self) -> Result<Self> {
        let mut pos_count = self.pos_count.clone();
        let mut neg_count = self.neg_count.clone();
        let mut transfer = self.transfer.clone();
        //Todo: merge transfer

        for (k, v) in other.pos_count.iter(){
            if pos_count.contains_key(&k){
                if pos_count.get(k).unwrap() < v{
                    pos_count.insert(*k, *v);
                }
            }
            else{
                pos_count.insert(*k, *v);
            }
        }

        for (k, v) in other.neg_count.iter(){
            if neg_count.contains_key(k){
                if neg_count.get(k).unwrap() < v{
                    neg_count.insert(*k, *v);
                }
            }
            else{
                neg_count.insert(*k, *v);
            }
        }

        //Transer: 1. take maximum value
        for (k, v) in other.transfer.iter(){
            if transfer.contains_key(k){
                if transfer.get(k).unwrap() < v{
                    transfer.insert(*k, *v);
                }
            }
            else{
                transfer.insert(*k, *v);
            }
        }
        //Transfer: 2. remove mutual borrow values
        let old_transfer = transfer.clone();
        for ((sender, receiver), v) in old_transfer.iter(){
            let mutual_borrower = old_transfer.keys().into_iter().filter(|(s, r)| {*r == *sender}).map(|key| *key).collect::<Vec<(usize, usize)>>();
            if mutual_borrower.len() != 0{
                //there is a mutual borrower
                let v_mutual = *transfer.get(&mutual_borrower[0]).unwrap();
                transfer.insert((*sender, *receiver), *v - min(*v, v_mutual));
                transfer.insert(mutual_borrower[0], v_mutual - min(*v, v_mutual));
            }
        }
        

        Ok(Self {
            pos_count,
            neg_count,
            transfer,
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
    async fn b_counter_test() -> Result<()> {
        let num_peers = 2;
        let prefix = Arc::new(zenoh::Path::try_from("/b_counter/").unwrap());

        let peer_futures = (0..num_peers).map(|id| {
            let prefix = prefix.clone();

            async move {
                let zenoh = Arc::new(Zenoh::new(Default::default()).await?);
                eprintln!("peer {} started zenoh", id);

                let b_counter = BCounter::new(zenoh.clone(), prefix, id).await?;
                eprintln!("peer {} joined b counter", id);
                async_std::task::sleep(Duration::from_millis(
                    (1000 * (id + 1)).try_into().unwrap(),
                ))
                .await;
                b_counter.increase(id*10+1).await;
                eprintln!("peer {} increased {}", id, id*10+1);
                b_counter.publish().await;
                async_std::task::sleep(Duration::from_millis(1000)).await;
                let mut cnt_value = b_counter.get().await;
                eprintln!("peer {} has value of {}", id, cnt_value);

                b_counter.decrease((id+1)*2).await;
                eprintln!("peer {} decreased {}", id, (id+1)*2);
                b_counter.publish().await;
                async_std::task::sleep(Duration::from_millis(1000)).await;

                cnt_value = b_counter.get().await;
                eprintln!("peer {} has value of {}", id, cnt_value);

                Fallible::Ok(())
            }
        });

        futures::future::try_join_all(peer_futures).await?;

        Ok(())
    }
}