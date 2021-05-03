use crate::common::*;
use async_std::sync::Mutex;

fn make_msg_id(peer: usize, item: usize) -> i64 {
    i64::from_le_bytes((((peer as u64) << 32) | item as u64).to_le_bytes())
}

pub struct EventualQueue {
    id: usize,
    zenoh: Arc<Zenoh>,
    request_key: Arc<zenoh::Path>,
    enqueue_key: Arc<zenoh::Path>,
    ack_base_dir: Arc<zenoh::Path>,
    state: Arc<Mutex<State>>,
    queue: Arc<Mutex<Vec<usize>>>,
    lock_rx: watch::Receiver<()>,
    deque_return: Arc<Mutex<Option<usize>>>,
    not_popped: Arc<Mutex<Vec<usize>>>,
}

#[derive(Debug, Clone)]
enum State {
    Idle,
    PreRequest {
        ack_count: usize,
        unattended_requests: Vec<(usize, zenoh::Timestamp, usize)>,
        idx: usize, //index of to_pop
    },
    Requesting {
        to_pop: usize,
        timestamp: zenoh::Timestamp,
        ack_count: usize,
        //pending_requests: Vec<usize>,
        unattended_requests: Vec<(usize, zenoh::Timestamp, usize)>,
        idx: usize,
    },
    Pop {
        to_pop: usize,
        pop_ack_cnt: usize,
    },
}

impl EventualQueue {
    /// Create a new instance of eventual queue
    pub async fn new(
        zenoh: Arc<Zenoh>,
        path: impl Borrow<zenoh::Path>,
        id: usize,
        num_peers: usize,
    ) -> Result<EventualQueue> {
        ensure!(num_peers > 0, "num_peers must be nonzero");
        ensure!(id < num_peers, "id must be in range [0, num_peers)");

        // borrow from an owned or a reference
        let path = path.borrow();

        // directory of request keys
        let request_dir: zenoh::Path = format!("{}/Request", path).try_into().unwrap();

        // directory of pop keys
        let pop_dir: zenoh::Path = format!("{}/Pop", path).try_into().unwrap();

        // directory of enqueue keys
        let enqueue_dir: zenoh::Path = format!("{}/Enqueue", path).try_into().unwrap();

        // request key owned by myself
        let request_key =
            Arc::new(zenoh::Path::try_from(format!("{}/{}", request_dir, id)).unwrap());

        // enqueue key owned by myself
        let enqueue_key =
            Arc::new(zenoh::Path::try_from(format!("{}/{}", enqueue_dir, id)).unwrap());

        // pop key owned by myself
        let pop_key = Arc::new(zenoh::Path::try_from(format!("{}/{}", pop_dir, id)).unwrap());

        // directory of ack keys
        let ack_base_dir = Arc::new(zenoh::Path::try_from(format!("{}/Ack", path)).unwrap());

        // glob of peer request keys
        let request_selector: Selector = format!("{}/*", request_dir).try_into().unwrap();

        // glob of peer pop keys
        let pop_selector: Selector = format!("{}/*", pop_dir).try_into().unwrap();

        // glob of peer enqueues
        let enqueue_selector: Selector = format!("{}/*", enqueue_dir).try_into().unwrap();

        // glob of peer ack keys which destination is myself
        let ack_selector: Selector = format!("{}/Ack/{}/**", path, id).try_into().unwrap();

        // glob of peer pop ack keys which destination is myself
        //let pop_ack_selector: Selector = format!("{}/Ack/Pop/{}/**", path, id).try_into().unwrap();

        let (lock_tx, lock_rx) = watch::channel(());
        let lock_tx = Arc::new(lock_tx);

        let state = Arc::new(Mutex::new(State::Idle));
        let queue = Arc::new(Mutex::new(vec![]));
        let deque_return = Arc::new(Mutex::new(None));
        let not_popped = Arc::new(Mutex::new(vec![]));

        {
            let state = state.clone();
            let queue = queue.clone();
            let zenoh = zenoh.clone();
            let ack_base_dir = ack_base_dir.clone();
            let request_key = request_key.clone();
            let pop_key = pop_key.clone();
            let not_popped = not_popped.clone();
            let deque_return = deque_return.clone();

            async_std::task::spawn(async move {
                let workspace = zenoh.workspace(None).await?;

                // start request processing worker
                let request_future = workspace
                    .subscribe(&request_selector)
                    .await?
                    .map(Fallible::Ok)
                    .try_for_each(|change| {
                        let zenoh = zenoh.clone();
                        let state = state.clone();
                        let queue = queue.clone();
                        let ack_base_dir = ack_base_dir.clone();
                        let request_key = request_key.clone();
                        let deque_return = deque_return.clone();
                        let lock_tx = lock_tx.clone();

                        async move {
                            eprintln!("peer {} received request from {}", id, change.path);
                            let mut state = state.lock().await;

                            match &mut *state {
                                State::Idle => {
                                    eprintln!(
                                        "peer {} enters Idle, change_path = {}",
                                        id, change.path
                                    );
                                    // reply acks to other peers' request in idle state
                                    let peer_id: usize =
                                        change.path.last_segment().parse().unwrap();
                                    let ack_key: zenoh::Path =
                                        format!("{}/{}/{}", ack_base_dir, peer_id, id)
                                            .try_into()
                                            .unwrap();
                                    zenoh
                                        .workspace(None)
                                        .await?
                                        .put(&ack_key, zenoh::Value::Integer(0)) //.put(&ack_key, "Ack_request".into())
                                        .await?;
                                    eprintln!("peer {} sends ack to {}", id, ack_key);
                                }
                                State::PreRequest {
                                    unattended_requests,
                                    ack_count,
                                    idx,
                                } => {
                                    eprintln!(
                                        "peer {} enters PreRequest, change_path = {}",
                                        id, change.path
                                    );
                                    let queue = queue.lock().await;
                                    let to_pop: usize;
                                    if !(queue.len() != 0 && queue.len() > *idx) {
                                        //Return to Idle and return none to caller
                                        *state = State::Idle;
                                        let peer_id: usize =
                                            change.path.last_segment().parse().unwrap();
                                        let ack_key: zenoh::Path =
                                            format!("{}/{}/{}", ack_base_dir, peer_id, id)
                                                .try_into()
                                                .unwrap();
                                        zenoh
                                            .workspace(None)
                                            .await?
                                            .put(&ack_key, zenoh::Value::Integer(0)) //.put(&ack_key, "Ack_request".into())
                                            .await?;
                                        let mut deque_return = deque_return.lock().await;
                                        *deque_return = None;
                                        lock_tx.send(())?;
                                    } else {
                                        to_pop = queue[*idx];
                                        if change.path == *request_key {
                                            // enter requesting state if self request is received
                                            *state = State::Requesting {
                                                timestamp: change.timestamp,
                                                ack_count: *ack_count,
                                                //pending_requests: vec![],
                                                unattended_requests: unattended_requests.to_vec(),
                                                to_pop: to_pop,
                                                idx: *idx,
                                            };
                                        } else {
                                            //Save all request to pending and compare later
                                            let peer_id: usize =
                                                change.path.last_segment().parse().unwrap();
                                            let request_item = format!("{:?}", change.value)
                                                .parse::<usize>()
                                                .unwrap();
                                            unattended_requests.push((
                                                peer_id,
                                                change.timestamp,
                                                request_item,
                                            ));
                                        }
                                    }
                                }
                                //Todo: process earlier_same_request
                                //      process incoming request
                                State::Requesting {
                                    timestamp,
                                    ack_count,
                                    //pending_requests,
                                    unattended_requests,
                                    to_pop,
                                    idx,
                                    ..
                                } => {
                                    eprintln!("peer {} enters Requesting", id);
                                    let mut earlier_same_request = vec![];
                                    if unattended_requests.len() != 0 {
                                        //deal with the request arriving before self timestamping
                                        eprintln!("peer {} dealing with unattended_requests", id);
                                        earlier_same_request = unattended_requests
                                            .into_iter()
                                            .filter(|(_id, timestamp_unattended, req_item)| {
                                                *timestamp_unattended < *timestamp
                                                    && *req_item == *to_pop
                                            })
                                            .map(|(id, _timestamp, _req_item)| *id)
                                            .collect::<Vec<usize>>();
                                        //*pending_requests = unattended_requests.into_iter().filter(|(_id, timestamp_unattended, req_item)| req_item.parse::<usize>().unwrap() == *to_pop).filter(|(_id, timestamp_unattended, _req_item)| *timestamp_unattended > *timestamp).map(|(id, _timestamp, _req_item)| *id).collect::<Vec<usize>>();
                                        let to_ack = unattended_requests
                                            .into_iter()
                                            .filter(|(_id, timestamp_unattended, req_item)| {
                                                *timestamp_unattended < *timestamp
                                                    || *req_item != *to_pop
                                            })
                                            .map(|(id, _timestamp, _req_item)| *id)
                                            .collect::<Vec<usize>>();
                                        for peer_id in to_ack {
                                            let ack_key: zenoh::Path =
                                                format!("{}/{}/{}", ack_base_dir, peer_id, id)
                                                    .try_into()
                                                    .unwrap();
                                            zenoh
                                                .workspace(None)
                                                .await?
                                                .put(&ack_key, zenoh::Value::Integer(0)) //.put(&ack_key, "Ack_request".into())
                                                .await?;
                                            eprintln!("peer {} sends ack to {}", id, ack_key);
                                        }
                                        unattended_requests.clear();
                                    }
                                    assert!(change.path != *request_key);
                                    let peer_id: usize =
                                        change.path.last_segment().parse().unwrap();
                                    eprintln!(
                                        "peer {} own_time = {}, change {} time = {}",
                                        id, timestamp, peer_id, change.timestamp
                                    );
                                    if change.timestamp < *timestamp {
                                        // if peer request has higher priority (smaller timestamp),
                                        // ack immediately
                                        let ack_key: zenoh::Path =
                                            format!("{}/{}/{}", ack_base_dir, peer_id, id)
                                                .try_into()
                                                .unwrap();
                                        zenoh
                                            .workspace(None)
                                            .await?
                                            .put(&ack_key, "Ack".into())
                                            .await?;
                                        eprintln!("peer {} sends ack to {}", id, ack_key);
                                        let request_item =
                                            format!("{:?}", change.value).parse::<usize>().unwrap();
                                        if request_item == *to_pop
                                            || earlier_same_request.len() != 0
                                        {
                                            //Someone has requested the item before,
                                            // if queue.len() > idx + 1 => retry with idx+1, go to PreRequest state
                                            // else return None and go to IDLE
                                            let queue = queue.lock().await;
                                            if queue.len() > *idx + 1 {
                                                zenoh
                                                    .workspace(None)
                                                    .await?
                                                    .put(
                                                        &request_key,
                                                        format!("{}", queue[*idx + 1]).into(),
                                                    )
                                                    .await?;
                                                *state = State::PreRequest {
                                                    ack_count: *ack_count,
                                                    unattended_requests: vec![],
                                                    idx: *idx + 1,
                                                }
                                            } else {
                                                *state = State::Idle;
                                                let mut deque_return = deque_return.lock().await;
                                                *deque_return = None;
                                                lock_tx.send(())?;
                                            }
                                        };
                                    }
                                    //else {
                                    // if peer request is later than ours, ignore
                                    //pending_requests.push(peer_id);
                                    //}
                                    else if earlier_same_request.len() != 0 {
                                        //Someone has requested the item before,
                                        // if queue.len() > idx + 1 => retry with idx+1, go to PreRequest state
                                        // else return None and go to IDLE
                                        let queue = queue.lock().await;
                                        if queue.len() > *idx + 1 {
                                            zenoh
                                                .workspace(None)
                                                .await?
                                                .put(
                                                    &request_key,
                                                    format!("{}", queue[*idx + 1]).into(),
                                                )
                                                .await?;
                                            *state = State::PreRequest {
                                                ack_count: *ack_count,
                                                unattended_requests: vec![],
                                                idx: *idx + 1,
                                            }
                                        } else {
                                            *state = State::Idle;
                                            let mut deque_return = deque_return.lock().await;
                                            *deque_return = None;
                                            lock_tx.send(())?;
                                        }
                                    }
                                }

                                State::Pop {
                                    pop_ack_cnt,
                                    to_pop,
                                } => {
                                    let peer_id: usize =
                                        change.path.last_segment().parse().unwrap();
                                    //if request is not the one we want to pop -> ack
                                    let item: Option<i64> = match change.value {
                                        Some(zenoh::Value::Integer(c)) => Some(c),
                                        _ => None,
                                    };
                                    let request_item = item.unwrap() as usize;
                                    if request_item != *to_pop {
                                        let ack_key: zenoh::Path =
                                            format!("{}/{}/{}", ack_base_dir, peer_id, id)
                                                .try_into()
                                                .unwrap();
                                        zenoh
                                            .workspace(None)
                                            .await?
                                            .put(&ack_key, zenoh::Value::Integer(0)) //.put(&ack_key, "Ack_request".into())
                                            .await?;
                                        eprintln!("peer {} sends ack to {}", id, ack_key);
                                    }
                                    //else, ignore the request
                                }
                            }
                            Fallible::Ok(())
                        }
                    });

                // start ack processing worker
                let ack_future = workspace
                    .subscribe(&ack_selector)
                    .await?
                    .map(Fallible::Ok)
                    .try_for_each(|change| {
                        let state = state.clone();
                        let lock_tx = lock_tx.clone();
                        let zenoh = zenoh.clone();
                        let pop_key = pop_key.clone();
                        let queue = queue.clone();
                        let deque_return = deque_return.clone();

                        async move {
                            let req_type: Option<i64> = match change.value {
                                Some(zenoh::Value::Integer(c)) => Some(c),
                                _ => None,
                            };
                            eprintln!(
                                "peer {} received {} from {}",
                                id,
                                req_type.unwrap(),
                                change.path
                            );
                            let mut state = state.lock().await;

                            match &mut *state {
                                State::Requesting {
                                    ack_count,
                                    to_pop,
                                    //pending_requests,
                                    ..
                                } => {
                                    if req_type.unwrap() == 0 {
                                        // Ack Request
                                        *ack_count += 1;
                                    }

                                    if *ack_count == num_peers - 1 {
                                        //Enough acks, send pop signal and go to Pop state to receive pop acks
                                        zenoh
                                            .workspace(None)
                                            .await?
                                            .put(&pop_key, zenoh::Value::Integer(*to_pop as i64)) //.put(&pop_key, format!("{}", *to_pop).into())
                                            .await?;
                                        *state = State::Pop {
                                            pop_ack_cnt: 0,
                                            to_pop: *to_pop,
                                        };

                                        //lock_tx.send(())?;
                                    }
                                }
                                State::PreRequest {
                                    ack_count,
                                    unattended_requests,
                                    idx,
                                } => {
                                    //let req_type = format!("{:?}", change.value);
                                    if req_type.unwrap() == 0 {
                                        // Ack Request
                                        *ack_count += 1;
                                    }

                                    if *ack_count == num_peers - 1 {
                                        //Enough acks, send pop signal and go to Pop state to receive pop acks
                                        let queue = queue.lock().await;
                                        let mut to_pop = None;
                                        if queue.len() != 0 && queue.len() > *idx {
                                            to_pop = Some(queue[*idx]);
                                        }
                                        if to_pop != None {
                                            *state = State::Pop {
                                                pop_ack_cnt: 0,
                                                to_pop: to_pop.unwrap(),
                                            };
                                            zenoh
                                                .workspace(None)
                                                .await?
                                                .put(
                                                    &pop_key,
                                                    zenoh::Value::Integer(to_pop.unwrap() as i64),
                                                )
                                                .await?;
                                        }

                                        //lock_tx.send(())?;
                                    }
                                }

                                State::Idle => unreachable!(
                                    "peer {} is in idle state, but received ack from {}",
                                    id, change.path
                                ),
                                State::Pop {
                                    pop_ack_cnt,
                                    to_pop,
                                } => {
                                    //let req_type = format!("{:?}", change.value);
                                    if req_type.unwrap() == 1 {
                                        //Ack Pop
                                        *pop_ack_cnt += 1;
                                    }
                                    if *pop_ack_cnt == num_peers - 1 {
                                        //Pop the item, return dequeue call, go to IDLE
                                        let mut queue = queue.lock().await;
                                        let mut deque_return = deque_return.lock().await;

                                        let index =
                                            queue.iter().position(|value| *value == *to_pop);
                                        if index != None {
                                            let index_i = index.unwrap();
                                            queue.remove(index_i);
                                        } else {
                                            eprintln!("Bug");
                                        }
                                        *deque_return = Some(*to_pop);
                                        *state = State::Idle;

                                        lock_tx.send(())?;
                                    }
                                }
                            }

                            Fallible::Ok(())
                        }
                    });

                // start enqueue processing worker
                let enqueue_future = workspace
                    .subscribe(&enqueue_selector)
                    .await?
                    .map(Fallible::Ok)
                    .try_for_each(|change| {
                        let queue = queue.clone();
                        let not_popped = not_popped.clone();
                        //let lock_tx = lock_tx.clone();

                        async move {
                            let item: Option<i64> = match change.value {
                                Some(zenoh::Value::Integer(c)) => Some(c),
                                _ => None,
                            };
                            eprintln!(
                                "peer {} received enqueue item = {}",
                                id,
                                format!("{:?}", item.unwrap())
                            );
                            let mut queue = queue.lock().await;
                            let not_popped = not_popped.lock().await;

                            let request_item = item.unwrap() as usize;
                            if !not_popped.contains(&request_item) {
                                queue.push(request_item);
                            }

                            Fallible::Ok(())
                        }
                    });

                // start pop processing worker
                let pop_future = workspace
                    .subscribe(&pop_selector)
                    .await?
                    .map(Fallible::Ok)
                    .try_for_each(|change| {
                        let queue = queue.clone();
                        let not_popped = not_popped.clone();
                        let zenoh = zenoh.clone();
                        let ack_base_dir = ack_base_dir.clone();
                        //let lock_tx = lock_tx.clone();

                        async move {
                            let mut queue = queue.lock().await;
                            let peer_id: usize = change.path.last_segment().parse().unwrap();
                            let item: Option<i64> = match change.value {
                                Some(zenoh::Value::Integer(c)) => Some(c),
                                _ => None,
                            };
                            if peer_id != id {
                                let to_pop = item.unwrap() as usize;
                                eprintln!("peer {} received pop item = {}", id, to_pop);
                                let index = queue.iter().position(|&value| value == to_pop);
                                if index != None {
                                    let index_i = index.unwrap();
                                    queue.remove(index_i);
                                } else {
                                    //save to not popped -> deal with it during enqueue
                                    let mut not_popped = not_popped.lock().await;
                                    not_popped.push(to_pop);
                                }
                                let ack_key: zenoh::Path =
                                    format!("{}/{}/{}", ack_base_dir, peer_id, id)
                                        .try_into()
                                        .unwrap();
                                zenoh
                                    .workspace(None)
                                    .await?
                                    .put(&ack_key, zenoh::Value::Integer(1))
                                    .await?;
                            }

                            Fallible::Ok(())
                        }
                    });

                // run request and ack workers together
                futures::try_join!(request_future, ack_future, enqueue_future, pop_future)?;

                Fallible::Ok(())
            })
        };

        Ok(EventualQueue {
            id,
            zenoh,
            request_key,
            ack_base_dir,
            state,
            queue,
            lock_rx,
            not_popped,
            deque_return,
            enqueue_key,
        })
    }

    pub async fn dequeue(&self) -> Option<usize> {
        let Self {
            ref zenoh,
            ref state,
            id,
            ref request_key,
            ref lock_rx,
            ref queue,
            ref deque_return,
            ..
        } = self;
        let mut non_flag = false;
        {
            let mut state = state.lock().await;
            let queue = queue.lock().await;
            assert!(matches!(&*state, State::Idle));

            *state = State::PreRequest {
                ack_count: 0,
                unattended_requests: vec![],
                idx: 0,
            };
            let workspace = zenoh.workspace(None).await.ok()?;
            let mut to_pop = None;
            if queue.len() != 0 {
                to_pop = Some(queue[0]);
            } else {
                non_flag = true;
            }
            if to_pop != None {
                workspace
                    .put(request_key, zenoh::Value::Integer(to_pop.unwrap() as i64))
                    .await
                    .ok()?;
                eprintln!(
                    "peer {} sends request {} to {}",
                    id,
                    to_pop.unwrap(),
                    request_key
                );
            }
        }

        if non_flag {
            return None;
        }
        lock_rx.clone().changed().await.ok()?;
        let deque_return = deque_return.lock().await;
        return *deque_return;
    }

    pub async fn enqueue(&self, item: usize) {
        let Self {
            ref zenoh,
            id,
            ref enqueue_key,
            ref queue,
            ..
        } = self;

        let workspace = zenoh.workspace(None).await.unwrap();
        let mut queue = queue.lock().await;
        eprintln!("peer {} sends Enqueue to {}", id, enqueue_key);
        queue.push(item);
        workspace
            .put(enqueue_key, zenoh::Value::Integer(item as i64))
            .await
            .unwrap();
    }
}

/*
///Try to lock the lamport mutex with blocking mode, i.e. wait until all machine acks
pub async fn lock(self) -> Result<LamportMutexGuard> {
    let Self {
        ref zenoh,
        ref state,
        id,
        ref request_key,
        ref lock_rx,
        ..
    } = self;

    {
        let mut state = state.lock().await;
        assert!(matches!(&*state, State::Idle));

        *state = State::PreRequest{
            ack_count: 0,
            unattended_requests: vec![],
        };
        drop(state);
        let workspace = zenoh.workspace(None).await?;
        let hlc = HLC::default();
        let local_timestamp = hlc.new_timestamp().await;
        //let z_local_timestamp = zenoh::Timestamp::new(*local_timestamp.get_time(), *local_timestamp.get_id());
        workspace.put(request_key, "Request".into()).await?;
        eprintln!("peer {} sends request to {}", id, request_key);
    }

    lock_rx.clone().changed().await?;

    Ok(LamportMutexGuard { parent: self })
}
*/

/*
impl LamportMutexGuard {
    pub async fn unlock(mut self) -> Result<LamportMutex> {
        {
            let LamportMutex {
                ref mut state,
                ref zenoh,
                ref ack_base_dir,
                id,
                ..
            } = self.parent;
            let mut state = state.lock().await;

            let pending_requests = match &mut *state {
                State::CriticalSection {
                    pending_requests, ..
                } => pending_requests,
                _ => unreachable!(),
            };

            // reply pending requests
            let workspace = zenoh.workspace(None).await?;

            for &peer_id in pending_requests.iter() {
                let ack_key: zenoh::Path = format!("{}/{}/{}", ack_base_dir, peer_id, id)
                    .try_into()
                    .unwrap();
                workspace.put(&ack_key, "Ack".into()).await?;
                eprintln!("peer {} sends ack to {}", id, ack_key);
            }

            *state = State::Idle;
        }

        Ok(self.parent)
    }
}
*/

#[cfg(test)]
mod tests {
    use super::*;

    #[async_std::test]
    async fn eventual_queue_test() -> Result<()> {
        let num_peers = 2;
        let prefix = Arc::new(zenoh::Path::try_from("/eventual_queue/").unwrap());

        let peer_futures = (0..num_peers).map(|id| {
            let prefix = prefix.clone();

            async move {
                let mut config =  zenoh::ConfigProperties::default();
                config.insert(zenoh::net::config::ZN_ADD_TIMESTAMP_KEY, "true".to_string());
                let zenoh = Arc::new(Zenoh::new(config).await?);
                eprintln!("peer {} started zenoh", id);

                let queue = EventualQueue::new(zenoh.clone(), prefix, id, num_peers).await?;
                eprintln!("peer {} joined queue", id);
                let mut to_enqueue: usize = (id + 1) * 100 + 1;
                //queue.enqueue(to_enqueue).await;
                async_std::task::sleep(Duration::from_millis(
                    (1000 * (id + 1)).try_into().unwrap(),
                ))
                .await;
                to_enqueue += 1;
                queue.enqueue(to_enqueue).await;
                async_std::task::sleep(Duration::from_millis(
                    (1000 * (id + 1)).try_into().unwrap(),
                ))
                .await;

                async_std::task::sleep(Duration::from_millis(
                    (1000 * (id + 1)).try_into().unwrap(),
                ))
                .await;
                let item = queue.dequeue().await;
                if item != None {
                    eprintln!("peer {} get {} from dequeue", id, item.unwrap());
                } else {
                    eprintln!("peer {} get None from dequeue", id);
                }

                async_std::task::sleep(Duration::from_millis(1000)).await;
                //let _mutex = guard.unlock().await?;
                //eprintln!("peer {} unlocked mutex", id);

                Fallible::Ok(())
            }
        });

        futures::future::try_join_all(peer_futures).await?;

        Ok(())
    }
}
