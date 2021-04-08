use crate::common::*;
use async_std::sync::Mutex;



pub struct LamportMutex {
    id: usize,
    zenoh: Arc<Zenoh>,
    request_key: Arc<zenoh::Path>,
    ack_base_dir: Arc<zenoh::Path>,
    state: Arc<Mutex<State>>,
    lock_rx: watch::Receiver<()>,
}

pub struct LamportMutexGuard {
    parent: LamportMutex,
}

#[derive(Debug, Clone)]
enum State {
    Idle,
    PreRequest{
    	ack_count:usize,
    	unattended_requests: Vec<(usize, zenoh::Timestamp)>,
    },
    Requesting {
        timestamp: zenoh::Timestamp,
        ack_count: usize,
        pending_requests: Vec<usize>,
        unattended_requests: Vec<(usize, zenoh::Timestamp)>,
    },
    CriticalSection {
        pending_requests: Vec<usize>,
    },
}

impl LamportMutex {
    /// Create a new instance of lamport mutex
    pub async fn new(
        zenoh: Arc<Zenoh>,
        path: impl Borrow<zenoh::Path>,
        id: usize,
        num_peers: usize,
    ) -> Result<LamportMutex> {
        ensure!(num_peers > 0, "num_peers must be nonzero");
        ensure!(id < num_peers, "id must be in range [0, num_peers)");

        // borrow from an owned or a reference
        let path = path.borrow();

        // directory of request keys
        let request_dir: zenoh::Path = format!("{}/Request", path).try_into().unwrap();

        // request key owned by myself
        let request_key =
            Arc::new(zenoh::Path::try_from(format!("{}/{}", request_dir, id)).unwrap());

        // directory of ack keys
        let ack_base_dir = Arc::new(zenoh::Path::try_from(format!("{}/Ack", path)).unwrap());

        // glob of peer request keys
        let request_selector: Selector = format!("{}/*", request_dir).try_into().unwrap();

        // glob of peer ack keys which destination is myself
        let ack_selector: Selector = format!("{}/Ack/{}/**", path, id).try_into().unwrap();

        let (lock_tx, lock_rx) = watch::channel(());
        let lock_tx = Arc::new(lock_tx);

        let state = Arc::new(Mutex::new(State::Idle));

        {
            let state = state.clone();
            let zenoh = zenoh.clone();
            let ack_base_dir = ack_base_dir.clone();
            let request_key = request_key.clone();

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
                        let ack_base_dir = ack_base_dir.clone();
                        let request_key = request_key.clone();

                        async move {
                            eprintln!("peer {} received request from {}", id, change.path);
                            let mut state = state.lock().await;

                            match &mut *state {
                                State::Idle => {
                                	eprintln!("peer {} enters Idle, change_path = {}", id, change.path);
                                    // reply acks to other peers in idle state
                                    let peer_id: usize =
                                        change.path.last_segment().parse().unwrap();
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
                                 
                                }
                                State::PreRequest{
                                	unattended_requests,
                                	ack_count,
                                }=> {
                                	eprintln!("peer {} enters PreRequest, change_path = {}", id, change.path);
                                	if change.path == *request_key {
                                        // enter requesting state if self request is sent
                                        *state = State::Requesting {
                                            timestamp: change.timestamp,
                                            ack_count: *ack_count,
                                            pending_requests: vec![],
                                            unattended_requests: unattended_requests.to_vec(),
                                        };
                                    } 
                                    else{
                                    	//Save all request to pending and compare later
                                    	let peer_id: usize =
                                        change.path.last_segment().parse().unwrap();
                                    	unattended_requests.push((peer_id, change.timestamp));
                                    }
                                }
                                State::Requesting {
                                    timestamp,
                                    pending_requests,
                                    unattended_requests,
                                    ..
                                } => {
                                	eprintln!("peer {} enters Requesting", id);
                                	if unattended_requests.len() != 0 {
                                		//deal with the request arriving before self timestamping
                                		eprintln!("peer {} dealing with unattended_requests", id);
                                		*pending_requests = unattended_requests.into_iter().filter(|(_id, timestamp_unattended)| *timestamp_unattended > *timestamp).map(|(id, _timestamp)| *id).collect::<Vec<usize>>();
                                		let to_ack = unattended_requests.into_iter().filter(|(_id, timestamp_unattended)| *timestamp_unattended < *timestamp).map(|(id, _timestamp)| *id).collect::<Vec<usize>>();
                                		for peer_id in to_ack{
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
                                		}
                                		unattended_requests.clear();
                                	}
                                    assert!(change.path != *request_key);
                                    let peer_id: usize =
                                        change.path.last_segment().parse().unwrap();
                                    eprintln!("peer {} own_time = {}, change {} time = {}", id, timestamp, peer_id, change.timestamp);
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
                                    } else {
                                        // if peer request is later than ours, put it to pending request queue
                                        pending_requests.push(peer_id);
                                    }
                                }
                                State::CriticalSection {
                                    pending_requests, ..
                                } => {
                                    // in critical section, defer requests from other peers
                                    assert!(change.path != *request_key);
                                    let peer_id: usize =
                                        change.path.last_segment().parse().unwrap();
                                    pending_requests.push(peer_id);
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

                        async move {
                            eprintln!("peer {} received ack from {}", id, change.path);
                            let mut state = state.lock().await;

                            match &mut *state {
                                State::Requesting {
                                    ack_count,
                                    pending_requests,
                                    ..
                                } => {
                                    *ack_count += 1;

                                    if *ack_count == num_peers - 1 {
                                        *state = State::CriticalSection {
                                            pending_requests: pending_requests.to_vec(),
                                        };

                                        lock_tx.send(())?;
                                    }
                                }
                                State::PreRequest {
                                	ack_count,
                                	unattended_requests, 
                                } => {
                                    *ack_count += 1;

                                    if *ack_count == num_peers - 1 {
                                        *state = State::CriticalSection {
                                            pending_requests: unattended_requests.into_iter().map(|(id, _timestamp)| *id).collect::<Vec<usize>>().to_vec(),
                                        };
                                        lock_tx.send(())?;
                                    }
                                }

                                State::Idle => unreachable!(
                                    "peer {} is in idle state, but received ack from {}",
                                    id, change.path
                                ),
                                State::CriticalSection { .. } => unreachable!(
                                    "peer {} is in critical section, but received ack from {}",
                                    id, change.path
                                ),
                            }

                            Fallible::Ok(())
                        }
                    });

                // run request and ack workers together
                futures::try_join!(request_future, ack_future)?;

                Fallible::Ok(())
            })
        };

        // let fut = async move { futures::try_join!(request_fut, ack_fut) };

        Ok(LamportMutex {
            id,
            zenoh,
            request_key,
            ack_base_dir,
            state,
            lock_rx,
        })
    }

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
            //let hlc = HLC::default();
            //let local_timestamp = hlc.new_timestamp().await;
            //let z_local_timestamp = zenoh::Timestamp::new(*local_timestamp.get_time(), *local_timestamp.get_id());
            workspace.put(request_key, "Request".into()).await?;
            eprintln!("peer {} sends request to {}", id, request_key);
        }

        lock_rx.clone().changed().await?;

        Ok(LamportMutexGuard { parent: self })
    }
}

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

#[cfg(test)]
mod tests {
    use super::*;

    #[async_std::test]
    async fn lamport_mutex_text() -> Result<()> {
        let num_peers = 2;
        let prefix = Arc::new(zenoh::Path::try_from("/lamport_mutex/").unwrap());

        let peer_futures = (0..num_peers).map(|id| {
            let prefix = prefix.clone();

            async move {
                let zenoh = Arc::new(Zenoh::new(Default::default()).await?);
                eprintln!("peer {} started zenoh", id);

                let mutex = LamportMutex::new(zenoh.clone(), prefix, id, num_peers).await?;
                eprintln!("peer {} joined mutex", id);
                async_std::task::sleep(Duration::from_millis((1000*(id+1)).try_into().unwrap())).await;
                let guard = mutex.lock().await?;
                eprintln!("peer {} locked mutex", id);

                async_std::task::sleep(Duration::from_millis(1000)).await;
                let _mutex = guard.unlock().await?;
                eprintln!("peer {} unlocked mutex", id);

                Fallible::Ok(())
            }
        });

        futures::future::try_join_all(peer_futures).await?;

        Ok(())
    }
}
