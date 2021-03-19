#![feature(async_closure)]
use futures::prelude::*;
use std::convert::TryInto;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;
use tokio;
use tokio::runtime::Runtime;
use uhlc::HLC;
use zenoh::*;

pub struct LamportMutex {
    pub id: i32,
    rt: Runtime,
    lock_name: String,
    whole_request_dir: String,
    whole_ack_dir: String,
    //pub zenoh_workspace: & 'a zenoh::Workspace<'a>,
    //change_stream_request: zenoh::ChangeStream<'a>,
    //change_stream_ack: zenoh::ChangeStream<'a>,
    num_machines: i32,
    to_lock: Arc<Mutex<bool>>,
    req_time_set: Arc<Mutex<bool>>,
    request_lst: Arc<Mutex<Vec<i32>>>,
    local_req_time: Option<zenoh::Timestamp>, //to remove
    lock_acqured: Arc<Mutex<bool>>,
    handler: Option<tokio::task::JoinHandle<()>>,
}

///Sends a request to the request key space of the machine
async fn send_request(whole_request_dir: &str) {
    let zenoh = Zenoh::new(net::config::default()).await.unwrap();
    let workspace = zenoh.workspace(None).await.unwrap();
    workspace
        .put(&whole_request_dir.try_into().unwrap(), "Request".into())
        .await
        .unwrap();
    zenoh.close().await.unwrap();
}

///Subscribe to request space of the lamport_mutex workspace. The Ack logic is also in this function
async fn change_req_loop(
    id: i32,
    request_dir: &str,
    to_lock: Arc<Mutex<bool>>,
    req_time_set: Arc<Mutex<bool>>,
    request_lst: Arc<Mutex<Vec<i32>>>,
) {
    //println!("Debug!");
    //println!("{:?}", whole_request_dir);
    let id_str = id.to_string();
    let zenoh = Zenoh::new(net::config::default()).await.unwrap();
    //println!("Debug2!");
    let workspace = zenoh.workspace(None).await.unwrap();

    let local_req_time: Option<zenoh::Timestamp> = None; //Check along with req_time_set
    let local_req_time_ref = &local_req_time;
    let whole_request_dir = format!("{}{}", request_dir, "**");
    let my_request_path = format!("{}{}", request_dir, &id_str);
    println!("{:?}", whole_request_dir);
    println!("{:?}", my_request_path);
    let mut change_stream_request = workspace
        .subscribe(&whole_request_dir.try_into().unwrap())
        .await
        .unwrap();

    let mut request_lst_with_time: Vec<(i32, zenoh::Timestamp)> = [].to_vec();

    while let Some(change) = change_stream_request.next().await {
        println!(
            "[Subscription listener]  {:?} for {} : {:?} at {}",
            change.kind, change.path, change.value, change.timestamp
        );
        let mut requesting_flag = to_lock.lock().unwrap();
        let mut time_set_flag = req_time_set.lock().unwrap();
        let mut local_req_lst = request_lst.lock().unwrap();
        //println!("{:?}", *local_req_lst);
        if *requesting_flag == false {
            //send ack
            todo!();
        } else if local_req_time == None && *requesting_flag == true && *time_set_flag == false {
            //record own time and save others request
            todo!();
        } else {
            if change.path.as_str() != my_request_path {
                if request_lst_with_time.len() == 0 {
                    if change.timestamp < *local_req_time_ref.as_ref().unwrap() {
                        //send ack
                    } else {
                        let mut split = change.path.as_str().split("/");
                        let vec = split.collect::<Vec<&str>>();
                        let target_id = vec[vec.len() - 1].parse().unwrap();
                        request_lst_with_time.push((target_id, change.timestamp));
                    }
                } else {
                    todo!();
                }
            }
        }
        drop(requesting_flag);
        drop(time_set_flag);
        drop(local_req_lst);
    }
}

impl LamportMutex {
    ///Create a new instance of lamport mutex
    pub fn new(id: i32, num_machines: i32, lock_name: &str) -> LamportMutex {
        let local_request_dir = "Request/";
        let id_str = id.to_string();
        let local_ack_dir_pre = "Ack/".to_owned();
        //let new_owned_string = owned_string + borrowed_string;
        let local_ack_dir = local_ack_dir_pre + &id_str;
        let whole_request_dir = format!("{}{}", lock_name, local_request_dir);
        let whole_ack_dir = format!("{}{}", lock_name, local_ack_dir);
        //let mut change_stream_request = workspace.subscribe(&whole_request_dir.try_into().unwrap()).await.unwrap();
        //let mut change_stream_ack = workspace.subscribe(&whole_ack_dir.try_into().unwrap()).await.unwrap();
        LamportMutex {
            id: id,
            rt: Runtime::new().unwrap(),
            lock_name: lock_name.to_string(),
            num_machines: num_machines,
            whole_request_dir: whole_request_dir,
            whole_ack_dir: whole_ack_dir,
            //change_stream_ack: change_stream_ack,
            to_lock: Arc::new(Mutex::new(false)),
            lock_acqured: Arc::new(Mutex::new(false)),
            req_time_set: Arc::new(Mutex::new(false)),
            request_lst: Arc::new(Mutex::new(vec![0; num_machines.try_into().unwrap()])),
            local_req_time: None,
            handler: None,
        }
    }

    ///Spawns a thread to run the method LamportMutex::begin()
    pub fn start(&self) {
        let whole_request_dir = self.whole_request_dir.clone();
        let to_lock = Arc::clone(&self.to_lock);
        let req_time_set = Arc::clone(&self.req_time_set);
        let request_lst = Arc::clone(&self.request_lst);
        let id = self.id;

        let handler = thread::spawn(move || {
            LamportMutex::begin(id, whole_request_dir, to_lock, req_time_set, request_lst)
        });
    }
    ///Spawns an asynchronous thread to deal with incoming requests and acks
    pub fn begin(
        id: i32,
        whole_request_dir: String,
        to_lock: Arc<Mutex<bool>>,
        req_time_set: Arc<Mutex<bool>>,
        request_lst: Arc<Mutex<Vec<i32>>>,
    ) {
        let rt = Runtime::new().unwrap();
        rt.spawn(async move {
            change_req_loop(id, &whole_request_dir, to_lock, req_time_set, request_lst).await
        });
        loop {}
    }
    ///Try to lock the lamport mutex with blocking mode, i.e. wait until all machine acks
    pub fn lock(&self) {
        let rt = Runtime::new().unwrap();
        let request_dir = self.whole_request_dir.clone();
        let id = self.id;
        let id_str = id.to_string();
        let my_request_path = format!("{}{}", request_dir, &id_str);
        rt.block_on(async move { send_request(&my_request_path).await });
        //Return only if lock is acquired
        todo!();
    }
}

#[async_std::main]
async fn main() {
    //let zenoh = Zenoh::new(net::config::default()).await.unwrap();
    //let workspace = zenoh.workspace(None).await.unwrap();
    let lamport_mutex = LamportMutex::new(1, 3, "/test_mutex/");
    lamport_mutex.start();
    println!("Debug3");
    thread::sleep(Duration::from_millis(4000));
    lamport_mutex.lock();
    //drop(lamport_mutex);
    //zenoh.close().await.unwrap();
}
