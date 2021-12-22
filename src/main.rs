#![allow(unused)]

mod common;
mod utils;
mod workers;
use common::*;
use utils::*;
use workers::*;

#[derive(Debug, StructOpt)]
struct Cli {
    #[structopt(short = "p", long, default_value = "1000")]
    /// The total number of publisher peers
    num_put_peer: usize,
    #[structopt(short = "s", long, default_value = "10")]
    /// The total number of subscriber peers
    num_sub_peer: usize,
    #[structopt(short = "t", long, default_value = "100")]
    /// The timeout for subscribers to stop receiving messages. Unit: milliseconds (ms).
    /// The subscriber will start receiving the messages at the same time as the publishers.
    round_timeout: u64,
    #[structopt(short = "i", long, default_value = "100")]
    /// The initial time for starting up futures.
    init_time: u64,
    #[structopt(short = "m", long, default_value = "1")]
    /// The number of messages each publisher peer will try to send.
    num_msgs_per_peer: usize,
    #[structopt(short = "n", long, default_value = "8")]
    /// The payload size of the message.
    payload_size: usize,
    #[structopt(long)]
    /// The number of tasks to spawn for dealing with futures related to publisher peers
    pub_cpu_num: Option<usize>,
    #[structopt(long)]
    /// The number of tasks to spawn for dealing with futures related to subscriber peers
    sub_cpu_num: Option<usize>,
}
#[async_std::main]
async fn main() {
    pretty_env_logger::init();
    let args = Cli::from_args();
    dbg!(&args);
    println!("# of CPU cores = {}", num_cpus::get());
    test_worker_1(args).await;
}

async fn test_worker_1(args: Cli) {
    let (tx, rx) = flume::unbounded::<(usize, Vec<Change>)>();
    let zenoh = Arc::new(Zenoh::new(net::config::default()).await.unwrap());

    let start = Instant::now();
    let start_until = start + Duration::from_millis(args.init_time);
    let timeout = start_until + Duration::from_millis(args.round_timeout);
    let total_sub_number = args.num_sub_peer;
    let total_put_number = args.num_put_peer;
    let total_cpu_num = num_cpus::get();
    let available_cpu_num = (total_cpu_num - 2).max(1);
    if args.sub_cpu_num.is_some() && args.pub_cpu_num.is_some() {
        if args.sub_cpu_num.unwrap() + args.pub_cpu_num.unwrap() > available_cpu_num {
            warn!("Spawning more than available cpu cores tasks for pub/sub.");
        }
    }

    // Old subscriber
    // let sub_handle_vec = (0..total_sub_number)
    //     .into_par_iter()
    //     .map(|peer_id: usize| {
    //         let sub_handle =
    //             subscribe_worker(zenoh.clone(), start_until, timeout, peer_id, tx.clone());
    //         sub_handle
    //     })
    //     .collect::<Vec<_>>();

    // new subscriber
    let sub_cpu_num;
    if let Some(args_sub_cpu_num) = args.sub_cpu_num {
        sub_cpu_num = args_sub_cpu_num;
        if args_sub_cpu_num > (available_cpu_num + 1) / 2 {
            warn!("Spawning more than half of available cpu cores tasks for subscribers.");
        }
    } else {
        sub_cpu_num = (available_cpu_num + 1) / 2;
    }

    let sub_per_peer_num = total_sub_number / sub_cpu_num;
    let mut sub_futs = (0..sub_cpu_num)
        .into_iter()
        .map(|core_idx| {
            let sub_futures = (0..sub_per_peer_num).map(|peer_index| {
                subscribe_worker(
                    zenoh.clone(),
                    start_until,
                    timeout,
                    peer_index + core_idx * sub_per_peer_num,
                    tx.clone(),
                )
            });
            async_std::task::spawn(futures::future::join_all(sub_futures))
        })
        .collect::<Vec<_>>();
    let remaining_sub = total_sub_number % sub_cpu_num;
    let mut remaining_sub_fut = (total_sub_number - remaining_sub..total_sub_number)
        .map(|peer_index| {
            subscribe_worker(zenoh.clone(), start_until, timeout, peer_index, tx.clone())
        })
        .collect::<Vec<_>>();

    let remain_sub_futs = async_std::task::spawn(futures::future::join_all(remaining_sub_fut));
    sub_futs.push(remain_sub_futs);

    // Old publisher futures
    // let pub_futures = (0..total_put_number).map(|peer_index| {
    //     publish_worker(
    //         zenoh.clone(),
    //         start_until,
    //         timeout,
    //         peer_index,
    //         args.num_msgs_per_peer,
    //         &msg_payload,
    //     )
    // });
    // futures::future::try_join_all(pub_futures).await.unwrap();

    // new publisher futures
    let pub_cpu_num;
    if let Some(args_pub_cpu_num) = args.pub_cpu_num {
        pub_cpu_num = args_pub_cpu_num;
        if args_pub_cpu_num > (available_cpu_num + 1) / 2 {
            warn!("Spawning more than half of available cpu cores tasks for publishers.");
        }
    } else {
        pub_cpu_num = (available_cpu_num + 1) / 2;
    }
    let pub_per_peer_num = total_put_number / pub_cpu_num;
    let mut pub_futs = (0..pub_cpu_num)
        .into_iter()
        .map(|core_idx| {
            let pub_futures = (0..pub_per_peer_num).map(|peer_index| {
                publish_worker(
                    zenoh.clone(),
                    start_until,
                    timeout,
                    peer_index + core_idx * pub_per_peer_num,
                    args.num_msgs_per_peer,
                    get_msg_payload(args.payload_size, peer_index),
                )
            });
            async_std::task::spawn(futures::future::join_all(pub_futures))
        })
        .collect::<Vec<_>>();
    let remaining = total_put_number % pub_cpu_num;
    let mut remaining_fut = (total_put_number - remaining..total_put_number)
        .map(|peer_index| {
            publish_worker(
                zenoh.clone(),
                start_until,
                timeout,
                peer_index,
                args.num_msgs_per_peer,
                get_msg_payload(args.payload_size, peer_index),
            )
        })
        .collect::<Vec<_>>();
    let mut additional_fut = (total_put_number..total_put_number + 1)
        .map(|peer_index| {
            publish_worker(
                zenoh.clone(),
                timeout,
                timeout + Duration::from_millis(100),
                peer_index,
                args.num_msgs_per_peer,
                get_msg_payload(args.payload_size, peer_index),
            )
        })
        .collect::<Vec<_>>();
    remaining_fut.append(&mut additional_fut);
    let remain_futs = async_std::task::spawn(futures::future::join_all(remaining_fut));

    pub_futs.push(remain_futs);

    let all_sub_fut = futures::future::join_all(sub_futs);

    let all_pub_fut = futures::future::join_all(pub_futs);

    let demo_fut = demonstration_worker(
        rx,
        total_put_number,
        total_sub_number,
        args.num_msgs_per_peer,
    );

    drop(tx);

    futures::join!(all_pub_fut, all_sub_fut, demo_fut);
}