# Zenoh Performance Tests

This is a repository that contains the codes to test the performance of [Zenoh](https://zenoh.io/).  
Currently (2022/02/09), the Zenoh version tested is 0.6.0-dev, rev="f7309d6af9aa8c5c6e55e744fc68ea4f6f18e8e9".

We use [psrecord](https://github.com/astrofrog/psrecord) to record the CPU and memory usage of the test program.

## Build

To build the program, please make sure you have [Rust](https://www.rust-lang.org/) installed.

Run the following command to build release version:
```bash
cargo build --release
```

If you need to parse the log file generated by [psrecord](https://github.com/astrofrog/psrecord), please build the `usage-parser` crate with the following command:
```bash
cd usage-parser
cargo build --release
```

## Usage

Make sure you have build all the programs.

To run the test, simply run:
This command will run the test with default configuration. 
```bash
./target/release/zenoh_performance_test
```

For how to configure the test, please use the following command:
```bash
./target/release/zenoh_performance_test -h
```

To parse the log file generated by [psrecord](https://github.com/astrofrog/psrecord), run:
```bash
./target/release/usage-parser -i <path to log file>
```

If the current `task_worker_1` function cannot fulfill your tests or the CLI input lacks the parameters you need, feel free to create a new async function or add members in the `Cli` structure. 
Please do not modify the `main` function.

## Development

For people who are running experiment testings with this program. You may store the psrecord generated files under the `experiment-results` folder which is ignored in `.gitignore` file.

## Issues

If you encountered the situation that the program didn't terminate, please interrupt it and reduce the number of messages sent or the number of peers created.

## Todo Lists
- [x] Basic put/sub worker implementation
- [x] Basic testing function with simple output msgs.
- [x] Add parser for [psrecord](https://github.com/astrofrog/psrecord) output
- [x] Add support for multiple subscribe workers
- [x] Implement CLI for parameter configuration   
- [x] Separate building blocks from main.rs 
- [x] Add adjustable message numbers for `test_worker_1`.
- [x] Add adjustable payload size for `test_worker_1`.
- [x] Parallelize the peers by using all cores.
- [x] Add adjustable number of `async_std::tasks` spawned for pub/sub workers.
- [x] Add support for peer mode (pub, sub/pub+sub)
- [x] Add logging for successful tests
- [x] Add support for multiple machines
- [x] Upgrade master to test zenoh 0.6.0-dev
- [x] Add test script for multiple test running
- [x] Add parsing script to parse the multiple test results
- [x] Apply parallelism to worker initialization