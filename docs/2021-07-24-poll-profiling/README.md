# Profiling of Elapsed Time per Polling

The project profiles the polling time inside the `async-std` runtime.
It aims to find time-consuming tasks that drags the performance of runtime.

## Method

It's done by hacking the `async-io` crate, the actual executor used by `async-std`.
The patch can be found at [async-io.patch](async-io.patch). We include the patched in our dependency by modifying the `Cargo.toml` like this.

```toml
[patch.crates-io]
async-io = { path = "/path/to/patched/async-io" }
```

Then, we run the program to collect the polling elapsed time. We do so by saving stderr and then do some text processing on output file.

```sh
export RUST_LOG=async_io::driver=debug
cargo test --release reliable 2> stderr
cut -c 27- stderr | sort -k1 -n > log.txt
```

The output histogram file is plotted by running GNU plot like this.

```sh
gnuplot histogram.gp
```

## Findings

The elapsed time is measured in nanoseconds. We plot the histogram with 10ms bin size. The histogram shows that 99% polling finishes in less than 10ms. However, remaining tasks can take up to 700ms. It would occupy the executor for a long time.
