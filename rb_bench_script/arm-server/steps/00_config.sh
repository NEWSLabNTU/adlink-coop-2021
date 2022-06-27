#!/bin/false "This script should be sourced in a shell, not executed directly"
set -e

num_peers=12

## The binary to be executed
binary_name="reliable-broadcast-benchmark"

## Timestamp used for logging
log_time="$(date +'%Y-%m-%d-%T')"

# Timeout for each test in seconds
remote_timeout=40

## Zenoh repo and commit for compiling zenohd
zenoh_git_url="https://github.com/eclipse-zenoh/zenoh.git"
zenoh_git_commit="90539129b1a7c9e8c7d7daaa84138d093f71fedf"

## Space-delimited payload sizes
# payload_sizes=$(cat "$script_dir/../scripts/exp_payload_list.txt")
# payload_sizes="128 256 512 1024 2048 4096 8192"
if [ -z "$psize" ]
then
    echo "'psize' is not set"
    exit 1
fi

if [ -z "$rsize" ]
then
    echo "'rsize' is not set"
    exit 1
fi

if [ -z "$esize" ]
then
    echo "'esize' is not set"
    exit 1
fi

## The RUST_LOG env set on RPi. It is intended for debug purpose.
# remote_rust_log=""
remote_rust_log="reliable_broadcast=debug,reliable_broadcast_benchmark=debug"
