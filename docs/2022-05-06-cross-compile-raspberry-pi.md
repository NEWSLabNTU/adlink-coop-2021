# Cross Compiling Raspberry Pi Binaries Guide

## TL;DR

Save the snipplet to `build.sh` in your Cargo project and run it.

```sh
#!/usr/bin/env bash
set -e  # exit when failing

script_dir="$( cd -- "$(dirname "$0")" >/dev/null 2>&1 ; pwd -P )"
cd "$script_dir"

if [ ! -d armv7l-linux-musleabihf-cross ] ; then
    wget -nc https://musl.cc/armv7l-linux-musleabihf-cross.tgz
    rm -rf armv7l-linux-musleabihf-cross/
    tar -xf armv7l-linux-musleabihf-cross.tgz
fi

# setup envs
export PATH="$script_dir/armv7l-linux-musleabihf-cross/bin:$PATH"
export CC="$script_dir/armv7l-linux-musleabihf-cross/bin/armv7l-linux-musleabihf-gcc"

# setup rust
rustup target add armv7-unknown-linux-musleabihf

if ! grep -q armv7l-linux-musleabihf-gcc .config/config.toml >/dev/null 2>&1; then
    mkdir -p .config
    cat >> .config/config.tml <<EOF
[target.armv7-unknown-linux-musleabihf]
linker = "$script_dir/armv7l-linux-musleabihf-cross/bin/armv7l-linux-musleabihf-gcc"
EOF
fi

# Build
cargo build --target armv7-unknown-linux-musleabihf --release
```

## Step-by-step Instructions

### Install MUSL Toolchain

1. Visit https://musl.cc/ and download
`armv7l-linux-musleabihf-cross.tgz`. Here is the direct link.

sh
```
wget https://musl.cc/armv7l-linux-musleabihf-cross.tgz
```

2. Extract the tgz file

```sh
tar -xvf armv7l-linux-musleabihf-cross.tgz
```

3. Add the binaries to your PATH.

```sh
export PATH="/path/to/armv7l-linux-musleabihf-cross/bin:$PATH"
```

### Configure Rust Toolchain

1. Install the cross compiling toolchain for rust.

```sh
rustup target add armv7-unknown-linux-musleabihf
```

2. Append the section in `~/.cargo.config` (or `$cargo_dir/.cargo/config.toml`) to use the correct linker.

```sh
[target.armv7-unknown-linux-musleabihf]
linker = "/path/to/armv7l-linux-musleabihf-cross/bin/armv7l-linux-musleabihf-gcc"
```

3. Tell the Rust compiler to use the correct C compiler.

```sh
export CC=/path/to/armv7l-linux-musleabihf-cross/bin/armv7l-linux-musleabihf-gcc
```

### Using the Cross Toolchain

To cross compile your Cargo project,

```sh
cargo --target armv7-unknown-linux-musleabihf --release
```


The binary will be located at

```
target/armv7-unknown-linux-musleabihf/release/xxx
```
