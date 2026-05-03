# Writing your own Rust-Based Actor Model
This repo is a companion repo to the article https://vpunk.sh/blog/actor-model-in-rust/

## Setup
If you have Nix, install [devenv](https://devenv.sh) with `nix profile install nixpkgs#devenv`, then run `devenv shell`.

Otherwise, install Rust via [rustup](https://rustup.rs).

## Usage

```rust
use crate::{Actor, Handler, Message, Sender};

struct Counter {
    count: i64,
}

impl Actor for Counter {}

#[derive(Message)]
struct Increment;

#[derive(Message)]
struct Decrement;

#[derive(Message)]
#[response(i64)]
struct GetCount;

#[async_trait]
impl Handler<Increment> for Counter {
    async fn handle(&mut self, _: Increment) {
        self.count += 1;
    }
}

#[async_trait]
impl Handler<Decrement> for Counter {
    async fn handle(&mut self, _: Decrement) {
        self.count -= 1;
    }
}

#[async_trait]
impl Handler<GetCount> for Counter {
    async fn handle(&mut self, _: GetCount) -> i64 {
        self.count
    }
}

```


## Running the tests
```bash
cargo test
```

