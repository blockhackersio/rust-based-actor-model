# Writing your own Rust-Based Actor Model
This repo is a companion repo to the article https://vpunk.sh/blog/actor-model-in-rust/

## Setup
If you have Nix, install [devenv](https://devenv.sh) with `nix profile install nixpkgs#devenv`, then run `devenv shell`.

Otherwise, install Rust via [rustup](https://rustup.rs).

## Usage

```rust
use macros::Message;
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

let counter = Counter { count: 0 }.start();

counter.tell(Increment);
counter.tell(Increment);
counter.tell(Decrement);

let count = counter.ask(GetCount).await;

assert_eq!(count, 2);
```


## Running the tests
```bash
$ cargo test -- --nocapture
    Finished `test` profile [unoptimized + debuginfo] target(s) in 0.02s
     Running unittests src/lib.rs (target/debug/deps/actor_simple-d3ce364961d411e4)

running 1 test
2.7 million msg/sec
test tests::it_works ... ok

test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 3.77s

   Doc-tests actor_simple

running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s
```

