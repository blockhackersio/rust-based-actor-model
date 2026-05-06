use async_trait::async_trait;
use futures::FutureExt;
use std::panic::AssertUnwindSafe;
use tokio::sync::{
    mpsc::{self},
    oneshot,
};

type PointerToActorMessage<A> = Box<dyn ActorMessage<A>>;

struct ActorConfig {
    restart: bool,
}

impl ActorConfig {
    pub fn no_restart() -> ActorConfig {
        ActorConfig { restart: false }
    }
}

impl Default for ActorConfig {
    fn default() -> Self {
        Self { restart: true }
    }
}

pub trait Actor: Send + Sized + 'static {
    fn start(self) -> Addr<Self> {
        let mut slot = Some(self);
        start_actor(
            move || {
                slot.take()
                    .expect("factory function cannot be called twice!")
            },
            ActorConfig::no_restart(),
        )
    }

    fn spawn<A, F>(&mut self, factory: F) -> Addr<A>
    where
        A: Actor,
        F: FnMut() -> A + Send + 'static,
    {
        start_actor(factory, ActorConfig::default())
    }
}

fn start_actor<A, F>(mut factory: F, config: ActorConfig) -> Addr<A>
where
    A: Actor,
    F: FnMut() -> A + Send + 'static,
{
    let (tx, mut rx) = mpsc::unbounded_channel::<PointerToActorMessage<A>>();
    let addr = Addr { tx };
    tokio::spawn(async move {
        loop {
            let mut actor = factory();

            while let Some(mut msg) = rx.recv().await {
                match AssertUnwindSafe(msg.process(&mut actor))
                    .catch_unwind()
                    .await
                {
                    Ok(()) => {}
                    Err(_) => {
                        eprintln!("Actor panicked!\nRestarting...");

                        break;
                    }
                }
            }
            if !config.restart {
                break;
            }
        }
    });
    addr
}

pub trait Message: Send + 'static {
    type Response: Send;
}

#[async_trait]
pub trait Handler<M>
where
    Self: Actor,
    M: Message,
{
    async fn handle(&mut self, msg: M) -> M::Response;
}

#[async_trait]
pub trait ActorMessage<A>: Send {
    async fn process(&mut self, act: &mut A);
}

pub struct Envelope<M>
where
    M: Message,
{
    pub msg: Option<M>,
    pub tx: Option<oneshot::Sender<M::Response>>,
}

impl<M> Envelope<M>
where
    M: Message,
{
    pub fn new(msg: Option<M>, tx: Option<oneshot::Sender<M::Response>>) -> Box<Self> {
        Box::new(Self { msg, tx })
    }
}

#[async_trait]
impl<A, M> ActorMessage<A> for Envelope<M>
where
    A: Actor + Handler<M>,
    M: Message,
{
    async fn process(&mut self, act: &mut A) {
        if let Some(msg) = self.msg.take() {
            let res = act.handle(msg).await;
            if let Some(tx) = self.tx.take() {
                let _ = tx.send(res);
            }
        }
    }
}

pub struct Addr<A>
where
    A: Actor,
{
    tx: mpsc::UnboundedSender<PointerToActorMessage<A>>,
}

impl<A> Clone for Addr<A>
where
    A: Actor,
{
    fn clone(&self) -> Self {
        Addr {
            tx: self.tx.clone(),
        }
    }
}

#[async_trait]
pub trait Sender<M>
where
    M: Message,
{
    async fn ask(&self, msg: M) -> M::Response;
    fn tell(&self, msg: M);
}

#[async_trait]
impl<M, A> Sender<M> for Addr<A>
where
    M: Message,
    A: Actor + Handler<M>,
{
    async fn ask(&self, msg: M) -> M::Response {
        let (tx, rx) = oneshot::channel();
        let _ = self.tx.send(Envelope::new(Some(msg), Some(tx)));
        rx.await.expect("actor dropped before responding")
    }

    fn tell(&self, msg: M) {
        let _ = self.tx.send(Envelope::new(Some(msg), None));
    }
}

#[cfg(test)]
mod tests {
    use crate::{Actor, Addr, Handler, Message, Sender};
    use async_trait::async_trait;
    use macros::Message;
    use std::time::{Duration, Instant};
    use tokio::time::sleep;

    #[tokio::test(flavor = "multi_thread")]
    async fn simple_counter() -> anyhow::Result<()> {
        // Simple counter
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

        let mut handles = vec![];

        let t1 = counter.clone();
        let t2 = counter.clone();

        let total = 10_000_000;
        let start = Instant::now();
        handles.push(tokio::task::spawn(async move {
            for _ in 0..(total / 2) {
                t1.tell(Increment);
            }
            Ok::<_, anyhow::Error>(())
        }));
        handles.push(tokio::task::spawn(async move {
            for _ in 0..(total / 2) {
                t2.tell(Decrement);
            }
            Ok::<_, anyhow::Error>(())
        }));
        for handle in handles {
            handle.await??;
        }
        let count = counter.ask(GetCount).await;
        assert_eq!(count, 0);
        let finished = start.elapsed();
        let msg_per_sec = total as f64 / finished.as_secs_f64();
        println!("{:.1} million msg/sec", msg_per_sec / 1_000_000.0);
        sleep(Duration::from_secs(1)).await;
        Ok(())
    }

    #[tokio::test]
    async fn restart_counter() -> anyhow::Result<()> {
        struct Db {
            value: i64,
        }

        impl Actor for Db {}

        #[derive(Message)]
        #[response(i64)]
        struct DbGet;

        #[derive(Message)]
        struct DbSet(i64);

        #[async_trait]
        impl Handler<DbGet> for Db {
            async fn handle(&mut self, _: DbGet) -> i64 {
                self.value
            }
        }

        #[async_trait]
        impl Handler<DbSet> for Db {
            async fn handle(&mut self, msg: DbSet) {
                self.value = msg.0;
            }
        }

        struct Counter {
            db: Addr<Db>,
        }

        impl Actor for Counter {}

        #[derive(Message)]
        #[response(i64)]
        struct Increment;

        #[derive(Message)]
        #[response(i64)]
        struct GetCount;

        #[derive(Message)]
        struct Poison;

        #[async_trait]
        impl Handler<Increment> for Counter {
            async fn handle(&mut self, _: Increment) -> i64 {
                let count = self.db.ask(DbGet).await + 1;
                self.db.tell(DbSet(count));
                count
            }
        }

        #[async_trait]
        impl Handler<GetCount> for Counter {
            async fn handle(&mut self, _: GetCount) -> i64 {
                self.db.ask(DbGet).await
            }
        }

        #[async_trait]
        impl Handler<Poison> for Counter {
            async fn handle(&mut self, _: Poison) {
                panic!("poisoned!");
            }
        }

        struct Root {}

        impl Actor for Root {}

        #[derive(Message)]
        #[response(Addr<Counter>)]
        struct GetCounter;

        #[async_trait]
        impl Handler<GetCounter> for Root {
            async fn handle(&mut self, _: GetCounter) -> Addr<Counter> {
                let db = self.spawn(|| Db { value: 0 });
                let counter = self.spawn(move || Counter { db: db.clone() });
                counter
            }
        }

        let root = Root {}.start();

        let counter = root.ask(GetCounter).await;
        for _ in 0..5 {
            counter.tell(Increment);
        }
        assert_eq!(counter.ask(GetCount).await, 5);

        counter.tell(Poison);
        counter.ask(Increment).await;

        let count = counter.ask(GetCount).await;
        assert_eq!(count, 6, "state survives because it lives in the db actor");

        Ok(())
    }
}
