use async_trait::async_trait;
use futures::FutureExt;
use std::panic::AssertUnwindSafe;
use std::time::Duration;
use tokio::sync::{
    mpsc::{self},
    oneshot,
};

type PointerToActorMessage<A> = Box<dyn ActorMessage<A>>;

pub trait Actor: Send + Sized + 'static {
    fn start(self) -> Addr<Self> {
        let mut slot = Some(self);
        start_actor(move || slot.take().expect(""))
    }
}

fn start_actor<A, F>(mut factory: F) -> Addr<A>
where
    A: Actor + 'static,
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
                        eprintln!("Actor panicked! Restarting...");
                        break;
                    }
                }
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

impl<A: Actor> Addr<A> {
    pub fn add_child<F, B>(&self, f: F) -> ChildBuilder<A, B, F>
    where
        B: Actor,
        F: FnMut() -> B + Send + 'static,
    {
        ChildBuilder::<A, B, F> {
            parent: self.clone(),
            max_restarts: 3,
            window: Duration::from_secs(8),
            factory: Box::new(f),
        }
    }
}

pub struct ChildBuilder<A, B, F>
where
    A: Actor,
    B: Actor,
    F: FnMut() -> B + Send + 'static,
{
    parent: Addr<A>,
    factory: Box<F>,
    max_restarts: usize,
    window: Duration,
}

impl<A, B, F> ChildBuilder<A, B, F>
where
    A: Actor,
    B: Actor,
    F: FnMut() -> B + Send + 'static,
{
    pub fn max_restarts(mut self, n: usize) -> Self {
        self.max_restarts = n;
        self
    }

    pub fn window(mut self, d: Duration) -> Self {
        self.window = d;
        self
    }

    pub fn start(self) -> Addr<B> {
        start_actor(self.factory)
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
    use std::time::Instant;

    use async_trait::async_trait;
    use macros::Message;

    use crate::{Actor, Addr, Handler, Message, Sender};

    #[ignore]
    #[tokio::test(flavor = "multi_thread")]
    async fn it_works() -> anyhow::Result<()> {
        struct Counter {
            count: i64,
        }

        impl Actor for Counter {}

        #[derive(Clone, Message)]
        struct Increment;

        #[derive(Clone, Message)]
        struct Decrement;

        #[derive(Clone, Message)]
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
        Ok(())
    }

    // #[ignore]
    #[tokio::test(flavor = "multi_thread")]
    async fn supervision() -> anyhow::Result<()> {
        struct Root {}

        impl Actor for Root {}

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

        let root = Root {}.start();
        let db = root.add_child(|| Db { value: 0 }).start();
        let dbc = db.clone();
        let counter = root.add_child(move || Counter { db: dbc.clone() }).start();

        for _ in 0..5 {
            counter.ask(Increment).await;
        }
        assert_eq!(counter.ask(GetCount).await, 5);
        assert_eq!(db.ask(DbGet).await, 5);

        // Poison the counter
        counter.tell(Poison);

        // Counter is stateless — all state lives in DB.
        // Supervision just needs to keep the loop alive.
        let count = counter.ask(GetCount).await;
        assert_eq!(count, 5, "state survives because it lives in the db actor");

        for _ in 0..3 {
            counter.ask(Increment).await;
        }
        assert_eq!(counter.ask(GetCount).await, 8);
        assert_eq!(db.ask(DbGet).await, 8);

        Ok(())
    }
}
