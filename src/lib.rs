use async_trait::async_trait;
use thiserror::Error;
use tokio::sync::{
    mpsc::{self},
    oneshot,
};

#[derive(Error, Debug)]
pub enum ActorError {
    #[error("Error receiving message")]
    ResponseError,
    #[error("Error sending message")]
    SendError,
}

type PointerToActorMessage<A> = Box<dyn ActorMessage<A>>;

pub trait Actor: Send + Sized + 'static {
    fn start(self) -> Addr<Self> {
        let (tx, mut rx) = mpsc::unbounded_channel::<PointerToActorMessage<Self>>();
        tokio::spawn(async move {
            let mut this = self;
            while let Some(mut msg) = rx.recv().await {
                msg.process(&mut this).await;
            }
        });
        Addr { tx }
    }
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
    use std::time::Instant;

    use async_trait::async_trait;
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

    #[tokio::test(flavor = "multi_thread")]
    async fn it_works() -> anyhow::Result<()> {
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
}
