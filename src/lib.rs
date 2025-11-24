use async_trait::async_trait;
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};

#[derive(Error, Debug)]
pub enum ActorError {
    #[error("Error receiving message")]
    ResponseError,
    #[error("Error sending message")]
    SendError,
}

pub trait Actor: Send + Sized + 'static {
    fn start(self) -> Addr<Self> {
        let (tx, mut rx) = mpsc::channel::<Box<dyn MsgToProcess<Self>>>(100);
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
pub trait MsgToProcess<A>: Send {
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
impl<A, M> MsgToProcess<A> for Envelope<M>
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

#[derive(Clone)]
pub struct Addr<A>
where
    A: Actor,
{
    tx: mpsc::Sender<Box<dyn MsgToProcess<A>>>,
}

#[async_trait]
pub trait Sender<M>
where
    M: Message,
{
    async fn ask(&self, msg: M) -> Result<M::Response, ActorError>;
    fn tell(&self, msg: M) -> Result<(), ActorError>;
}

#[async_trait]
impl<M, A> Sender<M> for Addr<A>
where
    M: Message,
    A: Actor + Handler<M>,
{
    async fn ask(&self, msg: M) -> Result<M::Response, ActorError> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(Envelope::new(Some(msg), Some(tx)))
            .await
            .map_err(|_| ActorError::SendError)?;
        rx.await.map_err(|_| ActorError::ResponseError)
    }

    fn tell(&self, msg: M) -> Result<(), ActorError> {
        self.tx
            .try_send(Envelope::new(Some(msg), None))
            .map_err(|_| ActorError::SendError)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;

    use crate::{Actor, Handler, Message, Sender};

    struct Counter {
        count: u64,
    }
    impl Actor for Counter {}

    struct Increment;
    impl Message for Increment {
        type Response = ();
    }

    struct Decrement;
    impl Message for Decrement {
        type Response = ();
    }

    struct GetCount;
    impl Message for GetCount {
        type Response = u64;
    }

    #[async_trait]
    impl Handler<Increment> for Counter {
        async fn handle(&mut self, _msg: Increment) {
            self.count += 1;
        }
    }

    #[async_trait]
    impl Handler<Decrement> for Counter {
        async fn handle(&mut self, _msg: Decrement) {
            self.count -= 1;
        }
    }

    #[async_trait]
    impl Handler<GetCount> for Counter {
        async fn handle(&mut self, _msg: GetCount) -> u64 {
            self.count
        }
    }

    #[tokio::test]
    async fn it_works() -> anyhow::Result<()> {
        let counter = Counter { count: 0 }.start();

        counter.tell(Increment)?;
        counter.tell(Increment)?;
        counter.tell(Decrement)?;
        let count = counter.ask(GetCount).await?;

        assert_eq!(count, 1);
        Ok(())
    }
}

