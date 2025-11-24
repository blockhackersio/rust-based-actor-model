use tokio::sync::{
    mpsc,
    oneshot::{self, error::RecvError},
};

trait Actor: Send + Sized + 'static {
    fn start(mut self) -> Addr<Self> {
        let (tx, mut rx) = mpsc::channel::<Box<dyn Envelope<Self>>>(100);
        tokio::spawn(async move {
            while let Some(mut env) = rx.recv().await {
                env.process(&mut self);
            }
        });
        Addr { tx }
    }
}

trait Message: Send + 'static {
    type Response: Send;
}

trait Handler<M>
where
    Self: Actor,
    M: Message,
{
    fn handle(&mut self, msg: M) -> M::Response;
}

trait Envelope<A>: Send {
    fn process(&mut self, act: &mut A);
}

struct WrappedMessage<M>
where
    M: Message,
{
    pub msg: Option<M>,
    pub tx: Option<oneshot::Sender<M::Response>>,
}

impl<A, M> Envelope<A> for WrappedMessage<M>
where
    A: Actor + Handler<M>,
    M: Message,
{
    fn process(&mut self, act: &mut A) {
        if let Some(msg) = self.msg.take() {
            let res = act.handle(msg);
            if let Some(tx) = self.tx.take() {
                let _ = tx.send(res);
            }
        }
    }
}

struct Addr<A>
where
    A: Actor,
{
    tx: mpsc::Sender<Box<dyn Envelope<A>>>,
}

trait Sender<M>
where
    M: Message,
{
    async fn ask(&self, msg: M) -> Result<M::Response, RecvError>;
    async fn tell(&self, msg: M);
}

impl<M, A> Sender<M> for Addr<A>
where
    M: Message,
    A: Actor + Handler<M>,
{
    async fn ask(&self, msg: M) -> Result<M::Response, RecvError> {
        let (tx, rx) = oneshot::channel();
        let _ = self
            .tx
            .send(Box::new(WrappedMessage {
                msg: Some(msg),
                tx: Some(tx),
            }))
            .await;
        rx.await
    }

    async fn tell(&self, msg: M) {
        let tx = self.tx.clone();
        let _ = tx
            .send(Box::new(WrappedMessage {
                msg: Some(msg),
                tx: None,
            }))
            .await;
    }
}

#[cfg(test)]
mod tests {
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

    impl Handler<Increment> for Counter {
        fn handle(&mut self, _msg: Increment) {
            self.count += 1;
        }
    }

    impl Handler<Decrement> for Counter {
        fn handle(&mut self, _msg: Decrement) {
            self.count -= 1;
        }
    }
    impl Handler<GetCount> for Counter {
        fn handle(&mut self, _msg: GetCount) -> u64 {
            self.count
        }
    }
    #[tokio::test]
    async fn it_works() -> anyhow::Result<()> {
        let counter = Counter { count: 0 }.start();

        counter.tell(Increment).await;
        counter.tell(Increment).await;
        counter.tell(Decrement).await;
        let count = counter.ask(GetCount).await?;

        assert_eq!(count, 1);
        Ok(())
    }
}

