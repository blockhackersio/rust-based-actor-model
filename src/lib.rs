use tokio::sync::mpsc;

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

trait Message: Send + 'static {}

trait Handler<M>
where
    Self: Actor,
    M: Message,
{
    fn handle(&mut self, msg: M);
}

trait Envelope<A>: Send {
    fn process(&mut self, act: &mut A);
}

struct WrappedMessage<M>
where
    M: Message,
{
    pub msg: Option<M>,
}

impl<A, M> Envelope<A> for WrappedMessage<M>
where
    A: Actor + Handler<M>,
    M: Message,
{
    fn process(&mut self, act: &mut A) {
        if let Some(msg) = self.msg.take() {
            act.handle(msg);
        }
    }
}

struct Addr<A>
where
    A: Actor,
{
    tx: mpsc::Sender<Box<dyn Envelope<A>>>,
}

trait Sender<M> {
    async fn send(&self, msg: M);
}

impl<M, A> Sender<M> for Addr<A>
where
    M: Message,
    A: Actor + Handler<M>,
{
    async fn send(&self, msg: M) {
        let _ = self
            .tx
            .send(Box::new(WrappedMessage { msg: Some(msg) }))
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
    impl Message for Increment {}

    struct Decrement;
    impl Message for Decrement {}

    impl Handler<Increment> for Counter {
        fn handle(&mut self, _msg: Increment) {
            self.count += 1;
        }
    }
    impl Handler<Decrement> for Counter {
        fn handle(&mut self, _msg: Decrement) {
            self.count += 1;
        }
    }

    #[tokio::test]
    async fn it_works() {
        let counter = Counter { count: 0 }.start();

        counter.send(Increment).await;
        counter.send(Increment).await;
        counter.send(Decrement).await;
    }
}

