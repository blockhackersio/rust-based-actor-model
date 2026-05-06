use async_trait::async_trait;
use futures::FutureExt;
use std::{
    panic::AssertUnwindSafe,
    sync::{Arc, Mutex},
    time::Instant,
};
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

type PointerToActorMessage<A> = Box<dyn ActorMessage<A>>;

#[async_trait]
pub trait Actor: Send + Sync + Sized + 'static {
    /// Start the actor
    fn start(self) -> Addr<Self> {
        let mut once = Some(self);
        let ctx = start_actor(
            move || once.take().expect("Factory can only be accessed once!"),
            CancellationToken::new(),
            mpsc::unbounded_channel().0,
        );
        ctx.address()
    }
    /// Override to run an action after started but before the first message
    async fn started(&self, _ctx: &Ctx<Self>) {}
    /// Override to run an action after messages have been drained and stop processed
    async fn stopped(&self, _ctx: &Ctx<Self>) {}
    /// Override to change the interrupt behaviour. Default behaviour on escalation is to restart the parent actor
    async fn child_escalated(&self, _ctx: &Ctx<Self>) -> Option<Interrupt> {
        Some(Interrupt::RestartToEscalate)
    }
}

pub enum Interrupt {
    Stop,
    RestartToEscalate,
    Bail,
}

fn start_actor<A, F>(
    mut factory: F,
    cancel: CancellationToken,
    escalate_to_parent: mpsc::UnboundedSender<()>,
) -> Ctx<A>
where
    A: Actor + Sync,
    F: FnMut() -> A + Send + 'static,
{
    let (tx, mut rx) = mpsc::unbounded_channel::<PointerToActorMessage<A>>();
    let (child_escalations, mut child_escalations_rx) = mpsc::unbounded_channel();
    let stopped = CancellationToken::new();
    let ctx = Ctx::<A> {
        addr: Addr {
            tx,
            stopped: stopped.clone(),
        },
        children: Arc::new(Mutex::new(Vec::new())),
        cancel,
        stopped: stopped.clone(),
        child_escalations,
    };
    let ctx_loop = ctx.clone();
    tokio::spawn(async move {
        let ctx = ctx_loop;
        let _stopped_guard = stopped.drop_guard();

        let mut restarts = 0u32;
        let mut first_restart = Instant::now();
        loop {
            let mut actor = factory();
            actor.started(&ctx).await;
            let code = loop {
                tokio::select! {
                    biased; // Important makes sure we check in order

                    // Check the cancel signal
                    _ = ctx.cancel.cancelled() => {
                        break Interrupt::Stop;
                    }

                    // Check if our children have escalated
                    Some(_) = child_escalations_rx.recv() => {
                        if let Some(interrupt) = actor.child_escalated(&ctx).await {
                            break interrupt;
                        }
                    }

                    // Receive a message
                    msg = rx.recv() => {
                        let Some(mut msg) = msg else {
                            break Interrupt::Bail;
                        };
                        if let Err(_) = AssertUnwindSafe(msg.process(&mut actor, &ctx))
                            .catch_unwind()
                            .await
                        {
                            eprintln!("Actor panicked! Restarting...");
                            break Interrupt::RestartToEscalate;
                        }
                    }
                }
            };

            // Stop and wait for all children regardless of why we exited.
            ctx.stop_all_children().await;

            match code {
                Interrupt::Stop => {
                    actor.stopped(&ctx).await;
                    break;
                }
                Interrupt::Bail => {
                    // restarting wont help
                    break;
                }
                Interrupt::RestartToEscalate => {
                    let window = 5;
                    let max_restarts = 3;
                    if first_restart.elapsed().as_secs() > window {
                        restarts = 0;
                        first_restart = Instant::now();
                    }
                    restarts += 1;
                    if restarts >= max_restarts {
                        eprintln!(
                            "Actor restarted {} times in {}s, escalating.",
                            max_restarts, window
                        );
                        let _ = escalate_to_parent.send(());
                        break;
                    }
                }
            }
        }
        // _stopped_guard drops here and signals `stopped`.
    });
    ctx
}

pub struct Ctx<A: Actor> {
    /// Addr for the actor
    addr: Addr<A>,
    /// Store children
    children: Arc<Mutex<Vec<Arc<dyn Stoppable + Send + Sync>>>>,
    /// Signal to stop
    cancel: CancellationToken,
    /// Signal that the actor has stopped
    stopped: CancellationToken,
    /// Channel to escalate to parent
    child_escalations: mpsc::UnboundedSender<()>,
}

impl<A: Actor> Clone for Ctx<A> {
    fn clone(&self) -> Self {
        Self {
            addr: self.addr.clone(),
            children: self.children.clone(),
            cancel: self.cancel.clone(),
            stopped: self.stopped.clone(),
            child_escalations: self.child_escalations.clone(),
        }
    }
}

impl<A: Actor> Ctx<A> {
    pub fn address(&self) -> Addr<A> {
        self.addr.clone()
    }

    pub fn spawn<B, F>(&self, factory: F) -> Addr<B>
    where
        F: FnMut() -> B + Send + 'static,
        B: Actor,
    {
        let child = start_actor(
            factory,
            self.cancel.child_token(),
            self.child_escalations.clone(),
        );
        self.children.lock().unwrap().push(Arc::new(child.clone()));
        child.address()
    }

    async fn stop_all_children(&self) {
        let children: Vec<_> = self.children.lock().unwrap().drain(..).collect();
        for child in children {
            child.stop();
            child.wait_until_stopped().await;
        }
    }
}

#[async_trait]
pub trait Stoppable {
    fn stop(&self);
    async fn wait_until_stopped(&self);
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
    async fn handle(&mut self, msg: M, ctx: &Ctx<Self>) -> M::Response;
}

#[async_trait]
pub trait ActorMessage<A: Actor>: Send {
    async fn process(&mut self, actor: &mut A, ctx: &Ctx<A>);
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
    async fn process(&mut self, act: &mut A, ctx: &Ctx<A>) {
        if let Some(msg) = self.msg.take() {
            let res = act.handle(msg, ctx).await;
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
    stopped: CancellationToken,
}

impl<A: Actor> Addr<A> {
    pub async fn wait_until_stopped(&self) {
        self.stopped.cancelled().await;
    }
}

impl<A> Clone for Addr<A>
where
    A: Actor,
{
    fn clone(&self) -> Self {
        Addr {
            tx: self.tx.clone(),
            stopped: self.stopped.clone(),
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

#[async_trait]
impl<A> Stoppable for Ctx<A>
where
    A: Actor,
{
    fn stop(&self) {
        self.cancel.cancel();
    }

    async fn wait_until_stopped(&self) {
        self.stopped.cancelled().await;
    }
}

#[cfg(test)]
mod tests {
    use crate::{Actor, Addr, Ctx, Handler, Message, Sender, Stoppable};
    use async_trait::async_trait;
    use macros::Message;
    use std::time::Instant;

    #[tokio::test(flavor = "multi_thread")]
    #[ignore]
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
            async fn handle(&mut self, _: Increment, _: &Ctx<Self>) {
                self.count += 1;
            }
        }

        #[async_trait]
        impl Handler<Decrement> for Counter {
            async fn handle(&mut self, _: Decrement, _: &Ctx<Self>) {
                self.count -= 1;
            }
        }

        #[async_trait]
        impl Handler<GetCount> for Counter {
            async fn handle(&mut self, _: GetCount, _: &Ctx<Self>) -> i64 {
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

    #[tokio::test]
    async fn restart_counter() -> anyhow::Result<()> {
        struct Db {
            value: i64,
        }

        #[async_trait]
        impl Actor for Db {
            async fn stopped(&self, _: &Ctx<Self>) {
                println!("Db stopped");
            }
        }

        #[derive(Message)]
        #[response(i64)]
        struct DbGet;

        #[derive(Message)]
        struct DbSet(i64);

        #[async_trait]
        impl Handler<DbGet> for Db {
            async fn handle(&mut self, _: DbGet, _: &Ctx<Self>) -> i64 {
                self.value
            }
        }

        #[async_trait]
        impl Handler<DbSet> for Db {
            async fn handle(&mut self, msg: DbSet, _: &Ctx<Self>) {
                self.value = msg.0;
            }
        }

        struct Counter {
            db: Addr<Db>,
        }

        #[async_trait]
        impl Actor for Counter {
            async fn stopped(&self, _: &Ctx<Self>) {
                println!("Counter stopped");
            }
        }

        #[derive(Message)]
        #[response(i64)]
        struct Increment;

        #[derive(Message)]
        #[response(i64)]
        struct GetCount;

        #[derive(Message)]
        struct Poison;

        #[derive(Message)]
        struct Stop;

        #[async_trait]
        impl Handler<Increment> for Counter {
            async fn handle(&mut self, _: Increment, _: &Ctx<Self>) -> i64 {
                let count = self.db.ask(DbGet).await + 1;
                self.db.tell(DbSet(count));
                count
            }
        }

        #[async_trait]
        impl Handler<GetCount> for Counter {
            async fn handle(&mut self, _: GetCount, _: &Ctx<Self>) -> i64 {
                self.db.ask(DbGet).await
            }
        }

        #[async_trait]
        impl Handler<Poison> for Counter {
            async fn handle(&mut self, _: Poison, _: &Ctx<Self>) {
                panic!("poisoned!");
            }
        }

        struct Root {}

        #[async_trait]
        impl Actor for Root {
            async fn stopped(&self, _: &Ctx<Self>) {
                println!("Root stopped");
            }
        }

        #[async_trait]
        impl Handler<Stop> for Root {
            async fn handle(&mut self, _: Stop, ctx: &Ctx<Self>) {
                println!("in Stop handler");
                ctx.stop();
            }
        }

        #[derive(Message)]
        #[response(Addr<Counter>)]
        struct GetCounter;

        #[async_trait]
        impl Handler<GetCounter> for Root {
            async fn handle(&mut self, _: GetCounter, ctx: &Ctx<Self>) -> Addr<Counter> {
                let db = ctx.spawn(|| Db { value: 0 });
                let counter = ctx.spawn(move || Counter { db: db.clone() });
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
        root.tell(Stop);
        println!("just called stop!");
        root.wait_until_stopped().await;
        println!("goodbye");
        Ok(())
    }
}
