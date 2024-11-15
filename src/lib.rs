use std::{ops::Deref, process::exit, thread};

use anyhow::{Error, Result};
use backend::DataFusionBackend;
use cli::*;
use crossbeam_channel as mpsc;
use enum_dispatch::enum_dispatch;
use reedline_repl_rs::CallBackMap;

pub use cli::ReplCommand;
use tokio::runtime::Runtime;

mod backend;
mod cli;

#[enum_dispatch]
trait CmdExecutor {
    async fn execute<T: Backend>(self, backend: &mut T) -> Result<String>;
}

trait Backend {
    async fn connect(&mut self, opts: &ConnectOpts) -> Result<()>;
    async fn list(&mut self) -> Result<impl ReplDisplay>;
    async fn schema(&self, name: &str) -> Result<impl ReplDisplay>;
    async fn describe(&mut self, name: &str) -> Result<impl ReplDisplay>;
    async fn head(&mut self, name: &str, size: usize) -> Result<impl ReplDisplay>;
    async fn sql(&mut self, sql: &str) -> Result<impl ReplDisplay>;
}

trait ReplDisplay {
    async fn display(self) -> Result<String>;
}

pub struct ReplContext {
    pub tx: mpsc::Sender<ReplMsg>,
}

pub struct ReplMsg {
    cmd: ReplCommand,
    tx: oneshot::Sender<String>,
}

pub type ReplCallbacks = CallBackMap<ReplContext, reedline_repl_rs::Error>;

pub fn get_callbacks() -> ReplCallbacks {
    let mut callbacks = ReplCallbacks::new();
    callbacks.insert("connect".to_string(), cli::connect);
    callbacks.insert("list".to_string(), cli::list);
    callbacks.insert("schema".to_string(), cli::schema);
    callbacks.insert("describe".to_string(), cli::describe);
    callbacks.insert("head".to_string(), cli::head);
    callbacks.insert("sql".to_string(), cli::sql);
    callbacks
}

impl ReplContext {
    pub fn new() -> Self {
        let (tx, rx) = mpsc::unbounded::<ReplMsg>();
        let rt = Runtime::new().expect("Failed to create tokio runtime");

        let mut backend = DataFusionBackend::new();
        thread::Builder::new()
            .name("ReplBackend".to_string())
            .spawn(move || {
                while let Ok(msg) = rx.recv() {
                    if let Err(e) = rt.block_on(async {
                        let ret = msg.cmd.execute(&mut backend).await?;
                        msg.tx.send(ret)?;
                        Ok::<_, Error>(())
                    }) {
                        eprintln!("Failed to process command: {}", e);
                    }
                }
            })
            .unwrap();

        Self { tx }
    }

    pub fn send(&self, msg: ReplMsg, rx: oneshot::Receiver<String>) -> Option<String> {
        if let Err(e) = self.tx.send(msg) {
            eprintln!("Repl Send Error: {}", e);
            exit(1);
        }

        // if the oneshot receiver is dropped, return None, because server had an error on the command
        rx.recv().ok()
    }
}

impl Default for ReplContext {
    fn default() -> Self {
        Self::new()
    }
}

impl Deref for ReplContext {
    type Target = mpsc::Sender<ReplMsg>;

    fn deref(&self) -> &Self::Target {
        &self.tx
    }
}

impl ReplMsg {
    pub fn new(cmd: impl Into<ReplCommand>) -> (Self, oneshot::Receiver<String>) {
        let (tx, rx) = oneshot::channel();
        (
            Self {
                cmd: cmd.into(),
                tx,
            },
            rx,
        )
    }
}
