use anyhow::Result;
use clap::{ArgMatches, Parser};

use crate::{Backend, CmdExecutor, ReplContext, ReplDisplay, ReplMsg};

use super::ReplResult;

#[derive(Debug, Parser)]
pub struct HeadOpts {
    #[arg(help = "The name of the dataset")]
    pub name: String,

    #[arg(short, long, help = "The number of rows to show")]
    pub n: Option<usize>,
}

pub fn head(args: ArgMatches, ctx: &mut ReplContext) -> ReplResult {
    let name = args
        .get_one::<String>("name")
        .expect("expect name")
        .to_string();

    let n = args.get_one::<usize>("n").copied();

    let (msg, rx) = ReplMsg::new(HeadOpts::new(name, n));
    Ok(ctx.send(msg, rx))
}

impl HeadOpts {
    pub fn new(name: String, n: Option<usize>) -> Self {
        Self { name, n }
    }
}

impl CmdExecutor for HeadOpts {
    async fn execute<T: Backend>(self, backend: &mut T) -> Result<String> {
        let df = backend.head(&self.name, self.n.unwrap_or(5)).await?;
        df.display().await
    }
}
