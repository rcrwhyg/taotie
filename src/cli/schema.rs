use anyhow::Result;
use clap::{ArgMatches, Parser};

use crate::{Backend, CmdExecutor, ReplContext, ReplDisplay, ReplMsg};

use super::ReplResult;

#[derive(Debug, Parser)]
pub struct SchemaOpts {
    #[arg(help = "The name of the dataset")]
    pub name: String,
}

pub fn schema(args: ArgMatches, ctx: &mut ReplContext) -> ReplResult {
    let name = args
        .get_one::<String>("name")
        .expect("expect name")
        .to_string();

    let (msg, rx) = ReplMsg::new(SchemaOpts::new(name));
    Ok(ctx.send(msg, rx))
}

impl SchemaOpts {
    pub fn new(name: String) -> Self {
        Self { name }
    }
}

impl CmdExecutor for SchemaOpts {
    async fn execute<T: Backend>(self, backend: &mut T) -> Result<String> {
        let df = backend.schema(&self.name).await?;
        df.display().await
    }
}
