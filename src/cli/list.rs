use anyhow::Result;
use clap::{ArgMatches, Parser};

use crate::{Backend, CmdExecutor, ReplContext, ReplDisplay, ReplMsg};

use super::ReplResult;

#[derive(Debug, Parser)]
pub struct ListOpts;

pub fn list(_args: ArgMatches, ctx: &mut ReplContext) -> ReplResult {
    let (msg, rx) = ReplMsg::new(ListOpts);
    Ok(ctx.send(msg, rx))
}

impl CmdExecutor for ListOpts {
    async fn execute<T: Backend>(self, backend: &mut T) -> Result<String> {
        let df = backend.list().await?;
        df.display().await
    }
}
