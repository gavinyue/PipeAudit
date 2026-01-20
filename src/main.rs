use anyhow::Result;
use clap::Parser;
use pipeaudit::cli::Cli;

#[tokio::main]
async fn main() -> Result<()> {
    let _cli = Cli::parse();
    Ok(())
}
