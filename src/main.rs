use anyhow::Result;
use clap::Parser;
use pipeaudit::cli::{Cli, Commands};

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Audit(args) => {
            println!("Auditing database: {}", args.db);
            println!("Tables: {:?}", args.tables);
            println!("Output: {:?}", args.out);
            // TODO: Implement audit logic in PR-24
        }
    }

    Ok(())
}
