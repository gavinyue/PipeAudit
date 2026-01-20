use anyhow::Result;
use clap::Parser;
use pipeaudit::ch::ChClient;
use pipeaudit::cli::{Cli, Commands};

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Audit(args) => {
            // Connect to ClickHouse
            let client = ChClient::new(&args.endpoint, &args.user, &args.password, &args.db);

            // Test connection
            client.ping().await?;
            println!("Connected to ClickHouse at {}", args.endpoint);

            println!("Auditing database: {}", args.db);
            println!("Tables: {:?}", args.tables);
            println!("Output: {:?}", args.out);
            // TODO: Implement audit logic in PR-24
        }
    }

    Ok(())
}
