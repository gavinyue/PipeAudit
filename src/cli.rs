use clap::Parser;

#[derive(Parser, Debug)]
#[command(name = "pipeaudit")]
#[command(about = "Audit tool for ClickHouse data pipelines")]
#[command(version)]
pub struct Cli {
    // Subcommands will be added in PR-03
}
