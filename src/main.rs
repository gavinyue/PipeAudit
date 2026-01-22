use anyhow::Result;
use clap::Parser;
use pipeaudit::ch::ChClient;
use pipeaudit::cli::{Cli, Commands};
use pipeaudit::collectors::{
    DiskCollector, MergesCollector, MutationsCollector, MvDagCollector, PartsCollector,
    QueryLogCollector,
};
use pipeaudit::output::{print_summary, write_report};
use pipeaudit::report::{ReportBuilder, Targets};
use pipeaudit::rules::RuleRegistry;

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Audit(args) => {
            run_audit(args).await?;
        }
    }

    Ok(())
}

async fn run_audit(args: pipeaudit::cli::AuditArgs) -> Result<()> {
    // 1. Connect to ClickHouse
    let client = ChClient::new(&args.endpoint, &args.user, &args.password, &args.db);
    client.ping().await?;
    eprintln!("Connected to ClickHouse at {}", args.endpoint);

    // 2. Initialize report builder
    let targets = Targets {
        endpoint: args.endpoint.clone(),
        database: args.db.clone(),
        tables: args.tables.clone(),
    };
    let mut builder = ReportBuilder::new(targets);

    // 3. Run collectors
    eprintln!("Collecting parts metrics...");
    let parts = PartsCollector::collect(&client, &args.db, &args.tables).await?;
    let parts_sql = PartsCollector::sql(&args.db, &args.tables);
    builder.with_parts(parts, &parts_sql);

    eprintln!("Collecting merge metrics...");
    let merges = MergesCollector::collect(&client, &args.db, &args.tables).await?;
    let merges_sql = MergesCollector::sql(&args.db, &args.tables);
    builder.with_merges(merges, &merges_sql);

    eprintln!("Collecting mutation metrics...");
    let mutations = MutationsCollector::collect(&client, &args.db, &args.tables).await?;
    let mutations_sql = MutationsCollector::sql(&args.db, &args.tables);
    builder.with_mutations(mutations, &mutations_sql);

    eprintln!("Collecting disk metrics...");
    let disk = DiskCollector::collect(&client).await?;
    let disk_sql = DiskCollector::sql();
    builder.with_disk(disk, &disk_sql);

    eprintln!("Collecting query log metrics...");
    let queries = QueryLogCollector::collect(&client, &args.db).await?;
    let queries_sql = QueryLogCollector::sql(&args.db);
    builder.with_queries(queries, &queries_sql);

    eprintln!("Collecting MV dependency graph...");
    let mv_dag = MvDagCollector::collect(&client, &args.db).await?;
    let mv_dag_sql = MvDagCollector::sql(&args.db);
    builder.with_mv_dag(mv_dag, &mv_dag_sql);

    // 4. Run rules
    eprintln!("Running audit rules...");
    let registry = RuleRegistry::with_default_rules();
    builder.run_rules(&registry);

    // 5. Build report
    let report = builder.build();

    // 6. Output
    write_report(&report, &args.out)?;
    print_summary(&report, &args.out.to_string_lossy());

    Ok(())
}
