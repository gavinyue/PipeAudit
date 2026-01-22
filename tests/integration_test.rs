//! Integration tests for PipeAudit
//!
//! These tests require a running ClickHouse instance.
//! Run with: `make test-integration` or `cargo test --ignored`

use anyhow::Result;
use pipeaudit::ch::ChClient;
use pipeaudit::collectors::{
    DiskCollector, MergesCollector, MutationsCollector, MvDagCollector, PartsCollector,
    QueryLogCollector,
};
use pipeaudit::report::{ReportBuilder, Targets};
use pipeaudit::rules::RuleRegistry;

const ENDPOINT: &str = "http://localhost:8123";
const USER: &str = "default";
const PASSWORD: &str = "test";
const DATABASE: &str = "testdb";

fn get_client() -> ChClient {
    ChClient::new(ENDPOINT, USER, PASSWORD, DATABASE)
}

#[tokio::test]
#[ignore = "requires ClickHouse"]
async fn test_clickhouse_ping() -> Result<()> {
    let client = get_client();
    client.ping().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires ClickHouse"]
async fn test_parts_collector() -> Result<()> {
    let client = get_client();
    let tables = vec!["events".to_string()];

    let metrics = PartsCollector::collect(&client, DATABASE, &tables).await?;

    assert!(
        !metrics.is_empty(),
        "Should have parts metrics for events table"
    );

    // Find the events table metrics
    let parts = metrics
        .iter()
        .find(|m| m.table == "events")
        .expect("Should have events metrics");

    assert!(parts.parts_count > 0, "Events table should have parts");
    assert!(parts.total_rows > 0, "Events table should have rows");
    assert!(parts.bytes_on_disk > 0, "Events table should have bytes");

    Ok(())
}

#[tokio::test]
#[ignore = "requires ClickHouse"]
async fn test_merges_collector() -> Result<()> {
    let client = get_client();
    let tables = vec!["events".to_string()];

    // Merges may or may not be running, just verify no errors
    let metrics = MergesCollector::collect(&client, DATABASE, &tables).await?;

    // Can be empty if no merges are in progress, which is fine
    let _ = metrics; // Silence unused warning

    Ok(())
}

#[tokio::test]
#[ignore = "requires ClickHouse"]
async fn test_mutations_collector() -> Result<()> {
    let client = get_client();
    let tables = vec!["events".to_string()];

    let metrics = MutationsCollector::collect(&client, DATABASE, &tables).await?;

    // Can have mutations or not - just checking the query works
    let _ = metrics; // Silence unused warning

    Ok(())
}

#[tokio::test]
#[ignore = "requires ClickHouse"]
async fn test_disk_collector() -> Result<()> {
    let client = get_client();

    let metrics = DiskCollector::collect(&client).await?;

    assert!(!metrics.is_empty(), "Should have disk metrics");

    // Default disk should exist
    let default_disk = metrics
        .iter()
        .find(|d| d.disk_name == "default")
        .expect("Should have default disk");
    assert!(
        default_disk.total_space > 0,
        "Default disk should have total bytes"
    );

    Ok(())
}

#[tokio::test]
#[ignore = "requires ClickHouse"]
async fn test_query_log_collector() -> Result<()> {
    let client = get_client();

    // Query log may or may not have entries depending on ClickHouse config
    let metrics = QueryLogCollector::collect(&client, DATABASE).await?;

    // Just verify query runs without error - metrics is a Vec
    let _ = metrics; // May be empty if no queries recorded yet

    Ok(())
}

#[tokio::test]
#[ignore = "requires ClickHouse"]
async fn test_mv_dag_collector() -> Result<()> {
    let client = get_client();

    let dag = MvDagCollector::collect(&client, DATABASE).await?;

    // We have events_raw -> events_daily_mv
    assert!(dag.total_tables >= 1, "Should have at least one table");
    assert!(dag.total_mvs >= 1, "Should have at least one MV");

    // Check that we have the expected MV
    let has_events_daily = dag.nodes.iter().any(|n| n.name.contains("events_daily"));
    assert!(has_events_daily, "Should have events_daily_mv");

    Ok(())
}

#[tokio::test]
#[ignore = "requires ClickHouse"]
async fn test_full_audit_flow() -> Result<()> {
    let client = get_client();
    let tables = vec!["events".to_string(), "events_raw".to_string()];

    // 1. Initialize report builder
    let targets = Targets {
        endpoint: ENDPOINT.to_string(),
        database: DATABASE.to_string(),
        tables: tables.clone(),
    };
    let mut builder = ReportBuilder::new(targets);

    // 2. Run all collectors
    let parts = PartsCollector::collect(&client, DATABASE, &tables).await?;
    let parts_sql = PartsCollector::sql(DATABASE, &tables);
    builder.with_parts(parts, &parts_sql);

    let merges = MergesCollector::collect(&client, DATABASE, &tables).await?;
    let merges_sql = MergesCollector::sql(DATABASE, &tables);
    builder.with_merges(merges, &merges_sql);

    let mutations = MutationsCollector::collect(&client, DATABASE, &tables).await?;
    let mutations_sql = MutationsCollector::sql(DATABASE, &tables);
    builder.with_mutations(mutations, &mutations_sql);

    let disk = DiskCollector::collect(&client).await?;
    let disk_sql = DiskCollector::sql();
    builder.with_disk(disk, &disk_sql);

    let queries = QueryLogCollector::collect(&client, DATABASE).await?;
    let queries_sql = QueryLogCollector::sql(DATABASE);
    builder.with_queries(queries, &queries_sql);

    let mv_dag = MvDagCollector::collect(&client, DATABASE).await?;
    let mv_dag_sql = MvDagCollector::sql(DATABASE);
    builder.with_mv_dag(mv_dag, &mv_dag_sql);

    // 3. Run rules
    let registry = RuleRegistry::with_default_rules();
    builder.run_rules(&registry);

    // 4. Build report
    let report = builder.build();

    // 5. Verify report structure
    assert!(!report.report_id.is_empty(), "Report should have ID");
    assert!(
        !report.generated_at.is_empty(),
        "Report should have timestamp"
    );
    assert_eq!(report.targets.database, DATABASE);
    assert!(!report.evidence.is_empty(), "Report should have evidence");

    // 6. Verify sections have data
    assert!(
        report.sections.parts.is_some(),
        "Report should have parts section"
    );
    assert!(
        report.sections.disk.is_some(),
        "Report should have disk section"
    );
    assert!(
        report.sections.mv_dag.is_some(),
        "Report should have MV DAG section"
    );

    // 7. Verify JSON serialization works
    let json = serde_json::to_string_pretty(&report)?;
    assert!(!json.is_empty());
    assert!(json.contains("report_id"));
    assert!(json.contains("testdb"));

    Ok(())
}

#[tokio::test]
#[ignore = "requires ClickHouse"]
async fn test_healthy_table_no_critical_findings() -> Result<()> {
    let client = get_client();
    let tables = vec!["events".to_string()];

    let targets = Targets {
        endpoint: ENDPOINT.to_string(),
        database: DATABASE.to_string(),
        tables: tables.clone(),
    };
    let mut builder = ReportBuilder::new(targets);

    // Collect only parts (should be healthy with ~10 parts for test data)
    let parts = PartsCollector::collect(&client, DATABASE, &tables).await?;
    let parts_sql = PartsCollector::sql(DATABASE, &tables);
    builder.with_parts(parts, &parts_sql);

    let registry = RuleRegistry::with_default_rules();
    builder.run_rules(&registry);

    let report = builder.build();

    // Healthy table should not have critical findings for parts explosion
    let has_parts_critical = report.findings.iter().any(|f| {
        f.rule_id == "parts_explosion" && f.severity == pipeaudit::report::Severity::Critical
    });

    assert!(
        !has_parts_critical,
        "Healthy test table should not have critical parts explosion"
    );

    Ok(())
}
