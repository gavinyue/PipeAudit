use crate::ch::ChClient;
use crate::report::PartsMetrics;
use anyhow::Result;
use clickhouse::Row;
use serde::Deserialize;

/// Collector for parts metrics from system.parts
pub struct PartsCollector;

#[derive(Debug, Row, Deserialize)]
struct PartsRow {
    database: String,
    table: String,
    parts_count: u64,
    active_parts: u64,
    total_rows: u64,
    bytes_on_disk: u64,
    oldest_part: Option<String>,
    newest_part: Option<String>,
}

impl PartsCollector {
    /// Build the SQL query for parts collection
    pub fn build_query(database: &str, tables: &[String]) -> String {
        let tables_list = tables
            .iter()
            .map(|t| format!("'{}'", t))
            .collect::<Vec<_>>()
            .join(", ");

        format!(
            r#"
            SELECT
                database,
                table,
                count() AS parts_count,
                countIf(active) AS active_parts,
                sum(rows) AS total_rows,
                sum(bytes_on_disk) AS bytes_on_disk,
                toString(min(modification_time)) AS oldest_part,
                toString(max(modification_time)) AS newest_part
            FROM system.parts
            WHERE database = '{database}' AND table IN ({tables_list})
            GROUP BY database, table
            ORDER BY table
            "#,
            database = database,
            tables_list = tables_list
        )
    }

    /// Collect parts metrics from ClickHouse
    pub async fn collect(
        client: &ChClient,
        database: &str,
        tables: &[String],
    ) -> Result<Vec<PartsMetrics>> {
        let sql = Self::build_query(database, tables);
        let rows: Vec<PartsRow> = client.fetch_all(&sql).await?;

        let metrics = rows
            .into_iter()
            .map(|row| PartsMetrics {
                database: row.database,
                table: row.table,
                parts_count: row.parts_count,
                active_parts: row.active_parts,
                total_rows: row.total_rows,
                bytes_on_disk: row.bytes_on_disk,
                oldest_part: row.oldest_part,
                newest_part: row.newest_part,
            })
            .collect();

        Ok(metrics)
    }

    /// Get the SQL query string for evidence tracking
    pub fn sql(database: &str, tables: &[String]) -> String {
        Self::build_query(database, tables)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parts_query_single_table() {
        let sql = PartsCollector::build_query("testdb", &["events".to_string()]);

        assert!(sql.contains("system.parts"));
        assert!(sql.contains("database = 'testdb'"));
        assert!(sql.contains("'events'"));
        assert!(sql.contains("count() AS parts_count"));
        assert!(sql.contains("countIf(active) AS active_parts"));
    }

    #[test]
    fn test_parts_query_multiple_tables() {
        let sql = PartsCollector::build_query(
            "testdb",
            &[
                "events".to_string(),
                "users".to_string(),
                "orders".to_string(),
            ],
        );

        assert!(sql.contains("'events'"));
        assert!(sql.contains("'users'"));
        assert!(sql.contains("'orders'"));
        assert!(sql.contains("table IN ("));
    }

    #[test]
    fn test_parts_query_has_aggregations() {
        let sql = PartsCollector::build_query("db", &["t".to_string()]);

        assert!(sql.contains("sum(rows) AS total_rows"));
        assert!(sql.contains("sum(bytes_on_disk) AS bytes_on_disk"));
        assert!(sql.contains("min(modification_time)"));
        assert!(sql.contains("max(modification_time)"));
    }

    #[test]
    fn test_parts_query_group_by() {
        let sql = PartsCollector::build_query("db", &["t".to_string()]);

        assert!(sql.contains("GROUP BY database, table"));
    }

    #[test]
    fn test_sql_evidence_matches_build_query() {
        let database = "testdb";
        let tables = vec!["events".to_string()];

        let sql1 = PartsCollector::build_query(database, &tables);
        let sql2 = PartsCollector::sql(database, &tables);

        assert_eq!(sql1, sql2);
    }
}
