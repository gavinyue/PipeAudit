use crate::ch::ChClient;
use crate::report::MergeMetrics;
use anyhow::Result;
use clickhouse::Row;
use serde::Deserialize;

/// Collector for merge metrics from system.merges
pub struct MergesCollector;

#[derive(Debug, Row, Deserialize)]
struct MergesRow {
    database: String,
    table: String,
    merges_in_queue: u64,
    merge_rows_read: u64,
    merge_bytes_read: u64,
    max_merge_elapsed_sec: f64,
}

impl MergesCollector {
    /// Build the SQL query for merges collection
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
                count() AS merges_in_queue,
                sum(rows_read) AS merge_rows_read,
                sum(bytes_read_uncompressed) AS merge_bytes_read,
                max(elapsed) AS max_merge_elapsed_sec
            FROM system.merges
            WHERE database = '{database}' AND table IN ({tables_list})
            GROUP BY database, table
            ORDER BY table
            "#,
            database = database,
            tables_list = tables_list
        )
    }

    /// Collect merge metrics from ClickHouse
    pub async fn collect(
        client: &ChClient,
        database: &str,
        tables: &[String],
    ) -> Result<Vec<MergeMetrics>> {
        let sql = Self::build_query(database, tables);
        let rows: Vec<MergesRow> = client.fetch_all(&sql).await?;

        let metrics = rows
            .into_iter()
            .map(|row| MergeMetrics {
                database: row.database,
                table: row.table,
                merges_in_queue: row.merges_in_queue,
                merge_rows_read: row.merge_rows_read,
                merge_bytes_read: row.merge_bytes_read,
                max_merge_elapsed_sec: row.max_merge_elapsed_sec,
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
    fn test_merges_query_contains_system_merges() {
        let sql = MergesCollector::build_query("testdb", &["events".to_string()]);
        assert!(sql.contains("system.merges"));
    }

    #[test]
    fn test_merges_query_has_aggregations() {
        let sql = MergesCollector::build_query("db", &["t".to_string()]);
        assert!(sql.contains("count() AS merges_in_queue"));
        assert!(sql.contains("sum(rows_read)"));
        assert!(sql.contains("max(elapsed)"));
    }

    #[test]
    fn test_merges_query_filters_database() {
        let sql = MergesCollector::build_query("mydb", &["t".to_string()]);
        assert!(sql.contains("database = 'mydb'"));
    }
}
