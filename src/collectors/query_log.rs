use crate::ch::ChClient;
use crate::report::QueryMetrics;
use anyhow::Result;
use clickhouse::Row;
use serde::Deserialize;

/// Collector for query metrics from system.query_log
pub struct QueryLogCollector;

#[derive(Debug, Row, Deserialize)]
struct QueryLogRow {
    query_fingerprint: String,
    execution_count: u64,
    avg_duration_ms: f64,
    total_read_rows: u64,
    total_read_bytes: u64,
    total_result_rows: u64,
    read_amplification: f64,
    avg_memory_bytes: u64,
    sample_query: Option<String>,
}

impl QueryLogCollector {
    /// Build the SQL query for query_log collection
    pub fn build_query(database: &str, limit: usize) -> String {
        format!(
            r#"
            SELECT
                normalizeQuery(query) AS query_fingerprint,
                count() AS execution_count,
                avg(query_duration_ms) AS avg_duration_ms,
                sum(read_rows) AS total_read_rows,
                sum(read_bytes) AS total_read_bytes,
                sum(result_rows) AS total_result_rows,
                round(sum(read_rows) / greatest(sum(result_rows), 1), 2) AS read_amplification,
                toUInt64(avg(memory_usage)) AS avg_memory_bytes,
                any(query) AS sample_query
            FROM system.query_log
            WHERE
                type = 'QueryFinish'
                AND query_kind = 'Select'
                AND event_date >= today() - 7
                AND has(databases, '{database}')
            GROUP BY query_fingerprint
            ORDER BY total_read_rows DESC
            LIMIT {limit}
            "#,
            database = database,
            limit = limit
        )
    }

    /// Collect query metrics from ClickHouse
    pub async fn collect(client: &ChClient, database: &str) -> Result<Vec<QueryMetrics>> {
        Self::collect_with_limit(client, database, 20).await
    }

    /// Collect query metrics with custom limit
    pub async fn collect_with_limit(
        client: &ChClient,
        database: &str,
        limit: usize,
    ) -> Result<Vec<QueryMetrics>> {
        let sql = Self::build_query(database, limit);
        let rows: Vec<QueryLogRow> = client.fetch_all(&sql).await?;

        let metrics = rows
            .into_iter()
            .map(|row| QueryMetrics {
                query_fingerprint: row.query_fingerprint,
                execution_count: row.execution_count,
                avg_duration_ms: row.avg_duration_ms,
                total_read_rows: row.total_read_rows,
                total_read_bytes: row.total_read_bytes,
                total_result_rows: row.total_result_rows,
                read_amplification: row.read_amplification,
                avg_memory_bytes: row.avg_memory_bytes,
                sample_query: row.sample_query,
            })
            .collect();

        Ok(metrics)
    }

    /// Get the SQL query string for evidence tracking
    pub fn sql(database: &str) -> String {
        Self::build_query(database, 20)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_querylog_query_contains_system_query_log() {
        let sql = QueryLogCollector::build_query("testdb", 20);
        assert!(sql.contains("system.query_log"));
    }

    #[test]
    fn test_querylog_query_normalizes_query() {
        let sql = QueryLogCollector::build_query("testdb", 20);
        assert!(sql.contains("normalizeQuery(query)"));
    }

    #[test]
    fn test_querylog_query_calculates_read_amplification() {
        let sql = QueryLogCollector::build_query("testdb", 20);
        assert!(sql.contains("sum(read_rows) / greatest(sum(result_rows), 1)"));
        assert!(sql.contains("read_amplification"));
    }

    #[test]
    fn test_querylog_query_filters_by_database() {
        let sql = QueryLogCollector::build_query("mydb", 20);
        assert!(sql.contains("has(databases, 'mydb')"));
    }

    #[test]
    fn test_querylog_query_filters_select_queries() {
        let sql = QueryLogCollector::build_query("testdb", 20);
        assert!(sql.contains("query_kind = 'Select'"));
        assert!(sql.contains("type = 'QueryFinish'"));
    }

    #[test]
    fn test_querylog_query_uses_limit() {
        let sql = QueryLogCollector::build_query("testdb", 50);
        assert!(sql.contains("LIMIT 50"));
    }

    #[test]
    fn test_querylog_query_orders_by_read_rows() {
        let sql = QueryLogCollector::build_query("testdb", 20);
        assert!(sql.contains("ORDER BY total_read_rows DESC"));
    }
}
