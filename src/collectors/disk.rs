use crate::ch::ChClient;
use crate::report::DiskMetrics;
use anyhow::Result;
use clickhouse::Row;
use serde::Deserialize;

/// Collector for disk metrics from system.disks
pub struct DiskCollector;

#[derive(Debug, Row, Deserialize)]
struct DiskRow {
    disk_name: String,
    path: String,
    total_space: u64,
    free_space: u64,
    free_percent: f64,
}

impl DiskCollector {
    /// Build the SQL query for disk collection
    pub fn build_query() -> String {
        r#"
        SELECT
            name AS disk_name,
            path,
            total_space,
            free_space,
            round(100.0 * free_space / total_space, 2) AS free_percent
        FROM system.disks
        ORDER BY disk_name
        "#
        .to_string()
    }

    /// Collect disk metrics from ClickHouse
    pub async fn collect(client: &ChClient) -> Result<Vec<DiskMetrics>> {
        let sql = Self::build_query();
        let rows: Vec<DiskRow> = client.fetch_all(&sql).await?;

        let metrics = rows
            .into_iter()
            .map(|row| DiskMetrics {
                disk_name: row.disk_name,
                path: row.path,
                total_space: row.total_space,
                free_space: row.free_space,
                free_percent: row.free_percent,
            })
            .collect();

        Ok(metrics)
    }

    /// Get the SQL query string for evidence tracking
    pub fn sql() -> String {
        Self::build_query()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_disk_query_contains_system_disks() {
        let sql = DiskCollector::build_query();
        assert!(sql.contains("system.disks"));
    }

    #[test]
    fn test_disk_query_calculates_free_percent() {
        let sql = DiskCollector::build_query();
        assert!(sql.contains("free_space / total_space"));
        assert!(sql.contains("free_percent"));
    }

    #[test]
    fn test_disk_query_selects_required_fields() {
        let sql = DiskCollector::build_query();
        assert!(sql.contains("name AS disk_name"));
        assert!(sql.contains("path"));
        assert!(sql.contains("total_space"));
        assert!(sql.contains("free_space"));
    }
}
