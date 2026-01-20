use crate::ch::ChClient;
use crate::report::MutationMetrics;
use anyhow::Result;
use clickhouse::Row;
use serde::Deserialize;

/// Collector for mutation metrics from system.mutations
pub struct MutationsCollector;

#[derive(Debug, Row, Deserialize)]
struct MutationsRow {
    database: String,
    table: String,
    total_mutations: u64,
    active_mutations: u64,
    latest_mutation_time: Option<String>,
    oldest_active_mutation_age_sec: Option<u64>,
}

impl MutationsCollector {
    /// Build the SQL query for mutations collection
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
                count() AS total_mutations,
                countIf(is_done = 0) AS active_mutations,
                toString(max(create_time)) AS latest_mutation_time,
                maxIf(
                    dateDiff('second', create_time, now()),
                    is_done = 0
                ) AS oldest_active_mutation_age_sec
            FROM system.mutations
            WHERE database = '{database}' AND table IN ({tables_list})
            GROUP BY database, table
            ORDER BY table
            "#,
            database = database,
            tables_list = tables_list
        )
    }

    /// Collect mutation metrics from ClickHouse
    pub async fn collect(
        client: &ChClient,
        database: &str,
        tables: &[String],
    ) -> Result<Vec<MutationMetrics>> {
        let sql = Self::build_query(database, tables);
        let rows: Vec<MutationsRow> = client.fetch_all(&sql).await?;

        let metrics = rows
            .into_iter()
            .map(|row| MutationMetrics {
                database: row.database,
                table: row.table,
                total_mutations: row.total_mutations,
                active_mutations: row.active_mutations,
                latest_mutation_time: row.latest_mutation_time,
                oldest_active_mutation_age_sec: row.oldest_active_mutation_age_sec,
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
    fn test_mutations_query_contains_system_mutations() {
        let sql = MutationsCollector::build_query("testdb", &["events".to_string()]);
        assert!(sql.contains("system.mutations"));
    }

    #[test]
    fn test_mutations_query_counts_active() {
        let sql = MutationsCollector::build_query("db", &["t".to_string()]);
        assert!(sql.contains("countIf(is_done = 0) AS active_mutations"));
    }

    #[test]
    fn test_mutations_query_calculates_age() {
        let sql = MutationsCollector::build_query("db", &["t".to_string()]);
        assert!(sql.contains("dateDiff('second', create_time, now())"));
        assert!(sql.contains("oldest_active_mutation_age_sec"));
    }
}
