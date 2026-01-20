use crate::ch::ChClient;
use crate::report::{MvDagEdge, MvDagNode, MvDagSection, TableType};
use anyhow::Result;
use clickhouse::Row;
use serde::Deserialize;
use std::collections::{HashMap, HashSet};

/// Collector for MV dependency DAG
pub struct MvDagCollector;

#[derive(Debug, Row, Deserialize)]
struct TableRow {
    database: String,
    name: String,
    engine: String,
}

#[derive(Debug, Row, Deserialize)]
struct DependencyRow {
    database: String,
    name: String,
    dep_database: String,
    dep_table: String,
}

impl MvDagCollector {
    /// Build the SQL query for tables
    pub fn build_tables_query(database: &str) -> String {
        format!(
            r#"
            SELECT
                database,
                name,
                engine
            FROM system.tables
            WHERE database = '{}'
            ORDER BY name
            "#,
            database
        )
    }

    /// Build the SQL query for dependencies
    pub fn build_dependencies_query(database: &str) -> String {
        format!(
            r#"
            SELECT
                database,
                name,
                dep_database,
                dep_table
            FROM (
                SELECT
                    database,
                    name,
                    arrayJoin(dependencies_database) AS dep_database,
                    arrayJoin(dependencies_table) AS dep_table
                FROM system.tables
                WHERE database = '{}' AND notEmpty(dependencies_table)
            )
            "#,
            database
        )
    }

    /// Collect MV DAG from ClickHouse
    pub async fn collect(client: &ChClient, database: &str) -> Result<MvDagSection> {
        let tables_sql = Self::build_tables_query(database);
        let tables: Vec<TableRow> = client.fetch_all(&tables_sql).await?;

        let deps_sql = Self::build_dependencies_query(database);
        let deps: Vec<DependencyRow> = client.fetch_all(&deps_sql).await?;

        Ok(Self::build_dag(database, tables, deps))
    }

    /// Build DAG from raw data
    pub fn build_dag(
        database: &str,
        tables: Vec<TableRow>,
        dependencies: Vec<DependencyRow>,
    ) -> MvDagSection {
        // Build dependency map: table -> [tables it depends on]
        let mut depends_on: HashMap<String, Vec<String>> = HashMap::new();
        let mut depended_by: HashMap<String, Vec<String>> = HashMap::new();

        for dep in &dependencies {
            let key = format!("{}.{}", dep.database, dep.name);
            let dep_key = format!("{}.{}", dep.dep_database, dep.dep_table);

            depends_on.entry(key.clone()).or_default().push(dep_key.clone());
            depended_by.entry(dep_key).or_default().push(key);
        }

        // Calculate depths using BFS
        let depths = Self::calculate_depths(&tables, &depends_on, database);

        // Build nodes
        let mut mv_count = 0;
        let nodes: Vec<MvDagNode> = tables
            .iter()
            .map(|t| {
                let key = format!("{}.{}", t.database, t.name);
                let table_type = if t.engine.contains("MaterializedView") {
                    mv_count += 1;
                    TableType::MaterializedView
                } else {
                    TableType::Table
                };

                MvDagNode {
                    name: t.name.clone(),
                    database: t.database.clone(),
                    table_type,
                    engine: t.engine.clone(),
                    depth: *depths.get(&key).unwrap_or(&0),
                }
            })
            .collect();

        // Build edges
        let edges: Vec<MvDagEdge> = dependencies
            .iter()
            .map(|d| MvDagEdge {
                from: format!("{}.{}", d.dep_database, d.dep_table),
                to: format!("{}.{}", d.database, d.name),
            })
            .collect();

        let max_depth = depths.values().copied().max().unwrap_or(0);

        MvDagSection {
            nodes,
            edges,
            max_depth,
            total_tables: tables.len() - mv_count,
            total_mvs: mv_count,
        }
    }

    fn calculate_depths(
        tables: &[TableRow],
        depends_on: &HashMap<String, Vec<String>>,
        database: &str,
    ) -> HashMap<String, usize> {
        let mut depths: HashMap<String, usize> = HashMap::new();

        // Find root tables (no dependencies)
        let table_keys: HashSet<String> = tables
            .iter()
            .map(|t| format!("{}.{}", t.database, t.name))
            .collect();

        for key in &table_keys {
            if !depends_on.contains_key(key) {
                depths.insert(key.clone(), 0);
            }
        }

        // BFS to calculate depths
        let mut changed = true;
        while changed {
            changed = false;
            for (table, deps) in depends_on {
                if !table.starts_with(&format!("{}.", database)) {
                    continue;
                }

                let max_dep_depth = deps
                    .iter()
                    .filter_map(|d| depths.get(d))
                    .max()
                    .copied();

                if let Some(max_d) = max_dep_depth {
                    let new_depth = max_d + 1;
                    let current = depths.get(table).copied();
                    if current.is_none() || current.unwrap() < new_depth {
                        depths.insert(table.clone(), new_depth);
                        changed = true;
                    }
                }
            }
        }

        depths
    }

    /// Get SQL for evidence
    pub fn sql(database: &str) -> String {
        format!(
            "Tables: {} | Dependencies: {}",
            Self::build_tables_query(database),
            Self::build_dependencies_query(database)
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn table(name: &str, engine: &str) -> TableRow {
        TableRow {
            database: "testdb".to_string(),
            name: name.to_string(),
            engine: engine.to_string(),
        }
    }

    fn dep(name: &str, depends_on: &str) -> DependencyRow {
        DependencyRow {
            database: "testdb".to_string(),
            name: name.to_string(),
            dep_database: "testdb".to_string(),
            dep_table: depends_on.to_string(),
        }
    }

    #[test]
    fn test_dag_single_table() {
        let tables = vec![table("events", "MergeTree")];
        let dag = MvDagCollector::build_dag("testdb", tables, vec![]);

        assert_eq!(dag.nodes.len(), 1);
        assert_eq!(dag.max_depth, 0);
        assert_eq!(dag.total_tables, 1);
        assert_eq!(dag.total_mvs, 0);
    }

    #[test]
    fn test_dag_with_mv() {
        let tables = vec![
            table("events", "MergeTree"),
            table("events_daily", "MaterializedView"),
        ];
        let deps = vec![dep("events_daily", "events")];

        let dag = MvDagCollector::build_dag("testdb", tables, deps);

        assert_eq!(dag.nodes.len(), 2);
        assert_eq!(dag.total_tables, 1);
        assert_eq!(dag.total_mvs, 1);
        assert_eq!(dag.max_depth, 1);
        assert_eq!(dag.edges.len(), 1);
    }

    #[test]
    fn test_dag_chain() {
        // events -> events_daily -> events_weekly
        let tables = vec![
            table("events", "MergeTree"),
            table("events_daily", "MaterializedView"),
            table("events_weekly", "MaterializedView"),
        ];
        let deps = vec![
            dep("events_daily", "events"),
            dep("events_weekly", "events_daily"),
        ];

        let dag = MvDagCollector::build_dag("testdb", tables, deps);

        assert_eq!(dag.max_depth, 2);
        assert_eq!(dag.edges.len(), 2);
    }

    #[test]
    fn test_dag_node_types() {
        let tables = vec![
            table("events", "MergeTree"),
            table("events_mv", "MaterializedView"),
        ];

        let dag = MvDagCollector::build_dag("testdb", tables, vec![]);

        let events = dag.nodes.iter().find(|n| n.name == "events").unwrap();
        let mv = dag.nodes.iter().find(|n| n.name == "events_mv").unwrap();

        assert_eq!(events.table_type, TableType::Table);
        assert_eq!(mv.table_type, TableType::MaterializedView);
    }

    #[test]
    fn test_tables_query() {
        let sql = MvDagCollector::build_tables_query("testdb");
        assert!(sql.contains("system.tables"));
        assert!(sql.contains("database = 'testdb'"));
    }

    #[test]
    fn test_dependencies_query() {
        let sql = MvDagCollector::build_dependencies_query("testdb");
        assert!(sql.contains("dependencies_table"));
        assert!(sql.contains("database = 'testdb'"));
    }
}
