use serde::{Deserialize, Serialize};

/// Main audit report structure
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Report {
    pub report_version: String,
    pub report_id: String,
    pub generated_at: String,
    pub targets: Targets,
    pub summary: Summary,
    pub sections: Sections,
    pub findings: Vec<Finding>,
    pub actions: Vec<Action>,
    pub evidence: Vec<Evidence>,
}

impl Report {
    pub fn new(targets: Targets) -> Self {
        Self {
            report_version: "1.0.0".to_string(),
            report_id: uuid::Uuid::new_v4().to_string(),
            generated_at: chrono::Utc::now().to_rfc3339(),
            targets,
            summary: Summary::default(),
            sections: Sections::default(),
            findings: Vec::new(),
            actions: Vec::new(),
            evidence: Vec::new(),
        }
    }
}

/// Audit targets (endpoint, database, tables)
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Targets {
    pub endpoint: String,
    pub database: String,
    pub tables: Vec<String>,
}

/// Report summary with overall status
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct Summary {
    pub status: ReportStatus,
    pub findings_count: usize,
    pub critical_count: usize,
    pub warning_count: usize,
}

/// Overall report status
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ReportStatus {
    #[default]
    Healthy,
    Warning,
    Critical,
}

/// Report sections containing collected metrics
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct Sections {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parts: Option<PartsSection>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub merges: Option<MergesSection>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mutations: Option<MutationsSection>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub disk: Option<DiskSection>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub query_log: Option<QueryLogSection>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mv_dag: Option<MvDagSection>,
}

/// Parts metrics section
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct PartsSection {
    pub tables: Vec<PartsMetrics>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct PartsMetrics {
    pub database: String,
    pub table: String,
    pub parts_count: u64,
    pub active_parts: u64,
    pub total_rows: u64,
    pub bytes_on_disk: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub oldest_part: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub newest_part: Option<String>,
}

/// Merges metrics section
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct MergesSection {
    pub tables: Vec<MergeMetrics>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct MergeMetrics {
    pub database: String,
    pub table: String,
    pub merges_in_queue: u64,
    pub merge_rows_read: u64,
    pub merge_bytes_read: u64,
    pub max_merge_elapsed_sec: f64,
}

/// Mutations metrics section
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct MutationsSection {
    pub tables: Vec<MutationMetrics>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct MutationMetrics {
    pub database: String,
    pub table: String,
    pub total_mutations: u64,
    pub active_mutations: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub latest_mutation_time: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub oldest_active_mutation_age_sec: Option<u64>,
}

/// Disk metrics section
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct DiskSection {
    pub disks: Vec<DiskMetrics>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct DiskMetrics {
    pub disk_name: String,
    pub path: String,
    pub total_space: u64,
    pub free_space: u64,
    pub free_percent: f64,
}

/// Query log metrics section
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct QueryLogSection {
    pub queries: Vec<QueryMetrics>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct QueryMetrics {
    pub query_fingerprint: String,
    pub execution_count: u64,
    pub avg_duration_ms: f64,
    pub total_read_rows: u64,
    pub total_read_bytes: u64,
    pub total_result_rows: u64,
    pub read_amplification: f64,
    pub avg_memory_bytes: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sample_query: Option<String>,
}

/// MV DAG section
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct MvDagSection {
    pub nodes: Vec<MvDagNode>,
    pub edges: Vec<MvDagEdge>,
    pub max_depth: usize,
    pub total_tables: usize,
    pub total_mvs: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MvDagNode {
    pub name: String,
    pub database: String,
    pub table_type: TableType,
    pub engine: String,
    pub depth: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TableType {
    Table,
    MaterializedView,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MvDagEdge {
    pub from: String,
    pub to: String,
}

/// Finding from rule evaluation
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Finding {
    pub id: String,
    pub rule_id: String,
    pub severity: Severity,
    pub target: String,
    pub message: String,
    pub evidence_refs: Vec<String>,
    pub confidence: f64,
}

/// Finding severity level
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Severity {
    Warning,
    Critical,
}

/// Recommended action
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Action {
    pub id: String,
    pub finding_ref: String,
    pub action_type: ActionType,
    pub priority: Priority,
    pub description: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sql: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ActionType {
    Recommendation,
    DdlProposal,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Priority {
    High,
    Medium,
    Low,
}

/// Evidence linking findings to source data
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Evidence {
    pub id: String,
    pub source: String,
    pub sql: String,
    pub collected_at: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;

    fn sample_report() -> Report {
        let targets = Targets {
            endpoint: "http://localhost:8123".to_string(),
            database: "testdb".to_string(),
            tables: vec!["events".to_string()],
        };
        let mut report = Report::new(targets);
        report.report_id = "test-id-123".to_string();
        report.generated_at = "2024-01-15T10:30:00Z".to_string();
        report
    }

    #[test]
    fn test_report_json_roundtrip() {
        let report = sample_report();

        let json = serde_json::to_string_pretty(&report).unwrap();
        let parsed: Report = serde_json::from_str(&json).unwrap();

        assert_eq!(report.report_id, parsed.report_id);
        assert_eq!(report.targets, parsed.targets);
        assert_eq!(report.summary.status, parsed.summary.status);
    }

    #[test]
    fn test_report_status_serialization() {
        assert_eq!(
            serde_json::to_string(&ReportStatus::Healthy).unwrap(),
            "\"healthy\""
        );
        assert_eq!(
            serde_json::to_string(&ReportStatus::Warning).unwrap(),
            "\"warning\""
        );
        assert_eq!(
            serde_json::to_string(&ReportStatus::Critical).unwrap(),
            "\"critical\""
        );
    }

    #[test]
    fn test_severity_serialization() {
        assert_eq!(
            serde_json::to_string(&Severity::Warning).unwrap(),
            "\"warning\""
        );
        assert_eq!(
            serde_json::to_string(&Severity::Critical).unwrap(),
            "\"critical\""
        );
    }

    #[test]
    fn test_report_default_values() {
        let report = sample_report();

        assert_eq!(report.report_version, "1.0.0");
        assert_eq!(report.summary.status, ReportStatus::Healthy);
        assert_eq!(report.summary.findings_count, 0);
        assert!(report.findings.is_empty());
        assert!(report.actions.is_empty());
    }

    #[test]
    fn test_parts_metrics_serialization() {
        let metrics = PartsMetrics {
            database: "testdb".to_string(),
            table: "events".to_string(),
            parts_count: 100,
            active_parts: 50,
            total_rows: 1_000_000,
            bytes_on_disk: 500_000_000,
            oldest_part: Some("2024-01-01T00:00:00Z".to_string()),
            newest_part: Some("2024-01-15T00:00:00Z".to_string()),
        };

        let json = serde_json::to_string(&metrics).unwrap();
        let parsed: PartsMetrics = serde_json::from_str(&json).unwrap();

        assert_eq!(metrics, parsed);
    }

    #[test]
    fn test_finding_serialization() {
        let finding = Finding {
            id: "f-001".to_string(),
            rule_id: "parts_explosion".to_string(),
            severity: Severity::Warning,
            target: "testdb.events".to_string(),
            message: "Table has 500 active parts".to_string(),
            evidence_refs: vec!["ev-001".to_string()],
            confidence: 0.95,
        };

        let json = serde_json::to_string(&finding).unwrap();
        assert!(json.contains("\"severity\":\"warning\""));
        assert!(json.contains("\"rule_id\":\"parts_explosion\""));

        let parsed: Finding = serde_json::from_str(&json).unwrap();
        assert_eq!(finding, parsed);
    }

    #[test]
    fn test_action_serialization() {
        let action = Action {
            id: "a-001".to_string(),
            finding_ref: "f-001".to_string(),
            action_type: ActionType::Recommendation,
            priority: Priority::High,
            description: "Run OPTIMIZE TABLE".to_string(),
            sql: Some("OPTIMIZE TABLE testdb.events FINAL".to_string()),
        };

        let json = serde_json::to_string(&action).unwrap();
        let parsed: Action = serde_json::from_str(&json).unwrap();

        assert_eq!(action, parsed);
    }

    #[test]
    fn test_skip_serializing_none() {
        let metrics = PartsMetrics {
            database: "testdb".to_string(),
            table: "events".to_string(),
            parts_count: 100,
            active_parts: 50,
            total_rows: 1_000_000,
            bytes_on_disk: 500_000_000,
            oldest_part: None,
            newest_part: None,
        };

        let json = serde_json::to_string(&metrics).unwrap();
        assert!(!json.contains("oldest_part"));
        assert!(!json.contains("newest_part"));
    }

    #[test]
    fn test_table_type_serialization() {
        assert_eq!(
            serde_json::to_string(&TableType::Table).unwrap(),
            "\"table\""
        );
        assert_eq!(
            serde_json::to_string(&TableType::MaterializedView).unwrap(),
            "\"materialized_view\""
        );
    }
}
