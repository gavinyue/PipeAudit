use super::types::*;
use crate::collectors::EvidenceCollector;
use crate::rules::{AuditContext, RuleRegistry};
use std::collections::HashMap;

/// Builder for constructing audit reports
pub struct ReportBuilder {
    targets: Targets,
    evidence: EvidenceCollector,
    parts: HashMap<String, PartsMetrics>,
    merges: HashMap<String, MergeMetrics>,
    mutations: HashMap<String, MutationMetrics>,
    disk: Vec<DiskMetrics>,
    queries: Vec<QueryMetrics>,
    mv_dag: Option<MvDagSection>,
    findings: Vec<Finding>,
    actions: Vec<Action>,
}

impl ReportBuilder {
    /// Create a new report builder
    pub fn new(targets: Targets) -> Self {
        Self {
            targets,
            evidence: EvidenceCollector::new(),
            parts: HashMap::new(),
            merges: HashMap::new(),
            mutations: HashMap::new(),
            disk: Vec::new(),
            queries: Vec::new(),
            mv_dag: None,
            findings: Vec::new(),
            actions: Vec::new(),
        }
    }

    /// Add parts metrics
    pub fn with_parts(&mut self, metrics: Vec<PartsMetrics>, sql: &str) -> &mut Self {
        self.evidence.record("system.parts", sql);
        for m in metrics {
            let key = format!("{}.{}", m.database, m.table);
            self.parts.insert(key, m);
        }
        self
    }

    /// Add merge metrics
    pub fn with_merges(&mut self, metrics: Vec<MergeMetrics>, sql: &str) -> &mut Self {
        self.evidence.record("system.merges", sql);
        for m in metrics {
            let key = format!("{}.{}", m.database, m.table);
            self.merges.insert(key, m);
        }
        self
    }

    /// Add mutation metrics
    pub fn with_mutations(&mut self, metrics: Vec<MutationMetrics>, sql: &str) -> &mut Self {
        self.evidence.record("system.mutations", sql);
        for m in metrics {
            let key = format!("{}.{}", m.database, m.table);
            self.mutations.insert(key, m);
        }
        self
    }

    /// Add disk metrics
    pub fn with_disk(&mut self, metrics: Vec<DiskMetrics>, sql: &str) -> &mut Self {
        self.evidence.record("system.disks", sql);
        self.disk = metrics;
        self
    }

    /// Add query metrics
    pub fn with_queries(&mut self, metrics: Vec<QueryMetrics>, sql: &str) -> &mut Self {
        self.evidence.record("system.query_log", sql);
        self.queries = metrics;
        self
    }

    /// Add MV DAG
    pub fn with_mv_dag(&mut self, dag: MvDagSection, sql: &str) -> &mut Self {
        self.evidence.record("system.tables", sql);
        self.mv_dag = Some(dag);
        self
    }

    /// Run rules and collect findings
    pub fn run_rules(&mut self, registry: &RuleRegistry) -> &mut Self {
        let ctx = self.build_context();
        let results = registry.evaluate_all(&ctx);

        for result in results {
            self.findings.push(result.finding);
            self.actions.extend(result.actions);
        }

        self
    }

    fn build_context(&self) -> AuditContext {
        let mut ctx = AuditContext::new();

        for m in self.parts.values() {
            ctx.add_parts(m.clone());
        }
        for m in self.merges.values() {
            ctx.add_merges(m.clone());
        }
        for m in self.mutations.values() {
            ctx.add_mutations(m.clone());
        }
        ctx.set_disk(self.disk.clone());
        ctx.set_queries(self.queries.clone());

        ctx
    }

    /// Build the final report
    pub fn build(self) -> Report {
        let mut report = Report::new(self.targets);

        // Build sections
        report.sections = Sections {
            parts: if self.parts.is_empty() {
                None
            } else {
                Some(PartsSection {
                    tables: self.parts.into_values().collect(),
                })
            },
            merges: if self.merges.is_empty() {
                None
            } else {
                Some(MergesSection {
                    tables: self.merges.into_values().collect(),
                })
            },
            mutations: if self.mutations.is_empty() {
                None
            } else {
                Some(MutationsSection {
                    tables: self.mutations.into_values().collect(),
                })
            },
            disk: if self.disk.is_empty() {
                None
            } else {
                Some(DiskSection { disks: self.disk })
            },
            query_log: if self.queries.is_empty() {
                None
            } else {
                Some(QueryLogSection {
                    queries: self.queries,
                })
            },
            mv_dag: self.mv_dag,
        };

        // Set findings and actions
        report.findings = self.findings;
        report.actions = self.actions;

        // Calculate summary
        let critical_count = report
            .findings
            .iter()
            .filter(|f| f.severity == Severity::Critical)
            .count();
        let warning_count = report
            .findings
            .iter()
            .filter(|f| f.severity == Severity::Warning)
            .count();

        report.summary = Summary {
            status: if critical_count > 0 {
                ReportStatus::Critical
            } else if warning_count > 0 {
                ReportStatus::Warning
            } else {
                ReportStatus::Healthy
            },
            findings_count: report.findings.len(),
            critical_count,
            warning_count,
        };

        // Set evidence
        report.evidence = self.evidence.get_all();

        report
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn targets() -> Targets {
        Targets {
            endpoint: "http://localhost:8123".to_string(),
            database: "testdb".to_string(),
            tables: vec!["events".to_string()],
        }
    }

    #[test]
    fn test_builder_empty_report() {
        let builder = ReportBuilder::new(targets());
        let report = builder.build();

        assert_eq!(report.summary.status, ReportStatus::Healthy);
        assert_eq!(report.findings.len(), 0);
        assert!(report.sections.parts.is_none());
    }

    #[test]
    fn test_builder_with_parts() {
        let mut builder = ReportBuilder::new(targets());
        builder.with_parts(
            vec![PartsMetrics {
                database: "testdb".to_string(),
                table: "events".to_string(),
                active_parts: 100,
                ..Default::default()
            }],
            "SELECT * FROM system.parts",
        );

        let report = builder.build();
        assert!(report.sections.parts.is_some());
        assert_eq!(report.evidence.len(), 1);
    }

    #[test]
    fn test_builder_with_findings() {
        let mut builder = ReportBuilder::new(targets());
        builder.with_parts(
            vec![PartsMetrics {
                database: "testdb".to_string(),
                table: "events".to_string(),
                active_parts: 1500, // Will trigger critical
                ..Default::default()
            }],
            "sql",
        );

        let registry = RuleRegistry::with_default_rules();
        builder.run_rules(&registry);

        let report = builder.build();
        assert_eq!(report.summary.status, ReportStatus::Critical);
        assert!(!report.findings.is_empty());
    }

    #[test]
    fn test_builder_status_escalation() {
        let mut builder = ReportBuilder::new(targets());

        // Add parts with critical threshold
        builder.with_parts(
            vec![PartsMetrics {
                database: "testdb".to_string(),
                table: "events".to_string(),
                active_parts: 1500,
                ..Default::default()
            }],
            "sql",
        );

        // Add disk with warning threshold
        builder.with_disk(
            vec![DiskMetrics {
                disk_name: "default".to_string(),
                path: "/".to_string(),
                total_space: 100_000_000_000,
                free_space: 15_000_000_000,
                free_percent: 15.0,
            }],
            "sql",
        );

        let registry = RuleRegistry::with_default_rules();
        builder.run_rules(&registry);

        let report = builder.build();
        // Critical takes precedence
        assert_eq!(report.summary.status, ReportStatus::Critical);
    }

    #[test]
    fn test_builder_evidence_tracking() {
        let mut builder = ReportBuilder::new(targets());
        builder
            .with_parts(vec![], "sql1")
            .with_merges(vec![], "sql2")
            .with_disk(vec![], "sql3");

        let report = builder.build();
        assert_eq!(report.evidence.len(), 3);
    }
}
