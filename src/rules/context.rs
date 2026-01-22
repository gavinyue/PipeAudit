use crate::report::{DiskMetrics, MergeMetrics, MutationMetrics, PartsMetrics, QueryMetrics};
use std::collections::HashMap;

/// Context passed to rules for evaluation
#[derive(Debug, Default)]
pub struct AuditContext {
    /// Parts metrics per table (key: "database.table")
    pub parts: HashMap<String, PartsMetrics>,
    /// Merge metrics per table
    pub merges: HashMap<String, MergeMetrics>,
    /// Mutation metrics per table
    pub mutations: HashMap<String, MutationMetrics>,
    /// Disk metrics (global)
    pub disk: Vec<DiskMetrics>,
    /// Query metrics
    pub queries: Vec<QueryMetrics>,
}

impl AuditContext {
    /// Create a new empty audit context
    pub fn new() -> Self {
        Self::default()
    }

    /// Add parts metrics for a table
    pub fn add_parts(&mut self, metrics: PartsMetrics) {
        let key = format!("{}.{}", metrics.database, metrics.table);
        self.parts.insert(key, metrics);
    }

    /// Add merge metrics for a table
    pub fn add_merges(&mut self, metrics: MergeMetrics) {
        let key = format!("{}.{}", metrics.database, metrics.table);
        self.merges.insert(key, metrics);
    }

    /// Add mutation metrics for a table
    pub fn add_mutations(&mut self, metrics: MutationMetrics) {
        let key = format!("{}.{}", metrics.database, metrics.table);
        self.mutations.insert(key, metrics);
    }

    /// Set disk metrics
    pub fn set_disk(&mut self, metrics: Vec<DiskMetrics>) {
        self.disk = metrics;
    }

    /// Set query metrics
    pub fn set_queries(&mut self, metrics: Vec<QueryMetrics>) {
        self.queries = metrics;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_context_new() {
        let ctx = AuditContext::new();
        assert!(ctx.parts.is_empty());
        assert!(ctx.merges.is_empty());
        assert!(ctx.disk.is_empty());
    }

    #[test]
    fn test_context_add_parts() {
        let mut ctx = AuditContext::new();
        ctx.add_parts(PartsMetrics {
            database: "testdb".to_string(),
            table: "events".to_string(),
            active_parts: 100,
            ..Default::default()
        });

        assert!(ctx.parts.contains_key("testdb.events"));
        assert_eq!(ctx.parts["testdb.events"].active_parts, 100);
    }

    #[test]
    fn test_context_add_multiple_tables() {
        let mut ctx = AuditContext::new();
        ctx.add_parts(PartsMetrics {
            database: "db".to_string(),
            table: "t1".to_string(),
            ..Default::default()
        });
        ctx.add_parts(PartsMetrics {
            database: "db".to_string(),
            table: "t2".to_string(),
            ..Default::default()
        });

        assert_eq!(ctx.parts.len(), 2);
    }
}
