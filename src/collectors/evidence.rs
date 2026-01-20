use crate::report::Evidence;

/// Collector for tracking evidence (source SQL queries and timestamps)
#[derive(Debug, Default)]
pub struct EvidenceCollector {
    evidence: Vec<Evidence>,
}

impl EvidenceCollector {
    /// Create a new evidence collector
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a piece of evidence and return its ID
    pub fn record(&mut self, source: &str, sql: &str) -> String {
        let id = format!("ev-{:03}", self.evidence.len() + 1);
        let evidence = Evidence {
            id: id.clone(),
            source: source.to_string(),
            sql: sql.trim().to_string(),
            collected_at: chrono::Utc::now().to_rfc3339(),
        };
        self.evidence.push(evidence);
        id
    }

    /// Get all collected evidence
    pub fn get_all(&self) -> Vec<Evidence> {
        self.evidence.clone()
    }

    /// Get evidence by ID
    pub fn get(&self, id: &str) -> Option<&Evidence> {
        self.evidence.iter().find(|e| e.id == id)
    }

    /// Get the count of evidence items
    pub fn len(&self) -> usize {
        self.evidence.len()
    }

    /// Check if evidence collector is empty
    pub fn is_empty(&self) -> bool {
        self.evidence.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_evidence_collector_new() {
        let collector = EvidenceCollector::new();
        assert!(collector.is_empty());
        assert_eq!(collector.len(), 0);
    }

    #[test]
    fn test_evidence_record() {
        let mut collector = EvidenceCollector::new();
        let id = collector.record("system.parts", "SELECT * FROM system.parts");

        assert!(!id.is_empty());
        assert_eq!(collector.len(), 1);
        assert!(!collector.is_empty());
    }

    #[test]
    fn test_evidence_record_returns_unique_ids() {
        let mut collector = EvidenceCollector::new();

        let id1 = collector.record("system.parts", "SELECT 1");
        let id2 = collector.record("system.merges", "SELECT 2");
        let id3 = collector.record("system.disks", "SELECT 3");

        assert_ne!(id1, id2);
        assert_ne!(id2, id3);
        assert_ne!(id1, id3);

        assert_eq!(id1, "ev-001");
        assert_eq!(id2, "ev-002");
        assert_eq!(id3, "ev-003");
    }

    #[test]
    fn test_evidence_get_by_id() {
        let mut collector = EvidenceCollector::new();
        let id = collector.record("system.parts", "SELECT * FROM system.parts");

        let evidence = collector.get(&id);
        assert!(evidence.is_some());

        let e = evidence.unwrap();
        assert_eq!(e.source, "system.parts");
        assert!(e.sql.contains("system.parts"));
    }

    #[test]
    fn test_evidence_get_nonexistent() {
        let collector = EvidenceCollector::new();
        assert!(collector.get("nonexistent").is_none());
    }

    #[test]
    fn test_evidence_has_timestamp() {
        let mut collector = EvidenceCollector::new();
        collector.record("test", "SELECT 1");

        let evidence = &collector.get_all()[0];
        assert!(!evidence.collected_at.is_empty());
        // Should be ISO8601 format
        assert!(evidence.collected_at.contains('T'));
    }

    #[test]
    fn test_evidence_get_all() {
        let mut collector = EvidenceCollector::new();
        collector.record("source1", "sql1");
        collector.record("source2", "sql2");

        let all = collector.get_all();
        assert_eq!(all.len(), 2);
        assert_eq!(all[0].source, "source1");
        assert_eq!(all[1].source, "source2");
    }

    #[test]
    fn test_evidence_trims_sql() {
        let mut collector = EvidenceCollector::new();
        collector.record("test", "  SELECT 1  \n  ");

        let evidence = &collector.get_all()[0];
        assert_eq!(evidence.sql, "SELECT 1");
    }
}
