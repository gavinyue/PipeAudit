use super::{AuditContext, Rule, RuleResult};
use crate::report::{Action, ActionType, Finding, Priority, Severity};

const MERGE_QUEUE_WARNING: u64 = 10;
const MERGE_ELAPSED_WARNING_SEC: f64 = 3600.0; // 1 hour

/// Rule to detect merge backlog
pub struct MergeBacklogRule;

impl Rule for MergeBacklogRule {
    fn id(&self) -> &'static str {
        "merge_backlog"
    }

    fn name(&self) -> &'static str {
        "Merge Backlog"
    }

    fn evaluate(&self, ctx: &AuditContext) -> Vec<RuleResult> {
        let mut results = Vec::new();

        for (table_key, metrics) in &ctx.merges {
            let queue_high = metrics.merges_in_queue > MERGE_QUEUE_WARNING;
            let elapsed_high = metrics.max_merge_elapsed_sec > MERGE_ELAPSED_WARNING_SEC;

            if queue_high || elapsed_high {
                let message = if queue_high && elapsed_high {
                    format!(
                        "Merge queue has {} items and longest merge running for {:.0}s",
                        metrics.merges_in_queue, metrics.max_merge_elapsed_sec
                    )
                } else if queue_high {
                    format!(
                        "Merge queue has {} items (threshold: {})",
                        metrics.merges_in_queue, MERGE_QUEUE_WARNING
                    )
                } else {
                    format!(
                        "Longest merge running for {:.0}s (threshold: {:.0}s)",
                        metrics.max_merge_elapsed_sec, MERGE_ELAPSED_WARNING_SEC
                    )
                };

                results.push(RuleResult {
                    finding: Finding {
                        id: format!("f-merge-{}", results.len() + 1),
                        rule_id: self.id().to_string(),
                        severity: Severity::Warning,
                        target: table_key.clone(),
                        message,
                        evidence_refs: vec![],
                        confidence: 1.0,
                    },
                    actions: vec![Action {
                        id: format!("a-merge-{}", results.len() + 1),
                        finding_ref: format!("f-merge-{}", results.len() + 1),
                        action_type: ActionType::Recommendation,
                        priority: Priority::Medium,
                        description: "Review write rate, consider throttling ingestion".to_string(),
                        sql: None,
                    }],
                });
            }
        }

        results
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::report::MergeMetrics;

    fn ctx_with_merges(queue: u64, elapsed: f64) -> AuditContext {
        let mut ctx = AuditContext::new();
        ctx.add_merges(MergeMetrics {
            database: "testdb".to_string(),
            table: "events".to_string(),
            merges_in_queue: queue,
            max_merge_elapsed_sec: elapsed,
            ..Default::default()
        });
        ctx
    }

    #[test]
    fn test_merge_backlog_healthy() {
        let rule = MergeBacklogRule;
        let ctx = ctx_with_merges(5, 100.0);
        let results = rule.evaluate(&ctx);
        assert!(results.is_empty());
    }

    #[test]
    fn test_merge_backlog_queue_warning() {
        let rule = MergeBacklogRule;
        let ctx = ctx_with_merges(15, 100.0);
        let results = rule.evaluate(&ctx);
        assert_eq!(results.len(), 1);
        assert!(results[0].finding.message.contains("15 items"));
    }

    #[test]
    fn test_merge_backlog_elapsed_warning() {
        let rule = MergeBacklogRule;
        let ctx = ctx_with_merges(5, 7200.0);
        let results = rule.evaluate(&ctx);
        assert_eq!(results.len(), 1);
        assert!(results[0].finding.message.contains("7200s"));
    }

    #[test]
    fn test_merge_backlog_both() {
        let rule = MergeBacklogRule;
        let ctx = ctx_with_merges(15, 7200.0);
        let results = rule.evaluate(&ctx);
        assert_eq!(results.len(), 1);
        assert!(results[0].finding.message.contains("15 items"));
        assert!(results[0].finding.message.contains("7200s"));
    }
}
