use crate::report::{Action, ActionType, Finding, Priority, Severity};
use super::{AuditContext, Rule, RuleResult};

const READ_AMP_WARNING: f64 = 100.0;
const READ_AMP_CRITICAL: f64 = 1000.0;

/// Rule to detect high read amplification in queries
pub struct QueryAmplificationRule;

impl Rule for QueryAmplificationRule {
    fn id(&self) -> &'static str {
        "query_amplification"
    }

    fn name(&self) -> &'static str {
        "Query Read Amplification"
    }

    fn evaluate(&self, ctx: &AuditContext) -> Vec<RuleResult> {
        let mut results = Vec::new();

        for query in &ctx.queries {
            let amp = query.read_amplification;

            if amp > READ_AMP_CRITICAL {
                results.push(RuleResult {
                    finding: Finding {
                        id: format!("f-query-{}", results.len() + 1),
                        rule_id: self.id().to_string(),
                        severity: Severity::Critical,
                        target: truncate_fingerprint(&query.query_fingerprint),
                        message: format!(
                            "Query has {:.0}x read amplification (critical threshold: {:.0}x)",
                            amp, READ_AMP_CRITICAL
                        ),
                        evidence_refs: vec![],
                        confidence: 1.0,
                    },
                    actions: vec![Action {
                        id: format!("a-query-{}", results.len() + 1),
                        finding_ref: format!("f-query-{}", results.len() + 1),
                        action_type: ActionType::Recommendation,
                        priority: Priority::High,
                        description: "Review query, add PREWHERE or adjust ORDER BY".to_string(),
                        sql: None,
                    }],
                });
            } else if amp > READ_AMP_WARNING {
                results.push(RuleResult {
                    finding: Finding {
                        id: format!("f-query-{}", results.len() + 1),
                        rule_id: self.id().to_string(),
                        severity: Severity::Warning,
                        target: truncate_fingerprint(&query.query_fingerprint),
                        message: format!(
                            "Query has {:.0}x read amplification (warning threshold: {:.0}x)",
                            amp, READ_AMP_WARNING
                        ),
                        evidence_refs: vec![],
                        confidence: 1.0,
                    },
                    actions: vec![Action {
                        id: format!("a-query-{}", results.len() + 1),
                        finding_ref: format!("f-query-{}", results.len() + 1),
                        action_type: ActionType::Recommendation,
                        priority: Priority::Medium,
                        description: "Consider optimizing query pattern".to_string(),
                        sql: None,
                    }],
                });
            }
        }

        results
    }
}

fn truncate_fingerprint(fp: &str) -> String {
    if fp.len() > 80 {
        format!("{}...", &fp[..77])
    } else {
        fp.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::report::QueryMetrics;

    fn ctx_with_query_amp(amp: f64) -> AuditContext {
        let mut ctx = AuditContext::new();
        ctx.set_queries(vec![QueryMetrics {
            query_fingerprint: "SELECT * FROM events WHERE user_id = ?".to_string(),
            read_amplification: amp,
            ..Default::default()
        }]);
        ctx
    }

    #[test]
    fn test_query_amp_healthy() {
        let rule = QueryAmplificationRule;
        let ctx = ctx_with_query_amp(50.0);
        let results = rule.evaluate(&ctx);
        assert!(results.is_empty());
    }

    #[test]
    fn test_query_amp_warning() {
        let rule = QueryAmplificationRule;
        let ctx = ctx_with_query_amp(500.0);
        let results = rule.evaluate(&ctx);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].finding.severity, Severity::Warning);
    }

    #[test]
    fn test_query_amp_critical() {
        let rule = QueryAmplificationRule;
        let ctx = ctx_with_query_amp(2000.0);
        let results = rule.evaluate(&ctx);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].finding.severity, Severity::Critical);
    }

    #[test]
    fn test_query_amp_multiple() {
        let rule = QueryAmplificationRule;
        let mut ctx = AuditContext::new();
        ctx.set_queries(vec![
            QueryMetrics {
                query_fingerprint: "q1".to_string(),
                read_amplification: 50.0,
                ..Default::default()
            },
            QueryMetrics {
                query_fingerprint: "q2".to_string(),
                read_amplification: 500.0,
                ..Default::default()
            },
            QueryMetrics {
                query_fingerprint: "q3".to_string(),
                read_amplification: 2000.0,
                ..Default::default()
            },
        ]);
        let results = rule.evaluate(&ctx);
        assert_eq!(results.len(), 2); // warning + critical, not healthy
    }
}
