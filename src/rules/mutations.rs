use super::{AuditContext, Rule, RuleResult};
use crate::report::{Action, ActionType, Finding, Priority, Severity};

const MUTATION_STUCK_SEC: u64 = 3600; // 1 hour

/// Rule to detect stuck mutations
pub struct StuckMutationRule;

impl Rule for StuckMutationRule {
    fn id(&self) -> &'static str {
        "stuck_mutation"
    }

    fn name(&self) -> &'static str {
        "Stuck Mutation"
    }

    fn evaluate(&self, ctx: &AuditContext) -> Vec<RuleResult> {
        let mut results = Vec::new();

        for (table_key, metrics) in &ctx.mutations {
            if metrics.active_mutations > 0 {
                if let Some(age) = metrics.oldest_active_mutation_age_sec {
                    if age > MUTATION_STUCK_SEC {
                        let hours = age as f64 / 3600.0;
                        results.push(RuleResult {
                            finding: Finding {
                                id: format!("f-mutation-{}", results.len() + 1),
                                rule_id: self.id().to_string(),
                                severity: Severity::Critical,
                                target: table_key.clone(),
                                message: format!(
                                    "Mutation running for {:.1} hours (threshold: 1 hour)",
                                    hours
                                ),
                                evidence_refs: vec![],
                                confidence: 1.0,
                            },
                            actions: vec![Action {
                                id: format!("a-mutation-{}", results.len() + 1),
                                finding_ref: format!("f-mutation-{}", results.len() + 1),
                                action_type: ActionType::Recommendation,
                                priority: Priority::High,
                                description: "Investigate mutation, consider KILL MUTATION if stuck".to_string(),
                                sql: Some(format!(
                                    "SELECT * FROM system.mutations WHERE database = '{}' AND table = '{}' AND is_done = 0",
                                    metrics.database, metrics.table
                                )),
                            }],
                        });
                    }
                }
            }
        }

        results
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::report::MutationMetrics;

    fn ctx_with_mutations(active: u64, age_sec: Option<u64>) -> AuditContext {
        let mut ctx = AuditContext::new();
        ctx.add_mutations(MutationMetrics {
            database: "testdb".to_string(),
            table: "events".to_string(),
            active_mutations: active,
            oldest_active_mutation_age_sec: age_sec,
            ..Default::default()
        });
        ctx
    }

    #[test]
    fn test_mutation_none() {
        let rule = StuckMutationRule;
        let ctx = ctx_with_mutations(0, None);
        let results = rule.evaluate(&ctx);
        assert!(results.is_empty());
    }

    #[test]
    fn test_mutation_active_recent() {
        let rule = StuckMutationRule;
        let ctx = ctx_with_mutations(1, Some(600)); // 10 minutes
        let results = rule.evaluate(&ctx);
        assert!(results.is_empty());
    }

    #[test]
    fn test_mutation_stuck() {
        let rule = StuckMutationRule;
        let ctx = ctx_with_mutations(1, Some(7200)); // 2 hours
        let results = rule.evaluate(&ctx);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].finding.severity, Severity::Critical);
    }

    #[test]
    fn test_mutation_at_threshold() {
        let rule = StuckMutationRule;
        let ctx = ctx_with_mutations(1, Some(3600)); // exactly 1 hour
        let results = rule.evaluate(&ctx);
        assert!(results.is_empty()); // 3600 is NOT > 3600
    }

    #[test]
    fn test_mutation_action_has_sql() {
        let rule = StuckMutationRule;
        let ctx = ctx_with_mutations(1, Some(7200));
        let results = rule.evaluate(&ctx);
        assert!(results[0].actions[0].sql.is_some());
        assert!(results[0].actions[0]
            .sql
            .as_ref()
            .unwrap()
            .contains("system.mutations"));
    }
}
