use crate::report::{Action, ActionType, Finding, Priority, Severity};
use super::{AuditContext, Rule, RuleResult};

/// Thresholds for parts explosion detection
const PARTS_WARNING: u64 = 300;
const PARTS_CRITICAL: u64 = 1000;

/// Rule to detect parts explosion in tables
pub struct PartsExplosionRule;

impl Rule for PartsExplosionRule {
    fn id(&self) -> &'static str {
        "parts_explosion"
    }

    fn name(&self) -> &'static str {
        "Parts Explosion"
    }

    fn evaluate(&self, ctx: &AuditContext) -> Vec<RuleResult> {
        let mut results = Vec::new();

        for (table_key, metrics) in &ctx.parts {
            let active_parts = metrics.active_parts;

            if active_parts > PARTS_CRITICAL {
                results.push(RuleResult {
                    finding: Finding {
                        id: format!("f-parts-{}", results.len() + 1),
                        rule_id: self.id().to_string(),
                        severity: Severity::Critical,
                        target: table_key.clone(),
                        message: format!(
                            "Table has {} active parts, exceeding critical threshold of {}",
                            active_parts, PARTS_CRITICAL
                        ),
                        evidence_refs: vec![],
                        confidence: 1.0,
                    },
                    actions: vec![Action {
                        id: format!("a-parts-{}", results.len() + 1),
                        finding_ref: format!("f-parts-{}", results.len() + 1),
                        action_type: ActionType::Recommendation,
                        priority: Priority::High,
                        description: "Run OPTIMIZE TABLE to reduce parts count".to_string(),
                        sql: Some(format!("OPTIMIZE TABLE {} FINAL", table_key)),
                    }],
                });
            } else if active_parts > PARTS_WARNING {
                results.push(RuleResult {
                    finding: Finding {
                        id: format!("f-parts-{}", results.len() + 1),
                        rule_id: self.id().to_string(),
                        severity: Severity::Warning,
                        target: table_key.clone(),
                        message: format!(
                            "Table has {} active parts, exceeding warning threshold of {}",
                            active_parts, PARTS_WARNING
                        ),
                        evidence_refs: vec![],
                        confidence: 1.0,
                    },
                    actions: vec![Action {
                        id: format!("a-parts-{}", results.len() + 1),
                        finding_ref: format!("f-parts-{}", results.len() + 1),
                        action_type: ActionType::Recommendation,
                        priority: Priority::Medium,
                        description: "Consider running OPTIMIZE TABLE to reduce parts".to_string(),
                        sql: Some(format!("OPTIMIZE TABLE {} FINAL", table_key)),
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
    use crate::report::PartsMetrics;

    fn ctx_with_parts(active_parts: u64) -> AuditContext {
        let mut ctx = AuditContext::new();
        ctx.add_parts(PartsMetrics {
            database: "testdb".to_string(),
            table: "events".to_string(),
            active_parts,
            ..Default::default()
        });
        ctx
    }

    #[test]
    fn test_parts_explosion_healthy() {
        let rule = PartsExplosionRule;
        let ctx = ctx_with_parts(100); // below 300 threshold

        let results = rule.evaluate(&ctx);
        assert!(results.is_empty());
    }

    #[test]
    fn test_parts_explosion_warning() {
        let rule = PartsExplosionRule;
        let ctx = ctx_with_parts(500); // between 300 and 1000

        let results = rule.evaluate(&ctx);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].finding.severity, Severity::Warning);
        assert_eq!(results[0].finding.rule_id, "parts_explosion");
    }

    #[test]
    fn test_parts_explosion_critical() {
        let rule = PartsExplosionRule;
        let ctx = ctx_with_parts(1500); // above 1000

        let results = rule.evaluate(&ctx);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].finding.severity, Severity::Critical);
    }

    #[test]
    fn test_parts_explosion_at_warning_threshold() {
        let rule = PartsExplosionRule;
        let ctx = ctx_with_parts(300); // exactly at threshold

        let results = rule.evaluate(&ctx);
        assert!(results.is_empty()); // 300 is NOT > 300
    }

    #[test]
    fn test_parts_explosion_just_above_warning() {
        let rule = PartsExplosionRule;
        let ctx = ctx_with_parts(301);

        let results = rule.evaluate(&ctx);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].finding.severity, Severity::Warning);
    }

    #[test]
    fn test_parts_explosion_action_has_sql() {
        let rule = PartsExplosionRule;
        let ctx = ctx_with_parts(1500);

        let results = rule.evaluate(&ctx);
        assert!(!results[0].actions.is_empty());
        assert!(results[0].actions[0].sql.as_ref().unwrap().contains("OPTIMIZE"));
    }

    #[test]
    fn test_parts_explosion_target_is_table() {
        let rule = PartsExplosionRule;
        let ctx = ctx_with_parts(1500);

        let results = rule.evaluate(&ctx);
        assert_eq!(results[0].finding.target, "testdb.events");
    }

    #[test]
    fn test_parts_explosion_multiple_tables() {
        let rule = PartsExplosionRule;
        let mut ctx = AuditContext::new();

        ctx.add_parts(PartsMetrics {
            database: "db".to_string(),
            table: "healthy".to_string(),
            active_parts: 50,
            ..Default::default()
        });
        ctx.add_parts(PartsMetrics {
            database: "db".to_string(),
            table: "warning".to_string(),
            active_parts: 500,
            ..Default::default()
        });
        ctx.add_parts(PartsMetrics {
            database: "db".to_string(),
            table: "critical".to_string(),
            active_parts: 1500,
            ..Default::default()
        });

        let results = rule.evaluate(&ctx);
        assert_eq!(results.len(), 2); // warning + critical, not healthy
    }
}
