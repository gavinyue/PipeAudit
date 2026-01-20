use super::{AuditContext, Rule, RuleResult};
use crate::report::{Action, ActionType, Finding, Priority, Severity};

const DISK_WARNING_PCT: f64 = 20.0;
const DISK_CRITICAL_PCT: f64 = 10.0;

/// Rule to detect low disk headroom
pub struct DiskHeadroomRule;

impl Rule for DiskHeadroomRule {
    fn id(&self) -> &'static str {
        "disk_headroom"
    }

    fn name(&self) -> &'static str {
        "Disk Headroom"
    }

    fn evaluate(&self, ctx: &AuditContext) -> Vec<RuleResult> {
        let mut results = Vec::new();

        for disk in &ctx.disk {
            let free_pct = disk.free_percent;
            let free_gb = disk.free_space as f64 / 1_073_741_824.0;

            if free_pct < DISK_CRITICAL_PCT {
                results.push(RuleResult {
                    finding: Finding {
                        id: format!("f-disk-{}", results.len() + 1),
                        rule_id: self.id().to_string(),
                        severity: Severity::Critical,
                        target: disk.disk_name.clone(),
                        message: format!(
                            "Disk {} has only {:.1}% free ({:.1}GB)",
                            disk.disk_name, free_pct, free_gb
                        ),
                        evidence_refs: vec![],
                        confidence: 1.0,
                    },
                    actions: vec![Action {
                        id: format!("a-disk-{}", results.len() + 1),
                        finding_ref: format!("f-disk-{}", results.len() + 1),
                        action_type: ActionType::Recommendation,
                        priority: Priority::High,
                        description: "Expand storage or implement TTL policy urgently".to_string(),
                        sql: None,
                    }],
                });
            } else if free_pct < DISK_WARNING_PCT {
                results.push(RuleResult {
                    finding: Finding {
                        id: format!("f-disk-{}", results.len() + 1),
                        rule_id: self.id().to_string(),
                        severity: Severity::Warning,
                        target: disk.disk_name.clone(),
                        message: format!(
                            "Disk {} has only {:.1}% free ({:.1}GB)",
                            disk.disk_name, free_pct, free_gb
                        ),
                        evidence_refs: vec![],
                        confidence: 1.0,
                    },
                    actions: vec![Action {
                        id: format!("a-disk-{}", results.len() + 1),
                        finding_ref: format!("f-disk-{}", results.len() + 1),
                        action_type: ActionType::Recommendation,
                        priority: Priority::Medium,
                        description: "Consider expanding storage or implementing TTL".to_string(),
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
    use crate::report::DiskMetrics;

    fn ctx_with_disk(free_percent: f64) -> AuditContext {
        let mut ctx = AuditContext::new();
        let total = 100_000_000_000u64;
        let free = (total as f64 * free_percent / 100.0) as u64;
        ctx.set_disk(vec![DiskMetrics {
            disk_name: "default".to_string(),
            path: "/var/lib/clickhouse".to_string(),
            total_space: total,
            free_space: free,
            free_percent,
        }]);
        ctx
    }

    #[test]
    fn test_disk_healthy() {
        let rule = DiskHeadroomRule;
        let ctx = ctx_with_disk(50.0);
        let results = rule.evaluate(&ctx);
        assert!(results.is_empty());
    }

    #[test]
    fn test_disk_warning() {
        let rule = DiskHeadroomRule;
        let ctx = ctx_with_disk(15.0);
        let results = rule.evaluate(&ctx);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].finding.severity, Severity::Warning);
    }

    #[test]
    fn test_disk_critical() {
        let rule = DiskHeadroomRule;
        let ctx = ctx_with_disk(5.0);
        let results = rule.evaluate(&ctx);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].finding.severity, Severity::Critical);
    }

    #[test]
    fn test_disk_at_warning_threshold() {
        let rule = DiskHeadroomRule;
        let ctx = ctx_with_disk(20.0);
        let results = rule.evaluate(&ctx);
        assert!(results.is_empty());
    }
}
