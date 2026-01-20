use crate::report::{Action, Finding};
use super::context::AuditContext;

/// Result of a rule evaluation
#[derive(Debug)]
pub struct RuleResult {
    pub finding: Finding,
    pub actions: Vec<Action>,
}

/// Trait for implementing audit rules
pub trait Rule: Send + Sync {
    /// Unique identifier for the rule
    fn id(&self) -> &'static str;

    /// Human-readable name
    fn name(&self) -> &'static str;

    /// Evaluate the rule against the audit context
    fn evaluate(&self, ctx: &AuditContext) -> Vec<RuleResult>;
}

/// Registry for managing and running rules
#[derive(Default)]
pub struct RuleRegistry {
    rules: Vec<Box<dyn Rule>>,
}

impl RuleRegistry {
    /// Create a new empty rule registry
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a rule
    pub fn register(&mut self, rule: Box<dyn Rule>) {
        self.rules.push(rule);
    }

    /// Evaluate all rules against the context
    pub fn evaluate_all(&self, ctx: &AuditContext) -> Vec<RuleResult> {
        self.rules
            .iter()
            .flat_map(|rule| rule.evaluate(ctx))
            .collect()
    }

    /// Get the number of registered rules
    pub fn len(&self) -> usize {
        self.rules.len()
    }

    /// Check if registry is empty
    pub fn is_empty(&self) -> bool {
        self.rules.is_empty()
    }

    /// Get list of registered rule IDs
    pub fn rule_ids(&self) -> Vec<&'static str> {
        self.rules.iter().map(|r| r.id()).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::report::Severity;

    // Mock rule for testing
    struct MockRule {
        should_trigger: bool,
    }

    impl Rule for MockRule {
        fn id(&self) -> &'static str {
            "mock_rule"
        }

        fn name(&self) -> &'static str {
            "Mock Rule"
        }

        fn evaluate(&self, _ctx: &AuditContext) -> Vec<RuleResult> {
            if self.should_trigger {
                vec![RuleResult {
                    finding: Finding {
                        id: "f-mock".to_string(),
                        rule_id: self.id().to_string(),
                        severity: Severity::Warning,
                        target: "test".to_string(),
                        message: "Mock finding".to_string(),
                        evidence_refs: vec![],
                        confidence: 1.0,
                    },
                    actions: vec![],
                }]
            } else {
                vec![]
            }
        }
    }

    #[test]
    fn test_registry_empty() {
        let registry = RuleRegistry::new();
        assert!(registry.is_empty());
        assert_eq!(registry.len(), 0);
    }

    #[test]
    fn test_registry_register() {
        let mut registry = RuleRegistry::new();
        registry.register(Box::new(MockRule { should_trigger: false }));

        assert!(!registry.is_empty());
        assert_eq!(registry.len(), 1);
    }

    #[test]
    fn test_registry_evaluate_empty() {
        let registry = RuleRegistry::new();
        let ctx = AuditContext::new();

        let results = registry.evaluate_all(&ctx);
        assert!(results.is_empty());
    }

    #[test]
    fn test_registry_evaluate_no_trigger() {
        let mut registry = RuleRegistry::new();
        registry.register(Box::new(MockRule { should_trigger: false }));

        let ctx = AuditContext::new();
        let results = registry.evaluate_all(&ctx);

        assert!(results.is_empty());
    }

    #[test]
    fn test_registry_evaluate_trigger() {
        let mut registry = RuleRegistry::new();
        registry.register(Box::new(MockRule { should_trigger: true }));

        let ctx = AuditContext::new();
        let results = registry.evaluate_all(&ctx);

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].finding.rule_id, "mock_rule");
    }

    #[test]
    fn test_registry_multiple_rules() {
        let mut registry = RuleRegistry::new();
        registry.register(Box::new(MockRule { should_trigger: true }));
        registry.register(Box::new(MockRule { should_trigger: true }));

        let ctx = AuditContext::new();
        let results = registry.evaluate_all(&ctx);

        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_registry_rule_ids() {
        let mut registry = RuleRegistry::new();
        registry.register(Box::new(MockRule { should_trigger: false }));

        let ids = registry.rule_ids();
        assert_eq!(ids, vec!["mock_rule"]);
    }
}
