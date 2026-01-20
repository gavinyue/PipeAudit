mod context;
mod disk;
mod engine;
mod merges;
mod mutations;
mod parts;
mod query;

pub use context::AuditContext;
pub use disk::DiskHeadroomRule;
pub use engine::{Rule, RuleRegistry, RuleResult};
pub use merges::MergeBacklogRule;
pub use mutations::StuckMutationRule;
pub use parts::PartsExplosionRule;
pub use query::QueryAmplificationRule;

impl RuleRegistry {
    /// Create a registry with all default rules
    pub fn with_default_rules() -> Self {
        let mut registry = Self::new();
        registry.register(Box::new(PartsExplosionRule));
        registry.register(Box::new(MergeBacklogRule));
        registry.register(Box::new(DiskHeadroomRule));
        registry.register(Box::new(QueryAmplificationRule));
        registry.register(Box::new(StuckMutationRule));
        registry
    }
}
