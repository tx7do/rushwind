//! ACL engine for the RushWind authorization contract.
//!
//! Rules evaluate in order. Each rule matches a subject, an action, and
//! a resource — each pattern either a literal or a
//! [`wildcard`](AclOptions::with_wildcard)-anchored prefix/suffix
//! pattern — and carries an effect, allow or deny.
//!
//! The decision walks every rule, collects whether any matching rule
//! allows and whether any denies, then resolves:
//!
//! - a deny wins when [`deny_overrides`](AclOptions::with_deny_overrides)
//!   is set (the default);
//! - else an allow match allows;
//! - else the [`default_deny`](AclOptions::with_default_allow) default
//!   decides — deny, by default.
//!
//! The project axis does not exist in this model: the project queries
//! pass through the subject/action/resource check per project.
//!
//! This is an in-process engine over a flat rule list: every evaluation
//! walks the whole list. It is the right shape for tens of rules and
//! the wrong one for tens of thousands.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use std::sync::RwLock;

use rushwind_authz::{
    Action, AuthzError, Engine, Pairs, PolicyMap, Project, Projects, Resource, RoleMap, Subject,
    Subjects,
};
use serde::{Deserialize, Serialize};

/// The effect of one matching rule.
pub const EFFECT_ALLOW: &str = "allow";
/// The deny effect.
pub const EFFECT_DENY: &str = "deny";

/// One ACL entry. An absent effect deserializes to the empty string,
/// which the
/// evaluation treats as allow.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Rule {
    /// The subject pattern.
    pub subject: String,
    /// The action pattern.
    pub action: String,
    /// The resource pattern.
    pub resource: String,
    /// The effect: `allow` (default) or `deny`. Any other value — a
    /// misspelled `"Deny"`, say — evaluates as allow, so
    /// [`Engine::set_policies`] rejects policies carrying one
    /// instead of installing them.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub effect: String,
}

/// The engine's live state, replaced wholesale by policy installs.
struct Config {
    rules: Vec<Rule>,
    wildcard: String,
    default_deny: bool,
    deny_overrides: bool,
}

/// Builder for [`AclEngine`].
pub struct AclOptions {
    rules: Vec<Rule>,
    wildcard: Option<String>,
    default_deny: bool,
    deny_overrides: bool,
}

impl AclOptions {
    /// Options with the defaults: no rules, `*` wildcard, deny by
    /// default, deny overrides.
    pub fn new() -> Self {
        Self {
            rules: vec![],
            wildcard: None,
            default_deny: true,
            deny_overrides: true,
        }
    }

    /// Replaces the rule list.
    pub fn with_rules(mut self, rules: Vec<Rule>) -> Self {
        self.rules = rules;
        self
    }

    /// Appends one allow rule.
    pub fn with_rule(mut self, subject: &str, action: &str, resource: &str) -> Self {
        self.rules.push(Rule {
            subject: subject.to_string(),
            action: action.to_string(),
            resource: resource.to_string(),
            effect: EFFECT_ALLOW.to_string(),
        });
        self
    }

    /// Appends one deny rule.
    pub fn with_deny_rule(mut self, subject: &str, action: &str, resource: &str) -> Self {
        self.rules.push(Rule {
            subject: subject.to_string(),
            action: action.to_string(),
            resource: resource.to_string(),
            effect: EFFECT_DENY.to_string(),
        });
        self
    }

    /// Overrides the wildcard character. Default `*`.
    pub fn with_wildcard(mut self, wildcard: &str) -> Self {
        self.wildcard = Some(wildcard.to_string());
        self
    }

    /// Sets the no-match default to allow.
    pub fn with_default_allow(mut self) -> Self {
        self.default_deny = false;
        self
    }

    /// Controls whether a deny rule overrides an allow rule. Default
    /// true.
    pub fn with_deny_overrides(mut self, deny_overrides: bool) -> Self {
        self.deny_overrides = deny_overrides;
        self
    }
}

impl Default for AclOptions {
    fn default() -> Self {
        Self::new()
    }
}

/// The ACL authorization engine.
pub struct AclEngine {
    config: RwLock<Config>,
}

impl AclEngine {
    /// Builds the engine from its options.
    pub fn new(options: AclOptions) -> Self {
        Self {
            config: RwLock::new(Config {
                rules: options.rules,
                wildcard: options.wildcard.unwrap_or_else(|| "*".to_string()),
                default_deny: options.default_deny,
                deny_overrides: options.deny_overrides,
            }),
        }
    }

    /// Replaces the rule list — the typed form of the `rules` policy
    /// entry.
    pub fn set_rules(&self, rules: Vec<Rule>) {
        self.config.write().expect("acl config lock poisoned").rules = rules;
    }
}

impl Engine for AclEngine {
    fn name(&self) -> String {
        "acl".to_string()
    }

    fn is_authorized(
        &self,
        subject: Subject,
        action: Action,
        resource: Resource,
        _project: Project,
    ) -> Result<bool, AuthzError> {
        let config = self.config.read().expect("acl config lock poisoned");
        Ok(evaluate(
            &config,
            subject.as_str(),
            action.as_str(),
            resource.as_str(),
        ))
    }

    fn projects_authorized(
        &self,
        subjects: Subjects,
        action: Action,
        resource: Resource,
        projects: Projects,
    ) -> Result<Projects, AuthzError> {
        // The ACL model has no project axis: the subject check decides
        // per project.
        let config = self.config.read().expect("acl config lock poisoned");
        let mut result = vec![];
        for project in projects {
            for subject in &subjects {
                if evaluate(
                    &config,
                    subject.as_str(),
                    action.as_str(),
                    resource.as_str(),
                ) {
                    result.push(project);
                    break;
                }
            }
        }
        Ok(result)
    }

    fn filter_authorized_pairs(
        &self,
        subjects: Subjects,
        pairs: Pairs,
    ) -> Result<Pairs, AuthzError> {
        let config = self.config.read().expect("acl config lock poisoned");
        let mut result = vec![];
        for pair in pairs {
            for subject in &subjects {
                if evaluate(
                    &config,
                    subject.as_str(),
                    pair.action.as_str(),
                    pair.resource.as_str(),
                ) {
                    result.push(pair);
                    break;
                }
            }
        }
        Ok(result)
    }

    fn filter_authorized_projects(&self, _subjects: Subjects) -> Result<Projects, AuthzError> {
        // No project axis: the empty list.
        Ok(vec![])
    }

    fn set_policies(&self, policies: PolicyMap, _roles: RoleMap) -> Result<(), AuthzError> {
        // A payload that does not deserialize into the rule
        // type is silently skipped. A payload that deserializes but
        // carries an unknown effect is REJECTED: an effect value only
        // ever evaluated as allow (a misspelled "deny", say) would
        // silently widen access instead of restricting it.
        if let Some(payload) = policies.get("rules") {
            if let Ok(rules) = serde_json::from_value::<Vec<Rule>>(payload.clone()) {
                if rules
                    .iter()
                    .any(|rule| !matches!(rule.effect.as_str(), "" | "allow" | "deny"))
                {
                    return Err(AuthzError::InvalidPolicy);
                }
                self.set_rules(rules);
            }
        }
        Ok(())
    }
}

/// The verdict: collect allow/deny over all matching rules, then apply
/// the override and default.
fn evaluate(config: &Config, subject: &str, action: &str, resource: &str) -> bool {
    let mut allowed = false;
    let mut denied = false;
    for rule in &config.rules {
        if !matches_pattern(&rule.subject, subject, &config.wildcard) {
            continue;
        }
        if !matches_pattern(&rule.action, action, &config.wildcard) {
            continue;
        }
        if !matches_pattern(&rule.resource, resource, &config.wildcard) {
            continue;
        }
        if rule.effect == EFFECT_DENY {
            denied = true;
        } else {
            allowed = true;
        }
    }
    if config.deny_overrides && denied {
        return false;
    }
    if allowed {
        return true;
    }
    !config.default_deny
}

/// Pattern matching: a pattern equal to the wildcard matches anything;
/// a literal matches itself; a trailing wildcard anchors a prefix match;
/// a leading wildcard anchors a suffix match. The suffix-wildcard arm
/// runs first.
fn matches_pattern(pattern: &str, value: &str, wildcard: &str) -> bool {
    if pattern == wildcard {
        return true;
    }
    if pattern == value {
        return true;
    }
    if let Some(prefix) = pattern.strip_suffix(wildcard) {
        return value.starts_with(prefix);
    }
    if let Some(suffix) = pattern.strip_prefix(wildcard) {
        return value.ends_with(suffix);
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use rushwind_authz::Pair;

    fn ask(engine: &AclEngine, subject: &str, action: &str, resource: &str) -> bool {
        engine
            .is_authorized(
                Subject::from(subject),
                Action::from(action),
                Resource::from(resource),
                Project::from(""),
            )
            .unwrap()
    }

    fn engine_with_rules(rules: &[(&str, &str, &str)]) -> AclEngine {
        let mut options = AclOptions::new();
        for (subject, action, resource) in rules {
            options = options.with_rule(subject, action, resource);
        }
        AclEngine::new(options)
    }

    #[test]
    fn the_engine_name_is_acl() {
        let engine: Box<dyn Engine> = Box::new(AclEngine::new(AclOptions::new()));
        assert_eq!(engine.name(), "acl");
    }

    #[test]
    fn default_deny_when_no_rule_matches() {
        let engine = AclEngine::new(AclOptions::new());
        assert!(!ask(&engine, "alice", "read", "doc:1"));
    }

    #[test]
    fn a_matching_allow_rule_allows() {
        let engine = engine_with_rules(&[("alice", "read", "doc:1")]);
        assert!(ask(&engine, "alice", "read", "doc:1"));
    }

    #[test]
    fn another_subject_does_not_match() {
        let engine = engine_with_rules(&[("alice", "read", "doc:1")]);
        assert!(!ask(&engine, "bob", "read", "doc:1"));
    }

    #[test]
    fn default_allow_flips_the_no_match_default() {
        let engine = AclEngine::new(AclOptions::new().with_default_allow());
        assert!(ask(&engine, "alice", "read", "doc:1"));
    }

    #[test]
    fn full_wildcards_allow_everything() {
        let engine = engine_with_rules(&[("admin", "*", "*")]);
        assert!(ask(&engine, "admin", "read", "doc:1"));
        assert!(ask(&engine, "admin", "write", "img:2"));
    }

    #[test]
    fn suffix_wildcards_match_the_prefix_family_only() {
        let engine = engine_with_rules(&[("alice", "read", "doc:*")]);
        assert!(ask(&engine, "alice", "read", "doc:1"));
        // A different prefix and a different action both fail.
        assert!(!ask(&engine, "alice", "read", "img:1"));
        assert!(!ask(&engine, "alice", "write", "doc:1"));
    }

    #[test]
    fn action_wildcards_match_the_resource_only() {
        let engine = engine_with_rules(&[("alice", "*", "doc:1")]);
        assert!(ask(&engine, "alice", "read", "doc:1"));
        assert!(ask(&engine, "alice", "write", "doc:1"));
        assert!(!ask(&engine, "alice", "read", "doc:2"));
    }

    #[test]
    fn deny_rules_override_wildcard_allows() {
        let engine = AclEngine::new(
            AclOptions::new()
                .with_rule("alice", "*", "*")
                .with_deny_rule("alice", "delete", "doc:1"),
        );
        // Allowed by the wildcard, denied specifically.
        assert!(!ask(&engine, "alice", "delete", "doc:1"));
        // Other actions keep the wildcard allow.
        assert!(ask(&engine, "alice", "read", "doc:1"));
    }

    #[test]
    fn deny_override_can_be_turned_off() {
        let engine = AclEngine::new(
            AclOptions::new()
                .with_rule("alice", "*", "*")
                .with_deny_rule("alice", "delete", "doc:1")
                .with_deny_overrides(false),
        );
        // The allow stands; the deny rule no longer overrides it.
        assert!(ask(&engine, "alice", "delete", "doc:1"));
    }

    #[test]
    fn projects_pass_through_the_subject_check() {
        let engine = engine_with_rules(&[("alice", "read", "doc:*")]);
        let projects: Projects = ["p1", "p2", "p3"]
            .iter()
            .map(|p| Project::from(*p))
            .collect();
        let result = engine
            .projects_authorized(
                vec![Subject::from("alice")],
                Action::from("read"),
                Resource::from("doc:1"),
                projects,
            )
            .unwrap();
        assert_eq!(result.len(), 3);
    }

    #[test]
    fn projects_filter_to_empty_for_unauthorized_subjects() {
        let engine = engine_with_rules(&[("alice", "read", "doc:*")]);
        let projects: Projects = ["p1", "p2"].iter().map(|p| Project::from(*p)).collect();
        let result = engine
            .projects_authorized(
                vec![Subject::from("bob")],
                Action::from("read"),
                Resource::from("doc:1"),
                projects,
            )
            .unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn pairs_filter_down_to_matching_pairs() {
        let engine = engine_with_rules(&[("alice", "read", "doc:*")]);
        let pairs: Pairs = vec![
            Pair {
                resource: Resource::from("doc:1"),
                action: Action::from("read"),
            },
            Pair {
                resource: Resource::from("doc:2"),
                action: Action::from("write"),
            },
            Pair {
                resource: Resource::from("img:1"),
                action: Action::from("read"),
            },
        ];
        let result = engine
            .filter_authorized_pairs(vec![Subject::from("alice")], pairs)
            .unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].resource.as_str(), "doc:1");
        assert_eq!(result[0].action.as_str(), "read");
    }

    #[test]
    fn policy_installs_flip_the_decision() {
        let engine = AclEngine::new(AclOptions::new());
        // Initially denied.
        assert!(!ask(&engine, "alice", "read", "doc:1"));
        // Install a rule through the interchange map.
        let rules = vec![Rule {
            subject: "alice".to_string(),
            action: "read".to_string(),
            resource: "doc:1".to_string(),
            effect: "allow".to_string(),
        }];
        let mut policies = PolicyMap::new();
        policies.insert(
            "rules".to_string(),
            serde_json::to_value(&rules).expect("rules serialize"),
        );
        engine.set_policies(policies, RoleMap::new()).unwrap();
        // Now allowed.
        assert!(ask(&engine, "alice", "read", "doc:1"));
    }

    #[test]
    fn a_policy_with_an_unknown_effect_is_rejected_not_installed() {
        let engine = AclEngine::new(AclOptions::new());
        // A misspelled deny would evaluate as allow — the install
        // refuses it instead of silently widening access.
        for effect in ["Deny", "DENY", "block", " permits "] {
            let rules = vec![Rule {
                subject: "alice".to_string(),
                action: "read".to_string(),
                resource: "doc:1".to_string(),
                effect: effect.to_string(),
            }];
            let mut policies = PolicyMap::new();
            policies.insert(
                "rules".to_string(),
                serde_json::to_value(&rules).expect("rules serialize"),
            );
            assert_eq!(
                engine.set_policies(policies, RoleMap::new()).unwrap_err(),
                AuthzError::InvalidPolicy,
                "effect {effect:?}"
            );
        }
        // Nothing was installed: the default deny holds.
        assert!(!ask(&engine, "alice", "read", "doc:1"));
    }

    #[test]
    fn an_empty_effect_still_installs_as_allow() {
        let engine = AclEngine::new(AclOptions::new());
        let rules = vec![Rule {
            subject: "alice".to_string(),
            action: "read".to_string(),
            resource: "doc:1".to_string(),
            effect: String::new(),
        }];
        let mut policies = PolicyMap::new();
        policies.insert(
            "rules".to_string(),
            serde_json::to_value(&rules).expect("rules serialize"),
        );
        engine.set_policies(policies, RoleMap::new()).unwrap();
        assert!(ask(&engine, "alice", "read", "doc:1"));
    }

    #[test]
    fn malformed_policy_payloads_are_skipped() {
        // A payload that is not a rule list: skipped, no error, no
        // change.
        let engine = AclEngine::new(AclOptions::new());
        let mut policies = PolicyMap::new();
        policies.insert("rules".to_string(), serde_json::json!(42));
        policies.insert("unknown-entry".to_string(), serde_json::json!({"x": 1}));
        engine.set_policies(policies, RoleMap::new()).unwrap();
        assert!(!ask(&engine, "alice", "read", "doc:1"));
    }
}
