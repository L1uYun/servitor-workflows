//! Declared capability registry, deterministic routing, and audit evidence.
//!
//! The workflow contract declares the candidates that may be selected. This
//! module does not probe providers or manufacture fallbacks: an explicit choice
//! is either admissible as written or rejected before transport submission.

use crate::agent::AgentOptions;
use crate::store::WorkflowStore;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub const CAPABILITY_SCHEMA_VERSION: u32 = 1;

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CapabilityPolicy {
    #[serde(default)]
    pub providers: Vec<ProviderCapability>,
    #[serde(default)]
    pub roles: BTreeMap<String, RoleContract>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ProviderCapability {
    pub agent: String,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub capabilities: Vec<String>,
    #[serde(default)]
    pub max_effort: Effort,
    #[serde(default)]
    pub context_tokens: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Effort {
    Low,
    #[default]
    Medium,
    High,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RoleContract {
    #[serde(default)]
    pub requires: Vec<String>,
    #[serde(default)]
    pub effort: Option<Effort>,
    #[serde(default)]
    pub context_tokens: Option<u64>,
    #[serde(default)]
    pub independent_from: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelChoice {
    pub agent: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CapabilityExclusion {
    pub candidate: ModelChoice,
    pub reason: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum CapabilityEvent {
    Declared {
        policy: CapabilityPolicy,
    },
    Selected {
        key: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        role: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        requested: Option<ModelChoice>,
        chosen: ModelChoice,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        excluded: Vec<CapabilityExclusion>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        degradation: Option<String>,
    },
    IndependenceViolation {
        key: String,
        role: String,
        conflict_role: String,
        chosen: ModelChoice,
        message: String,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CapabilityEnvelope {
    pub version: u32,
    pub sequence: u64,
    pub at: DateTime<Utc>,
    pub run_id: String,
    pub event: CapabilityEvent,
}

pub fn validate_policy(policy: &CapabilityPolicy) -> Result<(), String> {
    if policy.providers.is_empty() {
        return Err("meta.capabilities.providers must not be empty".to_owned());
    }
    for provider in &policy.providers {
        if provider.agent.trim().is_empty() {
            return Err("capability provider agent must not be empty".to_owned());
        }
        if provider.context_tokens == 0 {
            return Err(format!(
                "capability provider {} must declare positive contextTokens",
                provider.agent
            ));
        }
        if provider
            .capabilities
            .iter()
            .any(|value| value.trim().is_empty())
        {
            return Err(format!(
                "capability provider {} contains an empty capability",
                provider.agent
            ));
        }
    }
    for (role, contract) in &policy.roles {
        if role.trim().is_empty() {
            return Err("capability role name must not be empty".to_owned());
        }
        if contract
            .requires
            .iter()
            .any(|value| value.trim().is_empty())
        {
            return Err(format!(
                "capability role {role} contains an empty requirement"
            ));
        }
        if contract.context_tokens == Some(0) {
            return Err(format!(
                "capability role {role} must declare positive contextTokens"
            ));
        }
        for other in &contract.independent_from {
            if other == role || !policy.roles.contains_key(other) {
                return Err(format!(
                    "capability role {role} has unknown or self independence target {other}"
                ));
            }
        }
    }
    Ok(())
}

pub fn ensure_child_narrows(
    parent: &CapabilityPolicy,
    child: &CapabilityPolicy,
) -> Result<(), String> {
    for provider in &child.providers {
        if !parent
            .providers
            .iter()
            .any(|candidate| candidate == provider)
        {
            return Err(format!(
                "child capability provider/model widens parent policy: {}",
                display_choice(&ModelChoice {
                    agent: provider.agent.clone(),
                    model: provider.model.clone(),
                })
            ));
        }
    }
    for (role, child_contract) in &child.roles {
        let parent_contract = parent
            .roles
            .get(role)
            .ok_or_else(|| format!("child capability role widens parent policy: {role}"))?;
        if parent_contract
            .requires
            .iter()
            .any(|required| !child_contract.requires.contains(required))
        {
            return Err(format!(
                "child role {role} omits a capability required by parent contract"
            ));
        }
        if parent_contract
            .independent_from
            .iter()
            .any(|other| !child_contract.independent_from.contains(other))
        {
            return Err(format!(
                "child role {role} weakens parent independence contract"
            ));
        }
        if let Some(parent_effort) = parent_contract.effort.as_ref()
            && child_contract
                .effort
                .as_ref()
                .is_none_or(|effort| effort < parent_effort)
        {
            return Err(format!(
                "child role {role} weakens parent effort requirement"
            ));
        }
        if let Some(parent_context_tokens) = parent_contract.context_tokens
            && child_contract
                .context_tokens
                .is_none_or(|tokens| tokens < parent_context_tokens)
        {
            return Err(format!(
                "child role {role} weakens parent contextTokens requirement"
            ));
        }
    }
    Ok(())
}

pub fn resolve(
    store: &WorkflowStore,
    run_id: &str,
    key: &str,
    policy: &CapabilityPolicy,
    options: &AgentOptions,
) -> Result<ModelChoice, String> {
    let prior_events = store
        .read_capability_events(run_id)
        .map_err(|error| error.to_string())?;
    if let Some(chosen) = prior_events
        .iter()
        .find_map(|envelope| match &envelope.event {
            CapabilityEvent::Selected {
                key: event_key,
                chosen,
                ..
            } if event_key == key => Some(chosen.clone()),
            _ => None,
        })
    {
        if let Some(message) = prior_events
            .iter()
            .find_map(|envelope| match &envelope.event {
                CapabilityEvent::IndependenceViolation {
                    key: event_key,
                    message,
                    ..
                } if event_key == key => Some(message.clone()),
                _ => None,
            })
        {
            return Err(message);
        }
        return Ok(chosen);
    }
    let role = options.role.as_deref();
    let contract = role
        .map(|role| {
            policy
                .roles
                .get(role)
                .ok_or_else(|| format!("agent role is not declared: {role}"))
        })
        .transpose()?;
    let requested = options
        .agent
        .as_ref()
        .map(|agent| ModelChoice {
            agent: agent.clone(),
            model: options.model.clone(),
        })
        .or_else(|| {
            options.model.as_ref().map(|model| ModelChoice {
                agent: "pi".to_owned(),
                model: Some(model.clone()),
            })
        });
    let mut excluded = Vec::new();
    let mut matches = Vec::new();
    for provider in &policy.providers {
        let choice = ModelChoice {
            agent: provider.agent.clone(),
            model: provider.model.clone(),
        };
        if let Some(requested) = requested.as_ref()
            && !matches_explicit(&choice, requested)
        {
            continue;
        }
        match admissibility(provider, contract, options) {
            Ok(()) => matches.push(choice),
            Err(reason) => excluded.push(CapabilityExclusion {
                candidate: choice,
                reason,
            }),
        }
    }
    let Some(chosen) = matches.into_iter().next() else {
        let message = if requested.is_some() {
            format!("pinned provider/model does not satisfy declared capabilities for call {key}")
        } else {
            format!("missing capability for call {key}")
        };
        return Err(format!("{message}: {}", explain_exclusions(&excluded)));
    };
    let degradation = if requested.is_none() && !excluded.is_empty() {
        Some(format!(
            "preferred candidate(s) excluded; selected {}",
            display_choice(&chosen)
        ))
    } else {
        None
    };
    store
        .append_capability_event(
            run_id,
            CapabilityEvent::Selected {
                key: key.to_owned(),
                role: options.role.clone(),
                requested,
                chosen: chosen.clone(),
                excluded,
                degradation,
            },
        )
        .map_err(|error| error.to_string())?;
    if let Some(role) = role
        && let Some(contract) = contract
    {
        for conflict_role in &contract.independent_from {
            let conflict = store
                .read_capability_events(run_id)
                .map_err(|error| error.to_string())?
                .into_iter()
                .rev()
                .find_map(|envelope| match envelope.event {
                    CapabilityEvent::Selected {
                        role: Some(other),
                        chosen,
                        ..
                    } if other == *conflict_role => Some(chosen),
                    _ => None,
                });
            if conflict.as_ref() == Some(&chosen) {
                let message = format!(
                    "role {role} must be independent from {conflict_role}; both resolved to {}",
                    display_choice(&chosen)
                );
                store
                    .append_capability_event(
                        run_id,
                        CapabilityEvent::IndependenceViolation {
                            key: key.to_owned(),
                            role: role.to_owned(),
                            conflict_role: conflict_role.clone(),
                            chosen: chosen.clone(),
                            message: message.clone(),
                        },
                    )
                    .map_err(|error| error.to_string())?;
                return Err(message);
            }
        }
    }
    Ok(chosen)
}

fn matches_explicit(choice: &ModelChoice, requested: &ModelChoice) -> bool {
    choice.agent == requested.agent
        && requested
            .model
            .as_ref()
            .is_none_or(|model| choice.model.as_ref() == Some(model))
}

fn admissibility(
    provider: &ProviderCapability,
    contract: Option<&RoleContract>,
    options: &AgentOptions,
) -> Result<(), String> {
    let required_effort = options
        .effort
        .as_ref()
        .or_else(|| contract.and_then(|contract| contract.effort.as_ref()));
    if required_effort.is_some_and(|effort| provider.max_effort < *effort) {
        return Err(format!(
            "effort {:?} is below required {:?}",
            provider.max_effort, required_effort
        ));
    }
    let context_tokens = options
        .context_tokens
        .or_else(|| contract.and_then(|contract| contract.context_tokens));
    if context_tokens.is_some_and(|tokens| provider.context_tokens < tokens) {
        return Err(format!(
            "contextTokens {} is below required {}",
            provider.context_tokens,
            context_tokens.expect("checked")
        ));
    }
    if let Some(contract) = contract {
        let missing = contract
            .requires
            .iter()
            .filter(|required| !provider.capabilities.contains(required))
            .cloned()
            .collect::<Vec<_>>();
        if !missing.is_empty() {
            return Err(format!(
                "missing required capabilities: {}",
                missing.join(", ")
            ));
        }
    }
    Ok(())
}

fn display_choice(choice: &ModelChoice) -> String {
    choice.model.as_ref().map_or_else(
        || choice.agent.clone(),
        |model| format!("{}/{}", choice.agent, model),
    )
}

fn explain_exclusions(excluded: &[CapabilityExclusion]) -> String {
    if excluded.is_empty() {
        "no declared provider/model candidate matched".to_owned()
    } else {
        excluded
            .iter()
            .map(|entry| format!("{}: {}", display_choice(&entry.candidate), entry.reason))
            .collect::<Vec<_>>()
            .join("; ")
    }
}
