// SPDX-License-Identifier: Apache-2.0

use clap::ValueEnum;
use serde::{Deserialize, Serialize};

use crate::trace::AgentContext;

#[derive(Debug, Clone, Copy, Deserialize, Serialize, ValueEnum)]
#[serde(rename_all = "kebab-case")]
pub enum AgentKind {
    ClaudeCode,
    Codex,
    Opencode,
}

pub fn agent_headers(
    kind: AgentKind,
    context: Option<&AgentContext>,
) -> Vec<(&'static str, String)> {
    let Some(context) = context else {
        return Vec::new();
    };
    let mut headers = vec![("x-dynamo-session-id", context.session_id.clone())];
    if let Some(parent) = &context.parent_session_id {
        headers.push(("x-dynamo-parent-session-id", parent.clone()));
    }
    headers.extend(match kind {
        AgentKind::ClaudeCode => {
            if let Some(parent) = &context.parent_session_id {
                vec![
                    ("x-claude-code-session-id", parent.clone()),
                    ("x-claude-code-agent-id", context.session_id.clone()),
                    ("x-claude-code-parent-agent-id", parent.clone()),
                ]
            } else {
                vec![("x-claude-code-session-id", context.session_id.clone())]
            }
        }
        AgentKind::Codex => {
            let mut values = vec![("thread-id", context.session_id.clone())];
            if let Some(parent) = &context.parent_session_id {
                values.push(("x-codex-parent-thread-id", parent.clone()));
            }
            values
        }
        AgentKind::Opencode => {
            let mut values = vec![("x-session-id", context.session_id.clone())];
            if let Some(parent) = &context.parent_session_id {
                values.push(("x-parent-session-id", parent.clone()));
            }
            values
        }
    });
    if let Some(finality) = context.session_final {
        headers.push(("x-dynamo-session-final", finality.to_string()));
    }
    headers
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn codex_headers_preserve_parent() {
        let context = AgentContext {
            session_id: "child".to_string(),
            parent_session_id: Some("parent".to_string()),
            session_final: Some(true),
            input_trigger: None,
        };
        assert_eq!(
            agent_headers(AgentKind::Codex, Some(&context)),
            vec![
                ("x-dynamo-session-id", "child".to_string()),
                ("x-dynamo-parent-session-id", "parent".to_string()),
                ("thread-id", "child".to_string()),
                ("x-codex-parent-thread-id", "parent".to_string()),
                ("x-dynamo-session-final", "true".to_string())
            ]
        );
    }

    #[test]
    fn claude_child_sets_root_and_agent_headers() {
        let context = AgentContext {
            session_id: "child".to_string(),
            parent_session_id: Some("parent".to_string()),
            session_final: None,
            input_trigger: None,
        };
        assert_eq!(
            agent_headers(AgentKind::ClaudeCode, Some(&context)),
            vec![
                ("x-dynamo-session-id", "child".to_string()),
                ("x-dynamo-parent-session-id", "parent".to_string()),
                ("x-claude-code-session-id", "parent".to_string()),
                ("x-claude-code-agent-id", "child".to_string()),
                ("x-claude-code-parent-agent-id", "parent".to_string())
            ]
        );
    }
}
