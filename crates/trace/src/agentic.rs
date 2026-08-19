// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Agentic lowering derived from Dynamo's request-trace replay path.

use std::collections::{HashMap, VecDeque};

use anyhow::{Result, anyhow, bail};

use super::{LoadedTrace, RequestEntry};
use agent_loadgen_core::{TraceManifest, TraceRequest};

#[derive(Debug, Clone)]
pub struct AgenticTrace {
    pub manifest: TraceManifest,
    pub turns: Vec<AgenticTurn>,
}

#[derive(Debug, Clone)]
pub struct AgenticTurn {
    pub request: TraceRequest,
    pub dependencies: Vec<usize>,
    pub root_arrival_ms: Option<u64>,
    pub delay_after_dependencies_ms: u64,
}

pub(super) fn lower(loaded: LoadedTrace) -> Result<AgenticTrace> {
    let global_start_ms = loaded
        .requests
        .iter()
        .map(|request| request.start_ms)
        .min()
        .ok_or_else(|| anyhow!("no request records to lower"))?;

    let mut id_to_index = HashMap::with_capacity(loaded.requests.len());
    for (index, request) in loaded.requests.iter().enumerate() {
        if id_to_index
            .insert(request.request.source_request_id.clone(), index)
            .is_some()
        {
            bail!("duplicate request_id {}", request.request.source_request_id);
        }
    }

    let mut session_to_indices: HashMap<String, Vec<usize>> = HashMap::new();
    let mut parent_by_session: HashMap<String, String> = HashMap::new();
    for (index, request) in loaded.requests.iter().enumerate() {
        let context = request
            .request
            .agent_context
            .as_ref()
            .expect("trace loading requires agent context");
        session_to_indices
            .entry(context.session_id.clone())
            .or_default()
            .push(index);
        if let Some(parent) = &context.parent_session_id {
            match parent_by_session.get(&context.session_id) {
                Some(existing) if existing != parent => bail!(
                    "session {} has conflicting parent_session_id values: {} and {}",
                    context.session_id,
                    existing,
                    parent
                ),
                Some(_) => {}
                None => {
                    parent_by_session.insert(context.session_id.clone(), parent.clone());
                }
            }
        }
    }
    let mut explicit_tool_by_child = HashMap::new();
    for tool in &loaded.tools {
        let Some(claude) = &tool.claude else {
            continue;
        };
        if !matches!(claude.execution_mode.as_str(), "blocking" | "background") {
            bail!(
                "tool {} has unsupported execution_mode {}",
                tool.tool_call_id,
                claude.execution_mode
            );
        }
        for (request_id, required) in [
            (claude.source_request_id.as_str(), true),
            (claude.consumer_request_id.as_deref().unwrap_or(""), false),
        ] {
            if request_id.is_empty() {
                continue;
            }
            let Some(request_index) = id_to_index.get(request_id).copied() else {
                if required {
                    bail!(
                        "tool {} references unknown request_id {}",
                        tool.tool_call_id,
                        request_id
                    );
                }
                continue;
            };
            let request_session = loaded.requests[request_index]
                .request
                .agent_context
                .as_ref()
                .expect("trace loading requires agent context")
                .session_id
                .as_str();
            if request_session != tool.session_id {
                bail!(
                    "tool {} request {} belongs to a different session",
                    tool.tool_call_id,
                    request_id
                );
            }
        }
        let Some(child_session_id) = claude.child_session_id.as_deref() else {
            continue;
        };
        if !session_to_indices.contains_key(child_session_id) {
            continue;
        }
        if explicit_tool_by_child
            .insert(child_session_id.to_string(), tool)
            .is_some()
        {
            bail!("multiple tool events reference child session {child_session_id}");
        }
    }

    let mut dependencies = vec![Vec::new(); loaded.requests.len()];
    for indices in session_to_indices.values() {
        for (position, index) in indices.iter().copied().enumerate() {
            if position > 0 {
                let previous_index = indices[position - 1];
                push_unique(&mut dependencies[index], previous_index);
            }
        }
    }

    for (session_id, parent_id) in &parent_by_session {
        let Some(child_indices) = session_to_indices.get(session_id) else {
            continue;
        };
        let Some(parent_indices) = session_to_indices.get(parent_id) else {
            continue;
        };
        let first_child_index = child_indices[0];
        let last_finishing_child_index = *child_indices
            .iter()
            .max_by(|left, right| {
                let left = &loaded.requests[**left];
                let right = &loaded.requests[**right];
                (left.end_ms, left.start_ms, &left.request.source_request_id).cmp(&(
                    right.end_ms,
                    right.start_ms,
                    &right.request.source_request_id,
                ))
            })
            .expect("child session is non-empty");

        if let Some(tool) = explicit_tool_by_child.get(session_id) {
            let claude = tool
                .claude
                .as_ref()
                .expect("explicit child tool has Claude metadata");
            let parent_spawn_index =
                *id_to_index.get(&claude.source_request_id).ok_or_else(|| {
                    anyhow!(
                        "tool {} references unknown source request {}",
                        tool.tool_call_id,
                        claude.source_request_id
                    )
                })?;
            if !parent_indices.contains(&parent_spawn_index) {
                bail!(
                    "tool {} source request {} is not in parent session {}",
                    tool.tool_call_id,
                    claude.source_request_id,
                    parent_id
                );
            }
            push_unique(&mut dependencies[first_child_index], parent_spawn_index);
            if let Some(consumer_request_id) = claude.consumer_request_id.as_deref() {
                let Some(parent_join_index) = id_to_index.get(consumer_request_id).copied() else {
                    // A prefix selected with --max-requests may intentionally
                    // omit a later consumer. The selected graph has no join.
                    continue;
                };
                if !parent_indices.contains(&parent_join_index) {
                    bail!(
                        "tool {} consumer request {} is not in parent session {}",
                        tool.tool_call_id,
                        consumer_request_id,
                        parent_id
                    );
                }
                push_unique(
                    &mut dependencies[parent_join_index],
                    last_finishing_child_index,
                );
            }
            continue;
        }

        let child_start_ms = loaded.requests[first_child_index].start_ms;
        let child_end_ms = loaded.requests[last_finishing_child_index].end_ms;
        if let Some(parent_spawn_index) =
            latest_request_starting_before(&loaded.requests, parent_indices, child_start_ms)
        {
            push_unique(&mut dependencies[first_child_index], parent_spawn_index);
        }
        if let Some(parent_join_index) =
            first_request_starting_after(&loaded.requests, parent_indices, child_end_ms)
        {
            push_unique(
                &mut dependencies[parent_join_index],
                last_finishing_child_index,
            );
        }
    }
    validate_dependency_dag(&loaded.requests, &dependencies)?;

    let request_end_ms = loaded
        .requests
        .iter()
        .map(|request| request.end_ms)
        .collect::<Vec<_>>();
    let turns = loaded
        .requests
        .into_iter()
        .enumerate()
        .map(|(index, request)| {
            let dependency_end_ms = dependencies[index]
                .iter()
                .map(|dependency| request_end_ms[*dependency])
                .max();
            let (root_arrival_ms, delay_after_dependencies_ms) =
                if let Some(dependency_end_ms) = dependency_end_ms {
                    (
                        None,
                        request.start_ms.saturating_sub(dependency_end_ms).max(0) as u64,
                    )
                } else {
                    (
                        Some(request.start_ms.saturating_sub(global_start_ms).max(0) as u64),
                        0,
                    )
                };
            AgenticTurn {
                request: request.request,
                dependencies: std::mem::take(&mut dependencies[index]),
                root_arrival_ms,
                delay_after_dependencies_ms,
            }
        })
        .collect();

    Ok(AgenticTrace {
        manifest: loaded.manifest,
        turns,
    })
}

fn latest_request_starting_before(
    requests: &[RequestEntry],
    indices: &[usize],
    timestamp_ms: i64,
) -> Option<usize> {
    indices
        .iter()
        .copied()
        .filter(|index| requests[*index].start_ms <= timestamp_ms)
        .max_by_key(|index| requests[*index].start_ms)
}

fn first_request_starting_after(
    requests: &[RequestEntry],
    indices: &[usize],
    timestamp_ms: i64,
) -> Option<usize> {
    indices
        .iter()
        .copied()
        .filter(|index| requests[*index].start_ms >= timestamp_ms)
        .min_by_key(|index| requests[*index].start_ms)
}

fn push_unique(values: &mut Vec<usize>, value: usize) {
    if !values.contains(&value) {
        values.push(value);
    }
}

fn validate_dependency_dag(requests: &[RequestEntry], dependencies: &[Vec<usize>]) -> Result<()> {
    let mut indegree = dependencies.iter().map(Vec::len).collect::<Vec<_>>();
    let mut dependents = vec![Vec::new(); requests.len()];
    for (request_index, request_dependencies) in dependencies.iter().enumerate() {
        for dependency in request_dependencies {
            if *dependency >= requests.len() {
                bail!(
                    "request {} depends on unknown request index {}",
                    requests[request_index].request.source_request_id,
                    dependency
                );
            }
            if *dependency == request_index {
                bail!(
                    "request {} cannot depend on itself",
                    requests[request_index].request.source_request_id
                );
            }
            dependents[*dependency].push(request_index);
        }
    }

    let mut ready = indegree
        .iter()
        .enumerate()
        .filter_map(|(index, count)| (*count == 0).then_some(index))
        .collect::<VecDeque<_>>();
    let mut visited = 0;
    while let Some(index) = ready.pop_front() {
        visited += 1;
        for dependent in &dependents[index] {
            indegree[*dependent] -= 1;
            if indegree[*dependent] == 0 {
                ready.push_back(*dependent);
            }
        }
    }
    if visited != requests.len() {
        bail!("agentic request dependencies contain a cycle");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::super::{ClaudeToolReplayMetrics, ToolEntry};
    use super::*;
    use agent_loadgen_core::{AgentContext, TraceRequest};

    fn entry(
        request_id: &str,
        session_id: &str,
        parent_session_id: Option<&str>,
        start_ms: i64,
        end_ms: i64,
    ) -> RequestEntry {
        RequestEntry {
            start_ms,
            end_ms,
            request: TraceRequest {
                ordinal: 0,
                source_request_id: request_id.to_string(),
                source_x_request_id: None,
                source_model: None,
                input_tokens: 2,
                output_tokens: 1,
                request_received_ms: start_ms as u64,
                trace_block_size: 2,
                input_sequence_hashes: vec![11],
                agent_context: Some(AgentContext {
                    session_id: session_id.to_string(),
                    parent_session_id: parent_session_id.map(str::to_string),
                    compaction: None,
                    input_trigger: None,
                }),
            },
        }
    }

    fn manifest(count: usize) -> TraceManifest {
        TraceManifest {
            request_count: count,
            session_count: count,
            requests_with_agent_context: count,
            first_request_received_ms: 1_000,
            last_request_received_ms: 1_000,
            duration_ms: 0,
            input_tokens: (count * 2) as u64,
            output_tokens: count as u64,
            distinct_sequence_hashes: 1,
            trace_block_size: 2,
            source_digest_sha256: "source".to_string(),
        }
    }

    #[test]
    fn infers_subagent_launch_and_join() {
        let trace = lower(LoadedTrace {
            requests: vec![
                entry("parent-1", "root", None, 1_000, 1_100),
                entry("child-1", "child", Some("root"), 1_200, 1_300),
                entry("parent-2", "root", None, 1_500, 1_600),
            ],
            tools: Vec::new(),
            manifest: manifest(3),
        })
        .unwrap();

        assert_eq!(trace.turns[1].dependencies, vec![0]);
        assert_eq!(trace.turns[2].dependencies, vec![0, 1]);
        assert_eq!(trace.turns[2].delay_after_dependencies_ms, 200);
    }

    #[test]
    fn rejects_conflicting_session_parents() {
        let error = lower(LoadedTrace {
            requests: vec![
                entry("child-1", "child", Some("root-a"), 1_000, 1_100),
                entry("child-2", "child", Some("root-b"), 1_200, 1_300),
            ],
            tools: Vec::new(),
            manifest: manifest(2),
        })
        .unwrap_err();
        assert!(error.to_string().contains("conflicting parent_session_id"));
    }

    #[test]
    fn explicit_background_metadata_preserves_spawn_and_late_join() {
        let trace = lower(LoadedTrace {
            requests: vec![
                entry("parent-1", "root", None, 1_000, 1_100),
                entry("child-1", "child", Some("root"), 1_200, 1_700),
                entry("parent-2", "root", None, 1_300, 1_400),
                entry("parent-3", "root", None, 1_850, 1_950),
            ],
            tools: vec![ToolEntry {
                session_id: "root".to_string(),
                tool_call_id: "agent-call".to_string(),
                claude: Some(ClaudeToolReplayMetrics {
                    source_request_id: "parent-1".to_string(),
                    consumer_request_id: Some("parent-3".to_string()),
                    child_session_id: Some("child".to_string()),
                    execution_mode: "background".to_string(),
                }),
            }],
            manifest: manifest(4),
        })
        .unwrap();

        assert_eq!(trace.turns[1].dependencies, vec![0]);
        assert_eq!(trace.turns[2].dependencies, vec![0]);
        assert_eq!(trace.turns[3].dependencies, vec![2, 1]);
        assert_eq!(trace.turns[3].delay_after_dependencies_ms, 150);
    }
}
