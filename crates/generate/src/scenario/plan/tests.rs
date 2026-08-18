// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::scenario::config::UIntDistribution;
use agent_loadgen_core::AgentKind;

fn config(agent: AgentKind) -> ResolvedGeneratorConfig {
    let mut config = ResolvedGeneratorConfig::preset(agent, 42);
    config.root_sessions = 3;
    config.concurrent_agents = 3;
    config.turns = UIntDistribution::fixed(4);
    config.subagent_turns = UIntDistribution::fixed(2);
    config.system_prefix_tokens = UIntDistribution::fixed(64);
    config.tool_catalog_tokens = UIntDistribution::fixed(64);
    config.repository_tokens = UIntDistribution::fixed(32);
    config.session_tokens = UIntDistribution::fixed(16);
    config.user_tokens = UIntDistribution::fixed(16);
    config.output_tokens = UIntDistribution::fixed(8);
    config.context_window_tokens = 512;
    config.compaction_trigger_fraction = 0.75;
    config.max_nodes = 500;
    config.max_sessions = 100;
    config.max_total_input_tokens = 1_000_000;
    config
}

#[test]
fn generation_is_seed_deterministic() {
    let first = GeneratedScenario::generate(config(AgentKind::Codex)).unwrap();
    let second = GeneratedScenario::generate(config(AgentKind::Codex)).unwrap();
    assert_eq!(first.scenario_digest_sha256, second.scenario_digest_sha256);
    assert_eq!(first.nodes.len(), second.nodes.len());
}

#[test]
fn graph_dependencies_only_point_backward() {
    let scenario = GeneratedScenario::generate(config(AgentKind::ClaudeCode)).unwrap();
    assert!(scenario.nodes.iter().enumerate().all(|(ordinal, node)| {
        node.dependencies
            .iter()
            .all(|dependency| *dependency < ordinal)
    }));
    assert!(scenario.nodes.iter().any(|node| node.action == "tool"));
    assert!(
        scenario
            .nodes
            .iter()
            .filter(|node| node.action == "tool")
            .all(|node| !node.tool_events.is_empty())
    );
}

#[test]
fn profile_preserves_global_prefix_sharing() {
    let scenario = GeneratedScenario::generate(config(AgentKind::Opencode)).unwrap();
    let roots = scenario
        .nodes
        .iter()
        .filter(|node| node.root_arrival_ms.is_some())
        .collect::<Vec<_>>();
    assert_eq!(roots.len(), 3);
    assert_eq!(
        &roots[0].request.input_sequence_hashes[..8],
        &roots[1].request.input_sequence_hashes[..8]
    );
}

#[test]
fn compaction_rewrites_a_window_and_emits_metadata() {
    let mut config = config(AgentKind::Codex);
    config.root_sessions = 1;
    config.concurrent_agents = 1;
    config.context_window_tokens = 160;
    config.compaction_trigger_fraction = 0.75;
    let scenario = GeneratedScenario::generate(config).unwrap();
    let compactions = scenario
        .nodes
        .iter()
        .filter(|node| node.action == "compaction_attempt")
        .collect::<Vec<_>>();
    assert!(!compactions.is_empty());
    assert_eq!(scenario.compaction_operations.len(), compactions.len());
    assert!(compactions.iter().all(|node| {
        node.request
            .agent_context
            .as_ref()
            .and_then(|context| context.compaction.as_ref())
            .is_some()
    }));
    assert!(scenario.nodes.iter().any(|node| node.window_epoch > 0));
}

#[test]
fn compaction_attempts_share_identity_and_apply_once() {
    let mut config = config(AgentKind::Codex);
    config.root_sessions = 1;
    config.concurrent_agents = 1;
    config.context_window_tokens = 160;
    config.compaction_trigger_fraction = 0.75;
    config.compaction_abort_probability = 1.0;
    config.compaction_retry_probability = 1.0;
    config.compaction_max_attempts = 3;

    let scenario = GeneratedScenario::generate(config).unwrap();
    let operation = scenario.compaction_operations.first().unwrap();
    assert_eq!(operation.attempts.len(), 3);
    assert_eq!(operation.applied_attempt, 2);
    assert_eq!(operation.expected_apply_count, 1);

    let attempts = operation
        .attempts
        .iter()
        .map(|ordinal| &scenario.nodes[*ordinal])
        .collect::<Vec<_>>();
    assert_eq!(
        attempts
            .iter()
            .map(|node| node.compaction_attempt.as_ref().unwrap().expected_effect)
            .collect::<Vec<_>>(),
        vec![
            CompactionExpectedEffect::NoMutationAborted,
            CompactionExpectedEffect::ApplyOnce,
            CompactionExpectedEffect::DuplicateNoop,
        ]
    );
    assert!(attempts.iter().enumerate().all(|(index, node)| {
        let attempt = node.compaction_attempt.as_ref().unwrap();
        attempt.operation_id == operation.operation_id
            && attempt.phase == operation.phase
            && attempt.attempt == index + 1
            && node.output_budget_tokens.is_none()
    }));
}

#[test]
fn blocking_swarm_joins_all_children() {
    let mut config = config(AgentKind::ClaudeCode);
    config.root_sessions = 1;
    config.concurrent_agents = 1;
    config.turns = UIntDistribution::fixed(2);
    config.subagent_turns = UIntDistribution::fixed(1);
    config.tool_probability = 0.0;
    config.parallel_tool_probability = 0.0;
    config.subagent_probability = 0.0;
    config.swarm_probability = 1.0;
    config.completion_probability = 0.0;
    config.fanout = UIntDistribution::fixed(3);
    config.blocking_probability = 1.0;
    config.compaction_enabled = false;
    let scenario = GeneratedScenario::generate(config).unwrap();
    assert_eq!(scenario.sessions.len(), 4);
    let root_completion = scenario
        .nodes
        .iter()
        .find(|node| {
            node.action == "complete"
                && node
                    .request
                    .agent_context
                    .as_ref()
                    .unwrap()
                    .parent_session_id
                    .is_none()
        })
        .unwrap();
    assert_eq!(root_completion.dependencies.len(), 3);
    let swarm = scenario
        .nodes
        .iter()
        .find(|node| node.action == "swarm")
        .unwrap();
    assert_eq!(swarm.spawned_session_ids.len(), 3);
    assert!(scenario.sessions.iter().skip(1).all(|session| {
        session.parent_session_id.as_deref() == Some(scenario.sessions[0].session_id.as_str())
    }));
}

#[test]
fn closed_loop_slot_restarts_after_its_previous_root_completes() {
    let mut config = config(AgentKind::Codex);
    config.concurrent_agents = 1;
    config.turns = UIntDistribution::fixed(1);
    config.tool_probability = 0.0;
    config.parallel_tool_probability = 0.0;
    config.subagent_probability = 0.0;
    config.swarm_probability = 0.0;
    config.completion_probability = 0.0;
    config.compaction_enabled = false;
    config.restart_delay_ms = UIntDistribution::fixed(17);

    let scenario = GeneratedScenario::generate(config).unwrap();
    assert_eq!(scenario.nodes.len(), 3);
    assert_eq!(scenario.nodes[0].root_arrival_ms, Some(0));
    assert!(scenario.nodes[0].dependencies.is_empty());
    assert_eq!(scenario.nodes[1].root_arrival_ms, None);
    assert_eq!(scenario.nodes[1].dependencies, vec![0]);
    assert_eq!(scenario.nodes[1].delay_after_dependencies_ms, 17);
    assert_eq!(scenario.nodes[2].dependencies, vec![1]);
    assert_eq!(scenario.nodes[2].delay_after_dependencies_ms, 17);
    assert!(
        scenario
            .sessions
            .iter()
            .all(|session| session.root_agent_slot == 0)
    );
}

#[test]
fn reports_call_and_time_weighted_tool_parallelism() {
    let mut config = config(AgentKind::Codex);
    config.root_sessions = 1;
    config.concurrent_agents = 1;
    config.turns = UIntDistribution::fixed(2);
    config.tool_probability = 0.0;
    config.parallel_tool_probability = 1.0;
    config.subagent_probability = 0.0;
    config.swarm_probability = 0.0;
    config.completion_probability = 0.0;
    config.compaction_enabled = false;
    config.parallel_count = UIntDistribution::fixed(3);
    for class in &mut config.tool_classes {
        class.latency_ms = UIntDistribution::fixed(10);
    }

    let scenario = GeneratedScenario::generate(config).unwrap();
    assert_eq!(scenario.tool_parallelism.tool_phases, 1);
    assert_eq!(scenario.tool_parallelism.parallel_tool_phases, 1);
    assert_eq!(scenario.tool_parallelism.tool_calls, 3);
    assert_eq!(scenario.tool_parallelism.parallel_tool_calls, 3);
    assert_eq!(scenario.tool_parallelism.parallel_call_fraction, 1.0);
    assert_eq!(scenario.tool_parallelism.tool_work_ms, 30);
    assert_eq!(scenario.tool_parallelism.tool_wall_ms, 10);
    assert_eq!(scenario.tool_parallelism.parallel_wall_ms, 10);
    assert_eq!(scenario.tool_parallelism.parallel_wall_time_fraction, 1.0);
    assert_eq!(scenario.tool_parallelism.work_to_wall_ratio, 3.0);
}
