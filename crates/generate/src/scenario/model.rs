// SPDX-License-Identifier: Apache-2.0

use serde::{Deserialize, Serialize};

use super::config::ResolvedGeneratorConfig;
use agent_loadgen_core::{TraceManifest, TraceRequest};

#[derive(Debug, Clone, Serialize)]
pub struct GeneratedScenario {
    pub schema_version: u32,
    pub profile_digest_sha256: String,
    pub scenario_digest_sha256: String,
    pub config: ResolvedGeneratorConfig,
    pub sessions: Vec<GeneratedSession>,
    pub nodes: Vec<GeneratedNode>,
    pub tool_parallelism: GeneratedToolParallelism,
    pub compaction_operations: Vec<GeneratedCompactionOperation>,
    pub trace_manifest: TraceManifest,
}

#[derive(Debug, Clone, Serialize)]
pub struct GeneratedSession {
    pub session_id: String,
    pub parent_session_id: Option<String>,
    pub depth: usize,
    pub root_agent_slot: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct GeneratedNode {
    pub node_id: String,
    pub action: String,
    pub dependencies: Vec<usize>,
    pub delay_after_dependencies_ms: u64,
    pub root_arrival_ms: Option<u64>,
    pub window_epoch: usize,
    pub tool_events: Vec<GeneratedToolEvent>,
    pub spawned_session_ids: Vec<String>,
    pub output_budget_tokens: Option<u32>,
    pub compaction_attempt: Option<GeneratedCompactionAttempt>,
    pub request: TraceRequest,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CompactionExpectedEffect {
    NoMutationAborted,
    ApplyOnce,
    DuplicateNoop,
}

#[derive(Debug, Clone, Serialize)]
pub struct GeneratedCompactionAttempt {
    pub operation_id: String,
    pub phase: String,
    pub attempt: usize,
    pub expected_effect: CompactionExpectedEffect,
    pub abort_after_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct GeneratedCompactionOperation {
    pub operation_id: String,
    pub session_id: String,
    pub phase: String,
    pub attempts: Vec<usize>,
    pub applied_attempt: usize,
    pub expected_apply_count: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct GeneratedToolEvent {
    pub class: String,
    pub latency_ms: u64,
    pub result_tokens: u64,
    pub failed: bool,
    pub retried: bool,
}

/// Parallelism realized by the sampled tool phases in this scenario.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct GeneratedToolParallelism {
    pub tool_phases: usize,
    pub parallel_tool_phases: usize,
    pub tool_calls: usize,
    pub parallel_tool_calls: usize,
    pub parallel_call_fraction: f64,
    pub tool_work_ms: u64,
    pub tool_wall_ms: u64,
    pub parallel_wall_ms: u64,
    pub parallel_wall_time_fraction: f64,
    pub work_to_wall_ratio: f64,
}
