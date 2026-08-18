// SPDX-License-Identifier: Apache-2.0

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use clap::ValueEnum;
use serde::{Deserialize, Serialize};

use crate::agent::AgentKind;
use crate::scenario::{CompactionExpectedEffect, GeneratedCompactionAttempt};
use crate::token_shape::TokenDictionaryManifest;
use crate::trace::{AgentContext, TraceManifest};

mod artifacts;
mod captured;
mod generated;
mod request;

pub use captured::run_agentic_replay;
pub use generated::run_generated_scenario;

pub(crate) use artifacts::percentiles;

#[cfg(test)]
use artifacts::{RunAccumulator, dispatch_timing_matches};
#[cfg(test)]
use captured::run_replay;
#[cfg(test)]
use request::{chunk_contains_output, normalize_target};

#[derive(Debug, Clone, Copy, Serialize, ValueEnum)]
#[serde(rename_all = "kebab-case")]
pub enum HttpTransport {
    Auto,
    Http2PriorKnowledge,
}

#[derive(Debug, Clone)]
pub struct ReplayOptions {
    pub agent: AgentKind,
    pub model: String,
    pub target: String,
    pub output_dir: PathBuf,
    pub max_in_flight: usize,
    pub warmup_connections: usize,
    pub http_transport: HttpTransport,
    pub result_flush_interval: usize,
    pub max_dispatch_p99_ms: f64,
    pub max_dispatch_max_ms: f64,
    pub start_delay: Duration,
    pub timeout: Duration,
    pub time_scale: f64,
    pub token_path_verified: bool,
    pub engine_cache_mode: BTreeMap<String, String>,
    pub static_headers: Vec<(String, String)>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RequestResult {
    pub ordinal: usize,
    pub source_request_id: String,
    pub source_x_request_id: Option<String>,
    pub replay_request_id: String,
    pub session_id: Option<String>,
    pub parent_session_id: Option<String>,
    pub scheduled_offset_ms: f64,
    pub scheduler_wake_offset_ms: f64,
    pub scheduler_wake_lag_ms: f64,
    pub dispatch_offset_ms: f64,
    pub dispatch_lag_ms: f64,
    pub local_admission_lag_ms: f64,
    pub expected_input_tokens: usize,
    pub expected_output_tokens: Option<u32>,
    pub observed_output_tokens: Option<u64>,
    pub output_length_match: Option<bool>,
    pub compaction_operation_id: Option<String>,
    pub compaction_phase: Option<String>,
    pub compaction_attempt: Option<usize>,
    pub compaction_expected_effect: Option<CompactionExpectedEffect>,
    pub planned_abort_match: Option<bool>,
    pub status_code: Option<u16>,
    pub ttft_ms: Option<f64>,
    pub total_time_ms: f64,
    pub response_headers: BTreeMap<String, String>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct RunSummary {
    pub run_id: String,
    pub agent: AgentKind,
    pub model: String,
    pub target: String,
    pub time_scale: f64,
    pub max_in_flight: usize,
    pub warmup_connections: usize,
    pub http_transport: HttpTransport,
    pub result_flush_interval: usize,
    pub max_dispatch_p99_ms: f64,
    pub max_dispatch_max_ms: f64,
    pub timer_backend: &'static str,
    pub static_header_names: Vec<String>,
    pub protocol_surface: &'static str,
    pub traffic_kind: TrafficKind,
    pub scheduling_model: &'static str,
    pub token_path_verified: bool,
    pub engine_cache_mode: BTreeMap<String, String>,
    pub capacity_performance_conclusions_allowed: bool,
    pub conclusion_blockers: Vec<String>,
    pub source: TraceManifest,
    pub token_dictionary: TokenDictionaryManifest,
    pub request_count: usize,
    pub budgeted_requests: usize,
    pub unbudgeted_requests: usize,
    pub planned_aborts: usize,
    pub succeeded: usize,
    pub failed: usize,
    pub output_length_matches: usize,
    pub output_length_mismatches: usize,
    pub missing_output_usage: usize,
    pub planned_abort_matches: usize,
    pub planned_abort_mismatches: usize,
    pub scheduler_wake_lag_ms: Percentiles,
    pub dispatch_lag_ms: Percentiles,
    pub local_admission_lag_ms: Percentiles,
    pub request_fidelity_matches: bool,
    pub dispatch_timing_matches: bool,
    pub passed: bool,
    pub total_time_ms: f64,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TrafficKind {
    SyntheticKvShape,
    CapturedTrace,
}

impl TrafficKind {
    fn scheduling_model(self) -> &'static str {
        match self {
            Self::CapturedTrace => "agentic_causal",
            Self::SyntheticKvShape => "generated_causal",
        }
    }
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct Percentiles {
    pub min: f64,
    pub p50: f64,
    pub p95: f64,
    pub p99: f64,
    pub max: f64,
}

#[derive(Clone)]
struct ReplayContext {
    client: reqwest::Client,
    base: Instant,
}

struct PreparedRequest {
    metadata: PreparedMetadata,
    http_request: reqwest::Request,
}

#[derive(Clone)]
struct PreparedMetadata {
    ordinal: usize,
    source_request_id: String,
    source_x_request_id: Option<String>,
    replay_request_id: String,
    agent_context: Option<AgentContext>,
    input_tokens: usize,
    expected_output_tokens: Option<u32>,
    compaction_attempt: Option<GeneratedCompactionAttempt>,
}

struct RequestExecution {
    output_budget_tokens: Option<u32>,
    compaction_attempt: Option<GeneratedCompactionAttempt>,
}

fn millis(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1000.0
}

fn duration_ns(duration: Duration) -> u64 {
    duration.as_nanos().min(u64::MAX as u128) as u64
}

#[cfg(test)]
#[path = "replay/tests.rs"]
mod tests;
