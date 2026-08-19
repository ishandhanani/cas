// SPDX-License-Identifier: Apache-2.0

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct AgentContext {
    pub session_id: String,
    #[serde(default)]
    pub parent_session_id: Option<String>,
    #[serde(default)]
    pub compaction: Option<serde_json::Value>,
    #[serde(default)]
    pub input_trigger: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct TraceRequest {
    pub ordinal: usize,
    pub source_request_id: String,
    pub source_x_request_id: Option<String>,
    pub source_model: Option<String>,
    pub input_tokens: usize,
    pub output_tokens: u32,
    pub request_received_ms: u64,
    pub trace_block_size: usize,
    pub input_sequence_hashes: Vec<u64>,
    pub agent_context: Option<AgentContext>,
}

#[derive(Debug, Clone, Serialize)]
pub struct TraceManifest {
    pub request_count: usize,
    pub session_count: usize,
    pub requests_with_agent_context: usize,
    pub first_request_received_ms: u64,
    pub last_request_received_ms: u64,
    pub duration_ms: u64,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub distinct_sequence_hashes: usize,
    pub trace_block_size: usize,
    pub source_digest_sha256: String,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct Percentiles {
    pub min: f64,
    pub p50: f64,
    pub p95: f64,
    pub p99: f64,
    pub max: f64,
}

pub fn percentiles(mut values: Vec<f64>) -> Percentiles {
    if values.is_empty() {
        return Percentiles::default();
    }
    values.sort_by(f64::total_cmp);
    Percentiles {
        min: values[0],
        p50: percentile(&values, 0.50),
        p95: percentile(&values, 0.95),
        p99: percentile(&values, 0.99),
        max: *values.last().expect("values are not empty"),
    }
}

fn percentile(values: &[f64], quantile: f64) -> f64 {
    let rank = (values.len() as f64 * quantile).ceil() as usize;
    let index = rank.saturating_sub(1).min(values.len() - 1);
    values[index]
}
