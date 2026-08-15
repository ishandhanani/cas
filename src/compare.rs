// SPDX-License-Identifier: Apache-2.0

use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};

use crate::replay::Percentiles;
use crate::trace::{LoadedTrace, TraceRequest, load_trace};

#[derive(Debug, Deserialize)]
struct RequestMapping {
    source_request_id: String,
    replay_request_id: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct FidelityReport {
    pub source_request_count: usize,
    pub replay_request_count: usize,
    pub mapped_request_count: usize,
    pub missing_source_requests: usize,
    pub missing_replay_requests: usize,
    pub trace_block_size_matches: usize,
    pub input_length_matches: usize,
    pub output_length_matches: usize,
    pub agent_context_matches: usize,
    pub prefix_topology_matches: bool,
    pub arrival_error_ms: ArrivalError,
    pub mismatches: Vec<String>,
    pub passed: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct ArrivalError {
    pub absolute: Percentiles,
    pub mean_signed: f64,
}

pub fn compare_traces(
    source_paths: &[PathBuf],
    replay_paths: &[PathBuf],
    request_results_path: &Path,
) -> Result<FidelityReport> {
    let source = load_trace(source_paths, None, None).context("failed to load the source trace")?;
    let replay = load_trace(replay_paths, None, None).context("failed to load the replay trace")?;
    let mappings = load_mappings(request_results_path)?;
    compare_loaded(&source, &replay, &mappings)
}

fn load_mappings(path: &Path) -> Result<Vec<RequestMapping>> {
    let file = File::open(path)
        .with_context(|| format!("failed to open request results {}", path.display()))?;
    BufReader::new(file)
        .lines()
        .enumerate()
        .filter_map(|(line_index, line)| match line {
            Ok(line) if line.trim().is_empty() => None,
            Ok(line) => Some(serde_json::from_str(&line).with_context(|| {
                format!(
                    "invalid request result at {}:{}",
                    path.display(),
                    line_index + 1
                )
            })),
            Err(error) => {
                Some(Err(error).with_context(|| {
                    format!("failed to read {}:{}", path.display(), line_index + 1)
                }))
            }
        })
        .collect()
}

fn compare_loaded(
    source: &LoadedTrace,
    replay: &LoadedTrace,
    mappings: &[RequestMapping],
) -> Result<FidelityReport> {
    let source_by_id: HashMap<&str, &TraceRequest> = source
        .requests
        .iter()
        .map(|request| (request.source_request_id.as_str(), request))
        .collect();
    let replay_by_x_request_id: HashMap<&str, &TraceRequest> = replay
        .requests
        .iter()
        .filter_map(|request| {
            request
                .source_x_request_id
                .as_deref()
                .map(|x_request_id| (x_request_id, request))
        })
        .collect();
    if replay_by_x_request_id.is_empty() {
        bail!("the replay trace has no x_request_id values for result mapping");
    }

    let mut pairs = Vec::new();
    let mut missing_source_requests = 0;
    let mut missing_replay_requests = 0;
    let mut mismatches = Vec::new();
    for mapping in mappings {
        let Some(source_request) = source_by_id.get(mapping.source_request_id.as_str()) else {
            missing_source_requests += 1;
            add_mismatch(
                &mut mismatches,
                format!("missing source request {}", mapping.source_request_id),
            );
            continue;
        };
        let Some(replay_request) = replay_by_x_request_id.get(mapping.replay_request_id.as_str())
        else {
            missing_replay_requests += 1;
            add_mismatch(
                &mut mismatches,
                format!("missing replay request {}", mapping.replay_request_id),
            );
            continue;
        };
        pairs.push((*source_request, *replay_request));
    }
    pairs.sort_by_key(|(source_request, _)| source_request.ordinal);

    let mut trace_block_size_matches = 0;
    let mut input_length_matches = 0;
    let mut output_length_matches = 0;
    let mut agent_context_matches = 0;
    for (source_request, replay_request) in &pairs {
        compare_field(
            &mut trace_block_size_matches,
            &mut mismatches,
            source_request.trace_block_size == replay_request.trace_block_size,
            source_request,
            "trace_block_size",
            source_request.trace_block_size,
            replay_request.trace_block_size,
        );
        compare_field(
            &mut input_length_matches,
            &mut mismatches,
            source_request.input_tokens == replay_request.input_tokens,
            source_request,
            "input_tokens",
            source_request.input_tokens,
            replay_request.input_tokens,
        );
        compare_field(
            &mut output_length_matches,
            &mut mismatches,
            source_request.output_tokens == replay_request.output_tokens,
            source_request,
            "output_tokens",
            source_request.output_tokens,
            replay_request.output_tokens,
        );
        if source_request.agent_context == replay_request.agent_context {
            agent_context_matches += 1;
        } else {
            add_mismatch(
                &mut mismatches,
                format!(
                    "request {} agent_context mismatch: source={:?}, replay={:?}",
                    source_request.source_request_id,
                    source_request.agent_context,
                    replay_request.agent_context
                ),
            );
        }
    }

    let prefix_topology_matches =
        canonical_prefix_sequences(&pairs, true) == canonical_prefix_sequences(&pairs, false);
    if !prefix_topology_matches {
        add_mismatch(
            &mut mismatches,
            "canonical prefix topology does not match".to_string(),
        );
    }
    let arrival_error_ms = arrival_error(&pairs);
    let mapped_request_count = pairs.len();
    let passed = mapped_request_count == mappings.len()
        && trace_block_size_matches == mapped_request_count
        && input_length_matches == mapped_request_count
        && output_length_matches == mapped_request_count
        && agent_context_matches == mapped_request_count
        && prefix_topology_matches;

    Ok(FidelityReport {
        source_request_count: source.requests.len(),
        replay_request_count: replay.requests.len(),
        mapped_request_count,
        missing_source_requests,
        missing_replay_requests,
        trace_block_size_matches,
        input_length_matches,
        output_length_matches,
        agent_context_matches,
        prefix_topology_matches,
        arrival_error_ms,
        mismatches,
        passed,
    })
}

fn compare_field<T: std::fmt::Display>(
    matches: &mut usize,
    mismatches: &mut Vec<String>,
    equal: bool,
    source_request: &TraceRequest,
    field: &str,
    source: T,
    replay: T,
) {
    if equal {
        *matches += 1;
    } else {
        add_mismatch(
            mismatches,
            format!(
                "request {} {field} mismatch: source={source}, replay={replay}",
                source_request.source_request_id
            ),
        );
    }
}

fn canonical_prefix_sequences(
    pairs: &[(&TraceRequest, &TraceRequest)],
    use_source: bool,
) -> Vec<Vec<u64>> {
    let mut labels = HashMap::new();
    let mut next_label = 0_u64;
    pairs
        .iter()
        .map(|(source, replay)| {
            let request = if use_source { *source } else { *replay };
            request
                .input_sequence_hashes
                .iter()
                .map(|hash| {
                    *labels.entry(*hash).or_insert_with(|| {
                        let label = next_label;
                        next_label += 1;
                        label
                    })
                })
                .collect()
        })
        .collect()
}

fn arrival_error(pairs: &[(&TraceRequest, &TraceRequest)]) -> ArrivalError {
    let Some((first_source, first_replay)) = pairs.first() else {
        return ArrivalError {
            absolute: Percentiles::default(),
            mean_signed: 0.0,
        };
    };
    let mut signed = Vec::with_capacity(pairs.len());
    for (source, replay) in pairs {
        let source_offset =
            source.request_received_ms as i128 - first_source.request_received_ms as i128;
        let replay_offset =
            replay.request_received_ms as i128 - first_replay.request_received_ms as i128;
        signed.push((replay_offset - source_offset) as f64);
    }
    let mean_signed = signed.iter().sum::<f64>() / signed.len() as f64;
    let absolute = crate::replay::percentiles(signed.iter().map(|value| value.abs()).collect());
    ArrivalError {
        absolute,
        mean_signed,
    }
}

fn add_mismatch(mismatches: &mut Vec<String>, mismatch: String) {
    const MAX_MISMATCHES: usize = 100;
    if mismatches.len() < MAX_MISMATCHES {
        mismatches.push(mismatch);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(
        id: &str,
        x_request_id: Option<&str>,
        received: u64,
        hashes: &[u64],
    ) -> TraceRequest {
        TraceRequest {
            ordinal: 0,
            source_request_id: id.to_string(),
            source_x_request_id: x_request_id.map(str::to_string),
            source_model: None,
            input_tokens: hashes.len() * 2,
            output_tokens: 2,
            request_received_ms: received,
            trace_block_size: 2,
            input_sequence_hashes: hashes.to_vec(),
            agent_context: None,
        }
    }

    #[test]
    fn canonical_labels_compare_equality_instead_of_hash_values() {
        let source_a = request("a", None, 100, &[11, 22]);
        let source_b = request("b", None, 120, &[11, 33]);
        let replay_a = request("ra", Some("x-a"), 1000, &[101, 202]);
        let replay_b = request("rb", Some("x-b"), 1020, &[101, 303]);
        let pairs = vec![(&source_a, &replay_a), (&source_b, &replay_b)];
        assert_eq!(
            canonical_prefix_sequences(&pairs, true),
            canonical_prefix_sequences(&pairs, false)
        );
        assert_eq!(arrival_error(&pairs).absolute.max, 0.0);
    }
}
