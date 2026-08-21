// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Dynamo request-trace loading and agentic lowering inputs.

use std::collections::{BTreeSet, HashSet};
use std::fs::File;
use std::io::{BufRead, BufReader, Read};
use std::path::{Path, PathBuf};

pub use agent_loadgen_core::{AgentContext, TraceManifest, TraceRequest};
use anyhow::{Context, Result, bail};
use flate2::read::MultiGzDecoder;
use serde::Deserialize;
use serde::de::IgnoredAny;
use sha2::{Digest, Sha256};

mod agentic;
pub mod compare;

pub use agentic::{AgenticToolEvent, AgenticTrace, AgenticTurn};

const TRACE_SCHEMA_V1: &str = "dynamo.request.trace.v1";

#[derive(Debug, Clone, Deserialize)]
struct TraceRecord {
    schema: String,
    event_type: String,
    event_time_unix_ms: u64,
    #[serde(default)]
    agent_context: Option<AgentContext>,
    #[serde(default)]
    request: Option<RequestMetrics>,
    #[serde(default)]
    tool: Option<ToolMetrics>,
}

/// A single direct record or the nested `event` in a wrapped record.
///
/// All fields are optional while deserializing so `request_payload` records can
/// be discarded before their required request-end fields are validated.
#[derive(Debug, Deserialize)]
struct TraceRecordFields {
    #[serde(default)]
    schema: Option<String>,
    #[serde(default)]
    event_type: Option<String>,
    #[serde(default)]
    event_time_unix_ms: Option<u64>,
    #[serde(default)]
    agent_context: Option<AgentContext>,
    #[serde(default)]
    request: Option<RequestMetrics>,
    #[serde(default)]
    tool: Option<ToolMetrics>,
}

impl TraceRecordFields {
    fn event_type(&self) -> Option<&str> {
        self.event_type.as_deref()
    }

    fn finish(self) -> Result<TraceRecord> {
        Ok(TraceRecord {
            schema: self.schema.context("trace record has no schema")?,
            event_type: self.event_type.context("trace record has no event_type")?,
            event_time_unix_ms: self
                .event_time_unix_ms
                .context("trace record has no event_time_unix_ms")?,
            agent_context: self.agent_context,
            request: self.request,
            tool: self.tool,
        })
    }
}

#[derive(Debug, Clone, Deserialize)]
struct RequestMetrics {
    request_id: String,
    #[serde(default)]
    x_request_id: Option<String>,
    #[serde(default)]
    model: Option<String>,
    #[serde(default)]
    input_tokens: Option<u64>,
    #[serde(default)]
    output_tokens: Option<u64>,
    #[serde(default)]
    request_received_ms: Option<u64>,
    #[serde(default)]
    total_time_ms: Option<f64>,
    #[serde(default)]
    replay: Option<ReplayMetrics>,
}

#[derive(Debug, Clone, Deserialize)]
struct ReplayMetrics {
    trace_block_size: usize,
    input_length: usize,
    input_sequence_hashes: Vec<u64>,
}

#[derive(Debug, Clone, Deserialize)]
struct ToolMetrics {
    tool_call_id: String,
    tool_class: String,
    #[serde(default)]
    claude: Option<ClaudeToolReplayMetrics>,
    #[serde(default)]
    started_at_unix_ms: Option<u64>,
    #[serde(default)]
    ended_at_unix_ms: Option<u64>,
    #[serde(default)]
    duration_ms: Option<f64>,
    #[serde(default)]
    status: Option<String>,
    #[serde(default)]
    output_bytes: Option<u64>,
    #[serde(default)]
    output_tokens: Option<u64>,
    #[serde(default)]
    error_type: Option<String>,
}

/// Claude-only evidence used to disambiguate a subagent launch and join.
#[derive(Debug, Clone, Deserialize)]
pub(crate) struct ClaudeToolReplayMetrics {
    pub(crate) source_request_id: String,
    #[serde(default)]
    pub(crate) consumer_request_id: Option<String>,
    #[serde(default)]
    pub(crate) child_session_id: Option<String>,
    pub(crate) execution_mode: String,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum JsonLineEnvelope {
    Object(Box<TraceRecordEnvelope>),
    Other(IgnoredAny),
}

#[derive(Debug, Deserialize)]
struct TraceRecordEnvelope {
    #[serde(flatten)]
    record: TraceRecordFields,
    #[serde(default)]
    event: Option<TraceEventEnvelope>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum TraceEventEnvelope {
    Object(Box<TraceRecordFields>),
    Other(IgnoredAny),
}

#[derive(Debug, Clone)]
pub(crate) struct RequestEntry {
    pub(crate) request: TraceRequest,
    pub(crate) start_ms: i64,
    pub(crate) end_ms: i64,
}

#[derive(Debug, Clone)]
pub(crate) struct ToolEntry {
    pub(crate) session_id: String,
    pub(crate) start_ms: i64,
    pub(crate) end_ms: i64,
    pub(crate) tool_call_id: String,
    pub(crate) tool_class: String,
    pub(crate) claude: Option<ClaudeToolReplayMetrics>,
    pub(crate) status: String,
    pub(crate) duration_ms: f64,
    pub(crate) output_bytes: Option<u64>,
    pub(crate) output_tokens: Option<u64>,
    pub(crate) error_type: Option<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct LoadedTrace {
    pub(crate) requests: Vec<RequestEntry>,
    pub(crate) tools: Vec<ToolEntry>,
    pub(crate) manifest: TraceManifest,
}

pub fn load_agentic_trace(
    paths: &[PathBuf],
    max_requests: Option<usize>,
    session_id: Option<&str>,
) -> Result<AgenticTrace> {
    agentic::lower(load_trace(paths, max_requests, session_id)?)
}

pub(crate) fn load_trace(
    paths: &[PathBuf],
    max_requests: Option<usize>,
    session_id: Option<&str>,
) -> Result<LoadedTrace> {
    if paths.is_empty() {
        bail!("at least one trace path is required");
    }

    let mut requests = Vec::new();
    let mut tools = Vec::new();
    let mut request_ids = HashSet::new();
    let mut source_digest = Sha256::new();
    for path in paths {
        read_path(path, &mut source_digest, |record| {
            match record.event_type.as_str() {
                "request_payload" | "tool_start" => {}
                "request_end" => {
                    let entry = compile_request(record).context("invalid request_end record")?;
                    if !request_ids.insert(entry.request.source_request_id.clone()) {
                        bail!("duplicate request_id {}", entry.request.source_request_id);
                    }
                    requests.push(entry);
                }
                "tool_end" | "tool_error" => {
                    let terminal_event = record.event_type.clone();
                    if let Some(tool) = compile_tool(record, &terminal_event) {
                        tools.push(tool);
                    }
                }
                event_type => bail!(
                    "request trace only supports request_end, request_payload, and tool_* events, got {event_type}"
                ),
            }
            Ok(())
        })?;
    }
    if requests.is_empty() {
        bail!("the trace contains no request_end records with replay metadata");
    }
    if requests
        .iter()
        .any(|entry| entry.request.agent_context.is_none())
    {
        bail!("agent-loadgen requires agent_context on every request");
    }

    requests.sort_by(|left, right| {
        (left.start_ms, left.end_ms, &left.request.source_request_id).cmp(&(
            right.start_ms,
            right.end_ms,
            &right.request.source_request_id,
        ))
    });
    if let Some(session_id) = session_id {
        requests.retain(|entry| {
            entry
                .request
                .agent_context
                .as_ref()
                .is_some_and(|context| context.session_id == session_id)
        });
        if requests.is_empty() {
            bail!("the trace contains no requests for session {session_id:?}");
        }
    }
    if let Some(limit) = max_requests {
        requests.truncate(limit);
    }
    if requests.is_empty() {
        bail!("the selected trace contains no requests");
    }
    for (ordinal, entry) in requests.iter_mut().enumerate() {
        entry.request.ordinal = ordinal;
    }

    let selected_sessions = requests
        .iter()
        .filter_map(|entry| entry.request.agent_context.as_ref())
        .map(|context| context.session_id.as_str())
        .collect::<HashSet<_>>();
    tools.retain(|tool| selected_sessions.contains(tool.session_id.as_str()));

    let manifest = make_manifest(
        requests.iter().map(|entry| &entry.request),
        source_digest.finalize().as_slice(),
    )?;
    Ok(LoadedTrace {
        requests,
        tools,
        manifest,
    })
}

fn read_path(
    path: &Path,
    digest: &mut Sha256,
    mut visit: impl FnMut(TraceRecord) -> Result<()>,
) -> Result<()> {
    let reader = open_trace(path)?;
    for (line_index, line) in reader.lines().enumerate() {
        let line =
            line.with_context(|| format!("failed to read {}:{}", path.display(), line_index + 1))?;
        if line.trim().is_empty() {
            continue;
        }
        digest.update(line.as_bytes());
        digest.update(b"\n");
        let Some(record) = parse_trace_record(&line).with_context(|| {
            format!(
                "invalid trace record at {}:{}",
                path.display(),
                line_index + 1
            )
        })?
        else {
            continue;
        };
        if record.schema != TRACE_SCHEMA_V1 {
            bail!(
                "unsupported trace schema {:?} at {}:{}",
                record.schema,
                path.display(),
                line_index + 1
            );
        }
        visit(record).with_context(|| {
            format!(
                "invalid trace record at {}:{}",
                path.display(),
                line_index + 1
            )
        })?;
    }
    Ok(())
}

fn parse_trace_record(line: &str) -> Result<Option<TraceRecord>> {
    let envelope = match serde_json::from_str::<JsonLineEnvelope>(line)? {
        JsonLineEnvelope::Object(envelope) => *envelope,
        JsonLineEnvelope::Other(_) => return Ok(None),
    };
    let record = match envelope.event {
        Some(TraceEventEnvelope::Object(record)) => *record,
        Some(TraceEventEnvelope::Other(_)) => return Ok(None),
        None => envelope.record,
    };
    if record.event_type() == Some("request_payload") {
        return Ok(None);
    }
    Ok(Some(record.finish()?))
}

fn open_trace(path: &Path) -> Result<Box<dyn BufRead>> {
    let file = File::open(path).with_context(|| format!("failed to open {}", path.display()))?;
    let is_gzip = path.extension().is_some_and(|extension| extension == "gz");
    let reader: Box<dyn Read> = if is_gzip {
        Box::new(MultiGzDecoder::new(file))
    } else {
        Box::new(file)
    };
    Ok(Box::new(BufReader::new(reader)))
}

fn compile_request(record: TraceRecord) -> Result<RequestEntry> {
    let TraceRecord {
        event_time_unix_ms,
        agent_context,
        request,
        ..
    } = record;
    let RequestMetrics {
        request_id,
        x_request_id,
        model,
        input_tokens,
        output_tokens,
        request_received_ms,
        total_time_ms,
        replay,
    } = request.context("request_end has no request metrics")?;
    let ReplayMetrics {
        trace_block_size,
        input_length,
        input_sequence_hashes,
    } = replay.context("request_end has no replay metrics")?;
    if trace_block_size == 0 {
        bail!("trace_block_size must be greater than zero");
    }
    if input_length == 0 {
        bail!("input_length must be greater than zero");
    }
    let required_hashes = input_length.div_ceil(trace_block_size);
    if input_sequence_hashes.len() != required_hashes {
        bail!(
            "request {} has {} replay hashes, expected {}",
            request_id,
            input_sequence_hashes.len(),
            required_hashes
        );
    }
    if input_tokens.is_some_and(|input_tokens| input_tokens != input_length as u64) {
        bail!(
            "request {} input_tokens does not match replay input_length",
            request_id
        );
    }
    let output_tokens = output_tokens.context("request has no output_tokens")?;
    let output_tokens = u32::try_from(output_tokens).context("output_tokens does not fit u32")?;
    if output_tokens == 0 {
        bail!("request {} has zero output tokens", request_id);
    }
    let request_received_ms = request_received_ms.context("request has no request_received_ms")?;
    let total_ms = total_time_ms
        .map(|value| value.max(0.0).round() as u64)
        .unwrap_or_else(|| event_time_unix_ms.saturating_sub(request_received_ms));
    let request_completed_ms = request_received_ms.saturating_add(total_ms);

    Ok(RequestEntry {
        start_ms: saturating_i64(request_received_ms),
        end_ms: saturating_i64(request_completed_ms),
        request: TraceRequest {
            ordinal: 0,
            source_request_id: request_id,
            source_x_request_id: x_request_id,
            source_model: model,
            input_tokens: input_length,
            output_tokens,
            request_received_ms,
            trace_block_size,
            input_sequence_hashes,
            agent_context,
        },
    })
}

fn compile_tool(record: TraceRecord, terminal_event: &str) -> Option<ToolEntry> {
    let context = record.agent_context?;
    let tool = record.tool?;
    let end_ms = tool
        .ended_at_unix_ms
        .map(saturating_i64)
        .unwrap_or_else(|| saturating_i64(record.event_time_unix_ms));
    let start_ms = tool
        .started_at_unix_ms
        .map(saturating_i64)
        .or_else(|| {
            tool.duration_ms
                .map(|duration| end_ms.saturating_sub(duration.max(0.0).round() as i64))
        })
        .unwrap_or(end_ms);
    if end_ms < start_ms {
        return None;
    }
    let duration_ms = tool
        .duration_ms
        .unwrap_or_else(|| (end_ms - start_ms).max(0) as f64);
    Some(ToolEntry {
        session_id: context.session_id,
        start_ms,
        end_ms,
        tool_call_id: tool.tool_call_id,
        tool_class: tool.tool_class,
        claude: tool.claude,
        status: tool.status.unwrap_or_else(|| {
            if terminal_event == "tool_error" {
                "error".to_string()
            } else {
                "succeeded".to_string()
            }
        }),
        duration_ms,
        output_bytes: tool.output_bytes,
        output_tokens: tool.output_tokens,
        error_type: tool.error_type,
    })
}

fn make_manifest<'a>(
    requests: impl IntoIterator<Item = &'a TraceRequest>,
    source_digest: &[u8],
) -> Result<TraceManifest> {
    let mut requests = requests.into_iter();
    let first = requests.next().context("trace has no requests")?;
    let mut sessions = BTreeSet::new();
    let mut hashes = BTreeSet::new();
    let mut block_sizes = BTreeSet::new();
    let mut input_tokens = first.input_tokens as u64;
    let mut output_tokens = first.output_tokens as u64;
    let mut request_count = 1_usize;
    let mut last_request_received_ms = first.request_received_ms;

    let context = first
        .agent_context
        .as_ref()
        .context("agent-loadgen requires agent_context on every request")?;
    sessions.insert(&context.session_id);
    hashes.extend(first.input_sequence_hashes.iter().copied());
    block_sizes.insert(first.trace_block_size);

    for request in requests {
        let context = request
            .agent_context
            .as_ref()
            .context("agent-loadgen requires agent_context on every request")?;
        sessions.insert(&context.session_id);
        hashes.extend(request.input_sequence_hashes.iter().copied());
        block_sizes.insert(request.trace_block_size);
        input_tokens = input_tokens
            .checked_add(request.input_tokens as u64)
            .context("input token count overflow")?;
        output_tokens = output_tokens
            .checked_add(request.output_tokens as u64)
            .context("output token count overflow")?;
        last_request_received_ms = request.request_received_ms;
        request_count += 1;
    }
    if block_sizes.len() != 1 {
        bail!("shape-strict replay requires one trace_block_size, found {block_sizes:?}");
    }

    Ok(TraceManifest {
        request_count,
        session_count: sessions.len(),
        requests_with_agent_context: request_count,
        first_request_received_ms: first.request_received_ms,
        last_request_received_ms,
        duration_ms: last_request_received_ms.saturating_sub(first.request_received_ms),
        input_tokens,
        output_tokens,
        distinct_sequence_hashes: hashes.len(),
        trace_block_size: *block_sizes.first().expect("one block size exists"),
        source_digest_sha256: hex::encode(source_digest),
    })
}

fn saturating_i64(value: u64) -> i64 {
    value.min(i64::MAX as u64) as i64
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use tempfile::NamedTempFile;

    use super::*;

    fn request_record(
        request_id: &str,
        session_id: &str,
        received_ms: u64,
        completed_ms: u64,
        hashes: &[u64],
    ) -> serde_json::Value {
        serde_json::json!({
            "schema": TRACE_SCHEMA_V1,
            "event_type": "request_end",
            "event_time_unix_ms": completed_ms,
            "agent_context": {"session_id": session_id},
            "request": {
                "request_id": request_id,
                "input_tokens": hashes.len() * 2,
                "output_tokens": 2,
                "request_received_ms": received_ms,
                "replay": {
                    "trace_block_size": 2,
                    "input_length": hashes.len() * 2,
                    "input_sequence_hashes": hashes
                }
            }
        })
    }

    #[test]
    fn loads_and_lowers_agent_requests() {
        let mut file = NamedTempFile::new().unwrap();
        writeln!(
            file,
            "{}",
            request_record("r1", "thread-1", 1_000, 1_100, &[11])
        )
        .unwrap();
        writeln!(
            file,
            "{}",
            request_record("r2", "thread-1", 1_300, 1_400, &[11, 22])
        )
        .unwrap();

        let trace = load_agentic_trace(&[file.path().to_path_buf()], None, None).unwrap();
        assert_eq!(trace.turns.len(), 2);
        assert!(trace.turns[0].dependencies.is_empty());
        assert_eq!(trace.turns[0].root_arrival_ms, Some(0));
        assert_eq!(trace.turns[1].dependencies, vec![0]);
        assert_eq!(trace.turns[1].delay_after_dependencies_ms, 200);
    }

    #[test]
    fn rejects_context_free_requests() {
        let mut file = NamedTempFile::new().unwrap();
        let mut record = request_record("r1", "thread-1", 1_000, 1_100, &[11]);
        record.as_object_mut().unwrap().remove("agent_context");
        writeln!(file, "{record}").unwrap();

        let error = load_agentic_trace(&[file.path().to_path_buf()], None, None).unwrap_err();
        assert!(error.to_string().contains("requires agent_context"));
    }

    #[test]
    fn rejects_mixed_agent_and_context_free_requests() {
        let mut file = NamedTempFile::new().unwrap();
        writeln!(
            file,
            "{}",
            request_record("r1", "thread-1", 1_000, 1_100, &[11])
        )
        .unwrap();
        let mut record = request_record("r2", "thread-1", 1_200, 1_300, &[22]);
        record.as_object_mut().unwrap().remove("agent_context");
        writeln!(file, "{record}").unwrap();

        let error = load_agentic_trace(&[file.path().to_path_buf()], None, None).unwrap_err();
        assert!(error.to_string().contains("requires agent_context"));
    }

    #[test]
    fn rejects_duplicate_request_ids_during_loading() {
        let mut file = NamedTempFile::new().unwrap();
        writeln!(
            file,
            "{}",
            request_record("duplicate", "thread-1", 1_000, 1_100, &[11])
        )
        .unwrap();
        writeln!(
            file,
            "{}",
            request_record("duplicate", "thread-1", 1_200, 1_300, &[22])
        )
        .unwrap();

        let error = load_agentic_trace(&[file.path().to_path_buf()], None, None).unwrap_err();
        assert!(format!("{error:#}").contains("duplicate request_id duplicate"));
    }

    #[test]
    fn loads_wrapped_records_and_sorts_before_limit() {
        let mut file = NamedTempFile::new().unwrap();
        let late = request_record("late", "thread-1", 1_200, 1_300, &[22]);
        let early = request_record("early", "thread-1", 1_000, 1_100, &[11]);
        writeln!(
            file,
            "{}",
            serde_json::json!({"timestamp": 1, "event": late})
        )
        .unwrap();
        writeln!(
            file,
            "{}",
            serde_json::json!({"timestamp": 2, "event": early})
        )
        .unwrap();

        let trace = load_agentic_trace(&[file.path().to_path_buf()], Some(1), None).unwrap();
        assert_eq!(trace.turns.len(), 1);
        assert_eq!(trace.turns[0].request.source_request_id, "early");
    }

    #[test]
    fn parses_direct_and_wrapped_records_once_while_skipping_payloads() {
        let direct = request_record("direct", "thread-1", 1_000, 1_100, &[11]);
        let direct = parse_trace_record(&direct.to_string()).unwrap().unwrap();
        assert_eq!(direct.request.unwrap().request_id, "direct");

        let wrapped = serde_json::json!({
            "timestamp": 1,
            "event": request_record("wrapped", "thread-1", 1_000, 1_100, &[11]),
        });
        let wrapped = parse_trace_record(&wrapped.to_string()).unwrap().unwrap();
        assert_eq!(wrapped.request.unwrap().request_id, "wrapped");

        let payload = serde_json::json!({"event_type": "request_payload", "payload": [1, 2]});
        assert!(parse_trace_record(&payload.to_string()).unwrap().is_none());

        let malformed = serde_json::json!({"event_type": "request_end"});
        assert!(parse_trace_record(&malformed.to_string()).is_err());
    }
}
