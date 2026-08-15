// SPDX-License-Identifier: Apache-2.0

use std::collections::{BTreeSet, HashSet};
use std::fs::File;
use std::io::{BufRead, BufReader, Read};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use flate2::read::MultiGzDecoder;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

const TRACE_SCHEMA_V1: &str = "dynamo.request.trace.v1";

#[derive(Debug, Clone, Deserialize)]
struct TraceRecord {
    schema: String,
    event_type: String,
    #[serde(default)]
    agent_context: Option<AgentContext>,
    #[serde(default)]
    request: Option<RequestMetrics>,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct AgentContext {
    pub session_id: String,
    #[serde(default)]
    pub parent_session_id: Option<String>,
    #[serde(default)]
    pub session_final: Option<bool>,
    #[serde(default)]
    pub compaction: Option<serde_json::Value>,
    #[serde(default)]
    pub input_trigger: Option<String>,
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
    replay: Option<ReplayMetrics>,
}

#[derive(Debug, Clone, Deserialize)]
struct ReplayMetrics {
    trace_block_size: usize,
    input_length: usize,
    input_sequence_hashes: Vec<u64>,
}

#[derive(Debug, Clone)]
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
    pub zero_output_requests: usize,
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

#[derive(Debug, Clone)]
pub struct LoadedTrace {
    pub requests: Vec<TraceRequest>,
    pub manifest: TraceManifest,
}

pub fn load_trace(
    paths: &[PathBuf],
    max_requests: Option<usize>,
    session_id: Option<&str>,
) -> Result<LoadedTrace> {
    if paths.is_empty() {
        bail!("at least one trace path is required");
    }

    let mut requests = Vec::new();
    let mut source_digest = Sha256::new();
    for path in paths {
        load_path(path, &mut requests, &mut source_digest)?;
    }
    if requests.is_empty() {
        bail!("the trace contains no request_end records");
    }

    requests.sort_by_key(|request| (request.request_received_ms, request.ordinal));
    if let Some(session_id) = session_id {
        requests.retain(|request| {
            request
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
    for (ordinal, request) in requests.iter_mut().enumerate() {
        request.ordinal = ordinal;
    }

    let manifest = make_manifest(&requests, source_digest.finalize().as_slice())?;
    Ok(LoadedTrace { requests, manifest })
}

fn load_path(path: &Path, requests: &mut Vec<TraceRequest>, digest: &mut Sha256) -> Result<()> {
    let reader = open_trace(path)?;
    for (line_index, line) in reader.lines().enumerate() {
        let line =
            line.with_context(|| format!("failed to read {}:{}", path.display(), line_index + 1))?;
        if line.trim().is_empty() {
            continue;
        }
        digest.update(line.as_bytes());
        digest.update(b"\n");

        let value: serde_json::Value = serde_json::from_str(&line)
            .with_context(|| format!("invalid JSON at {}:{}", path.display(), line_index + 1))?;
        let record_value = value
            .get("event")
            .or_else(|| value.get("record"))
            .unwrap_or(&value);
        let record: TraceRecord =
            serde_json::from_value(record_value.clone()).with_context(|| {
                format!(
                    "invalid trace record at {}:{}",
                    path.display(),
                    line_index + 1
                )
            })?;
        if record.schema != TRACE_SCHEMA_V1 {
            bail!(
                "unsupported trace schema {:?} at {}:{}",
                record.schema,
                path.display(),
                line_index + 1
            );
        }
        if record.event_type != "request_end" {
            continue;
        }
        let request = compile_request(record, requests.len()).with_context(|| {
            format!(
                "invalid request_end at {}:{}",
                path.display(),
                line_index + 1
            )
        })?;
        requests.push(request);
    }
    Ok(())
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

fn compile_request(record: TraceRecord, ordinal: usize) -> Result<TraceRequest> {
    let request = record
        .request
        .context("request_end has no request metrics")?;
    let replay = request
        .replay
        .context("request_end has no replay metrics")?;
    if replay.trace_block_size == 0 {
        bail!("trace_block_size must be greater than zero");
    }
    if replay.input_length == 0 {
        bail!("input_length must be greater than zero");
    }
    let required_hashes = replay.input_length.div_ceil(replay.trace_block_size);
    if replay.input_sequence_hashes.len() != required_hashes {
        bail!(
            "request {} has {} replay hashes, expected {}",
            request.request_id,
            replay.input_sequence_hashes.len(),
            required_hashes
        );
    }
    if request
        .input_tokens
        .is_some_and(|input_tokens| input_tokens != replay.input_length as u64)
    {
        bail!(
            "request {} input_tokens does not match replay input_length",
            request.request_id
        );
    }
    let output_tokens = request
        .output_tokens
        .context("request has no output_tokens")?;
    let output_tokens = u32::try_from(output_tokens).context("output_tokens does not fit u32")?;
    let request_received_ms = request
        .request_received_ms
        .context("request has no request_received_ms")?;

    Ok(TraceRequest {
        ordinal,
        source_request_id: request.request_id,
        source_x_request_id: request.x_request_id,
        source_model: request.model,
        input_tokens: replay.input_length,
        output_tokens,
        request_received_ms,
        trace_block_size: replay.trace_block_size,
        input_sequence_hashes: replay.input_sequence_hashes,
        agent_context: record.agent_context,
    })
}

fn make_manifest(requests: &[TraceRequest], source_digest: &[u8]) -> Result<TraceManifest> {
    let first = requests.first().context("trace has no requests")?;
    let last = requests.last().context("trace has no requests")?;
    let mut request_ids = HashSet::with_capacity(requests.len());
    let mut sessions = BTreeSet::new();
    let mut hashes = BTreeSet::new();
    let mut block_sizes = BTreeSet::new();
    let mut input_tokens = 0_u64;
    let mut output_tokens = 0_u64;
    let mut requests_with_agent_context = 0;
    let mut zero_output_requests = 0;

    for request in requests {
        if !request_ids.insert(&request.source_request_id) {
            bail!("duplicate request_id {}", request.source_request_id);
        }
        if let Some(context) = &request.agent_context {
            requests_with_agent_context += 1;
            sessions.insert(&context.session_id);
        }
        hashes.extend(request.input_sequence_hashes.iter().copied());
        block_sizes.insert(request.trace_block_size);
        input_tokens = input_tokens
            .checked_add(request.input_tokens as u64)
            .context("input token count overflow")?;
        output_tokens = output_tokens
            .checked_add(request.output_tokens as u64)
            .context("output token count overflow")?;
        if request.output_tokens == 0 {
            zero_output_requests += 1;
        }
    }
    if block_sizes.len() != 1 {
        bail!("shape-strict replay requires one trace_block_size, found {block_sizes:?}");
    }

    Ok(TraceManifest {
        request_count: requests.len(),
        zero_output_requests,
        session_count: sessions.len(),
        requests_with_agent_context,
        first_request_received_ms: first.request_received_ms,
        last_request_received_ms: last.request_received_ms,
        duration_ms: last.request_received_ms - first.request_received_ms,
        input_tokens,
        output_tokens,
        distinct_sequence_hashes: hashes.len(),
        trace_block_size: *block_sizes.first().expect("one block size exists"),
        source_digest_sha256: hex::encode(source_digest),
    })
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use tempfile::NamedTempFile;

    use super::*;

    #[test]
    fn loads_raw_and_wrapped_records() {
        let mut file = NamedTempFile::new().unwrap();
        let record = serde_json::json!({
            "schema": TRACE_SCHEMA_V1,
            "event_type": "request_end",
            "event_time_unix_ms": 2000,
            "agent_context": {"session_id": "thread-1"},
            "request": {
                "request_id": "req-1",
                "input_tokens": 3,
                "output_tokens": 2,
                "request_received_ms": 1000,
                "replay": {
                    "trace_block_size": 2,
                    "input_length": 3,
                    "input_sequence_hashes": [11, 22]
                }
            }
        });
        writeln!(file, "{}", record).unwrap();
        let wrapped = serde_json::json!({"timestamp": 1, "event": {
            "schema": TRACE_SCHEMA_V1,
            "event_type": "request_end",
            "event_time_unix_ms": 2100,
            "request": {
                "request_id": "req-2",
                "output_tokens": 4,
                "request_received_ms": 1025,
                "replay": {
                    "trace_block_size": 2,
                    "input_length": 4,
                    "input_sequence_hashes": [11, 33]
                }
            }
        }});
        writeln!(file, "{}", wrapped).unwrap();

        let trace = load_trace(&[file.path().to_path_buf()], None, None).unwrap();
        assert_eq!(trace.manifest.request_count, 2);
        assert_eq!(trace.manifest.duration_ms, 25);
        assert_eq!(trace.manifest.distinct_sequence_hashes, 3);
        assert_eq!(trace.requests[1].input_tokens, 4);
    }

    #[test]
    fn rejects_incomplete_hash_coverage() {
        let record: TraceRecord = serde_json::from_value(serde_json::json!({
            "schema": TRACE_SCHEMA_V1,
            "event_type": "request_end",
            "request": {
                "request_id": "req-1",
                "output_tokens": 2,
                "request_received_ms": 1000,
                "replay": {
                    "trace_block_size": 2,
                    "input_length": 3,
                    "input_sequence_hashes": [11]
                }
            }
        }))
        .unwrap();
        assert!(compile_request(record, 0).is_err());
    }
}
