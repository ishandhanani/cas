// SPDX-License-Identifier: Apache-2.0

use std::collections::{BTreeMap, HashMap};
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};

use crate::RequestResult;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub(crate) struct EngineTelemetryRecord {
    #[serde(alias = "x_request_id")]
    pub request_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timestamp_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub cache_source_tokens: BTreeMap<String, u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub physical_free_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub physical_free_events: Option<u64>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub cache_ownership_tokens: BTreeMap<String, u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub occupancy_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub queue_time_ms: Option<f64>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, serde_json::Value>,
}

#[derive(Debug, Serialize)]
struct JoinedTelemetryRecord {
    request: RequestResult,
    telemetry: Vec<EngineTelemetryRecord>,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct TelemetryJoinSummary {
    pub request_records: usize,
    pub matched_requests: usize,
    pub unmatched_requests: usize,
    pub telemetry_records: usize,
    pub unmatched_telemetry_records: usize,
    pub output: PathBuf,
}

pub fn join_engine_telemetry(
    requests_path: &Path,
    telemetry_paths: &[PathBuf],
    output_path: &Path,
) -> Result<TelemetryJoinSummary> {
    if telemetry_paths.is_empty() {
        bail!("at least one engine telemetry file is required");
    }

    let mut telemetry_by_request = HashMap::<String, Vec<EngineTelemetryRecord>>::new();
    let mut telemetry_records = 0;
    for path in telemetry_paths {
        for_each_jsonl::<EngineTelemetryRecord, _>(path, |record| {
            validate_record(&record, path)?;
            telemetry_records += 1;
            telemetry_by_request
                .entry(record.request_id.clone())
                .or_default()
                .push(record);
            Ok(())
        })?;
    }

    let mut writer = BufWriter::new(
        File::create(output_path)
            .with_context(|| format!("failed to create {}", output_path.display()))?,
    );
    let mut request_records = 0;
    let mut matched_requests = 0;
    for_each_jsonl::<RequestResult, _>(requests_path, |request| {
        request_records += 1;
        let mut request_ids = Vec::with_capacity(3);
        for request_id in [
            Some(request.replay_request_id.as_str()),
            request.source_x_request_id.as_deref(),
            Some(request.source_request_id.as_str()),
        ]
        .into_iter()
        .flatten()
        {
            if !request_ids.contains(&request_id) {
                request_ids.push(request_id);
            }
        }
        let mut telemetry = Vec::new();
        for request_id in request_ids {
            if let Some(mut records) = telemetry_by_request.remove(request_id) {
                telemetry.append(&mut records);
            }
        }
        if !telemetry.is_empty() {
            matched_requests += 1;
        }
        serde_json::to_writer(&mut writer, &JoinedTelemetryRecord { request, telemetry })?;
        writer.write_all(b"\n")?;
        Ok(())
    })?;
    writer.flush()?;

    let unmatched_telemetry_records = telemetry_by_request.values().map(Vec::len).sum();
    Ok(TelemetryJoinSummary {
        request_records,
        matched_requests,
        unmatched_requests: request_records - matched_requests,
        telemetry_records,
        unmatched_telemetry_records,
        output: output_path.to_path_buf(),
    })
}

fn for_each_jsonl<T, F>(path: &Path, mut visit: F) -> Result<()>
where
    T: for<'de> Deserialize<'de>,
    F: FnMut(T) -> Result<()>,
{
    let reader = BufReader::new(
        File::open(path).with_context(|| format!("failed to open {}", path.display()))?,
    );
    for (line_number, line) in reader.lines().enumerate() {
        let line = line.with_context(|| {
            format!("failed to read {} line {}", path.display(), line_number + 1)
        })?;
        if line.trim().is_empty() {
            continue;
        }
        let value = serde_json::from_str(&line).with_context(|| {
            format!(
                "invalid JSON in {} line {}",
                path.display(),
                line_number + 1
            )
        })?;
        visit(value)?;
    }
    Ok(())
}

fn validate_record(record: &EngineTelemetryRecord, path: &Path) -> Result<()> {
    if record.request_id.trim().is_empty() {
        bail!("{} contains an empty telemetry request_id", path.display());
    }
    if record
        .queue_time_ms
        .is_some_and(|value| !value.is_finite() || value < 0.0)
    {
        bail!(
            "{} contains a negative or non-finite queue_time_ms",
            path.display()
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RequestResult;
    use tempfile::tempdir;

    fn request() -> RequestResult {
        RequestResult {
            ordinal: 0,
            source_request_id: "source".to_string(),
            source_x_request_id: None,
            replay_request_id: "replay".to_string(),
            session_id: Some("session".to_string()),
            parent_session_id: None,
            scheduled_offset_ms: 0.0,
            scheduler_wake_offset_ms: 0.0,
            scheduler_wake_lag_ms: 0.0,
            dispatch_offset_ms: 0.0,
            dispatch_lag_ms: 0.0,
            local_admission_lag_ms: 0.0,
            expected_input_tokens: 16,
            expected_output_tokens: Some(2),
            observed_output_tokens: Some(2),
            output_length_match: Some(true),
            compaction_operation_id: None,
            compaction_phase: None,
            compaction_attempt: None,
            compaction_expected_effect: None,
            planned_abort_match: None,
            status_code: Some(200),
            ttft_ms: Some(1.0),
            total_time_ms: 2.0,
            response_headers: BTreeMap::new(),
            error: None,
        }
    }

    #[test]
    fn joins_by_replay_request_id_without_interpreting_cache_policy() {
        let directory = tempdir().unwrap();
        let requests = directory.path().join("requests.jsonl");
        let telemetry = directory.path().join("engine.jsonl");
        let output = directory.path().join("joined.jsonl");
        std::fs::write(
            &requests,
            format!("{}\n", serde_json::to_string(&request()).unwrap()),
        )
        .unwrap();
        std::fs::write(
            &telemetry,
            concat!(
                "{\"request_id\":\"replay\",\"cache_source_tokens\":{\"gpu\":12},",
                "\"cache_ownership_tokens\":{\"shared\":8,\"session\":4},",
                "\"physical_free_tokens\":3,\"occupancy_tokens\":99,\"queue_time_ms\":1.5}\n"
            ),
        )
        .unwrap();

        let summary = join_engine_telemetry(&requests, &[telemetry], &output).unwrap();
        assert_eq!(summary.matched_requests, 1);
        assert_eq!(summary.unmatched_telemetry_records, 0);
        let joined: serde_json::Value =
            serde_json::from_str(std::fs::read_to_string(output).unwrap().trim()).unwrap();
        assert_eq!(joined["telemetry"][0]["cache_source_tokens"]["gpu"], 12);
        assert_eq!(
            joined["telemetry"][0]["cache_ownership_tokens"]["shared"],
            8
        );
    }

    #[test]
    fn joins_telemetry_from_replay_and_source_id_spaces() {
        let directory = tempdir().unwrap();
        let requests = directory.path().join("requests.jsonl");
        let telemetry = directory.path().join("engine.jsonl");
        let output = directory.path().join("joined.jsonl");
        std::fs::write(
            &requests,
            format!("{}\n", serde_json::to_string(&request()).unwrap()),
        )
        .unwrap();
        std::fs::write(
            &telemetry,
            concat!(
                "{\"request_id\":\"replay\",\"queue_time_ms\":1.0}\n",
                "{\"request_id\":\"source\",\"physical_free_tokens\":3}\n"
            ),
        )
        .unwrap();

        let summary = join_engine_telemetry(&requests, &[telemetry], &output).unwrap();
        assert_eq!(summary.telemetry_records, 2);
        assert_eq!(summary.unmatched_telemetry_records, 0);
        let joined: serde_json::Value =
            serde_json::from_str(std::fs::read_to_string(output).unwrap().trim()).unwrap();
        assert_eq!(joined["telemetry"].as_array().unwrap().len(), 2);
    }
}
