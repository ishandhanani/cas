// SPDX-License-Identifier: Apache-2.0

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use futures_util::StreamExt;
use reqwest::header::{HeaderName, HeaderValue};
use serde_json::json;
use tokio::sync::OwnedSemaphorePermit;
use tokio::task::JoinSet;

use super::{
    HttpTransport, PreparedMetadata, PreparedRequest, ReplayContext, ReplayOptions,
    RequestExecution, RequestResult, millis,
};
use crate::agent::{agent_headers, is_managed_header};
use crate::token_shape::TokenDictionary;
use crate::trace::TraceRequest;

pub(super) fn normalize_target(target: &str) -> String {
    let target = target.trim_end_matches('/');
    if target.ends_with("/v1/chat/completions") {
        target.to_string()
    } else {
        format!("{target}/v1/chat/completions")
    }
}

pub(super) fn prepare_request(
    client: &reqwest::Client,
    target: &str,
    run_id: &str,
    options: &ReplayOptions,
    dictionary: &TokenDictionary,
    request: TraceRequest,
    execution: RequestExecution,
) -> Result<PreparedRequest> {
    let replay_request_id = format!("agent-loadgen-{run_id}-{}", request.ordinal);
    let mut body = json!({
        "model": options.model,
        "messages": messages_for_trigger(
            request
                .agent_context
                .as_ref()
                .and_then(|agent_context| agent_context.input_trigger.as_deref())
        ),
        "stream": true,
        "stream_options": {"include_usage": true},
        "temperature": 0.0
    });
    let tokens = dictionary.synthesize(&request)?;
    body["nvext"] = json!({"token_data": tokens});
    if let Some(output_budget_tokens) = execution.output_budget_tokens {
        body["max_tokens"] = json!(output_budget_tokens);
        body["ignore_eos"] = json!(true);
    }

    let mut builder = client
        .post(target)
        .header("x-request-id", &replay_request_id)
        .json(&body);
    for (name, value) in &options.static_headers {
        builder = builder.header(name, value);
    }
    for (name, value) in agent_headers(options.agent, request.agent_context.as_ref()) {
        builder = builder.header(name, value);
    }
    let http_request = builder
        .build()
        .context("failed to build a replay request")?;
    let metadata = PreparedMetadata {
        ordinal: request.ordinal,
        source_request_id: request.source_request_id,
        source_x_request_id: request.source_x_request_id,
        replay_request_id,
        agent_context: request.agent_context,
        input_tokens: request.input_tokens,
        expected_output_tokens: execution.output_budget_tokens,
        compaction_attempt: execution.compaction_attempt,
    };
    Ok(PreparedRequest {
        metadata,
        http_request,
    })
}

pub(super) fn scaled_offset_ns(offset_ms: u64, time_scale: f64) -> Result<u64> {
    let scaled = offset_ms as f64 * 1_000_000.0 / time_scale;
    if !scaled.is_finite() || scaled < 0.0 || scaled > u64::MAX as f64 {
        bail!("scaled replay timestamp is outside the u64 nanosecond range");
    }
    Ok(scaled.round_ties_even() as u64)
}

pub(super) async fn warm_connections(
    client: &reqwest::Client,
    target: &str,
    options: &ReplayOptions,
) -> Result<()> {
    if options.warmup_connections == 0 {
        return Ok(());
    }
    let models_target = target
        .strip_suffix("/v1/chat/completions")
        .map_or_else(|| target.to_string(), |base| format!("{base}/v1/models"));
    let mut tasks = JoinSet::new();
    for _ in 0..options.warmup_connections {
        let client = client.clone();
        let target = models_target.clone();
        let headers = options.static_headers.clone();
        tasks.spawn(async move {
            let mut request = client.get(target);
            for (name, value) in headers {
                request = request.header(name, value);
            }
            let response = request.send().await.context("connection warmup failed")?;
            response
                .bytes()
                .await
                .context("failed to drain a connection warmup response")?;
            Ok::<(), anyhow::Error>(())
        });
    }
    while let Some(result) = tasks.join_next().await {
        result.context("a connection warmup task failed")??;
    }
    Ok(())
}

pub(super) fn build_http_client(options: &ReplayOptions) -> Result<reqwest::Client> {
    let mut builder = reqwest::Client::builder()
        .connect_timeout(options.timeout.min(Duration::from_secs(30)))
        .timeout(options.timeout)
        .pool_max_idle_per_host(options.max_in_flight);
    if matches!(options.http_transport, HttpTransport::Http2PriorKnowledge) {
        builder = builder.http2_prior_knowledge();
    }
    builder.build().context("failed to build the HTTP client")
}

pub(super) fn validate_options(options: &ReplayOptions) -> Result<()> {
    if options.model.trim().is_empty() {
        bail!("model must not be empty");
    }
    if options.max_in_flight == 0 {
        bail!("max_in_flight must be greater than zero");
    }
    if options.warmup_connections > options.max_in_flight {
        bail!("warmup_connections must not exceed max_in_flight");
    }
    if options.result_flush_interval == 0 {
        bail!("result_flush_interval must be greater than zero");
    }
    if !options.time_scale.is_finite() || options.time_scale <= 0.0 {
        bail!("time_scale must be a positive finite number");
    }
    if !options.max_dispatch_p99_ms.is_finite() || options.max_dispatch_p99_ms < 0.0 {
        bail!("max_dispatch_p99_ms must be a non-negative finite number");
    }
    if !options.max_dispatch_max_ms.is_finite() || options.max_dispatch_max_ms < 0.0 {
        bail!("max_dispatch_max_ms must be a non-negative finite number");
    }
    for (name, value) in &options.static_headers {
        if is_managed_header(name) {
            bail!("static header {name:?} is owned by agent-loadgen");
        }
        HeaderName::from_bytes(name.as_bytes())
            .with_context(|| format!("invalid static header name {name:?}"))?;
        HeaderValue::from_str(value)
            .with_context(|| format!("invalid value for static header {name:?}"))?;
    }
    Ok(())
}

#[cfg(unix)]
pub(super) fn prepare_open_file_limit(max_in_flight: usize) -> Result<()> {
    const RESERVED_FILE_DESCRIPTORS: usize = 64;

    let desired = max_in_flight
        .checked_add(RESERVED_FILE_DESCRIPTORS)
        .context("the requested open-file limit overflows usize")?;
    let desired = libc::rlim_t::try_from(desired)
        .context("the requested open-file limit does not fit rlim_t")?;
    let mut limit = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    if unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) } != 0 {
        return Err(std::io::Error::last_os_error()).context("failed to read RLIMIT_NOFILE");
    }
    if limit.rlim_cur >= desired {
        return Ok(());
    }
    if limit.rlim_max < desired {
        bail!(
            "max_in_flight requires at least {desired} open files, but RLIMIT_NOFILE hard limit is {}",
            limit.rlim_max
        );
    }
    limit.rlim_cur = desired;
    if unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &limit) } != 0 {
        return Err(std::io::Error::last_os_error())
            .with_context(|| format!("failed to raise RLIMIT_NOFILE soft limit to {desired}"));
    }
    Ok(())
}

#[cfg(not(unix))]
pub(super) fn prepare_open_file_limit(_max_in_flight: usize) -> Result<()> {
    Ok(())
}

pub(super) async fn send_request(
    context: &ReplayContext,
    scheduled: Instant,
    scheduler_wake: Instant,
    scheduled_offset_ns: u64,
    prepared: PreparedRequest,
    _permit: OwnedSemaphorePermit,
) -> Result<RequestResult> {
    let dispatch = Instant::now();
    let result = request_result_shell(
        context,
        scheduled,
        scheduler_wake,
        scheduled_offset_ns,
        &prepared,
        dispatch,
    );
    let abort_after_ms = prepared
        .metadata
        .compaction_attempt
        .as_ref()
        .and_then(|attempt| attempt.abort_after_ms);
    let request = perform_request(context, dispatch, prepared, result.clone());
    if let Some(abort_after_ms) = abort_after_ms {
        return match tokio::time::timeout(Duration::from_millis(abort_after_ms), request).await {
            Err(_) => {
                let mut aborted = result;
                aborted.total_time_ms = millis(dispatch.elapsed());
                aborted.planned_abort_match = Some(true);
                Ok(aborted)
            }
            Ok(result) => {
                let mut completed = result?;
                completed.planned_abort_match = Some(false);
                completed.error = Some(format!(
                    "planned compaction abort after {abort_after_ms} ms completed before cancellation"
                ));
                Ok(completed)
            }
        };
    }
    request.await
}

async fn perform_request(
    context: &ReplayContext,
    dispatch: Instant,
    prepared: PreparedRequest,
    mut result: RequestResult,
) -> Result<RequestResult> {
    let expected_output_tokens = prepared.metadata.expected_output_tokens;
    let response = context.client.execute(prepared.http_request).await;
    let response = match response {
        Ok(response) => response,
        Err(error) => {
            result.total_time_ms = millis(dispatch.elapsed());
            result.error = Some(error.to_string());
            return Ok(result);
        }
    };
    result.status_code = Some(response.status().as_u16());
    result.response_headers = selected_response_headers(response.headers());
    if !response.status().is_success() {
        let status = response.status();
        let error_body = response.text().await.unwrap_or_default();
        result.total_time_ms = millis(dispatch.elapsed());
        result.error = Some(format!(
            "HTTP {status}: {}",
            error_body.chars().take(512).collect::<String>()
        ));
        return Ok(result);
    }

    let stream = consume_sse(response, dispatch).await;
    result.total_time_ms = millis(dispatch.elapsed());
    match stream {
        Ok(stream) => {
            result.ttft_ms = stream.ttft_ms;
            result.observed_output_tokens = stream.output_tokens;
            result.output_length_match = expected_output_tokens
                .and_then(|expected| stream.output_tokens.map(|tokens| tokens == expected as u64));
        }
        Err(error) => result.error = Some(error.to_string()),
    }
    Ok(result)
}

pub(super) fn admission_failure(
    context: &ReplayContext,
    scheduled: Instant,
    scheduler_wake: Instant,
    scheduled_offset_ns: u64,
    prepared: PreparedRequest,
    max_in_flight: usize,
) -> RequestResult {
    let dispatch = Instant::now();
    let mut result = request_result_shell(
        context,
        scheduled,
        scheduler_wake,
        scheduled_offset_ns,
        &prepared,
        dispatch,
    );
    result.error = Some(format!(
        "local admission limit {max_in_flight} was exhausted at the recorded arrival; the request was not retimed"
    ));
    result
}

fn request_result_shell(
    context: &ReplayContext,
    scheduled: Instant,
    scheduler_wake: Instant,
    scheduled_offset_ns: u64,
    prepared: &PreparedRequest,
    dispatch: Instant,
) -> RequestResult {
    let dispatch_offset_ms = millis(dispatch.saturating_duration_since(context.base));
    let dispatch_lag_ms = millis(dispatch.saturating_duration_since(scheduled));
    let scheduler_wake_offset_ms = millis(scheduler_wake.saturating_duration_since(context.base));
    let scheduler_wake_lag_ms = millis(scheduler_wake.saturating_duration_since(scheduled));
    let local_admission_lag_ms = millis(dispatch.saturating_duration_since(scheduler_wake));
    let session_id = prepared
        .metadata
        .agent_context
        .as_ref()
        .map(|context| context.session_id.clone());
    let parent_session_id = prepared
        .metadata
        .agent_context
        .as_ref()
        .and_then(|context| context.parent_session_id.clone());

    RequestResult {
        ordinal: prepared.metadata.ordinal,
        source_request_id: prepared.metadata.source_request_id.clone(),
        source_x_request_id: prepared.metadata.source_x_request_id.clone(),
        replay_request_id: prepared.metadata.replay_request_id.clone(),
        session_id,
        parent_session_id,
        scheduled_offset_ms: scheduled_offset_ns as f64 / 1_000_000.0,
        scheduler_wake_offset_ms,
        scheduler_wake_lag_ms,
        dispatch_offset_ms,
        dispatch_lag_ms,
        local_admission_lag_ms,
        expected_input_tokens: prepared.metadata.input_tokens,
        expected_output_tokens: prepared.metadata.expected_output_tokens,
        observed_output_tokens: None,
        output_length_match: None,
        compaction_operation_id: prepared
            .metadata
            .compaction_attempt
            .as_ref()
            .map(|attempt| attempt.operation_id.clone()),
        compaction_phase: prepared
            .metadata
            .compaction_attempt
            .as_ref()
            .map(|attempt| attempt.phase.clone()),
        compaction_attempt: prepared
            .metadata
            .compaction_attempt
            .as_ref()
            .map(|attempt| attempt.attempt),
        compaction_expected_effect: prepared
            .metadata
            .compaction_attempt
            .as_ref()
            .map(|attempt| attempt.expected_effect),
        planned_abort_match: None,
        status_code: None,
        ttft_ms: None,
        total_time_ms: 0.0,
        response_headers: BTreeMap::new(),
        error: None,
    }
}

fn messages_for_trigger(input_trigger: Option<&str>) -> serde_json::Value {
    match input_trigger {
        Some("tool_result") => json!([
            {
                "role": "assistant",
                "content": null,
                "tool_calls": [{
                    "id": "agent-loadgen-shape-tool",
                    "type": "function",
                    "function": {"name": "shape_tool", "arguments": "{}"}
                }]
            },
            {
                "role": "tool",
                "tool_call_id": "agent-loadgen-shape-tool",
                "content": "shape replay"
            }
        ]),
        Some("other") => json!([{"role": "assistant", "content": "shape replay"}]),
        _ => json!([{"role": "user", "content": "shape replay"}]),
    }
}

#[derive(Debug)]
struct StreamMetrics {
    ttft_ms: Option<f64>,
    output_tokens: Option<u64>,
}

async fn consume_sse(
    response: reqwest::Response,
    request_started: Instant,
) -> Result<StreamMetrics> {
    let mut bytes = response.bytes_stream();
    let mut buffer = Vec::new();
    let mut ttft_ms = None;
    let mut output_tokens = None;

    while let Some(chunk) = bytes.next().await {
        buffer.extend_from_slice(&chunk.context("failed to read the response stream")?);
        while let Some(newline) = buffer.iter().position(|byte| *byte == b'\n') {
            let mut line: Vec<u8> = buffer.drain(..=newline).collect();
            while line
                .last()
                .is_some_and(|byte| *byte == b'\n' || *byte == b'\r')
            {
                line.pop();
            }
            let Some(data) = line.strip_prefix(b"data:") else {
                continue;
            };
            let data = trim_ascii(data);
            if data == b"[DONE]" || data.is_empty() {
                continue;
            }
            let value: serde_json::Value =
                serde_json::from_slice(data).context("the response contains invalid SSE JSON")?;
            if ttft_ms.is_none() && chunk_contains_output(&value) {
                ttft_ms = Some(millis(request_started.elapsed()));
            }
            if let Some(tokens) = value
                .get("usage")
                .and_then(|usage| usage.get("completion_tokens"))
                .and_then(serde_json::Value::as_u64)
            {
                output_tokens = Some(tokens);
            }
            if let Some(error) = value.get("error") {
                bail!("the response stream returned an error: {error}");
            }
        }
    }
    if !buffer.iter().all(u8::is_ascii_whitespace) {
        bail!("the response stream ended with an incomplete SSE line");
    }
    Ok(StreamMetrics {
        ttft_ms,
        output_tokens,
    })
}

fn trim_ascii(mut bytes: &[u8]) -> &[u8] {
    while bytes.first().is_some_and(u8::is_ascii_whitespace) {
        bytes = &bytes[1..];
    }
    while bytes.last().is_some_and(u8::is_ascii_whitespace) {
        bytes = &bytes[..bytes.len() - 1];
    }
    bytes
}

pub(super) fn chunk_contains_output(value: &serde_json::Value) -> bool {
    value
        .get("choices")
        .and_then(serde_json::Value::as_array)
        .is_some_and(|choices| {
            choices.iter().any(|choice| {
                let Some(delta) = choice.get("delta") else {
                    return choice.get("text").is_some_and(nonempty_value);
                };
                delta.get("content").is_some_and(nonempty_value)
                    || delta.get("reasoning_content").is_some_and(nonempty_value)
                    || delta.get("tool_calls").is_some_and(nonempty_value)
            })
        })
}

fn nonempty_value(value: &serde_json::Value) -> bool {
    match value {
        serde_json::Value::Null => false,
        serde_json::Value::String(value) => !value.is_empty(),
        serde_json::Value::Array(value) => !value.is_empty(),
        serde_json::Value::Object(value) => !value.is_empty(),
        _ => true,
    }
}

fn selected_response_headers(headers: &reqwest::header::HeaderMap) -> BTreeMap<String, String> {
    headers
        .iter()
        .filter_map(|(name, value)| {
            let name = name.as_str();
            if name == "x-request-id" || name.starts_with("x-dynamo-") {
                value
                    .to_str()
                    .ok()
                    .map(|value| (name.to_string(), value.to_string()))
            } else {
                None
            }
        })
        .collect()
}
