// SPDX-License-Identifier: Apache-2.0

use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use futures_util::StreamExt;
use reqwest::header::{HeaderName, HeaderValue};
use serde::Serialize;
use serde_json::json;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use uuid::Uuid;

use crate::agent::{AgentKind, agent_headers};
use crate::clock::{TIMER_BACKEND, sleep_until};
use crate::scheduler::ReadyQueue;
use crate::token_shape::{TokenDictionary, TokenDictionaryManifest};
use crate::trace::{AgentContext, LoadedTrace, TraceManifest, TraceRequest};

#[derive(Debug, Clone)]
pub struct ReplayOptions {
    pub agent: AgentKind,
    pub model: String,
    pub target: String,
    pub output_dir: PathBuf,
    pub max_in_flight: usize,
    pub warmup_connections: usize,
    pub start_delay: Duration,
    pub timeout: Duration,
    pub time_scale: f64,
    pub preserve_request_ids: bool,
    pub static_headers: Vec<(String, String)>,
}

#[derive(Debug, Clone, Serialize)]
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
    pub expected_output_tokens: u32,
    pub observed_output_tokens: Option<u64>,
    pub output_length_match: Option<bool>,
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
    pub timer_backend: &'static str,
    pub static_header_names: Vec<String>,
    pub source: TraceManifest,
    pub token_dictionary: TokenDictionaryManifest,
    pub request_count: usize,
    pub succeeded: usize,
    pub failed: usize,
    pub output_length_matches: usize,
    pub output_length_mismatches: usize,
    pub missing_output_usage: usize,
    pub scheduler_wake_lag_ms: Percentiles,
    pub dispatch_lag_ms: Percentiles,
    pub local_admission_lag_ms: Percentiles,
    pub total_time_ms: f64,
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

struct PreparedMetadata {
    ordinal: usize,
    source_request_id: String,
    source_x_request_id: Option<String>,
    replay_request_id: String,
    agent_context: Option<AgentContext>,
    input_tokens: usize,
    output_tokens: u32,
}

pub async fn run_replay(
    trace: LoadedTrace,
    dictionary: TokenDictionary,
    options: ReplayOptions,
) -> Result<RunSummary> {
    validate_options(&options)?;
    if trace.manifest.zero_output_requests > 0 {
        bail!(
            "shape-strict replay does not support {} zero-output requests",
            trace.manifest.zero_output_requests
        );
    }
    fs::create_dir_all(&options.output_dir).with_context(|| {
        format!(
            "failed to create output directory {}",
            options.output_dir.display()
        )
    })?;

    let client = reqwest::Client::builder()
        .connect_timeout(options.timeout.min(Duration::from_secs(30)))
        .timeout(options.timeout)
        .pool_max_idle_per_host(options.max_in_flight)
        .build()
        .context("failed to build the HTTP client")?;
    let target = normalize_target(&options.target);
    let run_id = Uuid::new_v4().to_string();
    let LoadedTrace {
        requests,
        manifest: source_manifest,
    } = trace;
    let token_manifest = dictionary.manifest().clone();
    let mut schedule = prepare_schedule(
        requests,
        &dictionary,
        &client,
        &target,
        &run_id,
        &options,
        source_manifest.first_request_received_ms,
    )?;
    warm_connections(&client, &target, &options).await?;

    let wall_started = Instant::now();
    let base = wall_started
        .checked_add(options.start_delay)
        .context("replay start delay exceeds the monotonic clock range")?;
    let semaphore = Arc::new(Semaphore::new(options.max_in_flight));
    let options = Arc::new(options);
    let context = ReplayContext { client, base };
    let mut tasks = JoinSet::new();

    while let Some(next_ready_ns) = schedule.next_ready_at_ns() {
        let deadline = base
            .checked_add(Duration::from_nanos(next_ready_ns))
            .context("a replay timestamp exceeds the monotonic clock range")?;
        sleep_until(deadline).await;

        let now_ns = duration_ns(Instant::now().saturating_duration_since(base));
        let due = schedule.pop_due(now_ns, usize::MAX);
        for ready in due {
            let scheduler_wake = Instant::now();
            let scheduled = base
                .checked_add(Duration::from_nanos(ready.ready_at_ns))
                .expect("the deadline was validated before queue release");
            let context = context.clone();
            let semaphore = Arc::clone(&semaphore);
            tasks.spawn(async move {
                let permit = semaphore
                    .acquire_owned()
                    .await
                    .context("the replay semaphore closed")?;
                let result = send_request(
                    &context,
                    scheduled,
                    scheduler_wake,
                    ready.ready_at_ns,
                    ready.value,
                )
                .await;
                drop(permit);
                result
            });
        }
    }

    let mut results = Vec::with_capacity(source_manifest.request_count);
    while let Some(result) = tasks.join_next().await {
        results.push(result.context("a replay task failed")??);
    }
    results.sort_by_key(|result| result.ordinal);

    write_request_results(&options.output_dir, &results)?;
    let summary = summarize(
        run_id,
        &target,
        &source_manifest,
        &token_manifest,
        &options,
        &results,
        wall_started.elapsed(),
    );
    write_json(&options.output_dir.join("run.json"), &summary)?;
    Ok(summary)
}

fn prepare_schedule(
    requests: Vec<TraceRequest>,
    dictionary: &TokenDictionary,
    client: &reqwest::Client,
    target: &str,
    run_id: &str,
    options: &ReplayOptions,
    first_received_ms: u64,
) -> Result<ReadyQueue<PreparedRequest>> {
    let mut schedule = ReadyQueue::with_capacity(requests.len());
    for request in requests {
        let ready_at_ns = scaled_offset_ns(
            request.request_received_ms - first_received_ms,
            options.time_scale,
        )?;
        let ordinal = request.ordinal;
        let prepared = prepare_request(client, target, run_id, options, dictionary, request)?;
        schedule.push(ready_at_ns, ordinal, prepared);
    }
    Ok(schedule)
}

fn prepare_request(
    client: &reqwest::Client,
    target: &str,
    run_id: &str,
    options: &ReplayOptions,
    dictionary: &TokenDictionary,
    request: TraceRequest,
) -> Result<PreparedRequest> {
    let tokens = dictionary.synthesize(&request)?;
    let replay_request_id = if options.preserve_request_ids {
        request.source_request_id.clone()
    } else {
        format!("agent-loadgen-{run_id}-{}", request.ordinal)
    };
    let body = json!({
        "model": options.model,
        "messages": messages_for_trigger(
            request
                .agent_context
                .as_ref()
                .and_then(|agent_context| agent_context.input_trigger.as_deref())
        ),
        "stream": true,
        "stream_options": {"include_usage": true},
        "max_tokens": request.output_tokens,
        "min_tokens": request.output_tokens,
        "ignore_eos": true,
        "temperature": 0.0,
        "nvext": {"token_data": tokens}
    });

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
        output_tokens: request.output_tokens,
    };
    Ok(PreparedRequest {
        metadata,
        http_request,
    })
}

fn scaled_offset_ns(offset_ms: u64, time_scale: f64) -> Result<u64> {
    let scaled = offset_ms as f64 * 1_000_000.0 / time_scale;
    if !scaled.is_finite() || scaled < 0.0 || scaled > u64::MAX as f64 {
        bail!("scaled replay timestamp is outside the u64 nanosecond range");
    }
    Ok(scaled.round_ties_even() as u64)
}

async fn warm_connections(
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

fn validate_options(options: &ReplayOptions) -> Result<()> {
    if options.model.trim().is_empty() {
        bail!("model must not be empty");
    }
    if options.max_in_flight == 0 {
        bail!("max_in_flight must be greater than zero");
    }
    if options.warmup_connections > options.max_in_flight {
        bail!("warmup_connections must not exceed max_in_flight");
    }
    if !options.time_scale.is_finite() || options.time_scale <= 0.0 {
        bail!("time_scale must be a positive finite number");
    }
    for (name, value) in &options.static_headers {
        HeaderName::from_bytes(name.as_bytes())
            .with_context(|| format!("invalid static header name {name:?}"))?;
        HeaderValue::from_str(value)
            .with_context(|| format!("invalid value for static header {name:?}"))?;
    }
    Ok(())
}

async fn send_request(
    context: &ReplayContext,
    scheduled: Instant,
    scheduler_wake: Instant,
    scheduled_offset_ns: u64,
    prepared: PreparedRequest,
) -> Result<RequestResult> {
    let dispatch = Instant::now();
    let response = context.client.execute(prepared.http_request).await;
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

    let mut result = RequestResult {
        ordinal: prepared.metadata.ordinal,
        source_request_id: prepared.metadata.source_request_id,
        source_x_request_id: prepared.metadata.source_x_request_id,
        replay_request_id: prepared.metadata.replay_request_id,
        session_id,
        parent_session_id,
        scheduled_offset_ms: scheduled_offset_ns as f64 / 1_000_000.0,
        scheduler_wake_offset_ms,
        scheduler_wake_lag_ms,
        dispatch_offset_ms,
        dispatch_lag_ms,
        local_admission_lag_ms,
        expected_input_tokens: prepared.metadata.input_tokens,
        expected_output_tokens: prepared.metadata.output_tokens,
        observed_output_tokens: None,
        output_length_match: None,
        status_code: None,
        ttft_ms: None,
        total_time_ms: 0.0,
        response_headers: BTreeMap::new(),
        error: None,
    };

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
            result.output_length_match = stream
                .output_tokens
                .map(|tokens| tokens == prepared.metadata.output_tokens as u64);
        }
        Err(error) => result.error = Some(error.to_string()),
    }
    Ok(result)
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

fn chunk_contains_output(value: &serde_json::Value) -> bool {
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

fn summarize(
    run_id: String,
    target: &str,
    source: &TraceManifest,
    dictionary: &TokenDictionaryManifest,
    options: &ReplayOptions,
    results: &[RequestResult],
    elapsed: Duration,
) -> RunSummary {
    let succeeded = results
        .iter()
        .filter(|result| {
            result.error.is_none() && result.status_code.is_some_and(|status| status < 400)
        })
        .count();
    let output_length_matches = results
        .iter()
        .filter(|result| result.output_length_match == Some(true))
        .count();
    let output_length_mismatches = results
        .iter()
        .filter(|result| result.output_length_match == Some(false))
        .count();
    let missing_output_usage = results
        .iter()
        .filter(|result| result.observed_output_tokens.is_none())
        .count();
    let lags = results
        .iter()
        .map(|result| result.dispatch_lag_ms)
        .collect::<Vec<_>>();
    let wake_lags = results
        .iter()
        .map(|result| result.scheduler_wake_lag_ms)
        .collect::<Vec<_>>();
    let admission_lags = results
        .iter()
        .map(|result| result.local_admission_lag_ms)
        .collect::<Vec<_>>();

    RunSummary {
        run_id,
        agent: options.agent,
        model: options.model.clone(),
        target: target.to_string(),
        time_scale: options.time_scale,
        max_in_flight: options.max_in_flight,
        warmup_connections: options.warmup_connections,
        timer_backend: TIMER_BACKEND,
        static_header_names: options
            .static_headers
            .iter()
            .map(|(name, _)| name.clone())
            .collect(),
        source: source.clone(),
        token_dictionary: dictionary.clone(),
        request_count: results.len(),
        succeeded,
        failed: results.len() - succeeded,
        output_length_matches,
        output_length_mismatches,
        missing_output_usage,
        scheduler_wake_lag_ms: percentiles(wake_lags),
        dispatch_lag_ms: percentiles(lags),
        local_admission_lag_ms: percentiles(admission_lags),
        total_time_ms: millis(elapsed),
    }
}

pub(crate) fn percentiles(mut values: Vec<f64>) -> Percentiles {
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

fn normalize_target(target: &str) -> String {
    let target = target.trim_end_matches('/');
    if target.ends_with("/v1/chat/completions") {
        target.to_string()
    } else {
        format!("{target}/v1/chat/completions")
    }
}

fn write_request_results(output_dir: &Path, results: &[RequestResult]) -> Result<()> {
    let path = output_dir.join("requests.jsonl");
    let mut writer = BufWriter::new(
        File::create(&path).with_context(|| format!("failed to create {}", path.display()))?,
    );
    for result in results {
        serde_json::to_writer(&mut writer, result)
            .with_context(|| format!("failed to write {}", path.display()))?;
        writer.write_all(b"\n")?;
    }
    writer.flush()?;
    Ok(())
}

fn write_json(path: &Path, value: &impl Serialize) -> Result<()> {
    let writer = BufWriter::new(
        File::create(path).with_context(|| format!("failed to create {}", path.display()))?,
    );
    serde_json::to_writer_pretty(writer, value)
        .with_context(|| format!("failed to write {}", path.display()))
}

fn millis(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1000.0
}

fn duration_ns(duration: Duration) -> u64 {
    duration.as_nanos().min(u64::MAX as u128) as u64
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use axum::body::Body;
    use axum::extract::State;
    use axum::http::{HeaderMap, Response, StatusCode};
    use axum::routing::post;
    use axum::{Json, Router};
    use tempfile::tempdir;

    use super::*;
    use crate::token_shape::SafeTokenAlphabet;
    use crate::trace::{AgentContext, TraceRequest};

    #[test]
    fn target_normalization_accepts_base_or_endpoint() {
        assert_eq!(
            normalize_target("http://localhost:8000"),
            "http://localhost:8000/v1/chat/completions"
        );
        assert_eq!(
            normalize_target("http://localhost:8000/v1/chat/completions"),
            "http://localhost:8000/v1/chat/completions"
        );
    }

    #[test]
    fn calculates_nearest_rank_percentiles() {
        let values = (1..=100).map(|value| value as f64).collect();
        let result = percentiles(values);
        assert_eq!(result.p50, 50.0);
        assert_eq!(result.p95, 95.0);
        assert_eq!(result.p99, 99.0);
    }

    #[test]
    fn detects_output_chunks() {
        assert!(!chunk_contains_output(
            &json!({"choices":[{"delta":{"role":"assistant"}}]})
        ));
        assert!(chunk_contains_output(
            &json!({"choices":[{"delta":{"content":"x"}}]})
        ));
    }

    #[derive(Clone, Default)]
    struct Capture(Arc<Mutex<Option<(HeaderMap, serde_json::Value)>>>);

    async fn shape_endpoint(
        State(capture): State<Capture>,
        headers: HeaderMap,
        Json(body): Json<serde_json::Value>,
    ) -> Response<Body> {
        *capture.0.lock().unwrap() = Some((headers, body));
        Response::builder()
            .status(StatusCode::OK)
            .header("content-type", "text/event-stream")
            .body(Body::from(concat!(
                "data: {\"choices\":[{\"delta\":{\"content\":\"x\"}}]}\n\n",
                "data: {\"choices\":[],\"usage\":{\"completion_tokens\":2}}\n\n",
                "data: [DONE]\n\n"
            )))
            .unwrap()
    }

    async fn slow_shape_endpoint(
        State(capture): State<Capture>,
        headers: HeaderMap,
        Json(body): Json<serde_json::Value>,
    ) -> Response<Body> {
        tokio::time::sleep(Duration::from_millis(15)).await;
        shape_endpoint(State(capture), headers, Json(body)).await
    }

    #[tokio::test]
    async fn replay_sends_shape_and_agent_headers() {
        let capture = Capture::default();
        let app = Router::new()
            .route("/v1/chat/completions", post(shape_endpoint))
            .with_state(capture.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        let request = TraceRequest {
            ordinal: 0,
            source_request_id: "source-1".to_string(),
            source_x_request_id: None,
            source_model: None,
            input_tokens: 3,
            output_tokens: 2,
            request_received_ms: 1000,
            trace_block_size: 2,
            input_sequence_hashes: vec![11, 22],
            agent_context: Some(AgentContext {
                session_id: "thread-1".to_string(),
                parent_session_id: None,
                session_final: None,
                compaction: Some(serde_json::json!({"phase": "mid_turn"})),
                input_trigger: Some("tool_result".to_string()),
            }),
        };
        let trace = LoadedTrace {
            requests: vec![request.clone()],
            manifest: TraceManifest {
                request_count: 1,
                zero_output_requests: 0,
                session_count: 1,
                requests_with_agent_context: 1,
                first_request_received_ms: 1000,
                last_request_received_ms: 1000,
                duration_ms: 0,
                input_tokens: 3,
                output_tokens: 2,
                distinct_sequence_hashes: 2,
                trace_block_size: 2,
                source_digest_sha256: "source".to_string(),
            },
        };
        let dictionary =
            TokenDictionary::build(&[request], SafeTokenAlphabet::new(100, 16).unwrap()).unwrap();
        let output = tempdir().unwrap();
        let summary = run_replay(
            trace,
            dictionary,
            ReplayOptions {
                agent: AgentKind::Codex,
                model: "test-model".to_string(),
                target: format!("http://{address}"),
                output_dir: output.path().to_path_buf(),
                max_in_flight: 1,
                warmup_connections: 0,
                start_delay: Duration::from_millis(5),
                timeout: Duration::from_secs(5),
                time_scale: 1.0,
                preserve_request_ids: false,
                static_headers: Vec::new(),
            },
        )
        .await
        .unwrap();

        assert_eq!(summary.succeeded, 1);
        assert_eq!(summary.output_length_matches, 1);
        let (headers, body) = capture.0.lock().unwrap().clone().unwrap();
        assert_eq!(headers.get("thread-id").unwrap(), "thread-1");
        assert_eq!(headers.get("x-dynamo-session-id").unwrap(), "thread-1");
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(
                headers
                    .get("x-codex-turn-metadata")
                    .unwrap()
                    .to_str()
                    .unwrap(),
            )
            .unwrap(),
            serde_json::json!({
                "request_kind": "compaction",
                "compaction": {"phase": "mid_turn"}
            })
        );
        assert_eq!(body["max_tokens"], 2);
        assert_eq!(body["min_tokens"], 2);
        assert_eq!(body["ignore_eos"], true);
        assert_eq!(body["messages"][0]["role"], "assistant");
        assert_eq!(body["messages"][1]["role"], "tool");
        assert_eq!(
            body["messages"][1]["tool_call_id"],
            "agent-loadgen-shape-tool"
        );
        assert_eq!(body["nvext"]["token_data"].as_array().unwrap().len(), 3);
        assert!(output.path().join("run.json").is_file());
        assert!(output.path().join("requests.jsonl").is_file());
        server.abort();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn recorded_scheduler_handles_tied_millisecond_arrivals() {
        let capture = Capture::default();
        let app = Router::new()
            .route("/v1/chat/completions", post(shape_endpoint))
            .with_state(capture);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        let requests = (0..96)
            .map(|ordinal| TraceRequest {
                ordinal,
                source_request_id: format!("source-{ordinal}"),
                source_x_request_id: None,
                source_model: None,
                input_tokens: 3,
                output_tokens: 2,
                request_received_ms: 1_000 + (ordinal / 3) as u64,
                trace_block_size: 2,
                input_sequence_hashes: vec![11, 22],
                agent_context: Some(AgentContext {
                    session_id: format!("thread-{}", ordinal % 12),
                    parent_session_id: None,
                    session_final: None,
                    compaction: None,
                    input_trigger: Some("user_message".to_string()),
                }),
            })
            .collect::<Vec<_>>();
        let dictionary =
            TokenDictionary::build(&requests, SafeTokenAlphabet::new(100, 16).unwrap()).unwrap();
        let trace = LoadedTrace {
            requests,
            manifest: TraceManifest {
                request_count: 96,
                zero_output_requests: 0,
                session_count: 12,
                requests_with_agent_context: 96,
                first_request_received_ms: 1_000,
                last_request_received_ms: 1_031,
                duration_ms: 31,
                input_tokens: 288,
                output_tokens: 192,
                distinct_sequence_hashes: 2,
                trace_block_size: 2,
                source_digest_sha256: "source".to_string(),
            },
        };
        let output = tempdir().unwrap();
        let summary = run_replay(
            trace,
            dictionary,
            ReplayOptions {
                agent: AgentKind::Codex,
                model: "test-model".to_string(),
                target: format!("http://{address}"),
                output_dir: output.path().to_path_buf(),
                max_in_flight: 96,
                warmup_connections: 0,
                start_delay: Duration::from_millis(25),
                timeout: Duration::from_secs(5),
                time_scale: 1.0,
                preserve_request_ids: false,
                static_headers: Vec::new(),
            },
        )
        .await
        .unwrap();

        assert_eq!(summary.succeeded, 96);
        assert_eq!(summary.output_length_matches, 96);
        assert!(summary.scheduler_wake_lag_ms.p99 < 100.0);
        assert!(summary.dispatch_lag_ms.p99 < 100.0);
        server.abort();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn reports_admission_backpressure_separately_from_scheduler_wake() {
        let capture = Capture::default();
        let app = Router::new()
            .route("/v1/chat/completions", post(slow_shape_endpoint))
            .with_state(capture);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        let requests = (0..3)
            .map(|ordinal| TraceRequest {
                ordinal,
                source_request_id: format!("source-{ordinal}"),
                source_x_request_id: None,
                source_model: None,
                input_tokens: 3,
                output_tokens: 2,
                request_received_ms: 1_000,
                trace_block_size: 2,
                input_sequence_hashes: vec![11, 22],
                agent_context: None,
            })
            .collect::<Vec<_>>();
        let dictionary =
            TokenDictionary::build(&requests, SafeTokenAlphabet::new(100, 16).unwrap()).unwrap();
        let trace = LoadedTrace {
            requests,
            manifest: TraceManifest {
                request_count: 3,
                zero_output_requests: 0,
                session_count: 0,
                requests_with_agent_context: 0,
                first_request_received_ms: 1_000,
                last_request_received_ms: 1_000,
                duration_ms: 0,
                input_tokens: 9,
                output_tokens: 6,
                distinct_sequence_hashes: 2,
                trace_block_size: 2,
                source_digest_sha256: "source".to_string(),
            },
        };
        let output = tempdir().unwrap();
        let summary = run_replay(
            trace,
            dictionary,
            ReplayOptions {
                agent: AgentKind::Codex,
                model: "test-model".to_string(),
                target: format!("http://{address}"),
                output_dir: output.path().to_path_buf(),
                max_in_flight: 1,
                warmup_connections: 0,
                start_delay: Duration::from_millis(10),
                timeout: Duration::from_secs(5),
                time_scale: 1.0,
                preserve_request_ids: false,
                static_headers: Vec::new(),
            },
        )
        .await
        .unwrap();

        assert_eq!(summary.succeeded, 3);
        assert!(summary.local_admission_lag_ms.max >= 20.0);
        assert!(summary.dispatch_lag_ms.max >= summary.local_admission_lag_ms.max);
        assert!(summary.scheduler_wake_lag_ms.max < summary.local_admission_lag_ms.max);
        server.abort();
    }

    #[test]
    fn scaled_offsets_use_integer_nanoseconds() {
        assert_eq!(scaled_offset_ns(1, 1.0).unwrap(), 1_000_000);
        assert_eq!(scaled_offset_ns(1, 2.0).unwrap(), 500_000);
        assert_eq!(scaled_offset_ns(1, 3.0).unwrap(), 333_333);
    }
}
