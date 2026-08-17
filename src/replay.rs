// SPDX-License-Identifier: Apache-2.0

use std::collections::{BTreeMap, VecDeque};
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use clap::ValueEnum;
use futures_util::StreamExt;
use hdrhistogram::Histogram;
use reqwest::header::{HeaderName, HeaderValue};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinSet;
use uuid::Uuid;

use crate::agent::{AgentKind, agent_headers};
use crate::clock::{TIMER_BACKEND, sleep_until};
use crate::scenario::{
    CompactionExpectedEffect, GeneratedCompactionAttempt, GeneratedNodeKind, GeneratedScenario,
};
use crate::scheduler::ReadyQueue;
use crate::token_shape::{TokenDictionary, TokenDictionaryManifest};
use crate::trace::{
    AgentContext, LoadedTrace, StoredTrace, StoredTraceReader, TraceManifest, TraceRequest,
    TraceStorageManifest,
};

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
    pub prepare_lookahead: Duration,
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
    pub request_kind: GeneratedNodeKind,
    pub expected_output_tokens: Option<u32>,
    pub observed_output_tokens: Option<u64>,
    pub output_length_match: Option<bool>,
    pub control_only_match: Option<bool>,
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
    pub workload_kind: &'static str,
    pub run_id: String,
    pub agent: AgentKind,
    pub model: String,
    pub target: String,
    pub time_scale: f64,
    pub max_in_flight: usize,
    pub warmup_connections: usize,
    pub http_transport: HttpTransport,
    pub prepare_lookahead_ms: u64,
    pub result_flush_interval: usize,
    pub max_dispatch_p99_ms: f64,
    pub max_dispatch_max_ms: f64,
    pub timer_backend: &'static str,
    pub static_header_names: Vec<String>,
    #[serde(flatten)]
    pub fidelity: FidelityLabels,
    pub source: TraceManifest,
    pub source_storage: Option<TraceStorageManifest>,
    pub token_dictionary: TokenDictionaryManifest,
    pub request_count: usize,
    pub model_turns: usize,
    pub session_closes: usize,
    pub budgeted_model_turns: usize,
    pub unbudgeted_model_turns: usize,
    pub planned_aborts: usize,
    pub succeeded: usize,
    pub failed: usize,
    pub output_length_matches: usize,
    pub output_length_mismatches: usize,
    pub missing_output_usage: usize,
    pub control_only_matches: usize,
    pub control_only_mismatches: usize,
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
pub enum ProtocolSurface {
    ChatCompletions,
    Responses,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TrafficKind {
    SyntheticKvShape,
    CapturedTrace,
    NativeAgent,
}

#[derive(Debug, Clone, Serialize)]
pub struct FidelityLabels {
    pub protocol_surface: ProtocolSurface,
    pub traffic_kind: TrafficKind,
    pub token_path_verified: bool,
    pub engine_cache_mode: BTreeMap<String, String>,
    pub capacity_performance_conclusions_allowed: bool,
    pub conclusion_blockers: Vec<String>,
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
    request_kind: GeneratedNodeKind,
    expected_output_tokens: Option<u32>,
    compaction_attempt: Option<GeneratedCompactionAttempt>,
}

struct RequestExecution {
    kind: GeneratedNodeKind,
    output_budget_tokens: Option<u32>,
    compaction_attempt: Option<GeneratedCompactionAttempt>,
}

trait RequestStream {
    fn next_request(&mut self) -> Result<Option<TraceRequest>>;
}

struct MemoryRequestStream {
    requests: std::vec::IntoIter<TraceRequest>,
}

impl RequestStream for MemoryRequestStream {
    fn next_request(&mut self) -> Result<Option<TraceRequest>> {
        Ok(self.requests.next())
    }
}

impl RequestStream for StoredTraceReader {
    fn next_request(&mut self) -> Result<Option<TraceRequest>> {
        StoredTraceReader::next_request(self)
    }
}

pub async fn run_replay(
    trace: LoadedTrace,
    dictionary: TokenDictionary,
    options: ReplayOptions,
) -> Result<RunSummary> {
    let LoadedTrace { requests, manifest } = trace;
    run_replay_stream(
        Box::new(MemoryRequestStream {
            requests: requests.into_iter(),
        }),
        manifest,
        None,
        dictionary,
        options,
    )
    .await
}

pub async fn run_stored_replay(
    trace: StoredTrace,
    dictionary: TokenDictionary,
    options: ReplayOptions,
) -> Result<RunSummary> {
    let manifest = trace.manifest.clone();
    let storage = trace.storage.clone();
    let reader = trace.reader();
    run_replay_stream(
        Box::new(reader),
        manifest,
        Some(storage),
        dictionary,
        options,
    )
    .await
}

async fn run_replay_stream(
    mut requests: Box<dyn RequestStream>,
    source_manifest: TraceManifest,
    source_storage: Option<TraceStorageManifest>,
    dictionary: TokenDictionary,
    options: ReplayOptions,
) -> Result<RunSummary> {
    validate_options(&options)?;
    prepare_open_file_limit(options.max_in_flight)?;
    fs::create_dir_all(&options.output_dir).with_context(|| {
        format!(
            "failed to create output directory {}",
            options.output_dir.display()
        )
    })?;

    let client = build_http_client(&options)?;
    let target = normalize_target(&options.target);
    let run_id = Uuid::new_v4().to_string();
    let token_manifest = dictionary.manifest().clone();
    let mut pending = requests.next_request()?;
    let mut schedule = VecDeque::<(u64, PreparedRequest)>::new();
    ReplayPreparationContext {
        dictionary: &dictionary,
        client: &client,
        target: &target,
        run_id: &run_id,
        options: &options,
        first_received_ms: source_manifest.first_request_received_ms,
    }
    .fill_queue(
        &mut *requests,
        &mut pending,
        &mut schedule,
        duration_ns(options.prepare_lookahead),
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
    let mut sink = ResultSink::create(
        &options.output_dir.join("requests.jsonl"),
        options.result_flush_interval,
        options.max_dispatch_p99_ms,
    )?;

    while pending.is_some() || !schedule.is_empty() || !tasks.is_empty() {
        while let Some(result) = tasks.try_join_next() {
            sink.record(result.context("a replay task failed")??)?;
        }

        let now_ns = duration_ns(Instant::now().saturating_duration_since(base));
        let prepare_horizon_ns = now_ns.saturating_add(duration_ns(options.prepare_lookahead));
        ReplayPreparationContext {
            dictionary: &dictionary,
            client: &context.client,
            target: &target,
            run_id: &run_id,
            options: &options,
            first_received_ms: source_manifest.first_request_received_ms,
        }
        .fill_queue(
            &mut *requests,
            &mut pending,
            &mut schedule,
            prepare_horizon_ns,
        )?;

        let mut dispatched = false;
        while schedule.front().is_some_and(|(ready_at_ns, _)| {
            base.checked_add(Duration::from_nanos(*ready_at_ns))
                .is_some_and(|deadline| Instant::now() >= deadline)
        }) {
            let (ready_at_ns, prepared) = schedule.pop_front().expect("a due request exists");
            let scheduler_wake = Instant::now();
            let scheduled = base
                .checked_add(Duration::from_nanos(ready_at_ns))
                .expect("the deadline was validated before queue release");
            let context = context.clone();
            let permit = match Arc::clone(&semaphore).try_acquire_owned() {
                Ok(permit) => permit,
                Err(_) => {
                    sink.record(admission_failure(
                        &context,
                        scheduled,
                        scheduler_wake,
                        ready_at_ns,
                        prepared,
                        options.max_in_flight,
                    ))?;
                    continue;
                }
            };
            tasks.spawn(async move {
                send_request(
                    &context,
                    scheduled,
                    scheduler_wake,
                    ready_at_ns,
                    prepared,
                    permit,
                )
                .await
            });
            dispatched = true;
        }

        if dispatched {
            continue;
        }
        let next_prepare_ns = pending
            .as_ref()
            .map(|request| {
                scaled_offset_ns(
                    request.request_received_ms - source_manifest.first_request_received_ms,
                    options.time_scale,
                )
                .map(|ready_at_ns| {
                    ready_at_ns.saturating_sub(duration_ns(options.prepare_lookahead))
                })
            })
            .transpose()?;
        let next_dispatch_ns = schedule.front().map(|(ready_at_ns, _)| *ready_at_ns);
        let next_wake_ns = next_prepare_ns.into_iter().chain(next_dispatch_ns).min();
        match (next_wake_ns, tasks.is_empty()) {
            (Some(next_wake_ns), false) => {
                let deadline = base
                    .checked_add(Duration::from_nanos(next_wake_ns))
                    .context("a replay timestamp exceeds the monotonic clock range")?;
                tokio::select! {
                    () = sleep_until(deadline) => {},
                    result = tasks.join_next() => {
                        if let Some(result) = result {
                            sink.record(result.context("a replay task failed")??)?;
                        }
                    }
                }
            }
            (Some(next_wake_ns), true) => {
                let deadline = base
                    .checked_add(Duration::from_nanos(next_wake_ns))
                    .context("a replay timestamp exceeds the monotonic clock range")?;
                sleep_until(deadline).await;
            }
            (None, false) => {
                if let Some(result) = tasks.join_next().await {
                    sink.record(result.context("a replay task failed")??)?;
                }
            }
            (None, true) => break,
        }
    }

    sink.flush()?;
    let summary = summarize(
        SummaryIdentity {
            workload_kind: "trace-replay",
            traffic_kind: TrafficKind::CapturedTrace,
            run_id,
            target: &target,
            source_storage,
        },
        &source_manifest,
        &token_manifest,
        &options,
        &sink.accumulator,
        wall_started.elapsed(),
    );
    write_json(&options.output_dir.join("run.json"), &summary)?;
    Ok(summary)
}

pub async fn run_generated_scenario(
    scenario: &GeneratedScenario,
    dictionary: TokenDictionary,
    options: ReplayOptions,
) -> Result<RunSummary> {
    validate_options(&options)?;
    if options.agent != scenario.config.agent {
        bail!("generated scenario agent does not match the request header adapter");
    }
    prepare_open_file_limit(options.max_in_flight)?;
    fs::create_dir_all(&options.output_dir).with_context(|| {
        format!(
            "failed to create output directory {}",
            options.output_dir.display()
        )
    })?;
    write_json(&options.output_dir.join("scenario.json"), scenario)?;

    let client = build_http_client(&options)?;
    let target = normalize_target(&options.target);
    let run_id = Uuid::new_v4().to_string();
    let token_manifest = dictionary.manifest().clone();
    let mut prepared = scenario
        .nodes
        .iter()
        .map(|node| {
            prepare_request(
                &client,
                &target,
                &run_id,
                &options,
                &dictionary,
                node.request.clone(),
                RequestExecution {
                    kind: node.kind,
                    output_budget_tokens: node.output_budget_tokens,
                    compaction_attempt: node.compaction_attempt.clone(),
                },
            )
            .map(Some)
        })
        .collect::<Result<Vec<_>>>()?;
    let remaining_dependencies = scenario
        .nodes
        .iter()
        .map(|node| node.dependencies.len())
        .collect::<Vec<_>>();
    let mut successors = vec![Vec::new(); scenario.nodes.len()];
    for (ordinal, node) in scenario.nodes.iter().enumerate() {
        for dependency in &node.dependencies {
            let outputs = successors
                .get_mut(*dependency)
                .with_context(|| format!("node {ordinal} has missing dependency {dependency}"))?;
            outputs.push(ordinal);
        }
    }
    let mut ready = ReadyQueue::with_capacity(scenario.nodes.len());
    for (ordinal, node) in scenario.nodes.iter().enumerate() {
        if node.dependencies.is_empty() {
            let arrival_ms = node
                .root_arrival_ms
                .with_context(|| format!("root node {ordinal} has no root arrival"))?;
            ready.push(
                scaled_offset_ns(arrival_ms, options.time_scale)?,
                ordinal,
                ordinal,
            );
        } else if node.root_arrival_ms.is_some() {
            bail!("dependent node {ordinal} also has a root arrival");
        }
    }
    warm_connections(&client, &target, &options).await?;

    let wall_started = Instant::now();
    let base = wall_started
        .checked_add(options.start_delay)
        .context("generation start delay exceeds the monotonic clock range")?;
    let context = ReplayContext { client, base };
    let semaphore = Arc::new(Semaphore::new(options.max_in_flight));
    let options = Arc::new(options);
    let mut tasks = JoinSet::new();
    let mut scheduler = GeneratedSchedulerState {
        scenario,
        successors,
        remaining_dependencies,
        dependency_completion_ns: vec![0; scenario.nodes.len()],
        ready,
        sink: ResultSink::create(
            &options.output_dir.join("requests.jsonl"),
            options.result_flush_interval,
            options.max_dispatch_p99_ms,
        )?,
        base,
        time_scale: options.time_scale,
    };

    while !scheduler.ready.is_empty() || !tasks.is_empty() {
        if let Some(next_ready_ns) = scheduler.ready.next_ready_at_ns() {
            let deadline = base
                .checked_add(Duration::from_nanos(next_ready_ns))
                .context("a generated timestamp exceeds the monotonic clock range")?;
            if Instant::now() < deadline {
                if tasks.is_empty() {
                    sleep_until(deadline).await;
                } else {
                    tokio::select! {
                        () = sleep_until(deadline) => {},
                        result = tasks.join_next() => {
                            if let Some(result) = result {
                                scheduler.release(
                                    result.context("a generated request task failed")??,
                                )?;
                                continue;
                            }
                        }
                    }
                }
            }
            let now_ns = duration_ns(Instant::now().saturating_duration_since(base));
            for item in scheduler.ready.pop_due(now_ns, usize::MAX) {
                let ordinal = item.value;
                let request = prepared
                    .get_mut(ordinal)
                    .and_then(Option::take)
                    .with_context(|| format!("generated node {ordinal} was released twice"))?;
                let scheduled = base
                    .checked_add(Duration::from_nanos(item.ready_at_ns))
                    .context("a generated timestamp exceeds the monotonic clock range")?;
                let scheduler_wake = Instant::now();
                let context = context.clone();
                let semaphore = Arc::clone(&semaphore);
                tasks.spawn(async move {
                    let permit = semaphore
                        .acquire_owned()
                        .await
                        .context("the generated request semaphore closed")?;
                    let result = send_request(
                        &context,
                        scheduled,
                        scheduler_wake,
                        item.ready_at_ns,
                        request,
                        permit,
                    )
                    .await?;
                    Ok::<_, anyhow::Error>((ordinal, Instant::now(), result))
                });
            }
        } else if let Some(result) = tasks.join_next().await {
            scheduler.release(result.context("a generated request task failed")??)?;
        }
    }

    scheduler.sink.flush()?;
    if scheduler.sink.accumulator.request_count != scenario.nodes.len() {
        bail!(
            "generated graph stalled after {} of {} requests",
            scheduler.sink.accumulator.request_count,
            scenario.nodes.len()
        );
    }
    let summary = summarize(
        SummaryIdentity {
            workload_kind: "generated-closed-loop",
            traffic_kind: TrafficKind::SyntheticKvShape,
            run_id,
            target: &target,
            source_storage: None,
        },
        &scenario.trace_manifest,
        &token_manifest,
        &options,
        &scheduler.sink.accumulator,
        wall_started.elapsed(),
    );
    write_json(&options.output_dir.join("run.json"), &summary)?;
    Ok(summary)
}

struct GeneratedSchedulerState<'a> {
    scenario: &'a GeneratedScenario,
    successors: Vec<Vec<usize>>,
    remaining_dependencies: Vec<usize>,
    dependency_completion_ns: Vec<u64>,
    ready: ReadyQueue<usize>,
    sink: ResultSink,
    base: Instant,
    time_scale: f64,
}

impl GeneratedSchedulerState<'_> {
    fn release(
        &mut self,
        (ordinal, completed_at, result): (usize, Instant, RequestResult),
    ) -> Result<()> {
        let completed_ns = duration_ns(completed_at.saturating_duration_since(self.base));
        self.sink.record(result)?;
        for successor in self
            .successors
            .get(ordinal)
            .with_context(|| format!("generated node {ordinal} has no successor slot"))?
        {
            let remaining = self
                .remaining_dependencies
                .get_mut(*successor)
                .with_context(|| format!("generated successor {successor} is missing"))?;
            *remaining = remaining
                .checked_sub(1)
                .with_context(|| format!("generated successor {successor} was released twice"))?;
            let dependency_completion_ns = self
                .dependency_completion_ns
                .get_mut(*successor)
                .with_context(|| format!("generated successor {successor} is missing timing"))?;
            *dependency_completion_ns = (*dependency_completion_ns).max(completed_ns);
            if *remaining == 0 {
                let delay_ns = scaled_offset_ns(
                    self.scenario.nodes[*successor].delay_after_dependencies_ms,
                    self.time_scale,
                )?;
                self.ready.push(
                    dependency_completion_ns
                        .checked_add(delay_ns)
                        .context("generated successor timestamp overflow")?,
                    *successor,
                    *successor,
                );
            }
        }
        Ok(())
    }
}

struct ReplayPreparationContext<'a> {
    dictionary: &'a TokenDictionary,
    client: &'a reqwest::Client,
    target: &'a str,
    run_id: &'a str,
    options: &'a ReplayOptions,
    first_received_ms: u64,
}

impl ReplayPreparationContext<'_> {
    fn fill_queue(
        &self,
        requests: &mut dyn RequestStream,
        pending: &mut Option<TraceRequest>,
        schedule: &mut VecDeque<(u64, PreparedRequest)>,
        prepare_horizon_ns: u64,
    ) -> Result<()> {
        while let Some(request) = pending.as_ref() {
            let ready_at_ns = scaled_offset_ns(
                request.request_received_ms - self.first_received_ms,
                self.options.time_scale,
            )?;
            if ready_at_ns > prepare_horizon_ns {
                break;
            }
            let request = pending.take().expect("the pending request exists");
            let ordinal = request.ordinal;
            let kind = if request.is_session_close() {
                GeneratedNodeKind::SessionClose
            } else if request.output_tokens == 0 {
                bail!(
                    "request {} has zero output but is not a session-final control request",
                    request.source_request_id
                );
            } else {
                GeneratedNodeKind::ModelTurn
            };
            let execution = RequestExecution {
                kind,
                output_budget_tokens: (kind == GeneratedNodeKind::ModelTurn)
                    .then_some(request.output_tokens),
                compaction_attempt: None,
            };
            let prepared = prepare_request(
                self.client,
                self.target,
                self.run_id,
                self.options,
                self.dictionary,
                request,
                execution,
            )?;
            if let Some((previous_ready_at_ns, previous)) = schedule.back()
                && (*previous_ready_at_ns, previous.metadata.ordinal) > (ready_at_ns, ordinal)
            {
                bail!("the trace request stream is not ordered by timestamp and ordinal");
            }
            schedule.push_back((ready_at_ns, prepared));
            *pending = requests.next_request()?;
        }
        Ok(())
    }
}

fn prepare_request(
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
    if execution.kind == GeneratedNodeKind::ModelTurn {
        let tokens = dictionary.synthesize(&request)?;
        body["nvext"] = json!({"token_data": tokens});
    }
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
        request_kind: execution.kind,
        expected_output_tokens: execution.output_budget_tokens,
        compaction_attempt: execution.compaction_attempt,
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

fn build_http_client(options: &ReplayOptions) -> Result<reqwest::Client> {
    let mut builder = reqwest::Client::builder()
        .connect_timeout(options.timeout.min(Duration::from_secs(30)))
        .timeout(options.timeout)
        .pool_max_idle_per_host(options.max_in_flight);
    if matches!(options.http_transport, HttpTransport::Http2PriorKnowledge) {
        builder = builder.http2_prior_knowledge();
    }
    builder.build().context("failed to build the HTTP client")
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
    if options.prepare_lookahead.is_zero() {
        bail!("prepare_lookahead must be greater than zero");
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
        HeaderName::from_bytes(name.as_bytes())
            .with_context(|| format!("invalid static header name {name:?}"))?;
        HeaderValue::from_str(value)
            .with_context(|| format!("invalid value for static header {name:?}"))?;
    }
    Ok(())
}

#[cfg(unix)]
fn prepare_open_file_limit(max_in_flight: usize) -> Result<()> {
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
fn prepare_open_file_limit(_max_in_flight: usize) -> Result<()> {
    Ok(())
}

async fn send_request(
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
    let request_kind = prepared.metadata.request_kind;
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
            match request_kind {
                GeneratedNodeKind::ModelTurn => {
                    result.output_length_match = expected_output_tokens.and_then(|expected| {
                        stream.output_tokens.map(|tokens| tokens == expected as u64)
                    });
                }
                GeneratedNodeKind::SessionClose => {
                    result.control_only_match =
                        Some(stream.ttft_ms.is_none() && stream.output_tokens.unwrap_or(0) == 0);
                }
            }
        }
        Err(error) => result.error = Some(error.to_string()),
    }
    Ok(result)
}

fn admission_failure(
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
        request_kind: prepared.metadata.request_kind,
        expected_output_tokens: prepared.metadata.expected_output_tokens,
        observed_output_tokens: None,
        output_length_match: None,
        control_only_match: None,
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

struct ResultSink {
    writer: BufWriter<File>,
    flush_interval: usize,
    records_since_flush: usize,
    accumulator: RunAccumulator,
}

impl ResultSink {
    fn create(path: &Path, flush_interval: usize, dispatch_p99_limit_ms: f64) -> Result<Self> {
        let writer = BufWriter::new(
            File::create(path).with_context(|| format!("failed to create {}", path.display()))?,
        );
        Ok(Self {
            writer,
            flush_interval,
            records_since_flush: 0,
            accumulator: RunAccumulator::new(dispatch_p99_limit_ms)?,
        })
    }

    fn record(&mut self, result: RequestResult) -> Result<()> {
        self.accumulator.record(&result)?;
        serde_json::to_writer(&mut self.writer, &result)
            .context("failed to write request result")?;
        self.writer.write_all(b"\n")?;
        self.records_since_flush += 1;
        if self.records_since_flush >= self.flush_interval {
            self.flush()?;
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        self.writer.flush()?;
        self.records_since_flush = 0;
        Ok(())
    }
}

struct RunAccumulator {
    request_count: usize,
    succeeded: usize,
    model_turns: usize,
    session_closes: usize,
    budgeted_model_turns: usize,
    unbudgeted_model_turns: usize,
    planned_aborts: usize,
    output_length_matches: usize,
    output_length_mismatches: usize,
    missing_output_usage: usize,
    control_only_matches: usize,
    control_only_mismatches: usize,
    planned_abort_matches: usize,
    planned_abort_mismatches: usize,
    scheduler_wake_lag_us: Histogram<u64>,
    dispatch_lag_us: Histogram<u64>,
    local_admission_lag_us: Histogram<u64>,
    dispatch_p99_limit_ms: f64,
    dispatch_at_or_below_p99_limit: usize,
    dispatch_max_ms: f64,
}

impl RunAccumulator {
    fn new(dispatch_p99_limit_ms: f64) -> Result<Self> {
        Ok(Self {
            request_count: 0,
            succeeded: 0,
            model_turns: 0,
            session_closes: 0,
            budgeted_model_turns: 0,
            unbudgeted_model_turns: 0,
            planned_aborts: 0,
            output_length_matches: 0,
            output_length_mismatches: 0,
            missing_output_usage: 0,
            control_only_matches: 0,
            control_only_mismatches: 0,
            planned_abort_matches: 0,
            planned_abort_mismatches: 0,
            scheduler_wake_lag_us: Histogram::new(3)?,
            dispatch_lag_us: Histogram::new(3)?,
            local_admission_lag_us: Histogram::new(3)?,
            dispatch_p99_limit_ms,
            dispatch_at_or_below_p99_limit: 0,
            dispatch_max_ms: 0.0,
        })
    }

    fn record(&mut self, result: &RequestResult) -> Result<()> {
        self.request_count += 1;
        let planned_abort_succeeded = result.planned_abort_match == Some(true);
        if planned_abort_succeeded
            || (result.error.is_none() && result.status_code.is_some_and(|status| status < 400))
        {
            self.succeeded += 1;
        }
        match result.request_kind {
            GeneratedNodeKind::ModelTurn => {
                self.model_turns += 1;
                if result.expected_output_tokens.is_some() {
                    self.budgeted_model_turns += 1;
                    match result.output_length_match {
                        Some(true) => self.output_length_matches += 1,
                        Some(false) => self.output_length_mismatches += 1,
                        None => self.missing_output_usage += 1,
                    }
                } else {
                    self.unbudgeted_model_turns += 1;
                }
            }
            GeneratedNodeKind::SessionClose => {
                self.session_closes += 1;
                match result.control_only_match {
                    Some(true) => self.control_only_matches += 1,
                    Some(false) | None => self.control_only_mismatches += 1,
                }
            }
        }
        if result.compaction_expected_effect == Some(CompactionExpectedEffect::NoMutationAborted) {
            self.planned_aborts += 1;
            match result.planned_abort_match {
                Some(true) => self.planned_abort_matches += 1,
                Some(false) | None => self.planned_abort_mismatches += 1,
            }
        }
        record_milliseconds(
            &mut self.scheduler_wake_lag_us,
            result.scheduler_wake_lag_ms,
        )?;
        record_milliseconds(&mut self.dispatch_lag_us, result.dispatch_lag_ms)?;
        record_milliseconds(
            &mut self.local_admission_lag_us,
            result.local_admission_lag_ms,
        )?;
        if result.dispatch_lag_ms <= self.dispatch_p99_limit_ms {
            self.dispatch_at_or_below_p99_limit += 1;
        }
        self.dispatch_max_ms = self.dispatch_max_ms.max(result.dispatch_lag_ms);
        Ok(())
    }
}

fn record_milliseconds(histogram: &mut Histogram<u64>, value: f64) -> Result<()> {
    if !value.is_finite() || value < 0.0 {
        bail!("a request timing value is not a non-negative finite number");
    }
    let microseconds = (value * 1000.0).round_ties_even();
    if microseconds > u64::MAX as f64 {
        bail!("a request timing value exceeds the histogram range");
    }
    histogram.record(microseconds as u64)?;
    Ok(())
}

fn histogram_percentiles(histogram: &Histogram<u64>) -> Percentiles {
    if histogram.is_empty() {
        return Percentiles::default();
    }
    Percentiles {
        min: histogram.min() as f64 / 1000.0,
        p50: histogram.value_at_quantile(0.50) as f64 / 1000.0,
        p95: histogram.value_at_quantile(0.95) as f64 / 1000.0,
        p99: histogram.value_at_quantile(0.99) as f64 / 1000.0,
        max: histogram.max() as f64 / 1000.0,
    }
}

struct SummaryIdentity<'a> {
    workload_kind: &'static str,
    traffic_kind: TrafficKind,
    run_id: String,
    target: &'a str,
    source_storage: Option<TraceStorageManifest>,
}

fn summarize(
    identity: SummaryIdentity<'_>,
    source: &TraceManifest,
    dictionary: &TokenDictionaryManifest,
    options: &ReplayOptions,
    accumulator: &RunAccumulator,
    elapsed: Duration,
) -> RunSummary {
    let scheduler_wake_lag_ms = histogram_percentiles(&accumulator.scheduler_wake_lag_us);
    let mut dispatch_lag_ms = histogram_percentiles(&accumulator.dispatch_lag_us);
    // Histograms are microsecond-quantized. Preserve the exact maximum so a
    // value just over the configured hard limit cannot round down and pass.
    dispatch_lag_ms.max = accumulator.dispatch_max_ms;
    let local_admission_lag_ms = histogram_percentiles(&accumulator.local_admission_lag_us);
    let request_fidelity_matches = accumulator.request_count == source.request_count
        && accumulator.succeeded == source.request_count
        && accumulator.output_length_matches == accumulator.budgeted_model_turns
        && accumulator.missing_output_usage == 0
        && accumulator.control_only_matches == accumulator.session_closes
        && accumulator.planned_abort_matches == accumulator.planned_aborts;
    let dispatch_timing_matches = dispatch_timing_matches(accumulator, options.max_dispatch_max_ms);
    let passed = request_fidelity_matches && dispatch_timing_matches;
    let mut conclusion_blockers = Vec::new();
    if !options.token_path_verified {
        conclusion_blockers.push("token_path_unverified".to_string());
    }
    if options.engine_cache_mode.is_empty() {
        conclusion_blockers.push("engine_cache_mode_undeclared".to_string());
    }
    let fidelity = FidelityLabels {
        protocol_surface: ProtocolSurface::ChatCompletions,
        traffic_kind: identity.traffic_kind,
        token_path_verified: options.token_path_verified,
        engine_cache_mode: options.engine_cache_mode.clone(),
        capacity_performance_conclusions_allowed: conclusion_blockers.is_empty(),
        conclusion_blockers,
    };

    RunSummary {
        workload_kind: identity.workload_kind,
        run_id: identity.run_id,
        agent: options.agent,
        model: options.model.clone(),
        target: identity.target.to_string(),
        time_scale: options.time_scale,
        max_in_flight: options.max_in_flight,
        warmup_connections: options.warmup_connections,
        http_transport: options.http_transport,
        prepare_lookahead_ms: options.prepare_lookahead.as_millis().min(u64::MAX as u128) as u64,
        result_flush_interval: options.result_flush_interval,
        max_dispatch_p99_ms: options.max_dispatch_p99_ms,
        max_dispatch_max_ms: options.max_dispatch_max_ms,
        timer_backend: TIMER_BACKEND,
        static_header_names: options
            .static_headers
            .iter()
            .map(|(name, _)| name.clone())
            .collect(),
        fidelity,
        source: source.clone(),
        source_storage: identity.source_storage,
        token_dictionary: dictionary.clone(),
        request_count: accumulator.request_count,
        model_turns: accumulator.model_turns,
        session_closes: accumulator.session_closes,
        budgeted_model_turns: accumulator.budgeted_model_turns,
        unbudgeted_model_turns: accumulator.unbudgeted_model_turns,
        planned_aborts: accumulator.planned_aborts,
        succeeded: accumulator.succeeded,
        failed: accumulator.request_count - accumulator.succeeded,
        output_length_matches: accumulator.output_length_matches,
        output_length_mismatches: accumulator.output_length_mismatches,
        missing_output_usage: accumulator.missing_output_usage,
        control_only_matches: accumulator.control_only_matches,
        control_only_mismatches: accumulator.control_only_mismatches,
        planned_abort_matches: accumulator.planned_abort_matches,
        planned_abort_mismatches: accumulator.planned_abort_mismatches,
        scheduler_wake_lag_ms,
        dispatch_lag_ms,
        local_admission_lag_ms,
        request_fidelity_matches,
        dispatch_timing_matches,
        passed,
        total_time_ms: millis(elapsed),
    }
}

fn dispatch_timing_matches(accumulator: &RunAccumulator, max_limit_ms: f64) -> bool {
    let p99_rank = usize::try_from((accumulator.request_count as u128 * 99).div_ceil(100))
        .unwrap_or(usize::MAX);
    accumulator.dispatch_at_or_below_p99_limit >= p99_rank
        && accumulator.dispatch_max_ms <= max_limit_ms
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
    use crate::scenario::{GeneratedScenario, GeneratorConfig};
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
    fn hard_dispatch_limit_uses_the_unrounded_maximum() {
        let mut accumulator = RunAccumulator::new(10.0).unwrap();
        accumulator.request_count = 1;
        accumulator.dispatch_at_or_below_p99_limit = 1;
        accumulator.dispatch_max_ms = 5.000_4;
        assert!(!dispatch_timing_matches(&accumulator, 5.0));
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
        if capture
            .0
            .lock()
            .unwrap()
            .as_ref()
            .and_then(|(headers, _)| headers.get("x-dynamo-session-final"))
            .is_some_and(|value| value == "true")
        {
            return Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "text/event-stream")
                .body(Body::from("data: [DONE]\n\n"))
                .unwrap();
        }
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
            trace_block_size: 16,
            input_sequence_hashes: vec![11],
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
                distinct_sequence_hashes: 1,
                trace_block_size: 16,
                source_digest_sha256: "source".to_string(),
            },
        };
        let dictionary = TokenDictionary::build(
            &[request],
            SafeTokenAlphabet::from_unverified_range(100, 1024, &[]).unwrap(),
        )
        .unwrap();
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
                http_transport: HttpTransport::Http2PriorKnowledge,
                prepare_lookahead: Duration::from_millis(100),
                result_flush_interval: 1,
                max_dispatch_p99_ms: 100.0,
                max_dispatch_max_ms: 100.0,
                start_delay: Duration::from_millis(5),
                timeout: Duration::from_secs(5),
                time_scale: 1.0,
                token_path_verified: false,
                engine_cache_mode: BTreeMap::new(),
                static_headers: Vec::new(),
            },
        )
        .await
        .unwrap();

        assert_eq!(summary.succeeded, 1);
        assert_eq!(summary.output_length_matches, 1);
        assert!(summary.passed);
        assert!(summary.total_time_ms >= 5.0);
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
        assert!(body.get("min_tokens").is_none());
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

    #[tokio::test]
    async fn replay_sends_session_close_without_model_shape() {
        let capture = Capture::default();
        let app = Router::new()
            .route("/v1/chat/completions", post(shape_endpoint))
            .with_state(capture.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        let request = TraceRequest {
            ordinal: 0,
            source_request_id: "source-close".to_string(),
            source_x_request_id: None,
            source_model: None,
            input_tokens: 3,
            output_tokens: 0,
            request_received_ms: 1000,
            trace_block_size: 16,
            input_sequence_hashes: vec![11],
            agent_context: Some(AgentContext {
                session_id: "thread-1".to_string(),
                parent_session_id: None,
                session_final: Some(true),
                compaction: None,
                input_trigger: Some("other".to_string()),
            }),
        };
        let trace = LoadedTrace {
            requests: vec![request.clone()],
            manifest: TraceManifest {
                request_count: 1,
                zero_output_requests: 1,
                session_count: 1,
                requests_with_agent_context: 1,
                first_request_received_ms: 1000,
                last_request_received_ms: 1000,
                duration_ms: 0,
                input_tokens: 3,
                output_tokens: 0,
                distinct_sequence_hashes: 1,
                trace_block_size: 16,
                source_digest_sha256: "source".to_string(),
            },
        };
        let dictionary = TokenDictionary::build(
            &[request],
            SafeTokenAlphabet::from_unverified_range(100, 1024, &[]).unwrap(),
        )
        .unwrap();
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
                http_transport: HttpTransport::Http2PriorKnowledge,
                prepare_lookahead: Duration::from_millis(100),
                result_flush_interval: 1,
                max_dispatch_p99_ms: 100.0,
                max_dispatch_max_ms: 100.0,
                start_delay: Duration::from_millis(5),
                timeout: Duration::from_secs(5),
                time_scale: 1.0,
                token_path_verified: true,
                engine_cache_mode: BTreeMap::from([(
                    "ownership".to_string(),
                    "session".to_string(),
                )]),
                static_headers: Vec::new(),
            },
        )
        .await
        .unwrap();

        assert_eq!(summary.model_turns, 0);
        assert_eq!(summary.session_closes, 1);
        assert_eq!(summary.control_only_matches, 1);
        assert!(summary.passed);
        assert!(summary.fidelity.capacity_performance_conclusions_allowed);
        let (headers, body) = capture.0.lock().unwrap().clone().unwrap();
        assert_eq!(headers.get("x-dynamo-session-final").unwrap(), "true");
        assert!(body.get("nvext").is_none());
        assert!(body.get("max_tokens").is_none());
        assert!(body.get("ignore_eos").is_none());
        let run: serde_json::Value =
            serde_json::from_reader(std::fs::File::open(output.path().join("run.json")).unwrap())
                .unwrap();
        assert_eq!(run["protocol_surface"], "chat_completions");
        assert_eq!(run["traffic_kind"], "captured_trace");
        assert_eq!(run["token_path_verified"], true);
        assert_eq!(run["engine_cache_mode"]["ownership"], "session");
        assert!(run.get("fidelity").is_none());
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
                trace_block_size: 16,
                input_sequence_hashes: vec![11],
                agent_context: Some(AgentContext {
                    session_id: format!("thread-{}", ordinal % 12),
                    parent_session_id: None,
                    session_final: None,
                    compaction: None,
                    input_trigger: Some("user_message".to_string()),
                }),
            })
            .collect::<Vec<_>>();
        let dictionary = TokenDictionary::build(
            &requests,
            SafeTokenAlphabet::from_unverified_range(100, 1024, &[]).unwrap(),
        )
        .unwrap();
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
                distinct_sequence_hashes: 1,
                trace_block_size: 16,
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
                http_transport: HttpTransport::Auto,
                prepare_lookahead: Duration::from_millis(100),
                result_flush_interval: 1,
                max_dispatch_p99_ms: 100.0,
                max_dispatch_max_ms: 100.0,
                start_delay: Duration::from_millis(25),
                timeout: Duration::from_secs(5),
                time_scale: 1.0,
                token_path_verified: false,
                engine_cache_mode: BTreeMap::new(),
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn generated_graph_releases_tool_successor_after_completion() {
        let capture = Capture::default();
        let app = Router::new()
            .route("/v1/chat/completions", post(shape_endpoint))
            .with_state(capture);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let config: GeneratorConfig = toml::from_str(
            r#"
                schema_version = 2
                agent = "codex"
                seed = 9

                [load]
                root_sessions = 1
                concurrent_agents = 1

                [trajectory]
                turns = { kind = "fixed", value = 2 }
                output_tokens = { kind = "fixed", value = 2 }

                [tokens]
                system_prefix_tokens = { kind = "fixed", value = 16 }
                tool_catalog_tokens = { kind = "fixed", value = 16 }
                repository_tokens = { kind = "fixed", value = 16 }
                session_tokens = { kind = "fixed", value = 16 }
                user_tokens = { kind = "fixed", value = 16 }

                [behavior]
                tool_probability = 1.0
                parallel_tool_probability = 0.0
                subagent_probability = 0.0
                swarm_probability = 0.0
                completion_probability = 0.0
                background_request_probability = 0.0

                [compaction]
                enabled = false

                [subagents]
                max_depth = 0
            "#,
        )
        .unwrap();
        let scenario = GeneratedScenario::generate(config.resolve().unwrap()).unwrap();
        assert_eq!(scenario.nodes.len(), 3);
        assert_eq!(scenario.nodes[1].dependencies, vec![0]);
        assert_eq!(scenario.nodes[2].dependencies, vec![1]);
        let dictionary = TokenDictionary::new(
            scenario.trace_manifest.trace_block_size,
            scenario.trace_manifest.distinct_sequence_hashes,
            SafeTokenAlphabet::from_unverified_range(100, 1024, &[]).unwrap(),
        )
        .unwrap();
        let output = tempdir().unwrap();
        let summary = run_generated_scenario(
            &scenario,
            dictionary,
            ReplayOptions {
                agent: AgentKind::Codex,
                model: "test-model".to_string(),
                target: format!("http://{address}"),
                output_dir: output.path().to_path_buf(),
                max_in_flight: 2,
                warmup_connections: 0,
                http_transport: HttpTransport::Auto,
                prepare_lookahead: Duration::from_millis(1),
                result_flush_interval: 1,
                max_dispatch_p99_ms: 100.0,
                max_dispatch_max_ms: 100.0,
                start_delay: Duration::from_millis(5),
                timeout: Duration::from_secs(5),
                time_scale: 100.0,
                token_path_verified: false,
                engine_cache_mode: BTreeMap::new(),
                static_headers: Vec::new(),
            },
        )
        .await
        .unwrap();
        assert_eq!(summary.workload_kind, "generated-closed-loop");
        assert_eq!(summary.succeeded, 3);
        assert_eq!(summary.model_turns, 2);
        assert_eq!(summary.session_closes, 1);
        assert!(summary.passed);
        assert!(output.path().join("scenario.json").is_file());
        server.abort();
    }

    #[test]
    fn scaled_offsets_use_integer_nanoseconds() {
        assert_eq!(scaled_offset_ns(1, 1.0).unwrap(), 1_000_000);
        assert_eq!(scaled_offset_ns(1, 2.0).unwrap(), 500_000);
        assert_eq!(scaled_offset_ns(1, 3.0).unwrap(), 333_333);
    }
}
