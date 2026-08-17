// SPDX-License-Identifier: Apache-2.0

use std::collections::{BTreeMap, VecDeque};
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use clap::ValueEnum;
use serde::{Deserialize, Serialize};
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use uuid::Uuid;

use crate::agent::AgentKind;
use crate::clock::sleep_until;
use crate::scenario::{
    CompactionExpectedEffect, GeneratedCompactionAttempt, GeneratedNodeKind, GeneratedScenario,
};
use crate::scheduler::ReadyQueue;
use crate::token_shape::{TokenDictionary, TokenDictionaryManifest};
use crate::trace::{
    AgentContext, StoredTrace, StoredTraceReader, TraceManifest, TraceRequest, TraceStorageManifest,
};

mod artifacts;
mod request;

pub(crate) use artifacts::percentiles;
use artifacts::{ResultSink, SummaryIdentity, summarize, write_json};
use request::{
    admission_failure, build_http_client, normalize_target, prepare_open_file_limit,
    prepare_request, scaled_offset_ns, send_request, validate_options, warm_connections,
};

#[cfg(test)]
use artifacts::{RunAccumulator, dispatch_timing_matches};
#[cfg(test)]
use request::chunk_contains_output;

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
    pub prepare_lookahead: Option<Duration>,
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
    pub run_id: String,
    pub agent: AgentKind,
    pub model: String,
    pub target: String,
    pub time_scale: f64,
    pub max_in_flight: usize,
    pub warmup_connections: usize,
    pub http_transport: HttpTransport,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prepare_lookahead_ms: Option<u64>,
    pub result_flush_interval: usize,
    pub max_dispatch_p99_ms: f64,
    pub max_dispatch_max_ms: f64,
    pub timer_backend: &'static str,
    pub static_header_names: Vec<String>,
    pub protocol_surface: &'static str,
    pub traffic_kind: TrafficKind,
    pub token_path_verified: bool,
    pub engine_cache_mode: BTreeMap<String, String>,
    pub capacity_performance_conclusions_allowed: bool,
    pub conclusion_blockers: Vec<String>,
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
pub enum TrafficKind {
    SyntheticKvShape,
    CapturedTrace,
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

#[cfg(test)]
struct MemoryRequestStream {
    requests: std::vec::IntoIter<TraceRequest>,
}

#[cfg(test)]
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

#[cfg(test)]
async fn run_replay(
    trace: crate::trace::LoadedTrace,
    dictionary: TokenDictionary,
    options: ReplayOptions,
) -> Result<RunSummary> {
    let crate::trace::LoadedTrace { requests, manifest } = trace;
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
    let prepare_lookahead = options
        .prepare_lookahead
        .context("captured replay requires a preparation lookahead")?;
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
        duration_ns(prepare_lookahead),
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
        let prepare_horizon_ns = now_ns.saturating_add(duration_ns(prepare_lookahead));
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
                .map(|ready_at_ns| ready_at_ns.saturating_sub(duration_ns(prepare_lookahead)))
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
    let options = Arc::new(options);
    let request_factory = GeneratedRequestFactory {
        scenario,
        dictionary: &dictionary,
        client: client.clone(),
        target: target.clone(),
        run_id: run_id.clone(),
        options: Arc::clone(&options),
    };
    let mut prepared = std::iter::repeat_with(|| None)
        .take(scenario.nodes.len())
        .collect::<Vec<_>>();
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
            prepared[ordinal] = Some(request_factory.prepare(ordinal)?);
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
    let mut tasks = JoinSet::new();
    let mut scheduler = GeneratedSchedulerState {
        scenario,
        request_factory,
        prepared,
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
                let request = scheduler.take_request(ordinal)?;
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

struct GeneratedRequestFactory<'a> {
    scenario: &'a GeneratedScenario,
    dictionary: &'a TokenDictionary,
    client: reqwest::Client,
    target: String,
    run_id: String,
    options: Arc<ReplayOptions>,
}

impl GeneratedRequestFactory<'_> {
    fn prepare(&self, ordinal: usize) -> Result<PreparedRequest> {
        let node = self
            .scenario
            .nodes
            .get(ordinal)
            .with_context(|| format!("generated node {ordinal} is missing"))?;
        prepare_request(
            &self.client,
            &self.target,
            &self.run_id,
            &self.options,
            self.dictionary,
            node.request.clone(),
            RequestExecution {
                kind: node.kind,
                output_budget_tokens: node.output_budget_tokens,
                compaction_attempt: node.compaction_attempt.clone(),
            },
        )
    }
}

struct GeneratedSchedulerState<'a> {
    scenario: &'a GeneratedScenario,
    request_factory: GeneratedRequestFactory<'a>,
    prepared: Vec<Option<PreparedRequest>>,
    successors: Vec<Vec<usize>>,
    remaining_dependencies: Vec<usize>,
    dependency_completion_ns: Vec<u64>,
    ready: ReadyQueue<usize>,
    sink: ResultSink,
    base: Instant,
    time_scale: f64,
}

impl GeneratedSchedulerState<'_> {
    fn take_request(&mut self, ordinal: usize) -> Result<PreparedRequest> {
        self.prepared
            .get_mut(ordinal)
            .and_then(Option::take)
            .with_context(|| format!("generated node {ordinal} was released twice"))
    }

    fn release(
        &mut self,
        (ordinal, completed_at, result): (usize, Instant, RequestResult),
    ) -> Result<()> {
        let completed_ns = duration_ns(completed_at.saturating_duration_since(self.base));
        self.sink.record(result)?;
        let successors = self
            .successors
            .get(ordinal)
            .with_context(|| format!("generated node {ordinal} has no successor slot"))?
            .clone();
        for successor in successors {
            let ready_at_ns = {
                let remaining = self
                    .remaining_dependencies
                    .get_mut(successor)
                    .with_context(|| format!("generated successor {successor} is missing"))?;
                *remaining = remaining.checked_sub(1).with_context(|| {
                    format!("generated successor {successor} was released twice")
                })?;
                let dependency_completion_ns = self
                    .dependency_completion_ns
                    .get_mut(successor)
                    .with_context(|| {
                        format!("generated successor {successor} is missing timing")
                    })?;
                *dependency_completion_ns = (*dependency_completion_ns).max(completed_ns);
                (*remaining == 0).then_some(*dependency_completion_ns)
            };
            if let Some(dependency_completion_ns) = ready_at_ns {
                let delay_ns = scaled_offset_ns(
                    self.scenario.nodes[successor].delay_after_dependencies_ms,
                    self.time_scale,
                )?;
                let slot = self
                    .prepared
                    .get_mut(successor)
                    .with_context(|| format!("generated successor {successor} is missing"))?;
                if slot.is_some() {
                    bail!("generated successor {successor} was prepared twice");
                }
                *slot = Some(self.request_factory.prepare(successor)?);
                self.ready.push(
                    dependency_completion_ns
                        .checked_add(delay_ns)
                        .context("generated successor timestamp overflow")?,
                    successor,
                    successor,
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

fn millis(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1000.0
}

fn duration_ns(duration: Duration) -> u64 {
    duration.as_nanos().min(u64::MAX as u128) as u64
}

#[cfg(test)]
#[path = "replay/tests.rs"]
mod tests;
