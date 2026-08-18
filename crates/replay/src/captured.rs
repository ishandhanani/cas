// SPDX-License-Identifier: Apache-2.0

use std::fs;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use uuid::Uuid;

use super::artifacts::{ResultSink, SummaryIdentity, summarize, write_json};
use super::request::{
    build_http_client, normalize_target, prepare_open_file_limit, prepare_request,
    scaled_offset_ns, send_request, validate_options, warm_connections,
};
use super::{
    PreparedRequest, ReplayContext, ReplayOptions, RequestExecution, RequestResult, RunSummary,
    TrafficKind, duration_ns,
};
use crate::clock::sleep_until;
use crate::token_shape::TokenDictionary;
use agent_loadgen_core::scheduler::ReadyQueue;
use agent_loadgen_trace::AgenticTrace;

pub async fn run_agentic_replay(
    trace: AgenticTrace,
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
    let options = Arc::new(options);
    let request_factory = CapturedRequestFactory {
        trace: &trace,
        dictionary: &dictionary,
        client: client.clone(),
        target: target.clone(),
        run_id: run_id.clone(),
        options: Arc::clone(&options),
    };
    let mut prepared = std::iter::repeat_with(|| None)
        .take(trace.turns.len())
        .collect::<Vec<_>>();
    let remaining_dependencies = trace
        .turns
        .iter()
        .map(|turn| turn.dependencies.len())
        .collect::<Vec<_>>();
    let mut successors = vec![Vec::new(); trace.turns.len()];
    for (ordinal, turn) in trace.turns.iter().enumerate() {
        for dependency in &turn.dependencies {
            successors
                .get_mut(*dependency)
                .with_context(|| format!("turn {ordinal} has missing dependency {dependency}"))?
                .push(ordinal);
        }
    }
    let mut ready = ReadyQueue::with_capacity(trace.turns.len());
    for (ordinal, turn) in trace.turns.iter().enumerate() {
        if turn.dependencies.is_empty() {
            let arrival_ms = turn
                .root_arrival_ms
                .with_context(|| format!("root turn {ordinal} has no root arrival"))?;
            prepared[ordinal] = Some(request_factory.prepare(ordinal)?);
            ready.push(
                scaled_offset_ns(arrival_ms, options.time_scale)?,
                ordinal,
                ordinal,
            );
        } else if turn.root_arrival_ms.is_some() {
            bail!("dependent turn {ordinal} also has a root arrival");
        }
    }
    warm_connections(&client, &target, &options).await?;

    let wall_started = Instant::now();
    let base = wall_started
        .checked_add(options.start_delay)
        .context("replay start delay exceeds the monotonic clock range")?;
    let context = ReplayContext { client, base };
    let semaphore = Arc::new(Semaphore::new(options.max_in_flight));
    let mut tasks = JoinSet::new();
    let mut scheduler = CapturedSchedulerState {
        trace: &trace,
        request_factory,
        prepared,
        successors,
        remaining_dependencies,
        dependency_completion_ns: vec![0; trace.turns.len()],
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
                .context("an agentic replay timestamp exceeds the monotonic clock range")?;
            if Instant::now() < deadline {
                if tasks.is_empty() {
                    sleep_until(deadline).await;
                } else {
                    tokio::select! {
                        () = sleep_until(deadline) => {},
                        result = tasks.join_next() => {
                            if let Some(result) = result {
                                scheduler.release(
                                    result.context("an agentic replay task failed")??,
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
                    .context("an agentic replay timestamp exceeds the monotonic clock range")?;
                let scheduler_wake = Instant::now();
                let context = context.clone();
                let semaphore = Arc::clone(&semaphore);
                tasks.spawn(async move {
                    let permit = semaphore
                        .acquire_owned()
                        .await
                        .context("the replay request semaphore closed")?;
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
            scheduler.release(result.context("an agentic replay task failed")??)?;
        }
    }

    scheduler.sink.flush()?;
    if scheduler.sink.accumulator.request_count != trace.turns.len() {
        bail!(
            "agentic replay graph stalled after {} of {} requests",
            scheduler.sink.accumulator.request_count,
            trace.turns.len()
        );
    }
    let summary = summarize(
        SummaryIdentity {
            traffic_kind: TrafficKind::CapturedTrace,
            run_id,
            target: &target,
        },
        &trace.manifest,
        &token_manifest,
        &options,
        &scheduler.sink.accumulator,
        wall_started.elapsed(),
    );
    write_json(&options.output_dir.join("run.json"), &summary)?;
    Ok(summary)
}

struct CapturedRequestFactory<'a> {
    trace: &'a AgenticTrace,
    dictionary: &'a TokenDictionary,
    client: reqwest::Client,
    target: String,
    run_id: String,
    options: Arc<ReplayOptions>,
}

impl CapturedRequestFactory<'_> {
    fn prepare(&self, ordinal: usize) -> Result<PreparedRequest> {
        let turn = self
            .trace
            .turns
            .get(ordinal)
            .with_context(|| format!("agentic replay turn {ordinal} is missing"))?;
        prepare_request(
            &self.client,
            &self.target,
            &self.run_id,
            &self.options,
            self.dictionary,
            turn.request.clone(),
            RequestExecution {
                output_budget_tokens: Some(turn.request.output_tokens),
                compaction_attempt: None,
            },
        )
    }
}

struct CapturedSchedulerState<'a> {
    trace: &'a AgenticTrace,
    request_factory: CapturedRequestFactory<'a>,
    prepared: Vec<Option<PreparedRequest>>,
    successors: Vec<Vec<usize>>,
    remaining_dependencies: Vec<usize>,
    dependency_completion_ns: Vec<u64>,
    ready: ReadyQueue<usize>,
    sink: ResultSink,
    base: Instant,
    time_scale: f64,
}

impl CapturedSchedulerState<'_> {
    fn take_request(&mut self, ordinal: usize) -> Result<PreparedRequest> {
        self.prepared
            .get_mut(ordinal)
            .and_then(Option::take)
            .with_context(|| format!("agentic replay turn {ordinal} was released twice"))
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
            .with_context(|| format!("agentic replay turn {ordinal} has no successor slot"))?
            .clone();
        for successor in successors {
            let ready_at_ns = {
                let remaining = self
                    .remaining_dependencies
                    .get_mut(successor)
                    .with_context(|| format!("agentic successor {successor} is missing"))?;
                *remaining = remaining
                    .checked_sub(1)
                    .with_context(|| format!("agentic successor {successor} was released twice"))?;
                let dependency_completion_ns = self
                    .dependency_completion_ns
                    .get_mut(successor)
                    .with_context(|| format!("agentic successor {successor} has no timing slot"))?;
                *dependency_completion_ns = (*dependency_completion_ns).max(completed_ns);
                (*remaining == 0).then_some(*dependency_completion_ns)
            };
            if let Some(dependency_completion_ns) = ready_at_ns {
                let delay_ns = scaled_offset_ns(
                    self.trace.turns[successor].delay_after_dependencies_ms,
                    self.time_scale,
                )?;
                let slot = self
                    .prepared
                    .get_mut(successor)
                    .with_context(|| format!("agentic successor {successor} is missing"))?;
                if slot.is_some() {
                    bail!("agentic successor {successor} was prepared twice");
                }
                *slot = Some(self.request_factory.prepare(successor)?);
                self.ready.push(
                    dependency_completion_ns
                        .checked_add(delay_ns)
                        .context("agentic successor timestamp overflow")?,
                    successor,
                    successor,
                );
            }
        }
        Ok(())
    }
}

#[cfg(test)]
pub(super) async fn run_replay(
    trace: AgenticTrace,
    dictionary: TokenDictionary,
    options: ReplayOptions,
) -> Result<RunSummary> {
    run_agentic_replay(trace, dictionary, options).await
}
