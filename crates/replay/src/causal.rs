// SPDX-License-Identifier: Apache-2.0

//! Shared closed-loop execution for captured and generated agent graphs.

use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use tokio::sync::Semaphore;
use tokio::task::JoinSet;

use super::artifacts::{ResultSink, RunAccumulator};
use super::request::{scaled_offset_ns, send_request, warm_connections};
use super::{PreparedRequest, ReplayContext, ReplayOptions, RequestResult, duration_ns};
use crate::clock::sleep_until;
use agent_loadgen_core::scheduler::ReadyQueue;

/// A preplanned request DAG that can be executed by the common causal loop.
///
/// Workload implementations keep their own request-shape and artifact details;
/// this interface contains only readiness and request-preparation behavior.
pub(crate) trait CausalWorkload {
    fn node_count(&self) -> usize;
    fn dependencies(&self, ordinal: usize) -> &[usize];
    fn initial_arrival_ms(&self, ordinal: usize) -> Option<u64>;
    fn delay_after_dependencies_ms(&self, ordinal: usize) -> u64;
    fn prepare(&self, ordinal: usize) -> Result<PreparedRequest>;
    fn name(&self) -> &'static str;
}

pub(crate) struct CausalExecution {
    pub(crate) accumulator: RunAccumulator,
    pub(crate) elapsed: Duration,
}

pub(crate) async fn execute_causally<W: CausalWorkload>(
    workload: &W,
    client: reqwest::Client,
    target: &str,
    options: &ReplayOptions,
) -> Result<CausalExecution> {
    let node_count = workload.node_count();
    let mut prepared = std::iter::repeat_with(|| None)
        .take(node_count)
        .collect::<Vec<_>>();
    let remaining_dependencies = (0..node_count)
        .map(|ordinal| workload.dependencies(ordinal).len())
        .collect::<Vec<_>>();
    let mut successors = vec![Vec::new(); node_count];
    for ordinal in 0..node_count {
        for dependency in workload.dependencies(ordinal) {
            successors
                .get_mut(*dependency)
                .with_context(|| {
                    format!(
                        "{} node {ordinal} has missing dependency {dependency}",
                        workload.name()
                    )
                })?
                .push(ordinal);
        }
    }

    let mut ready = ReadyQueue::with_capacity(node_count);
    for (ordinal, slot) in prepared.iter_mut().enumerate() {
        let dependencies = workload.dependencies(ordinal);
        let initial_arrival_ms = workload.initial_arrival_ms(ordinal);
        if dependencies.is_empty() {
            let initial_arrival_ms = initial_arrival_ms.with_context(|| {
                format!(
                    "{} root node {ordinal} has no initial arrival",
                    workload.name()
                )
            })?;
            *slot = Some(workload.prepare(ordinal)?);
            ready.push(
                scaled_offset_ns(initial_arrival_ms, options.time_scale)?,
                ordinal,
                ordinal,
            );
        } else if initial_arrival_ms.is_some() {
            bail!(
                "{} dependent node {ordinal} also has an initial arrival",
                workload.name()
            );
        }
    }

    warm_connections(&client, target, options).await?;
    let wall_started = Instant::now();
    let base = wall_started
        .checked_add(options.start_delay)
        .context("causal replay start delay exceeds the monotonic clock range")?;
    let context = ReplayContext { client, base };
    let semaphore = Arc::new(Semaphore::new(options.max_in_flight));
    let mut tasks = JoinSet::new();
    let mut scheduler = CausalSchedulerState {
        workload,
        prepared,
        successors,
        remaining_dependencies,
        dependency_completion_ns: vec![0; node_count],
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
                .context("a causal replay timestamp exceeds the monotonic clock range")?;
            if Instant::now() < deadline {
                if tasks.is_empty() {
                    sleep_until(deadline).await;
                } else {
                    tokio::select! {
                        () = sleep_until(deadline) => {},
                        result = tasks.join_next() => {
                            if let Some(result) = result {
                                scheduler.release(
                                    result.context("a causal replay request task failed")??,
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
                    .context("a causal replay timestamp exceeds the monotonic clock range")?;
                let scheduler_wake = Instant::now();
                let context = context.clone();
                let semaphore = Arc::clone(&semaphore);
                tasks.spawn(async move {
                    let permit = semaphore
                        .acquire_owned()
                        .await
                        .context("the causal replay request semaphore closed")?;
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
            scheduler.release(result.context("a causal replay request task failed")??)?;
        }
    }

    scheduler.sink.flush()?;
    if scheduler.sink.accumulator.request_count != node_count {
        bail!(
            "{} graph stalled after {} of {node_count} requests",
            workload.name(),
            scheduler.sink.accumulator.request_count,
        );
    }
    Ok(CausalExecution {
        accumulator: scheduler.sink.accumulator,
        elapsed: wall_started.elapsed(),
    })
}

struct CausalSchedulerState<'a, W> {
    workload: &'a W,
    prepared: Vec<Option<PreparedRequest>>,
    successors: Vec<Vec<usize>>,
    remaining_dependencies: Vec<usize>,
    dependency_completion_ns: Vec<u64>,
    ready: ReadyQueue<usize>,
    sink: ResultSink,
    base: Instant,
    time_scale: f64,
}

impl<W: CausalWorkload> CausalSchedulerState<'_, W> {
    fn take_request(&mut self, ordinal: usize) -> Result<PreparedRequest> {
        self.prepared
            .get_mut(ordinal)
            .and_then(Option::take)
            .with_context(|| format!("{} node {ordinal} was released twice", self.workload.name()))
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
            .with_context(|| {
                format!(
                    "{} node {ordinal} has no successor slot",
                    self.workload.name()
                )
            })?
            .clone();
        for successor in successors {
            let ready_at_ns = {
                let remaining = self
                    .remaining_dependencies
                    .get_mut(successor)
                    .with_context(|| {
                        format!("{} successor {successor} is missing", self.workload.name())
                    })?;
                *remaining = remaining.checked_sub(1).with_context(|| {
                    format!(
                        "{} successor {successor} was released twice",
                        self.workload.name()
                    )
                })?;
                let dependency_completion_ns = self
                    .dependency_completion_ns
                    .get_mut(successor)
                    .with_context(|| {
                        format!(
                            "{} successor {successor} is missing timing",
                            self.workload.name()
                        )
                    })?;
                *dependency_completion_ns = (*dependency_completion_ns).max(completed_ns);
                (*remaining == 0).then_some(*dependency_completion_ns)
            };
            if let Some(dependency_completion_ns) = ready_at_ns {
                let delay_ns = scaled_offset_ns(
                    self.workload.delay_after_dependencies_ms(successor),
                    self.time_scale,
                )?;
                let slot = self.prepared.get_mut(successor).with_context(|| {
                    format!("{} successor {successor} is missing", self.workload.name())
                })?;
                if slot.is_some() {
                    bail!(
                        "{} successor {successor} was prepared twice",
                        self.workload.name()
                    );
                }
                *slot = Some(self.workload.prepare(successor)?);
                self.ready.push(
                    dependency_completion_ns
                        .checked_add(delay_ns)
                        .context("causal successor timestamp overflow")?,
                    successor,
                    successor,
                );
            }
        }
        Ok(())
    }
}
