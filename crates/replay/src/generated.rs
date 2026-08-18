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
use agent_loadgen_generate::scenario::GeneratedScenario;

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
