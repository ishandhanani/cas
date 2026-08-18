// SPDX-License-Identifier: Apache-2.0

use std::collections::VecDeque;
use std::fs;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use uuid::Uuid;

use super::artifacts::{ResultSink, SummaryIdentity, summarize, write_json};
use super::request::{
    admission_failure, build_http_client, normalize_target, prepare_open_file_limit,
    prepare_request, scaled_offset_ns, send_request, validate_options, warm_connections,
};
use super::{
    PreparedRequest, ReplayContext, ReplayOptions, RequestExecution, RunSummary, TrafficKind,
    duration_ns,
};
use crate::clock::sleep_until;
use crate::token_shape::TokenDictionary;
use crate::trace::{
    StoredTrace, StoredTraceReader, TraceManifest, TraceRequest, TraceStorageManifest,
};

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
pub(super) async fn run_replay(
    trace: crate::trace::LoadedTrace,
    dictionary: TokenDictionary,
    prepare_lookahead: Duration,
    options: ReplayOptions,
) -> Result<RunSummary> {
    let crate::trace::LoadedTrace { requests, manifest } = trace;
    run_replay_stream(
        MemoryRequestStream {
            requests: requests.into_iter(),
        },
        manifest,
        None,
        dictionary,
        prepare_lookahead,
        options,
    )
    .await
}

pub async fn run_stored_replay(
    trace: StoredTrace,
    dictionary: TokenDictionary,
    prepare_lookahead: Duration,
    options: ReplayOptions,
) -> Result<RunSummary> {
    let manifest = trace.manifest.clone();
    let storage = trace.storage.clone();
    run_replay_stream(
        trace.reader(),
        manifest,
        Some(storage),
        dictionary,
        prepare_lookahead,
        options,
    )
    .await
}

async fn run_replay_stream(
    mut requests: impl RequestStream,
    source_manifest: TraceManifest,
    source_storage: Option<TraceStorageManifest>,
    dictionary: TokenDictionary,
    prepare_lookahead: Duration,
    options: ReplayOptions,
) -> Result<RunSummary> {
    validate_options(&options)?;
    if prepare_lookahead.is_zero() {
        bail!("prepare_lookahead must be greater than zero");
    }
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
        &mut requests,
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
            &mut requests,
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
            prepare_lookahead: Some(prepare_lookahead),
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
        requests: &mut impl RequestStream,
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
            if request.output_tokens == 0 {
                bail!(
                    "request {} has zero output tokens",
                    request.source_request_id
                );
            }
            let execution = RequestExecution {
                output_budget_tokens: Some(request.output_tokens),
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
