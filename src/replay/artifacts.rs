// SPDX-License-Identifier: Apache-2.0

use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use hdrhistogram::Histogram;
use serde::Serialize;

use super::{Percentiles, ReplayOptions, RequestResult, RunSummary, TrafficKind, millis};
use crate::clock::TIMER_BACKEND;
use crate::scenario::{CompactionExpectedEffect, GeneratedNodeKind};
use crate::token_shape::TokenDictionaryManifest;
use crate::trace::{TraceManifest, TraceStorageManifest};

pub(super) struct ResultSink {
    writer: BufWriter<File>,
    flush_interval: usize,
    records_since_flush: usize,
    pub(super) accumulator: RunAccumulator,
}

impl ResultSink {
    pub(super) fn create(
        path: &Path,
        flush_interval: usize,
        dispatch_p99_limit_ms: f64,
    ) -> Result<Self> {
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

    pub(super) fn record(&mut self, result: RequestResult) -> Result<()> {
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

    pub(super) fn flush(&mut self) -> Result<()> {
        self.writer.flush()?;
        self.records_since_flush = 0;
        Ok(())
    }
}

pub(super) struct RunAccumulator {
    pub(super) request_count: usize,
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
    pub(super) dispatch_at_or_below_p99_limit: usize,
    pub(super) dispatch_max_ms: f64,
}

impl RunAccumulator {
    pub(super) fn new(dispatch_p99_limit_ms: f64) -> Result<Self> {
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

pub(super) struct SummaryIdentity<'a> {
    pub(super) traffic_kind: TrafficKind,
    pub(super) run_id: String,
    pub(super) target: &'a str,
    pub(super) source_storage: Option<TraceStorageManifest>,
}

pub(super) fn summarize(
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
    RunSummary {
        run_id: identity.run_id,
        agent: options.agent,
        model: options.model.clone(),
        target: identity.target.to_string(),
        time_scale: options.time_scale,
        max_in_flight: options.max_in_flight,
        warmup_connections: options.warmup_connections,
        http_transport: options.http_transport,
        prepare_lookahead_ms: options
            .prepare_lookahead
            .map(|duration| duration.as_millis().min(u64::MAX as u128) as u64),
        result_flush_interval: options.result_flush_interval,
        max_dispatch_p99_ms: options.max_dispatch_p99_ms,
        max_dispatch_max_ms: options.max_dispatch_max_ms,
        timer_backend: TIMER_BACKEND,
        static_header_names: options
            .static_headers
            .iter()
            .map(|(name, _)| name.clone())
            .collect(),
        protocol_surface: "chat_completions",
        traffic_kind: identity.traffic_kind,
        token_path_verified: options.token_path_verified,
        engine_cache_mode: options.engine_cache_mode.clone(),
        capacity_performance_conclusions_allowed: conclusion_blockers.is_empty(),
        conclusion_blockers,
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

pub(super) fn dispatch_timing_matches(accumulator: &RunAccumulator, max_limit_ms: f64) -> bool {
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

pub(super) fn write_json(path: &Path, value: &impl Serialize) -> Result<()> {
    let writer = BufWriter::new(
        File::create(path).with_context(|| format!("failed to create {}", path.display()))?,
    );
    serde_json::to_writer_pretty(writer, value)
        .with_context(|| format!("failed to write {}", path.display()))
}
