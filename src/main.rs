// SPDX-License-Identifier: Apache-2.0

use std::time::Duration;

use agent_loadgen::compare::{CompareOptions, compare_traces};
use agent_loadgen::replay::{ReplayOptions, run_generated_scenario, run_stored_replay};
use agent_loadgen::scenario::{GeneratedScenario, GeneratorConfig};
use agent_loadgen::token_shape::TokenDictionary;
use agent_loadgen::trace::load_stored_trace;
use anyhow::{Context, Result, bail};
use clap::Parser;

mod cli;

use cli::{Cli, Command};

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Inspect {
            trace,
            selection,
            tokens,
            store,
        } => {
            let trace = load_stored_trace(
                &trace,
                selection.max_requests,
                selection.session_id.as_deref(),
                store.trace_spool_directory.as_deref(),
                store.trace_request_batch_size,
            )?;
            let dictionary = TokenDictionary::new(
                trace.manifest.trace_block_size,
                trace.manifest.distinct_sequence_hashes,
                tokens.load()?,
            )?;
            let output = serde_json::json!({
                "fidelity": "shape-strict",
                "source": trace.manifest,
                "source_storage": trace.storage,
                "token_dictionary": dictionary.manifest()
            });
            println!("{}", serde_json::to_string_pretty(&output)?);
        }
        Command::Replay {
            trace,
            agent,
            model,
            target,
            output,
            selection,
            tokens,
            store,
            max_in_flight,
            warmup_connections,
            http_transport,
            prepare_lookahead_ms,
            result_flush_interval,
            serialize_sessions,
            max_dispatch_p99_ms,
            max_dispatch_max_ms,
            start_delay_ms,
            timeout_seconds,
            time_scale,
            preserve_request_ids,
            headers,
        } => {
            let trace = load_stored_trace(
                &trace,
                selection.max_requests,
                selection.session_id.as_deref(),
                store.trace_spool_directory.as_deref(),
                store.trace_request_batch_size,
            )?;
            let dictionary = TokenDictionary::new(
                trace.manifest.trace_block_size,
                trace.manifest.distinct_sequence_hashes,
                tokens.load()?,
            )?;
            let summary = run_stored_replay(
                trace,
                dictionary,
                ReplayOptions {
                    agent,
                    model,
                    target,
                    output_dir: output,
                    max_in_flight,
                    warmup_connections,
                    http_transport,
                    prepare_lookahead: Duration::from_millis(prepare_lookahead_ms),
                    result_flush_interval,
                    serialize_sessions,
                    max_dispatch_p99_ms,
                    max_dispatch_max_ms,
                    start_delay: Duration::from_millis(start_delay_ms),
                    timeout: Duration::from_secs(timeout_seconds),
                    time_scale,
                    preserve_request_ids,
                    static_headers: parse_headers(headers)?,
                },
            )
            .await?;
            println!("{}", serde_json::to_string_pretty(&summary)?);
            if !summary.passed {
                bail!("shape-strict replay failed request or dispatch-timing fidelity checks");
            }
        }
        Command::Generate {
            config,
            model,
            target,
            output,
            plan_only,
            tokens,
            max_in_flight,
            warmup_connections,
            http_transport,
            result_flush_interval,
            max_dispatch_p99_ms,
            max_dispatch_max_ms,
            start_delay_ms,
            timeout_seconds,
            time_scale,
            headers,
        } => {
            let scenario = GeneratedScenario::generate(GeneratorConfig::load(&config)?.resolve()?)?;
            std::fs::create_dir_all(&output).with_context(|| {
                format!("failed to create output directory {}", output.display())
            })?;
            let scenario_path = output.join("scenario.json");
            if plan_only {
                let writer = std::io::BufWriter::new(
                    std::fs::File::create(&scenario_path)
                        .with_context(|| format!("failed to create {}", scenario_path.display()))?,
                );
                serde_json::to_writer_pretty(writer, &scenario)
                    .with_context(|| format!("failed to write {}", scenario_path.display()))?;
                let output = serde_json::json!({
                    "scenario_digest_sha256": scenario.scenario_digest_sha256,
                    "profile_digest_sha256": scenario.profile_digest_sha256,
                    "requests": scenario.nodes.len(),
                    "sessions": scenario.sessions.len(),
                    "trace_manifest": scenario.trace_manifest,
                    "scenario_path": scenario_path,
                });
                println!("{}", serde_json::to_string_pretty(&output)?);
                return Ok(());
            }
            let model = model.context("--model is required unless --plan-only is set")?;
            let target = target.context("--target is required unless --plan-only is set")?;
            let dictionary = TokenDictionary::new(
                scenario.trace_manifest.trace_block_size,
                scenario.trace_manifest.distinct_sequence_hashes,
                tokens.load()?,
            )?;
            let summary = run_generated_scenario(
                &scenario,
                dictionary,
                ReplayOptions {
                    agent: scenario.config.agent,
                    model,
                    target,
                    output_dir: output,
                    max_in_flight,
                    warmup_connections,
                    http_transport,
                    prepare_lookahead: Duration::from_millis(1),
                    result_flush_interval,
                    serialize_sessions: false,
                    max_dispatch_p99_ms,
                    max_dispatch_max_ms,
                    start_delay: Duration::from_millis(start_delay_ms),
                    timeout: Duration::from_secs(timeout_seconds),
                    time_scale,
                    preserve_request_ids: false,
                    static_headers: parse_headers(headers)?,
                },
            )
            .await?;
            println!("{}", serde_json::to_string_pretty(&summary)?);
            if !summary.passed {
                bail!("generated traffic failed request or dispatch-timing checks");
            }
        }
        Command::Compare {
            source,
            replay,
            requests,
            time_scale,
            max_arrival_p99_ms,
            max_arrival_max_ms,
        } => {
            let report = compare_traces(
                &source,
                &replay,
                &requests,
                CompareOptions {
                    time_scale,
                    max_arrival_p99_ms,
                    max_arrival_max_ms,
                },
            )?;
            println!("{}", serde_json::to_string_pretty(&report)?);
            if !report.passed {
                bail!("replay fidelity comparison failed");
            }
        }
    }
    Ok(())
}

fn parse_headers(headers: Vec<String>) -> Result<Vec<(String, String)>> {
    headers
        .into_iter()
        .map(|header| {
            let (name, value) = header
                .split_once('=')
                .with_context(|| format!("header {header:?} must use NAME=VALUE"))?;
            if name.is_empty() {
                bail!("header name must not be empty");
            }
            Ok((name.to_string(), value.to_string()))
        })
        .collect()
}
