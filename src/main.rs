// SPDX-License-Identifier: Apache-2.0

use std::time::Duration;

use agent_loadgen::compare::{CompareOptions, compare_traces};
use agent_loadgen::replay::{ReplayOptions, run_agentic_replay, run_generated_scenario};
use agent_loadgen::scenario::{GeneratedScenario, GeneratorConfig};
use agent_loadgen::telemetry::join_engine_telemetry;
use agent_loadgen::token_shape::TokenDictionary;
use agent_loadgen::trace::load_agentic_trace;
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
        } => {
            let trace = load_agentic_trace(
                &trace,
                selection.max_requests,
                selection.session_id.as_deref(),
            )?;
            let dictionary = TokenDictionary::new(
                trace.manifest.trace_block_size,
                trace.manifest.distinct_sequence_hashes,
                tokens.load()?,
            )?;
            let root_requests = trace
                .turns
                .iter()
                .filter(|turn| turn.dependencies.is_empty())
                .count();
            let dependency_edges = trace
                .turns
                .iter()
                .map(|turn| turn.dependencies.len())
                .sum::<usize>();
            let output = serde_json::json!({
                "fidelity": "agentic-causal",
                "source": trace.manifest,
                "root_requests": root_requests,
                "dependency_edges": dependency_edges,
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
            fidelity,
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
            let trace = load_agentic_trace(
                &trace,
                selection.max_requests,
                selection.session_id.as_deref(),
            )?;
            let dictionary = TokenDictionary::new(
                trace.manifest.trace_block_size,
                trace.manifest.distinct_sequence_hashes,
                tokens.load()?,
            )?;
            let summary = run_agentic_replay(
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
                    result_flush_interval,
                    max_dispatch_p99_ms,
                    max_dispatch_max_ms,
                    start_delay: Duration::from_millis(start_delay_ms),
                    timeout: Duration::from_secs(timeout_seconds),
                    time_scale,
                    token_path_verified: fidelity.token_path_verified,
                    engine_cache_mode: parse_key_values(
                        fidelity.engine_cache_mode,
                        "engine cache mode",
                    )?,
                    static_headers: parse_headers(headers)?,
                },
            )
            .await?;
            println!("{}", serde_json::to_string_pretty(&summary)?);
            if !summary.passed {
                bail!("shape-strict replay failed request or dispatch-timing fidelity checks");
            }
        }
        Command::Plan { config, output } => {
            let scenario = GeneratedScenario::generate(GeneratorConfig::load(&config)?.resolve()?)?;
            std::fs::create_dir_all(&output).with_context(|| {
                format!("failed to create output directory {}", output.display())
            })?;
            let scenario_path = output.join("scenario.json");
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
        }
        Command::Generate {
            config,
            model,
            target,
            output,
            tokens,
            fidelity,
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
                    result_flush_interval,
                    max_dispatch_p99_ms,
                    max_dispatch_max_ms,
                    start_delay: Duration::from_millis(start_delay_ms),
                    timeout: Duration::from_secs(timeout_seconds),
                    time_scale,
                    token_path_verified: fidelity.token_path_verified,
                    engine_cache_mode: parse_key_values(
                        fidelity.engine_cache_mode,
                        "engine cache mode",
                    )?,
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
            max_arrival_p99_ms,
            max_arrival_max_ms,
        } => {
            let report = compare_traces(
                &source,
                &replay,
                &requests,
                CompareOptions {
                    max_arrival_p99_ms,
                    max_arrival_max_ms,
                },
            )?;
            println!("{}", serde_json::to_string_pretty(&report)?);
            if !report.passed {
                bail!("replay fidelity comparison failed");
            }
        }
        Command::JoinTelemetry {
            requests,
            engine_telemetry,
            output,
        } => {
            let summary = join_engine_telemetry(&requests, &engine_telemetry, &output)?;
            println!("{}", serde_json::to_string_pretty(&summary)?);
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

fn parse_key_values(
    values: Vec<String>,
    label: &str,
) -> Result<std::collections::BTreeMap<String, String>> {
    let mut parsed = std::collections::BTreeMap::new();
    for value in values {
        let (name, setting) = value
            .split_once('=')
            .with_context(|| format!("{label} {value:?} must use NAME=VALUE"))?;
        if name.is_empty() || setting.is_empty() {
            bail!("{label} names and values must not be empty");
        }
        if parsed
            .insert(name.to_string(), setting.to_string())
            .is_some()
        {
            bail!("{label} {name:?} is declared more than once");
        }
    }
    Ok(parsed)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cache_mode_rejects_duplicate_keys() {
        let result = parse_key_values(
            vec![
                "ownership=session".to_string(),
                "ownership=shared".to_string(),
            ],
            "engine cache mode",
        );
        assert!(result.is_err());
    }
}
