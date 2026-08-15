// SPDX-License-Identifier: Apache-2.0

use std::path::PathBuf;
use std::time::Duration;

use agent_loadgen::agent::AgentKind;
use agent_loadgen::compare::compare_traces;
use agent_loadgen::replay::{ReplayOptions, run_replay};
use agent_loadgen::token_shape::{SafeTokenAlphabet, TokenDictionary};
use agent_loadgen::trace::load_trace;
use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand};

#[derive(Debug, Parser)]
#[command(name = "agent-loadgen", version, about)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Check that a trace has all shape-replay fields.
    Inspect {
        /// Raw or wrapped Dynamo request trace files.
        #[arg(required = true)]
        trace: Vec<PathBuf>,

        /// Stop after this many requests.
        #[arg(long)]
        max_requests: Option<usize>,

        /// Replay only one agent session.
        #[arg(long)]
        session_id: Option<String>,

        /// First valid synthetic token ID.
        #[arg(long, default_value_t = 1000)]
        token_start: u32,

        /// Number of valid token IDs in the synthetic alphabet.
        #[arg(long, default_value_t = 1024)]
        token_alphabet_size: u32,
    },

    /// Replay a trace against a live Dynamo frontend.
    Replay {
        /// Raw or wrapped Dynamo request trace files.
        #[arg(required = true)]
        trace: Vec<PathBuf>,

        /// Coding-agent header mapping.
        #[arg(long, value_enum)]
        agent: AgentKind,

        /// Model name sent to Dynamo.
        #[arg(long)]
        model: String,

        /// Dynamo base URL or full Chat Completions URL.
        #[arg(long)]
        target: String,

        /// Output directory for run.json and requests.jsonl.
        #[arg(long)]
        output: PathBuf,

        /// Stop after this many requests.
        #[arg(long)]
        max_requests: Option<usize>,

        /// Replay only one agent session.
        #[arg(long)]
        session_id: Option<String>,

        /// First valid synthetic token ID.
        #[arg(long, default_value_t = 1000)]
        token_start: u32,

        /// Number of valid token IDs in the synthetic alphabet.
        #[arg(long, default_value_t = 1024)]
        token_alphabet_size: u32,

        /// Maximum simultaneous HTTP requests.
        #[arg(long, default_value_t = 4096)]
        max_in_flight: usize,

        /// Number of idle HTTP connections prepared through /v1/models before replay.
        #[arg(long, default_value_t = 1)]
        warmup_connections: usize,

        /// Wait for each prior same-session response. This transforms recorded timing.
        #[arg(long)]
        serialize_sessions: bool,

        /// Delay before the first scheduled request.
        #[arg(long, default_value_t = 100)]
        start_delay_ms: u64,

        /// Per-request HTTP timeout.
        #[arg(long, default_value_t = 600)]
        timeout_seconds: u64,

        /// Divide recorded arrival offsets by this value.
        #[arg(long, default_value_t = 1.0)]
        time_scale: f64,

        /// Use source request IDs as x-request-id values.
        #[arg(long)]
        preserve_request_ids: bool,

        /// Add a static target header as NAME=VALUE. Repeat the flag for more headers.
        #[arg(long = "header")]
        headers: Vec<String>,
    },

    /// Compare a source trace with a trace captured during replay.
    Compare {
        /// Original Dynamo request trace files.
        #[arg(long = "source", required = true)]
        source: Vec<PathBuf>,

        /// Dynamo request trace files captured during replay.
        #[arg(long = "replay", required = true)]
        replay: Vec<PathBuf>,

        /// requests.jsonl from the replay run.
        #[arg(long)]
        requests: PathBuf,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Inspect {
            trace,
            max_requests,
            session_id,
            token_start,
            token_alphabet_size,
        } => {
            let trace = load_trace(&trace, max_requests, session_id.as_deref())?;
            let dictionary = TokenDictionary::build(
                &trace.requests,
                SafeTokenAlphabet::new(token_start, token_alphabet_size)?,
            )?;
            let output = serde_json::json!({
                "fidelity": "shape-strict",
                "source": trace.manifest,
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
            max_requests,
            session_id,
            token_start,
            token_alphabet_size,
            max_in_flight,
            warmup_connections,
            serialize_sessions,
            start_delay_ms,
            timeout_seconds,
            time_scale,
            preserve_request_ids,
            headers,
        } => {
            let trace = load_trace(&trace, max_requests, session_id.as_deref())?;
            let dictionary = TokenDictionary::build(
                &trace.requests,
                SafeTokenAlphabet::new(token_start, token_alphabet_size)?,
            )?;
            let summary = run_replay(
                trace,
                dictionary,
                ReplayOptions {
                    agent,
                    model,
                    target,
                    output_dir: output,
                    max_in_flight,
                    warmup_connections,
                    serialize_sessions,
                    start_delay: Duration::from_millis(start_delay_ms),
                    timeout: Duration::from_secs(timeout_seconds),
                    time_scale,
                    preserve_request_ids,
                    static_headers: parse_headers(headers)?,
                },
            )
            .await?;
            println!("{}", serde_json::to_string_pretty(&summary)?);
        }
        Command::Compare {
            source,
            replay,
            requests,
        } => {
            let report = compare_traces(&source, &replay, &requests)?;
            println!("{}", serde_json::to_string_pretty(&report)?);
            if !report.passed {
                bail!("shape fidelity comparison failed");
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
