// SPDX-License-Identifier: Apache-2.0

use std::path::PathBuf;

use agent_loadgen::agent::AgentKind;
use agent_loadgen::replay::HttpTransport;
use agent_loadgen::token_shape::{SafeTokenAlphabet, TokenAlphabetSource};
use anyhow::{Context, Result, bail};
use clap::{Args, Parser, Subcommand};

#[derive(Debug, Parser)]
#[command(name = "agent-loadgen", version, about)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Args)]
pub struct TokenArgs {
    /// Local tokenizer file/directory or Hugging Face model ID.
    #[arg(long)]
    pub tokenizer: Option<String>,

    /// First ID in a caller-certified manual token range.
    #[arg(long)]
    pub token_start: Option<u32>,

    /// Maximum number of safe token IDs to use.
    #[arg(long, default_value_t = 1024)]
    pub token_alphabet_size: usize,

    /// Exclude one token ID. Repeat this option for more IDs.
    #[arg(long = "exclude-token-id")]
    pub excluded_token_ids: Vec<u32>,

    /// Accept a manual token range that the load generator cannot verify.
    #[arg(long)]
    pub allow_unverified_token_range: bool,
}

impl TokenArgs {
    pub fn load(self) -> Result<SafeTokenAlphabet> {
        let source = match (self.tokenizer, self.token_start) {
            (Some(tokenizer), None) => {
                if self.allow_unverified_token_range {
                    bail!("--allow-unverified-token-range requires --token-start");
                }
                TokenAlphabetSource::Tokenizer(tokenizer)
            }
            (None, Some(start)) => {
                if !self.allow_unverified_token_range {
                    bail!(
                        "--token-start requires --allow-unverified-token-range; use --tokenizer for verified token safety"
                    );
                }
                TokenAlphabetSource::UnverifiedRange {
                    start,
                    size: u32::try_from(self.token_alphabet_size)
                        .context("--token-alphabet-size does not fit u32")?,
                }
            }
            (Some(_), Some(_)) => bail!("use either --tokenizer or --token-start, not both"),
            (None, None) => bail!(
                "--tokenizer is required for verified token safety; a manual range requires --token-start and --allow-unverified-token-range"
            ),
        };
        SafeTokenAlphabet::load(source, self.token_alphabet_size, &self.excluded_token_ids)
    }
}

#[derive(Debug, Args)]
pub struct TraceSelectionArgs {
    /// Stop after this many requests.
    #[arg(long)]
    pub max_requests: Option<usize>,

    /// Replay only one agent session.
    #[arg(long)]
    pub session_id: Option<String>,
}

#[derive(Debug, Args)]
pub struct TraceStoreArgs {
    /// Parent directory for the temporary disk-backed trace index.
    #[arg(long)]
    pub trace_spool_directory: Option<PathBuf>,

    /// Number of ordered trace requests read from the spool per batch.
    #[arg(long, default_value_t = 1024)]
    pub trace_request_batch_size: usize,
}

#[derive(Debug, Args)]
pub struct FidelityArgs {
    /// Declare that supplied token IDs reach the engine without re-tokenization.
    #[arg(long)]
    pub token_path_verified: bool,

    /// Declare one engine cache setting as NAME=VALUE. Repeat for more settings.
    #[arg(long = "engine-cache-mode")]
    pub engine_cache_mode: Vec<String>,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Check that a trace has all shape-replay fields.
    Inspect {
        /// Raw or wrapped Dynamo request trace files.
        #[arg(required = true)]
        trace: Vec<PathBuf>,

        #[command(flatten)]
        selection: TraceSelectionArgs,

        #[command(flatten)]
        tokens: TokenArgs,

        #[command(flatten)]
        store: TraceStoreArgs,
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

        #[command(flatten)]
        selection: TraceSelectionArgs,

        #[command(flatten)]
        tokens: TokenArgs,

        #[command(flatten)]
        store: TraceStoreArgs,

        #[command(flatten)]
        fidelity: FidelityArgs,

        /// Maximum simultaneous HTTP requests.
        #[arg(long, default_value_t = 4096)]
        max_in_flight: usize,

        /// Number of idle HTTP connections prepared through /v1/models before replay.
        #[arg(long, default_value_t = 1)]
        warmup_connections: usize,

        /// HTTP transport used for target requests.
        #[arg(long, value_enum, default_value_t = HttpTransport::Http2PriorKnowledge)]
        http_transport: HttpTransport,

        /// Prepare request bodies this far before their recorded arrival.
        #[arg(long, default_value_t = 5000)]
        prepare_lookahead_ms: u64,

        /// Flush requests.jsonl after this many completed requests.
        #[arg(long, default_value_t = 1)]
        result_flush_interval: usize,

        /// Maximum allowed p99 client-offer lag.
        #[arg(long, default_value_t = 2.0)]
        max_dispatch_p99_ms: f64,

        /// Maximum allowed client-offer lag.
        #[arg(long, default_value_t = 5.0)]
        max_dispatch_max_ms: f64,

        /// Delay before the first scheduled request.
        #[arg(long, default_value_t = 100)]
        start_delay_ms: u64,

        /// Per-request HTTP timeout.
        #[arg(long, default_value_t = 600)]
        timeout_seconds: u64,

        /// Divide recorded arrival offsets by this value.
        #[arg(long, default_value_t = 1.0)]
        time_scale: f64,

        /// Add a static target header as NAME=VALUE. Repeat the flag for more headers.
        #[arg(long = "header")]
        headers: Vec<String>,
    },

    /// Generate a deterministic scenario without sending traffic.
    Plan {
        /// Versioned TOML generator config.
        #[arg(long)]
        config: PathBuf,

        /// Output directory for scenario.json.
        #[arg(long)]
        output: PathBuf,
    },

    /// Generate and run seeded coding-agent trajectories.
    Generate {
        /// Versioned TOML generator config.
        #[arg(long)]
        config: PathBuf,

        /// Model name sent to Dynamo.
        #[arg(long)]
        model: String,

        /// Dynamo base URL or full Chat Completions URL.
        #[arg(long)]
        target: String,

        /// Output directory for scenario.json, run.json, and requests.jsonl.
        #[arg(long)]
        output: PathBuf,

        #[command(flatten)]
        tokens: TokenArgs,

        #[command(flatten)]
        fidelity: FidelityArgs,

        /// Maximum simultaneous HTTP requests.
        #[arg(long, default_value_t = 256)]
        max_in_flight: usize,

        /// Number of idle HTTP connections prepared through /v1/models.
        #[arg(long, default_value_t = 1)]
        warmup_connections: usize,

        /// HTTP transport used for target requests.
        #[arg(long, value_enum, default_value_t = HttpTransport::Http2PriorKnowledge)]
        http_transport: HttpTransport,

        /// Flush requests.jsonl after this many completed requests.
        #[arg(long, default_value_t = 1)]
        result_flush_interval: usize,

        /// Maximum allowed p99 dispatch lag after a graph node becomes ready.
        #[arg(long, default_value_t = 10.0)]
        max_dispatch_p99_ms: f64,

        /// Maximum allowed dispatch lag after a graph node becomes ready.
        #[arg(long, default_value_t = 50.0)]
        max_dispatch_max_ms: f64,

        /// Delay before the first root session starts.
        #[arg(long, default_value_t = 100)]
        start_delay_ms: u64,

        /// Per-request HTTP timeout.
        #[arg(long, default_value_t = 600)]
        timeout_seconds: u64,

        /// Divide startup, restart, tool, think, and join delays by this value.
        #[arg(long, default_value_t = 1.0)]
        time_scale: f64,

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

        /// Divide source arrival offsets by this value before comparison.
        #[arg(long, default_value_t = 1.0)]
        time_scale: f64,

        /// Maximum allowed p99 frontend arrival error.
        #[arg(long, default_value_t = 5.0)]
        max_arrival_p99_ms: f64,

        /// Maximum allowed frontend arrival error.
        #[arg(long, default_value_t = 20.0)]
        max_arrival_max_ms: f64,
    },

    /// Join request results with normalized engine telemetry by request ID.
    JoinTelemetry {
        /// requests.jsonl from a replay or generated run.
        #[arg(long)]
        requests: PathBuf,

        /// Engine telemetry JSONL. Repeat for more files.
        #[arg(long = "engine-telemetry", required = true)]
        engine_telemetry: Vec<PathBuf>,

        /// Joined JSONL output path.
        #[arg(long)]
        output: PathBuf,
    },
}
