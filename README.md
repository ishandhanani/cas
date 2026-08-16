# agent-loadgen

`agent-loadgen` is a standalone Rust load generator for coding-agent traffic through a Dynamo OpenAI-compatible frontend. It has two workload modes:

- Replay the KV shape, output length, agent headers, and absolute arrival schedule from `dynamo.request.trace.v1` request traces.
- Generate deterministic Claude Code, Codex, or OpenCode trajectories with tools, compaction, background work, subagents, and swarms.

Prompt content is intentionally synthetic. The contract is token count, KV-prefix topology, causal structure, and timing.

## Data flow

```text
request-trace shards -> SQLite ordering/index -> bounded lookahead -> monotonic clock -> Dynamo frontend -> incremental results

versioned TOML -> seeded causal DAG -> fixed agent slots + dependency completion -> Dynamo frontend -> incremental results
```

The HTTP execution model follows the useful parts of AIPerf-style load generation: streaming requests, explicit concurrency, connection warmup, and machine-readable fidelity artifacts. Trace fields and coding-agent headers follow Dynamo's request-trace and agent-harness contracts.

Replay and generation intentionally use different timing feedback:

```text
recorded replay: absolute clock -> offer every recorded request, independent of responses
generated traffic: fixed active-agent slots -> response/tool/join -> next turn or task
```

## Replay contract

- Reads raw or wrapped JSONL and gzip JSONL shards.
- Streams source rows into a temporary SQLite spool, globally orders by `(request_received_ms, source ordinal)`, and applies `--max-requests` after ordering.
- Reads selected requests in fixed-size batches and prepares request bodies only within `--prepare-lookahead-ms` of dispatch.
- Preserves exact input length, requested output length, block-level prefix equality, recorded millisecond arrival offsets, and stable ordering for tied arrivals.
- Uses an injective, deterministic `u64` hash-to-token-block encoding with entropy at the start of each block, without retaining a hash dictionary in memory.
- Maps Claude Code, Codex, and OpenCode context to their agent-specific headers plus Dynamo session headers.
- Fails the run when requests miss their absolute arrival because the configured concurrency cap is full. Recorded replay never silently retimes those requests.
- Writes each completed result incrementally. Histogram memory and request preparation are bounded independently of trace length.

Shape-strict replay rejects incomplete replay fields, duplicate request IDs, mixed trace block sizes, and zero-output records before network traffic starts.

## Token safety

Verified mode loads `tokenizer.json` from a local file, local model directory, or Hugging Face model ID. It excludes added special tokens and special IDs found in common tokenizer/model metadata, records exact tokenizer and alphabet digests, and fails closed when no tokenizer is supplied.

A caller-certified numeric range is available only with the explicit `--allow-unverified-token-range` flag. Use it for controlled mockers, not an unknown production model.

## Build

```bash
cargo build --release
```

## Inspect a trace

```bash
target/release/agent-loadgen inspect trace.jsonl.gz \
  --tokenizer /models/GLM-4.7-Flash \
  --trace-spool-directory /ephemeral/agent-loadgen
```

Use `--session-id ID` to select one session or `--max-requests N` for a globally ordered prefix.

## Replay a trace

```bash
target/release/agent-loadgen replay trace.jsonl.gz \
  --agent codex \
  --model zai-org/GLM-4.7-Flash \
  --target http://127.0.0.1:8000 \
  --output artifacts/replay \
  --tokenizer /models/GLM-4.7-Flash \
  --trace-spool-directory /ephemeral/agent-loadgen \
  --max-in-flight 4096 \
  --warmup-connections 16
```

The target can be a base URL or a full `/v1/chat/completions` URL. Linux uses a one-shot `timerfd` backed by `CLOCK_MONOTONIC`; other platforms use Tokio's monotonic timer.

Replay sends synthetic prompt IDs in `nvext.token_data`. Run Dynamo with its default Rust chat processor (`--dyn-chat-processor dynamo`). The Python SGLang processor rejects these requests because it cannot preserve the supplied prompt IDs.

The default strict gates are 2 ms p99 and 5 ms maximum client-offer lag. The implementation records this as `dispatch_lag_ms`: the time at which the prepared request is submitted to the HTTP client, not a claim about when its bytes arrive at Dynamo. The maximum is checked at full measured precision; fixed-memory histograms provide the reported percentiles. `requests.jsonl` separates scheduler-wake lag, local-admission lag, client-offer lag, response time, and output-length fidelity.

HTTP/2 prior knowledge is the default because one multiplexed connection avoids HTTP/1 connection-pool response serialization during bursts. Use `--http-transport auto` for a target that does not accept cleartext HTTP/2. Target-observed frontend arrival timing remains a separate `compare` diagnostic and gate.

`--serialize-sessions` is an explicit causal transformation. It waits for the previous same-session response and then preserves the recorded inter-request gap. It does not claim the original absolute-arrival timing contract.

## Generate coding-agent traffic

Plan without sending traffic:

```bash
target/release/agent-loadgen generate \
  --config profiles/codex-balanced.toml \
  --output artifacts/codex-plan \
  --plan-only
```

Run the same seeded plan:

```bash
target/release/agent-loadgen generate \
  --config profiles/codex-balanced.toml \
  --model zai-org/GLM-4.7-Flash \
  --target http://127.0.0.1:8000 \
  --output artifacts/codex-run \
  --tokenizer /models/GLM-4.7-Flash
```

Generated traffic is closed-loop. Each configured active-agent slot waits for model responses, tool time, compaction, and blocking child joins. When its root task completes, the slot starts its next planned task; a slower target therefore produces a lower request rate. The checked-in Claude Code, Codex, and OpenCode profiles are structural starting points based on observed harness trajectories. They are not statistically fitted production distributions. See [Generator configuration](docs/generator.md) for the full schema and timing model.

## Compare captured traces

Enable Dynamo request tracing during replay, then compare the captured trace with the source:

```bash
target/release/agent-loadgen compare \
  --source source-trace.jsonl.gz \
  --replay captured-trace.jsonl.gz \
  --requests artifacts/replay/requests.jsonl
```

Comparison checks exact ISL, exact OSL, routable agent context, canonical prefix topology, and normalized frontend arrival offsets. Frontend timing is deliberately separate from the replay run's client-offer timing: it includes the HTTP stack and Dynamo frontend. The current comparison path materializes both selected traces in memory; replay and inspection use the bounded SQLite path.

Current Dynamo `AgentContext` captures session, parent, final-turn, and input-trigger fields, but does not project Codex's opaque compaction header into the request trace. The comparator reports unavailable compaction metadata as a warning; the changed KV topology remains a strict comparison gate.

## Outputs

- `scenario.json`: resolved generator config, causal graph, sampled tool outcomes, spawned sessions, KV-shape manifest, and deterministic digests.
- `run.json`: replay/generation configuration, source and token manifests, timing percentiles, fidelity gates, and overall pass/fail.
- `requests.jsonl`: per-request scheduling, response, headers, output usage, and error data, written in completion order with source ordinals retained.

## Known limits

- Only `dynamo.request.trace.v1` `request_end` replay fields are accepted. Older traces without replay hashes cannot be reconstructed from counts alone.
- Source timing has millisecond resolution. Tool durations and dependency edges are not present in a plain request-only trace, so faithful trace replay uses recorded absolute arrivals; generated mode is a separate closed-loop workload with an explicit causal graph.
- Dummy tokens preserve block-level equality topology, not literal production KV hash values or prompt semantics. Extremely short partial blocks have less encoding capacity than a full `u64`; a captured-trace comparison is the end-to-end proof that the selected trace has no synthetic-prefix collision against a specific Dynamo/frontend version.
- Operating-system scheduling and target backpressure can miss deadlines. The runner measures and fails those runs instead of hiding the error.
- Generated token segments are block-size quantized. The generator is bounded by `max_nodes`, `max_sessions`, and `max_total_input_tokens`, but it intentionally materializes its bounded plan before execution.
- Target request failures fail the run but still release preplanned successors so the load generator can drain the scenario and report all failures. Model-request retry policies are not sampled yet; tool failure and retry behavior is sampled by the generator.

## License

Apache-2.0. Files derived from NVIDIA Dynamo retain their NVIDIA copyright headers. See `NOTICE`.
