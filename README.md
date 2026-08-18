# agent-loadgen

`agent-loadgen` is a Rust load generator for coding-agent traffic through a Dynamo OpenAI-compatible frontend.

It supports two workload modes:

- `replay`: reproduce token counts, KV-prefix topology, agent headers, output lengths, root arrivals, turn delays, and subagent dependencies from `dynamo.request.trace.v1` traces.
- `generate`: create deterministic Claude Code, Codex, or OpenCode traffic with tools, compaction, subagents, and swarms.

Prompt text is synthetic. The fidelity contract is request shape, KV reuse, agent lineage, and timing.

## Build

```bash
cargo build --release
```

## Replay a trace

Inspect the trace and token dictionary first:

```bash
target/release/agent-loadgen inspect trace.jsonl.gz \
  --tokenizer /models/GLM-4.7-Flash
```

Replay it against Dynamo:

```bash
target/release/agent-loadgen replay trace.jsonl.gz \
  --agent codex \
  --model zai-org/GLM-4.7-Flash \
  --target http://127.0.0.1:8000 \
  --output artifacts/replay \
  --tokenizer /models/GLM-4.7-Flash \
  --token-path-verified \
  --engine-cache-mode ownership=session
```

Captured replay follows Dynamo's agentic policy. Independent roots keep their recorded arrival offsets. Dependent turns wait for actual replay completions plus the delay observed in the source trace.

## Generate agent traffic

Create a deterministic plan without sending traffic:

```bash
target/release/agent-loadgen plan \
  --config profiles/codex-balanced.toml \
  --output artifacts/codex-plan
```

Run the plan:

```bash
target/release/agent-loadgen generate \
  --config profiles/codex-balanced.toml \
  --model zai-org/GLM-4.7-Flash \
  --target http://127.0.0.1:8000 \
  --output artifacts/codex-run \
  --tokenizer /models/GLM-4.7-Flash
```

Generated traffic is closed-loop: model latency, tool time, compaction, and blocking subagent joins determine when the next request becomes ready. Every subagent is a separate child session with explicit parent lineage. Non-blocking children may continue after their parent advances.

## Outputs

- `scenario.json`: resolved generated profile and causal graph.
- `requests.jsonl`: one result per completed request, including timing and output fidelity.
- `run.json`: run configuration, bounded timing summaries, fidelity gates, and pass/fail status.

## Documentation

- [How it works](docs/how-it-works.md): timing models, dependency scheduling, subagents, request construction, and module boundaries.
- [Trace replay](docs/replay.md): trace contract, token synthesis, timing gates, comparison, and telemetry joins.
- [Generator configuration](docs/generator.md): profiles, distributions, tools, and safety limits.
- [Codex behavior and simulation controls](docs/codex-behavior.md): source-derived agent-loop behavior, interpretation of generator knobs, and known fidelity gaps.
- [Subagents and swarms](docs/subagents.md): delegation graphs, joins, lineage, KV sharing, and current limits.
- [Compaction](docs/compaction.md): trigger logic, physical attempts, KV-window changes, and telemetry boundaries.

## Current boundaries

- Chat Completions is the only supported protocol surface.
- Every captured request must carry `agent_context`; context-free and mixed traces are rejected.
- Captured requests must have positive input and output token counts.
- The tool does not generate meaningful prompt text or execute tools.
- Cache policy and routing behavior remain owned by Dynamo and the inference engine.
- Capacity or performance conclusions require a verified token path and declared engine cache mode.

## License

Apache-2.0. Files derived from NVIDIA Dynamo retain their NVIDIA copyright headers. See `NOTICE`.
