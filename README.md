# agent-loadgen

`agent-loadgen` replays the KV shape and arrival timing from Dynamo `dynamo.request.trace.v1` request traces.

The replayer does not use prompt content. It maps recorded sequence hashes to deterministic synthetic token blocks. It sends the tokens through `nvext.token_data` and forces the recorded output length.

## Current scope

- Read raw or wrapped JSONL and gzip JSONL traces.
- Reject incomplete request-end records before network traffic starts.
- Preserve block-level prefix equality, exact input length, exact requested output length, and recorded arrival offsets.
- Map Claude Code, Codex, and OpenCode session context to Dynamo headers.
- Preserve user-message, tool-result, and other input triggers with valid dummy Chat Completions messages.
- Preserve Codex compaction metadata through `x-codex-turn-metadata` when the trace contains it.
- Send streaming Chat Completions requests and write request and run results.
- Prepare request bodies before the timing anchor, then dispatch them from one stable ready-time queue.
- Use Linux `timerfd` pacing and optional HTTP connection warmup before the recorded schedule starts.

The generator for sampled coding-agent traffic is a later phase.

## Build

```bash
cargo build --release
```

## Inspect a trace

```bash
agent-loadgen inspect trace.jsonl.gz
```

Use `--session-id ID` to inspect or replay one agent session from a combined trace.

## Replay a trace

```bash
agent-loadgen replay trace.jsonl.gz \
  --agent codex \
  --model zai-org/GLM-4.7-Flash \
  --target http://127.0.0.1:8000 \
  --output artifacts/replay \
  --warmup-connections 4 \
  --token-start 1000 \
  --token-alphabet-size 1024
```

The target can be a base URL or a full `/v1/chat/completions` URL. The token range must contain valid, non-special token IDs for the target model.

Strict replay rejects zero-output records. Such records need a separate prefill-only or cancellation policy.

The default timing mode never retimes late requests. The run is invalid when local admission or target backpressure causes excessive dispatch lag.

## Compare traces

Enable Dynamo request tracing during replay. Then compare the captured trace with the source trace.

```bash
agent-loadgen compare \
  --source source-trace.jsonl.gz \
  --replay replay-trace.jsonl.gz \
  --requests artifacts/replay/requests.jsonl
```

The comparison checks exact ISL, exact OSL, agent context, canonical prefix topology, and normalized arrival offsets.

## Outputs

- `run.json` contains the run configuration, shape manifest, timer backend, timing percentiles, and length fidelity.
- `requests.jsonl` contains scheduler-wake, local-admission, dispatch, response, and fidelity data for each request.
