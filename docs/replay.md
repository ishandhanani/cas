<!-- SPDX-License-Identifier: Apache-2.0 -->

# Trace replay

Replay reconstructs an agent dependency graph from Dynamo `dynamo.request.trace.v1` records, then reproduces its KV shape and causal timing against a live frontend. This follows Dynamo's agentic request-trace lowering policy.

## Required trace data

Every request must be a `request_end` record with:

- `agent_context.session_id` on every request. `parent_session_id` identifies subagents when present.
- `request_id`, `request_received_ms`, and positive `output_tokens`.
- `event_time_unix_ms`, or `total_time_ms`, so source completion time can be recovered.
- Replay metadata with `trace_block_size`, positive `input_length`, and exactly `ceil(input_length / trace_block_size)` `input_sequence_hashes`.

The loader accepts raw JSONL, wrapped trace records, gzip JSONL, and multiple shards. Context-free and mixed traces are rejected; there is no standard open-loop fallback.

`tool_end` and `tool_error` records are optional. Claude records with `source_request_id`, `consumer_request_id`, `child_session_id`, and `execution_mode` provide exact subagent launch and join evidence.

## Dependency lowering

Replay creates one graph node per model request.

1. Requests sharing `session_id` are chained in recorded start order.
2. The first request in a child session waits for the parent request that spawned it.
3. A blocking or later explicit join waits for the child session's last-finishing request.
4. Claude tool metadata selects the exact parent source and consumer requests when available.
5. Without exact tool metadata, launch is associated with the latest parent request that started before the child and join with the first parent request that started after the child finished.
6. The complete graph is validated as acyclic before traffic starts.

For a dependent request, the recorded residual delay is:

```text
request start - latest recorded dependency completion
```

Overlapping tool spans are attributed by interval union, so parallel tools are not double-counted. Tool and non-tool components are retained in the lowered IR, while their sum controls request readiness.

## Causal timing

Roots and dependent turns use different clocks:

```text
root ready time = recorded root offset

dependent ready time = latest actual replay dependency completion
                     + recorded residual delay
```

This preserves external session arrivals without allowing a tool continuation or subagent join to run before its prerequisite response completes. A slower target shifts dependent traffic and lowers achieved request rate naturally.

`--time-scale` divides both root offsets and dependency delays. `--max-in-flight` is a client concurrency bound; ready requests wait for a slot and report that wait as `local_admission_lag_ms`.

The default client-offer gates are:

- `--max-dispatch-p99-ms 2`
- `--max-dispatch-max-ms 5`

These measure delay after a graph node becomes ready. They do not measure frontend queueing or model service time.

## Synthetic tokens

Replay hashes describe block equality, not original prompt tokens. The token dictionary maps each hash to a deterministic synthetic codeword:

- Equal hashes produce equal token blocks.
- Different hashes produce different full-block codewords.
- Partial final blocks preserve as much hash entropy as their token capacity allows.

Verified mode loads `tokenizer.json` from a local file, model directory, or Hugging Face model ID. It excludes known special IDs and records tokenizer and alphabet digests.

A manual range requires both `--token-start` and `--allow-unverified-token-range`. Synthetic IDs are sent through `nvext.token_data`; use Dynamo's Rust chat processor so they reach the engine unchanged.

## Inspect and replay

```bash
target/release/agent-loadgen inspect trace.jsonl.gz \
  --tokenizer /models/GLM-4.7-Flash

target/release/agent-loadgen replay trace.jsonl.gz \
  --agent codex \
  --model zai-org/GLM-4.7-Flash \
  --target http://127.0.0.1:8000 \
  --output artifacts/replay \
  --tokenizer /models/GLM-4.7-Flash
```

`inspect` reports root count and dependency-edge count in addition to shape statistics. `run.json` labels captured runs with `scheduling_model: agentic_causal`.

## Compare a captured replay

Enable Dynamo request tracing while replay runs, then compare the source with the new trace:

```bash
target/release/agent-loadgen compare \
  --source source-trace.jsonl.gz \
  --replay captured-trace.jsonl.gz \
  --requests artifacts/replay/requests.jsonl
```

Comparison checks exact input/output lengths, block size, agent context, canonical KV-prefix topology, and frontend arrival error relative to the causal client dispatch times recorded in `requests.jsonl`. It does not compare dependent requests against their original absolute arrivals, because replay latency is intentionally allowed to move them.

## Engine telemetry

Cache policy stays outside the load generator. Optional engine JSONL can be joined after a run:

```bash
target/release/agent-loadgen join-telemetry \
  --requests artifacts/replay/requests.jsonl \
  --engine-telemetry engine.jsonl \
  --output artifacts/replay/requests-with-engine.jsonl
```

Records are matched across replay request ID, source `x_request_id`, and source request ID. Unknown fields are retained without interpreting engine policy.

## Limits

- Parent/child headers identify session topology but not always exact request-level launch and join points. Timestamp inference is used when explicit tool evidence is absent.
- `--max-requests` selects the earliest requests and may intentionally omit later joins.
- Source timestamps have millisecond resolution.
- Dummy tokens preserve equality shape, not literal production KV hash values.
- The loader materializes the selected trace and causal graph in memory, matching Dynamo's lowering model.
