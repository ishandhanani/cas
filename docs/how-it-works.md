<!-- SPDX-License-Identifier: Apache-2.0 -->

# How agent-loadgen works

`agent-loadgen` has two workload engines that share token synthesis, HTTP execution, result recording, and fidelity reporting:

```text
captured request trace                         generated agent profile
          |                                             |
          v                                             v
SQLite order + bounded reader                  seeded causal graph
          |                                             |
          v                                             v
absolute monotonic schedule                    dependency-ready schedule
          |                                             |
          +------------------+--------------------------+
                             |
                             v
                  synthetic token request
                             |
                             v
                    Dynamo Chat Completions
                             |
                             v
                requests.jsonl + run.json
```

The two engines deliberately do not share a timing policy:

- Captured replay is open-loop. A response never delays or changes a later recorded arrival.
- Generated traffic is closed-loop. A model response, tool delay, child join, or session close releases its successors.

This distinction is the central load-generation contract.

## Captured replay

The `replay` command follows this path:

1. `trace.rs` streams raw or wrapped JSONL and gzip shards into a temporary SQLite database.
2. SQLite establishes one global order by `(request_received_ms, source ordinal)` and applies request/session selection after ordering.
3. `StoredTraceReader` returns fixed-size batches. The runner holds only a bounded preparation window in memory.
4. `token_shape.rs` converts every recorded sequence hash into a deterministic synthetic token block. Equal source hashes produce equal blocks; different hashes produce different blocks within the validated dictionary capacity.
5. `replay.rs` waits on an absolute monotonic schedule. It offers every due request immediately and records a fidelity failure instead of retiming a request when the local admission limit is exhausted.
6. `replay/request.rs` builds and streams the Chat Completions request. It records TTFT, usage, selected response headers, and exact-output or control-only results.
7. `replay/artifacts.rs` appends each result to `requests.jsonl`, maintains bounded histograms, evaluates the run gates, and writes `run.json`.

The original request ID stays in `source_request_id`. Every execution receives a new `replay_request_id`, which is also sent as `x-request-id`. This avoids collisions while retaining source correlation.

### What replay preserves

- Exact input token count.
- Exact requested output token count for budgeted model turns.
- Block-level prefix-equality topology.
- Recorded absolute arrival offsets and stable ordering for ties.
- Session, parent-session, lifecycle-finality, and input-trigger headers for the selected agent adapter.

Replay does not preserve prompt meaning or literal production token IDs. It preserves the shape that determines KV reuse and request scheduling.

## Generated traffic

Generated workloads have two phases.

### Planning

The `plan` command loads a strict, versioned TOML profile and resolves it over the selected Claude Code, Codex, or OpenCode preset. `scenario/plan.rs` then uses one seeded RNG to materialize a bounded dependency graph.

Every graph node contains:

- Its request and KV-block sequence.
- Its session and parent-session identity.
- Its dependencies and delay after those dependencies.
- Its lifecycle kind: `model_turn` or `session_close`.
- Sampled tool, subagent, swarm, or compaction metadata.

The resulting `scenario.json` is the complete workload. Runtime model output never mutates this plan.

### Closed-loop execution

The `generate` command builds the same deterministic scenario and executes it with a dependency scheduler:

```text
all dependencies complete
          |
          +----> schedule ready time = completion + sampled delay
          |
          +----> prepare request body while delay counts down
                         |
                         v
              scheduled ready time arrives
          |
          v
node enters ready queue
          |
          v
HTTP request completes
          |
          +----> release successors
```

`load.concurrent_agents` creates fixed root-agent slots. A slot starts its next root task only after the previous root session closes and its restart delay expires. Child sessions have their own lineage and may run concurrently. Non-blocking children can outlive their parent session because they are separate sessions; the generator does not create untracked concurrent requests inside one session.

Target latency therefore applies natural backpressure: a slower frontend reduces generated request rate without changing the planned population or causal graph.

Only root request bodies are prepared before the clock anchor. A successor body is prepared when its last dependency completes while its sampled delay counts down. Preparation never shifts the planned ready time; excess local work appears as dispatch lag. The plan stays deterministic while token payload memory is bounded by the runnable dependency frontier instead of the total graph.

## Session lifecycle

A response-producing final turn and a lifecycle close are separate requests:

```text
ModelTurn
  output budget > 0
  session_final = false
        |
        v
SessionClose
  input tokens = 0
  output budget absent
  session_final = true
```

This lets routers and engines interpret the close as control rather than accidentally suppressing the final model response. Captured replay accepts the same explicit zero-output final shape and rejects ambiguous zero-output non-final records.

## Compaction

Compaction is one logical operation with one or more physical attempts:

```text
operation_id = stable across attempts

attempt 1  no_mutation_aborted   optional
attempt 2  apply_once            exactly one
attempt 3  duplicate_noop        optional
```

The summary changes later KV shape exactly once, at the `apply_once` attempt. Physical compaction requests intentionally omit a forced output budget because real agent compaction output is not a fixed-length benchmark response. The planned summary size controls the retained synthetic context.

An aborted client wait cannot prove that an engine made no cache mutation. That claim requires request-scoped engine telemetry joined after the run.

## Request construction

All model traffic currently uses `/v1/chat/completions`.

- Synthetic prompt IDs are sent through `nvext.token_data`.
- Budgeted model turns set `max_tokens` and `ignore_eos` to enforce output shape.
- Unbudgeted compaction turns omit those fields.
- Session closes omit token data and the output budget.
- Tool-result turns use a valid assistant tool call followed by its tool result.
- Agent adapters add native Claude Code, Codex, or OpenCode lineage headers plus canonical Dynamo session headers.

The project does not expose Responses or native-agent wire modes until it can execute and validate those protocols.

## Timing measurements

`requests.jsonl` separates four clocks:

- `scheduled_offset_ms`: intended release time.
- `scheduler_wake_lag_ms`: monotonic timer wakeup error.
- `local_admission_lag_ms`: time waiting inside the load generator after wakeup.
- `dispatch_lag_ms`: time from the intended release until the request is offered to the HTTP client.

TTFT and total response time start at client dispatch. Target-observed frontend arrival is measured separately by capturing a Dynamo request trace and running `compare`.

`prepare_lookahead_ms` appears in `run.json` only for captured replay. Generated scheduling has no lookahead window.

## Artifacts and claims

`scenario.json` records the generated graph and resolved profile. `requests.jsonl` is append-oriented execution evidence. `run.json` is the bounded summary and pass/fail decision.

Every run labels:

- `protocol_surface`: currently `chat_completions`.
- `traffic_kind`: `captured_trace` or `synthetic_kv_shape`.
- `token_path_verified`: whether the caller proved that supplied token IDs reach the engine unchanged.
- `engine_cache_mode`: caller-declared cache settings.

Capacity or performance conclusions are blocked unless the token path is verified and cache mode is declared. Engine telemetry can be joined by request ID, but cache policy remains owned by Dynamo and the inference engine.

## Module map

| Module | Responsibility |
|---|---|
| `cli.rs` | Command-line schema only |
| `main.rs` | Command orchestration and user-facing output |
| `trace.rs` | Trace parsing, validation, SQLite ordering, and bounded readers |
| `token_shape.rs` | Safe token alphabet loading and hash-to-token block synthesis |
| `scenario/config.rs` | Strict profile schema, presets, resolution, and validation |
| `scenario/plan.rs` | Seeded causal graph and KV-shape planning |
| `scheduler.rs` | Stable ready-time queue |
| `replay.rs` | Open-loop and closed-loop scheduling orchestration |
| `replay/request.rs` | Request encoding, HTTP transport, SSE parsing, and per-request results |
| `replay/artifacts.rs` | Incremental result sink, histograms, run gates, and summaries |
| `compare.rs` | Source-versus-captured shape and frontend-arrival comparison |
| `telemetry.rs` | Policy-neutral request-ID telemetry join |
| `agent.rs` | Agent-specific and canonical Dynamo header mapping |
| `clock.rs` | Monotonic absolute-deadline sleep |

## Deliberate non-features

- No prompt semantics or meaningful model text.
- No response-driven mutation of captured replay or generated plans.
- No engine cache policy, eviction, demotion, prefetch, or routing decisions.
- No hidden same-session serialization in captured replay.
- No untracked same-session background requests; independent work uses child sessions with explicit lineage.
- No claim that client dispatch time equals Dynamo frontend arrival time.
