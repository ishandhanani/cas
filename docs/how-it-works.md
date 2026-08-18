<!-- SPDX-License-Identifier: Apache-2.0 -->

# How agent-loadgen works

`agent-loadgen` has two schedulers with different timing contracts:

```text
captured trace                         generated profile
      |                                      |
      v                                      v
absolute arrival times                seeded dependency graph
      |                                      |
      v                                      v
open-loop scheduler                    closed-loop scheduler
      |                                      |
      +------------------+-------------------+
                         |
                         v
               synthetic token request
                         |
                         v
                Dynamo Chat Completions
```

- Captured replay is open-loop. Responses do not move later recorded arrivals.
- Generated traffic is closed-loop. A dependency must complete before its successor can become ready.

## Shared request path

Both modes use the same request and result path:

1. Convert trace hashes or generated block labels into deterministic token IDs.
2. Send the IDs through `nvext.token_data` with valid dummy Chat Completions messages.
3. Add the selected Claude Code, Codex, or OpenCode lineage headers.
4. Stream the response and record TTFT, completion usage, status, and selected Dynamo headers.
5. Append the result to `requests.jsonl` and update bounded timing histograms.

Equal source block hashes always produce equal synthetic token blocks. Different hashes receive different codewords within the validated dictionary capacity. This preserves KV-prefix equality without preserving prompt text or production token IDs.

## Captured replay

```text
JSONL or gzip shards
        |
        v
temporary SQLite index
        |
        v
global order by arrival + source ordinal
        |
        v
bounded preparation lookahead
        |
        v
absolute monotonic deadlines
```

The trace loader validates all selected records before traffic starts. It rejects missing replay hashes, mixed block sizes, duplicate request IDs, and zero-token input or output records.

At runtime, request bodies are prepared only within `--prepare-lookahead-ms` of their arrival. When a deadline arrives, the scheduler offers the request immediately. If the local concurrency limit is full, the request fails fidelity instead of being delayed to a quieter time.

See [Trace replay](replay.md) for the exact trace and timing contract.

## Generated traffic

Generation has a planning phase and an execution phase.

### Planning

`plan` resolves a strict TOML profile over the selected agent preset and uses one seeded RNG to build a finite dependency graph. Each node records:

- Session and parent-session identity.
- KV block sequence and output budget.
- Dependencies and delay after dependency completion.
- Tool, subagent, swarm, or compaction metadata.

The complete graph is written to `scenario.json`. Runtime model text never changes the plan.

### Execution

```text
dependencies complete
        |
        +----> ready time = completion + sampled delay
        |
        +----> prepare request while the delay counts down
                         |
                         v
                  dispatch at ready time
                         |
                         v
                 response completes
                         |
                         v
                 release successors
```

`load.concurrent_agents` creates fixed root-agent slots. The first root in each slot follows the configured startup spacing. After a root's last model response completes, that slot starts its next root after `restart_delay_ms`.

A slower target therefore lowers generated request rate naturally. Independent root slots and child sessions can still overlap.

Only root request bodies are prepared before the clock anchor. Successor bodies are prepared when their final dependency completes. Token-payload memory therefore follows the runnable graph frontier rather than the total planned input-token count.

## Subagents and swarms

A subagent is a separate generated session, not another request inside the parent session.

```text
parent model response
        |
        +---- spawn delay ----> child session
        |                         parent_session_id = parent
        |                         independent turns and tools
        |
        +---- blocking ----------> parent waits for child's last response
        |
        +---- non-blocking ------> parent continues; child runs independently
```

Children share global and root-scoped KV labels. Each child receives a unique session prefix and explicit parent lineage. Blocking children delay the parent. Non-blocking children continue independently.

See [Subagents and swarms](subagents.md) for the dependency rules, supported patterns, and current limits.

## Compaction

Compaction is one logical operation with one or more physical attempts:

```text
stable operation_id
  attempt 1  no_mutation_aborted   optional
  attempt 2  apply_once            exactly one
  attempt 3  duplicate_noop        optional
```

The successful attempt changes later KV shape once. All attempts keep the same operation ID. Cache-mutation claims require engine telemetry.

See [Compaction](compaction.md) for trigger logic, attempt semantics, retained context, and proof boundaries.

## Timing fields

`requests.jsonl` separates local timing stages:

- `scheduled_offset_ms`: intended request release.
- `scheduler_wake_lag_ms`: timer wakeup error.
- `local_admission_lag_ms`: time waiting for local concurrency.
- `dispatch_lag_ms`: time from intended release until the HTTP client receives the request.
- `ttft_ms`: time from dispatch to first output.
- `total_time_ms`: time from dispatch to stream completion.

Frontend-observed arrival is different from client dispatch. Capture a Dynamo request trace during replay and use `compare` to measure that path.

## Artifacts and claims

`run.json` labels the protocol surface, traffic kind, token-path verification, and declared engine cache mode. Capacity or performance conclusions are blocked unless token IDs are verified to reach the engine unchanged and cache mode is declared.

Cache policy remains outside this project. `join-telemetry` only correlates request-scoped engine records.

## Module map

| Module | Responsibility |
|---|---|
| `trace.rs` | Trace v1 parsing, validation, and in-memory comparison input |
| `trace/storage.rs` | SQLite ordering, selection, manifests, and bounded reads |
| `token_shape.rs` | Safe token alphabet and deterministic block synthesis |
| `scenario/config.rs` | Strict profile schema, presets, and validation |
| `scenario/distribution.rs` | Deterministic fixed, uniform, and log-normal sampling |
| `scenario/model.rs` | Serialized scenario, node, session, tool, and compaction types |
| `scenario/plan.rs` | Seeded agent, tool, subagent, swarm, and compaction graph |
| `scheduler.rs` | Stable ready-time queue |
| `replay.rs` | Shared run options, results, and request execution types |
| `replay/captured.rs` | Absolute-time open-loop trace scheduler |
| `replay/generated.rs` | Dependency-driven closed-loop scenario scheduler |
| `replay/request.rs` | Request encoding, HTTP transport, and SSE parsing |
| `replay/artifacts.rs` | Incremental results, histograms, and run gates |
| `compare.rs` | Source-versus-captured KV shape and arrival comparison |
| `telemetry.rs` | Policy-neutral request-ID telemetry join |
| `agent.rs` | Agent-native and canonical Dynamo lineage headers |
| `clock.rs` | Absolute monotonic sleep |
