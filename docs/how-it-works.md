<!-- SPDX-License-Identifier: Apache-2.0 -->

# How agent-loadgen works

`agent-loadgen` has two graph producers and one causal timing contract:

```text
captured trace                         generated profile
      |                                      |
      v                                      v
Dynamo-style DAG lowering              seeded dependency graph
      |                                      |
      +------------------+-------------------+
                         |
                         v
               synthetic token request
                         |
                         v
                Dynamo Chat Completions
```

- Captured roots use recorded offsets. Captured dependent turns use actual replay completions plus recorded residual delays.
- Generated roots use configured offsets. Generated dependent turns use actual completions plus sampled delays.

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
request ends + terminal tool events
        |
        v
session and parent-session DAG lowering
        |
        v
roots: recorded offsets
dependents: completion + recorded gap
```

The trace loader validates all selected records before traffic starts. It rejects missing agent context, missing replay hashes, mixed block sizes, duplicate request IDs, zero-token input/output records, and cyclic dependencies.

Each session is sequential. Parent/child context adds subagent launch and join edges. Exact Claude tool metadata wins when present; otherwise Dynamo's timestamp association rules infer the parent request around the child lifetime. Ready requests wait for the local concurrency bound instead of being dropped.

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

The artifact also reports realized tool parallelism in two forms: the fraction of calls belonging to multi-call phases and the fraction of synthetic tool wall time with concurrent calls. This keeps high call batching from being mistaken for equally high time parallelism.

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

`load.concurrent_sessions` creates fixed top-level session streams. The first session tree in each stream follows the configured startup spacing. After its last model response completes, that stream starts its next top-level session tree after `restart_delay_ms`.

A slower target therefore lowers generated request rate naturally. Independent top-level streams and child sessions can still overlap.

Only initial top-level request bodies are prepared before the clock anchor. Successor bodies are prepared when their final dependency completes. Token-payload memory therefore follows the runnable graph frontier rather than the total planned input-token count.

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

Children share global and top-level-tree-scoped KV labels. Each child receives a unique session prefix and explicit parent lineage. Blocking children delay the parent. Non-blocking children continue independently.

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

- `scheduled_offset_ms`: intended causal release time.
- `scheduler_wake_lag_ms`: timer wakeup error.
- `local_admission_lag_ms`: time waiting for local concurrency.
- `dispatch_lag_ms`: time from intended release until the HTTP client receives the request.
- `ttft_ms`: time from dispatch to first output.
- `total_time_ms`: time from dispatch to stream completion.

Frontend-observed arrival is different from client dispatch. Capture a Dynamo request trace during replay and use `compare` to measure that path.

## Artifacts and claims

`run.json` labels the protocol surface, traffic kind, token-path verification, and declared engine cache mode. Capacity or performance conclusions are blocked unless token IDs are verified to reach the engine unchanged and cache mode is declared.

Cache policy remains outside this project. `join-telemetry` only correlates request-scoped engine records.

## Crate map

```text
                 agent-loadgen CLI
                        |
                        v
                      replay
                    /        \
                trace       generate
                    \        /
                      core
```

`core` has no I/O or workload policy. Trace ingestion and synthetic planning stay independent; replay is the runtime that consumes either planned workload.

| Crate | Responsibility |
|---|---|
| `agent-loadgen-core` | Public request and agent-context contracts, percentile summaries, and the stable ready-time queue. |
| `agent-loadgen-trace` | Trace v1 parsing, agentic causal lowering, and source-versus-captured comparison. |
| `agent-loadgen-generate` | Presets, strict TOML profile validation, seeded distributions, and generated scenario planning. |
| `agent-loadgen-replay` | Token shaping, request transport, captured and generated causal schedulers, run artifacts, and telemetry joins. |
| `agent-loadgen` | CLI flags and command wiring only. |

See [Workspace architecture](workspace.md) for the dependency rules and change-routing guide.
