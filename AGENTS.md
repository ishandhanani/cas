# Agent-loadgen guide

## Purpose

Agent-loadgen measures request shape and timing for coding-agent workloads. Request text is synthetic. Token identity, prefix reuse, request order, and timing are the important contracts.

Keep agent behavior separate from serving policy. Dynamo and the inference engine own routing, cache policy, prefetch, demotion, and eviction.

## Traffic models

Captured replay is open-loop. Recorded client send times determine request readiness. Slow responses do not move later send times, but concurrency and admission gates can delay dispatch.

Generated traffic is closed-loop. A model response or sampled agent delay releases the next request. Target latency therefore changes the achieved request rate.

Captured requests and ordinary generated turns have positive input and output token counts. Compaction attempts can omit an output budget. A session completes with its last model response. Do not add lifecycle-control requests, zero-output close nodes, or final-request headers without a new design decision.

## Subagents

Each child is a separate session with a parent session ID. A child inherits the global, tool-catalog, and root-repository prefixes. It also receives a unique session prefix.

The generator supports these patterns:

- One child spawned from a parent turn.
- A swarm that spawns multiple sibling children.
- Recursive delegation up to `subagents.max_depth`.
- A blocking join that waits for all direct children.
- Non-blocking delegation that lets the parent and children continue independently.

Child trajectories can include tools, compaction, and more delegation. `load.concurrent_agents` limits root slots only. Child sessions can increase live concurrency.

The generator does not support child roles, per-child profiles, child-to-child messages, voting, work stealing, persistent pools, or runtime model decisions. Planning samples the full graph before execution.

## Repository map

| Path | Purpose |
|---|---|
| `src/trace.rs` | Trace v1 parsing and validation. |
| `src/trace/storage.rs` | SQLite ordering, selection, and bounded reads. |
| `src/agent.rs` | Agent-specific context headers. |
| `src/replay.rs` | Shared run and result contracts. |
| `src/replay/captured.rs` | Open-loop captured-trace scheduling. |
| `src/replay/generated.rs` | Closed-loop generated-graph scheduling. |
| `src/replay/request.rs` | HTTP construction, execution, and SSE parsing. |
| `src/replay/artifacts.rs` | Result writing, histograms, and run gates. |
| `src/scenario/config.rs` | Profile schema, presets, and validation. |
| `src/scenario/distribution.rs` | Deterministic distribution sampling. |
| `src/scenario/model.rs` | Serialized generated-scenario types. |
| `src/scenario/plan.rs` | Generated agent graph planning. |
| `src/token_shape.rs` | Safe dummy-token synthesis. |
| `src/compare.rs` | Structural trace comparison. |
| `profiles/` | Small example profiles for supported agents. |
| `docs/` | Architecture and detailed usage guides. |

## Change rules

- Make the smallest change that satisfies the request.
- Preserve deterministic planning for a fixed profile and seed.
- Keep the profile schema strict. Reject unknown fields.
- Keep prompt semantics and tool execution out of scope.
- Keep cache-policy decisions out of this repository.
- Add behavior details to `docs/`. Keep `README.md` as a short entry point.
- Keep compaction and subagent behavior in their dedicated guides.
- Update tests and docs when a timing, token-shape, or dependency contract changes.

## Validation

Run these commands before a commit:

```bash
cargo fmt --all
cargo test --all-targets
cargo clippy --all-targets -- -D warnings
cargo package --allow-dirty
```

Plan each checked-in profile after generator changes:

```bash
plan_root="$(mktemp -d)"
for profile in profiles/*.toml; do
  profile_name="${profile##*/}"
  cargo run --quiet -- plan --config "$profile" --output "$plan_root/${profile_name%.toml}"
done
```

The ignored 100,000-request stress test is useful after trace-storage or scheduler changes:

```bash
cargo test --all-targets trace::tests::stored_trace_handles_one_hundred_thousand_requests_in_batches -- --ignored --exact
```

## Git

Use `origin` for NVIDIA-dev/agent-loadgen. Do not push this repository to the old `cas` remote. Push only when the user asks. Sign commits with DCO and keep one logical change in each commit. Do not add generated attribution or coauthor lines.
