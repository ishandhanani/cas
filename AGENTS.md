# Agent-loadgen guide

## Purpose

Agent-loadgen measures request shape and timing for coding-agent workloads. Request text is synthetic. Token identity, prefix reuse, request order, and timing are the important contracts.

Keep agent behavior separate from serving policy. Dynamo and the inference engine own routing, cache policy, prefetch, demotion, and eviction.

## Traffic models

All traffic is agentic and causal. Captured replay uses Dynamo's lowering policy: independent roots retain recorded arrival offsets, while same-session turns, tool continuations, subagent launches, and joins wait for actual replay completions plus their recorded residual delays. Traces without `agent_context` are rejected.

Generated traffic is also causal. A model response or sampled agent delay releases the next request. Target latency therefore changes the achieved request rate in both modes.

Captured requests and ordinary generated turns have positive input and output token counts. Compaction attempts can omit an output budget. A session completes with its last model response. Do not add lifecycle-control requests, zero-output close nodes, or final-request headers without a new design decision.

## Subagents

Each child is a separate session with a parent session ID. A child inherits the global, tool-catalog, and root-repository prefixes. It also receives a unique session prefix.

The generator supports these patterns:

- One child spawned from a parent turn.
- A swarm that spawns multiple sibling children.
- Recursive delegation up to `subagents.max_depth`.
- A blocking join that waits for all direct children.
- Non-blocking delegation that lets the parent and children continue independently.

Child trajectories can include tools, compaction, and more delegation. `load.concurrent_sessions` limits top-level session streams only. Child sessions are generated descendants and can increase live concurrency.

The generator does not support child roles, per-child profiles, child-to-child messages, voting, work stealing, persistent pools, or runtime model decisions. Planning samples the full graph before execution.

## Repository map

| Path | Purpose |
|---|---|
| `crates/core/` | Agent context, trace contracts, percentiles, and stable ready-time queue. No I/O or workload policy. |
| `crates/trace/` | Trace v1 parsing, validation, Dynamo-derived causal lowering, and structural comparison. |
| `crates/generate/` | Strict profile schema, deterministic sampling, serialized scenarios, and synthetic graph planning. |
| `crates/replay/` | Token synthesis, HTTP/SSE transport, causal schedulers, artifacts, and telemetry joins. |
| `src/` | Thin `agent-loadgen` CLI: flags, command composition, and exit behavior. |
| `profiles/` | Small example profiles for supported agents. |
| `docs/` | Architecture and detailed usage guides. |

## Change rules

- Make the smallest change that satisfies the request.
- Preserve deterministic planning for a fixed profile and seed.
- Keep the profile schema strict. Reject unknown fields.
- Keep prompt semantics and tool execution out of scope.
- Keep cache-policy decisions out of this repository.
- Keep `core` free of HTTP, Tokio, tokenizers, trace parsing, and generator policy. Among this workspace's crates, `trace` and `generate` may depend only on `core`; `replay` may consume both but neither may depend on `replay`.
- Add behavior details to `docs/`. Keep `README.md` as a short entry point.
- Keep compaction and subagent behavior in their dedicated guides.
- Update tests and docs when a timing, token-shape, or dependency contract changes.

## Validation

Run these commands before a commit:

```bash
cargo fmt --all
cargo test --workspace --all-targets
cargo clippy --workspace --all-targets -- -D warnings
cargo package --workspace --list --allow-dirty
```

The workspace crates are private and resolve each other by local path, so Cargo cannot prepare them for a crates.io upload. `cargo package --list` validates the files each package would include; the workspace test command is the compilation check.

Plan each checked-in profile after generator changes:

```bash
plan_root="$(mktemp -d)"
for profile in profiles/*.toml; do
  profile_name="${profile##*/}"
  cargo run --quiet -- plan --config "$profile" --output "$plan_root/${profile_name%.toml}"
done
```

## Git

Use `origin` for NVIDIA-dev/agent-loadgen. Do not push this repository to the old `cas` remote. Push only when the user asks. Sign commits with DCO and keep one logical change in each commit. Do not add generated attribution or coauthor lines.
