# Generator configuration

Generator profiles are strict, versioned TOML. Unknown fields and unsupported schema versions fail during planning. `scenario.json` contains the fully resolved configuration, so a run does not depend on hidden defaults after planning.

Schema version 4 calls the load controls `num_sessions` and `concurrent_sessions`. They count only top-level agent session trees; subagent sessions are generated descendants in those trees. This version also retains the removal of `behavior.background_request_probability`: independent work uses subagents with separate session lineage and explicit dependencies.

Schema 3 profiles are intentionally rejected. Replace `root_sessions` with `num_sessions` and `concurrent_agents` with `concurrent_sessions`; there are no compatibility aliases.

## Execution model

```text
top-level stream -> session tree -> model request
                                  |
                                  +--text----------think delay------> next request
                                  +--tool----------tool latency-----> next request
                                  +--parallel tools-max latency-----> next request
                                  +--compaction attempt-compaction latency----> next request
                                  +--subagent------child completion-> parent join
                                  +--swarm---------all children-----> parent join
                                  +--complete response--restart delay------> next session tree in stream
```

Generated traffic is closed-loop. The first top-level session tree in each active stream starts during the configured ramp-up. Every later request becomes ready only after all graph dependencies complete, plus its sampled virtual delay. The final model response completes that session tree. The stream starts its next top-level session tree after `restart_delay_ms`. Target response latency therefore controls the generated request rate. The scheduler can still run independent streams and sibling subagents concurrently. A blocking child or swarm joins the parent. A non-blocking child continues in its own session while the parent continues.

The generator does not execute tools or generate meaningful text. Tool classes sample delay, result-token geometry, failure, and retry behavior. Those sampled outcomes and direct child-session IDs are recorded on their originating node in `scenario.json`.

## Plan graph

`plan` writes `plan.dot` beside `scenario.json`; `generate` writes the same graph into its run directory before traffic starts. It is the canonical causal overview: top-level session trees contain nested child-session clusters, node labels show action and ISL/OSL, and edges label continuation, stream restart, spawn, blocking join, and sampled client-side delay. A non-blocking spawn has a `parent continues` edge. A multi-child blocking join converges on a `join all N children` diamond; only its outgoing edge to the parent successor carries the sampled join delay. A `stream restart` edge joins successive top-level trees in one configured stream and carries the sampled `restart_delay_ms`.

When Graphviz's `dot` executable is on `PATH`, the commands also write `plan.svg`. Otherwise the plan remains successful with `plan.dot`, which can be rendered later:

```bash
dot -Tsvg plan.dot -o plan.svg
```

The graph intentionally does not assign durations to model requests. Generated traffic is closed-loop, so model service time comes from the target at execution. It shows all control-flow and client-side timing a user can tune in TOML: initial stream arrival, tool and think delays, child spawn, blocking joins, compaction attempts, and stream restart dependencies.

## Minimal profile

```toml
schema_version = 4
agent = "codex"
seed = 42
```

`agent` selects the `claude-code`, `codex`, or `opencode` structural preset. Every remaining field is an optional override. Checked-in profiles contain only intentional deviations from those presets; the sections below define the complete surface.

See [Built-in generator presets](presets.md) for every resolved default, the checked-in profile overrides, and the rationale behind each value.

The override surface is intentionally detailed. Some fields correspond to native agent settings, while others make model-selected runtime outcomes deterministic and tunable for simulation. See [Codex behavior and simulation controls](codex-behavior.md) for a source-derived interpretation of the Codex preset and each class of knob.

## Distributions

Token sizes, turn counts, and delays accept one of these forms:

```toml
value = { kind = "fixed", value = 8 }
value = { kind = "uniform", min = 4, max = 12 }
value = { kind = "log_normal", median = 450.0, sigma = 0.9, min = 16, max = 4096 }
```

All sampling uses the profile seed. Log-normal samples are rounded and clamped to the configured inclusive bounds.

## Load

- `load.num_sessions`: total top-level agent session trees in the finite generated run.
- `load.concurrent_sessions`: number of closed-loop top-level session streams. It must not exceed `num_sessions`.
- `load.startup_interval_ms`: spacing between the first session tree in each stream; zero starts the full population together.
- `load.restart_delay_ms`: sampled idle delay between a completed session tree and its stream's next tree.

For example, `num_sessions = 100` and `concurrent_sessions = 10` runs ten active top-level streams, each processing ten session trees. With a zero restart delay, every stream replaces a completed tree immediately. Subagents are generated inside those trees and are not counted by either setting, but can temporarily push live request concurrency above ten.

## Trajectory and token shape

- `trajectory.turns`: maximum foreground model turns per top-level session.
- `trajectory.think_time_ms`: delay after a sampled text response.
- `trajectory.output_tokens`: requested output length.
- `tokens.block_size`: generated KV block size and token-segment quantization.
- `tokens.system_prefix_tokens`: global prefix shared by every session.
- `tokens.tool_catalog_tokens`: global tool-definition prefix shared by every session.
- `tokens.repository_tokens`: prefix shared by one top-level session tree and all of its descendants.
- `tokens.session_tokens`: environment/instruction prefix unique to a session.
- `tokens.user_tokens`: appended user-message size.
- `tokens.tool_result_tokens`: appended parent join-result size after child agents.
- `tokens.context_window_tokens`: compaction threshold basis.

The configured token sizes are rounded up to complete KV blocks. This keeps prefix construction simple and deterministic while preserving the intended cache geometry.

## Behavior

- `behavior.tool_probability`: one external tool call.
- `behavior.parallel_tool_probability`: two or more concurrent external tool calls represented by their maximum latency.
- `behavior.subagent_probability`: one child session.
- `behavior.swarm_probability`: sampled child fanout.
- `behavior.completion_probability`: early completion after the minimum trajectory prefix.

The first five probabilities must sum to at most 1.0. Remaining probability becomes a text/user turn. The last configured turn is always a completion so every non-truncated session terminates.

These are probabilities per model turn, not fractions of tool calls. A parallel phase contributes every sampled call in `tools.parallel_count`. For example, a serial probability of `0.39`, a parallel probability of `0.19`, and an average parallel count of three produce about 59% of tool calls in parallel phases even though only 19% of model turns select that action.

## Tool classes

`tools.classes` replaces the preset list. Weights are relative and do not need to sum to one. Each selected class samples latency and result size, then independently samples `error_probability`. `tools.retry_probability` controls whether a failed call adds one more latency and result sample. `tools.parallel_count` controls tool fanout for a parallel action.

```toml
[tools]
parallel_count = { kind = "uniform", min = 2, max = 4 }
retry_probability = 0.35

[[tools.classes]]
name = "shell"
weight = 0.5
latency_ms = { kind = "log_normal", median = 600.0, sigma = 1.0, min = 100, max = 2500 }
result_tokens = { kind = "log_normal", median = 1200.0, sigma = 1.1, min = 64, max = 8000 }
error_probability = 0.08
```

## Parallelism validation

`scenario.json` records `tool_parallelism` from the realized samples:

- `parallel_call_fraction`: fraction of tool calls that belong to a multi-call phase.
- `parallel_wall_time_fraction`: fraction of synthetic tool-phase wall time with at least two calls active.
- `tool_work_ms`: sum of every individual tool duration.
- `tool_wall_ms`: sum of each phase's critical-path duration.
- `work_to_wall_ratio`: tool work divided by tool wall time.

Generated calls in a parallel phase start together. Its wall time is the longest call, and its interval with at least two active calls is the second-longest call. These metrics describe harness-internal batching; they do not count incidental overlap between independent top-level streams or subagents.

## Compaction

- `compaction.enabled`: enables context-window compaction.
- `compaction.trigger_fraction`: fraction of `context_window_tokens` that triggers compaction before the next turn.
- `compaction.summary_input_tokens`: synthetic compaction-instruction tokens added to the compaction request.
- `compaction.summary_output_tokens`: summary output length and summary size retained in the next context window.
- `compaction.retained_recent_tokens`: recent non-stable context retained beside the summary.
- `compaction.abort_probability`: probability that a logical compaction starts with one client-aborted physical attempt.
- `compaction.retry_probability`: probability that a successful attempt is followed by a duplicate physical attempt.
- `compaction.max_attempts`: maximum physical attempts per logical operation.
- `compaction.abort_after_ms`: sampled delay before canceling an aborted attempt.

See [Compaction](compaction.md) for the logical operation, physical attempts, KV-window changes, and validation limits.

## Subagents and swarms

- `subagents.max_depth`: maximum recursive child depth.
- `subagents.turns`: foreground turns per child.
- `subagents.fanout`: child count for a swarm.
- `subagents.spawn_delay_ms`: delay between parent completion and child readiness, and the synthetic join delay.
- `subagents.blocking_probability`: probability that the parent next turn depends on every child completion.

See [Subagents and swarms](subagents.md) for graph shape, joins, KV sharing, recursion, and current limits.

## Safety limits

- `limits.max_nodes`: hard bound on physical model requests in the materialized graph.
- `limits.max_sessions`: hard bound on all protocol sessions, including top-level sessions and generated descendants.
- `limits.max_total_input_tokens`: hard bound on the sum of request input lengths.

Planning fails when a hard limit is exceeded. These limits bound generated-plan memory; they do not cap live HTTP concurrency, which is controlled by `--max-in-flight`.

## Reproducibility

The plan records a resolved-profile SHA-256 digest and a scenario SHA-256 digest. Given the same binary behavior and config, the same seed produces the same top-level session trees, descendant graph, token labels, and samples. Runtime request IDs and measured timing are intentionally run-specific. `session_topology` in `scenario.json` separates configured top-level streams from generated child protocol sessions.
