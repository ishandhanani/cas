# Generator configuration

Generator profiles are strict, versioned TOML. Unknown fields and unsupported schema versions fail during planning. `scenario.json` contains the fully resolved configuration, so a run does not depend on hidden defaults after planning.

Schema version 3 removes `behavior.background_request_probability`. Independent work uses subagents with separate session lineage and explicit dependencies.

## Execution model

```text
active-agent slot -> root task -> model request
                                  |
                                  +--text----------think delay------> next request
                                  +--tool----------tool latency-----> next request
                                  +--parallel tools-max latency-----> next request
                                  +--subagent------child completion-> parent join
                                  +--swarm---------all children-----> parent join
                                  +--complete response--restart delay------> next root task in slot
```

Generated traffic is closed-loop. The first task in each active-agent slot starts during the configured ramp-up. Every later request becomes ready only after all graph dependencies complete, plus its sampled virtual delay. The final model response completes the task. The slot starts its next root task after `restart_delay_ms`. Target response latency therefore controls the generated request rate. The scheduler can still run independent slots and sibling subagents concurrently. A blocking child or swarm joins the parent. A non-blocking child continues in its own session while the parent continues.

The generator does not execute tools or generate meaningful text. Tool classes sample delay, result-token geometry, failure, and retry behavior. Those sampled outcomes and direct child-session IDs are recorded on their originating node in `scenario.json`.

## Minimal profile

```toml
schema_version = 3
agent = "codex"
seed = 42
```

`agent` selects the `claude-code`, `codex`, or `opencode` structural preset. Every remaining field is an optional override. Checked-in profiles contain only intentional deviations from those presets; the sections below define the complete surface.

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

- `load.root_sessions`: total root tasks in the finite generated run.
- `load.concurrent_agents`: number of closed-loop root-agent slots. It must not exceed `root_sessions`.
- `load.startup_interval_ms`: spacing between the first task in each slot; zero starts the full population together.
- `load.restart_delay_ms`: sampled idle delay between a completed root task and its slot's next task.

For example, `root_sessions = 100` and `concurrent_agents = 10` runs ten active root agents, each processing ten tasks. With a zero restart delay, every slot replaces a completed task immediately. Subagents can temporarily push live request concurrency above ten.

## Trajectory and token shape

- `trajectory.turns`: maximum foreground model turns per root session.
- `trajectory.think_time_ms`: delay after a sampled text response.
- `trajectory.output_tokens`: requested output length.
- `tokens.block_size`: generated KV block size and token-segment quantization.
- `tokens.system_prefix_tokens`: global prefix shared by every session.
- `tokens.tool_catalog_tokens`: global tool-definition prefix shared by every session.
- `tokens.repository_tokens`: prefix shared by one root and all of its descendants.
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
- `limits.max_sessions`: hard bound on root and child sessions.
- `limits.max_total_input_tokens`: hard bound on the sum of request input lengths.

Planning fails when a hard limit is exceeded. These limits bound generated-plan memory; they do not cap live HTTP concurrency, which is controlled by `--max-in-flight`.

## Reproducibility

The plan records a resolved-profile SHA-256 digest and a scenario SHA-256 digest. Given the same binary behavior and config, the same seed produces the same sessions, graph, token labels, and samples. Runtime request IDs and measured timing are intentionally run-specific.
