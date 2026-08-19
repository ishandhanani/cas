# Built-in generator presets

This page records every default resolved by generator schema version 4. A profile only needs `schema_version` and `agent`; omitted fields inherit these values. `scenario.json.config` is always the authoritative resolved configuration for a particular plan.

The presets are starting points for repeatable simulation, not claims that every Claude Code, Codex, or OpenCode deployment has these exact distributions. Values fall into four broad categories:

- Agent-calibrated: supported by observed agent behavior, such as the Claude Code versus Codex parallel-tool split.
- Synthetic baseline: plausible traffic geometry chosen to exercise the load generator before workload-specific calibration.
- Stress-oriented: deliberately more aggressive than a native default so short runs cover compaction, recursion, or failures.
- Safety bound: protects planning memory and does not describe agent behavior.

Use trace replay when reproducing one captured workload. For generated traffic, override these defaults from a representative trace corpus whenever performance conclusions depend on the exact distribution.

## Distribution notation

- `fixed(x)` always returns `x`.
- `uniform(a, b)` samples the inclusive integer range.
- `log_normal(median, sigma, min, max)` samples a log-normal value, rounds it, and clamps it to the inclusive bounds.

All sampling is deterministic for the resolved seed. Token segments are rounded up to `tokens.block_size` when the KV-shape graph is built.

## Required identity and seed

| Field | Default | Why |
|---|---:|---|
| `schema_version` | Required: `4` | Prevents an older profile from silently acquiring new semantics. |
| `agent` | Required | Selects Claude Code, Codex, or OpenCode context headers and agent-specific preset values. |
| `seed` | `0` | Makes a minimal profile deterministic. Checked-in balanced profiles explicitly use `42` for stable examples. |

## Load defaults

| Field | Default | Why |
|---|---:|---|
| `load.num_sessions` | `16` | Keeps the default finite plan small enough for a quick smoke while producing multiple top-level session trees. |
| `load.concurrent_sessions` | `8` | Exercises closed-loop top-level stream concurrency without treating the preset as a production capacity target. |
| `load.startup_interval_ms` | `0` | Starts the initial top-level population together so concurrency is visible immediately. |
| `load.restart_delay_ms` | `fixed(0)` | Replaces a completed top-level session tree immediately; users should add measured user/task idle time when modeling an interactive fleet. |

These are top-level session streams. Subagents are generated within those session trees and can push live session and request concurrency above `load.concurrent_sessions`.

## Trajectory defaults

| Field | Default | Why |
|---|---:|---|
| `trajectory.turns` | `uniform(4, 10)` | Produces multi-turn KV growth without making the smoke plan excessively large. This is a synthetic cap, not a native agent setting. |
| `trajectory.think_time_ms` | `log_normal(750, 0.8, 25, 15000)` | Represents non-tool continuation or later-user delay with a long tail. Native agents do not insert a fixed hidden-reasoning delay. |
| `trajectory.output_tokens` | `log_normal(450, 0.9, 16, 4096)` | Provides varied decode work while bounding each synthetic model response. Native Codex normally does not force this exact output budget. |

## Token-shape defaults

| Field | Default | Why |
|---|---:|---|
| `tokens.block_size` | `16` | Matches the common Dynamo replay block granularity and keeps prefix topology compact. Override it to match the target trace or engine contract. |
| `tokens.system_prefix_tokens` | Claude `12000`; Codex `11000`; OpenCode `7000` | Approximates different harness instruction surfaces. These are synthetic calibration values and should be measured for a specific agent version. |
| `tokens.tool_catalog_tokens` | Claude `14000`; Codex `9000`; OpenCode `11000` | Approximates the different tool-schema surfaces exposed by each harness. |
| `tokens.repository_tokens` | `log_normal(2000, 0.7, 256, 12000)` | Creates a root-tree prefix shared by a parent and all descendants, with a long tail for larger repository context. |
| `tokens.session_tokens` | `uniform(128, 1024)` | Adds session-specific environment and instruction state after the shared prefixes. |
| `tokens.user_tokens` | `log_normal(180, 0.9, 16, 4096)` | Models mostly short user turns with occasional large task descriptions. |
| `tokens.tool_result_tokens` | `log_normal(700, 1.1, 16, 16384)` | Models the parent-visible result appended after a subagent join; the wide tail covers large findings and logs. |
| `tokens.context_window_tokens` | `128000` | Supplies a broadly useful context-window basis for synthetic compaction. Set it to the deployed model's actual usable window for calibrated runs. |

## Behavior defaults

The action probabilities are sampled once per non-final model turn. They are not fractions of tool calls: one parallel action contributes every call sampled by `tools.parallel_count`.

| Agent | Serial tool | Parallel tool | One subagent | Swarm | Early completion | Remaining text/user action | Why |
|---|---:|---:|---:|---:|---:|---:|---|
| Claude Code | `0.62` | `0.0` | `0.10` | `0.025` | `0.10` | `0.155` | SWE-chat showed effectively no Claude tool batching, so the previous total tool-phase mass is assigned to serial calls. |
| Codex | `0.39` | `0.19` | `0.08` | `0.025` | `0.10` | `0.215` | With an average parallel fanout of three, this produces about 59% of calls in parallel phases, matching the observed 55–65% range while preserving the previous total tool-phase rate. |
| OpenCode | `0.48` | `0.08` | `0.10` | `0.025` | `0.10` | `0.215` | Retains the earlier neutral baseline because the current evidence does not provide an OpenCode-specific parallelism calibration. |

The remaining probability becomes a text/user continuation. At the configured maximum subagent depth, delegation probability also falls through to text because another child cannot be created.

## Tool defaults

| Field | Default | Why |
|---|---:|---|
| `tools.parallel_count` | `uniform(2, 4)` | Covers small batches centered on three calls, the scale seen in Codex command clusters. |
| `tools.retry_probability` | `0.35` after a failed call | Exercises retry-shaped delay and KV growth without retrying successful calls. Treat it as a synthetic stress value unless traces support it. |

The built-in tool list is a generic cross-agent baseline. Latency distributions use `sigma=0.9`; result-size distributions use `sigma=1.0`.

| Class | Weight | Latency milliseconds | Result tokens | Error probability | Rough role |
|---|---:|---|---|---:|---|
| `read` | `0.27` | `log_normal(195, 0.9, 40, 350)` | `log_normal(1064, 1.0, 128, 2000)` | `0.01` | Fast local file reads with moderate returned context. |
| `search` | `0.21` | `log_normal(490, 0.9, 80, 900)` | `log_normal(2064, 1.0, 128, 4000)` | `0.03` | Repository search with larger and more variable output. |
| `shell` | `0.21` | `log_normal(1300, 0.9, 100, 2500)` | `log_normal(4032, 1.0, 64, 8000)` | `0.08` | Commands, builds, and tests; an important source of Codex parallel batches. |
| `patch` | `0.13` | `log_normal(960, 0.9, 120, 1800)` | `log_normal(1532, 1.0, 64, 3000)` | `0.05` | File-edit operations with bounded acknowledgements or diagnostics. |
| `network` | `0.08` | `log_normal(2650, 0.9, 300, 5000)` | `log_normal(6064, 1.0, 128, 12000)` | `0.10` | Slow, failure-prone remote operations with large results. |
| `orchestration` | `0.10` | `log_normal(135, 0.9, 20, 250)` | `log_normal(272, 1.0, 32, 512)` | `0.03` | Lightweight spawn, message, wait, and coordination actions. |

Tool class inclusion materially changes time-weighted parallelism. Fit weights and per-class latency from raw tool spans when matching a workload; do not infer time overlap from the action probabilities alone.

## Compaction defaults

| Field | Default | Why |
|---|---:|---|
| `compaction.enabled` | `true` | Makes long generated contexts exercise KV-window replacement automatically. |
| `compaction.trigger_fraction` | `0.78` | Stress-oriented threshold that makes compaction reachable in shorter synthetic runs. The studied Codex source uses a threshold no greater than `0.90`; use that value for a source-aligned Codex profile. |
| `compaction.summary_input_tokens` | `uniform(256, 1024)` | Models a bounded synthetic summarization instruction. |
| `compaction.summary_output_tokens` | `uniform(512, 2048)` | Creates a compact retained summary with varied decode and future-prefix cost. |
| `compaction.retained_recent_tokens` | `8192` | Keeps a meaningful recent tail beside the summary after old context is replaced. |
| `compaction.abort_probability` | `0.0` | Normal traffic has no injected client-aborted attempt. |
| `compaction.retry_probability` | `0.0` | Normal traffic has no injected duplicate attempt. |
| `compaction.max_attempts` | `3` | Bounds fault scenarios when abort or retry injection is explicitly enabled; dormant at the zero probabilities. |
| `compaction.abort_after_ms` | `fixed(10)` | Supplies a deterministic early-cancel delay only when abort injection is enabled. |

## Subagent defaults

| Field | Default | Why |
|---|---:|---|
| `subagents.max_depth` | `2` | Stress-oriented recursion coverage. The studied Codex default permits nesting depth one, so use `1` for that source-aligned workload. |
| `subagents.turns` | `uniform(2, 5)` | Keeps child trajectories shorter than roots while retaining multi-turn KV growth. |
| `subagents.fanout` | `uniform(2, 4)` | Models small sibling swarms without exploding the graph. |
| `subagents.spawn_delay_ms` | `uniform(5, 100)` | Adds lightweight spawn/coordination overhead after the parent model response. |
| `subagents.blocking_probability` | `0.72` | Makes most delegations feed a parent join while retaining non-blocking parent/child overlap. This is a synthetic coordination baseline, not a native Codex knob. |

## Safety defaults

| Field | Default | Why |
|---|---:|---|
| `limits.max_nodes` | `10000` | Bounds the number of materialized model-request nodes. |
| `limits.max_sessions` | `1000` | Bounds roots plus recursively generated child sessions. |
| `limits.max_total_input_tokens` | `250000000` | Bounds aggregate plan size before HTTP payload construction. |

These are planner memory guards. Runtime HTTP concurrency is separately bounded by `generate --max-in-flight`.

## Checked-in balanced profiles

`profiles/claude-code-balanced.toml` and `profiles/opencode-balanced.toml` select their built-in preset and set `seed = 42`; they add no behavioral overrides.

`profiles/codex-balanced.toml` sets `seed = 42`, widens root turns to `uniform(4, 12)`, and replaces the generic tool list with this Codex-oriented calibration:

| Class | Weight | Latency milliseconds | Result tokens | Error probability |
|---|---:|---|---|---:|
| `read` | `0.27` | `log_normal(120, 0.8, 40, 350)` | `log_normal(500, 1.0, 128, 2000)` | `0.01` |
| `search` | `0.21` | `log_normal(250, 0.9, 80, 900)` | `log_normal(900, 1.0, 128, 4000)` | `0.03` |
| `shell` | `0.21` | `log_normal(600, 1.0, 100, 2500)` | `log_normal(1200, 1.1, 64, 8000)` | `0.08` |
| `patch` | `0.13` | `log_normal(500, 0.9, 120, 1800)` | `log_normal(700, 0.9, 64, 3000)` | `0.05` |
| `network` | `0.08` | `log_normal(1500, 1.0, 300, 5000)` | `log_normal(1800, 1.2, 128, 12000)` | `0.10` |
| `orchestration` | `0.10` | `log_normal(80, 0.7, 20, 250)` | `log_normal(128, 0.7, 32, 512)` | `0.03` |

The Codex balanced profile is intentionally a useful load shape, not a full native-agent calibration. Reasoning versus visible output, exact per-class parallel eligibility, history-fork modes, and model-specific compaction output still require additional trace evidence.
