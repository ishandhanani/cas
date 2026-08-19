<!-- SPDX-License-Identifier: Apache-2.0 -->

# Codex behavior and simulation controls

This guide records what the generator can learn from Codex's implementation and how that behavior maps to agent-loadgen profiles. The source study used Codex snapshot `58ad79b60eeb386cda50663cdc0aaff009dd1493`.

The profile surface is intentionally broader than Codex's user-facing configuration. Codex lets the model choose tools, follow-up requests, and delegation at runtime. Agent-loadgen must turn those runtime outcomes into deterministic distributions so users can reproduce observed traffic, sweep alternative workloads, and stress specific KV-cache patterns.

The knobs therefore fall into three groups:

- Source-aligned controls represent settings or limits that Codex exposes directly.
- Calibrated controls represent runtime outcomes that should be fitted from traces.
- Fault controls deliberately create retries or aborts that are not ordinary agent behavior.

## Agent loop

One Codex user turn can produce several model requests:

```text
user input
    |
    v
model request -- final response --------------------> turn complete
    |
    +-- tool calls --> execute tools --> append outputs --+
    |                                                    |
    +-- subagent tools --> spawn/message/wait -----------+--> next model request
```

Codex sends normalized conversation history, executes the tool calls returned by the model, records their outputs, and continues until the response no longer requires a follow-up. It does not configure a fixed request count or a probability of completion.

Agent-loadgen preplans that response-driven loop. These fields sample its physical shape:

- `trajectory.turns` bounds foreground model turns in one generated session.
- `behavior.completion_probability` samples early termination after the minimum trajectory prefix.
- Tool and delegation probabilities sample which follow-up caused the next request.

These are useful simulation controls, but they are not native Codex settings. Fit them from request traces when reproducing a measured workload. Change them directly when exploring traffic outside the captured distribution.

## Request and context shape

Codex constructs a Responses request from model instructions, accumulated response items, available tool schemas, reasoning settings, and a conversation cache key. The initial context can include developer instructions, AGENTS.md, permissions, collaboration mode, skills, plugins, and environment state. Later turns normally carry accumulated history plus context changes.

Agent-loadgen uses synthetic Chat Completions messages and `nvext.token_data`. It represents the corresponding KV regions with:

- `tokens.system_prefix_tokens`: globally shared model and agent instructions.
- `tokens.tool_catalog_tokens`: globally shared tool schemas.
- `tokens.repository_tokens`: root-tree context shared by a parent and descendants.
- `tokens.session_tokens`: session-specific instructions and environment.
- `tokens.user_tokens`: new user input.
- Tool-class `result_tokens`: external tool output.
- `tokens.tool_result_tokens`: parent-visible result added after child work.

The source components are derived dynamically, but their resulting token geometry still needs explicit distributions in a synthetic simulator. Calibrate these fields from real tokenized requests or captured KV shapes rather than treating the preset values as universal Codex constants.

Native Codex does not normally send a forced output-token budget on its Responses requests. `trajectory.output_tokens` remains useful because agent-loadgen needs deterministic decode load and exact output-length validation. It is a load-shaping control, not a native Codex option.

## Timing

Codex does not add a synthetic think delay between model requests inside an active tool loop. The main gaps are:

- Model service latency.
- Tool execution latency.
- Explicit subagent waits.
- Transport retry backoff.
- Time before a later user task.

Agent-loadgen maps those effects onto `tools.classes[].latency_ms`, `subagents.spawn_delay_ms`, `trajectory.think_time_ms`, and `load.restart_delay_ms`. Interpret `trajectory.think_time_ms` as a calibrated non-tool continuation delay, not hidden model reasoning time. Use `restart_delay_ms` for the gap between independent top-level session trees.

Generated traffic remains closed-loop: target response time and sampled client-side delays determine when successors become ready.

## Tool use and parallelism

Codex sends tool schemas with automatic tool choice. The model decides whether to call a tool and which tool to call. Parallel tool calls are advertised when the selected model supports them, and compatible tool futures can execute concurrently.

Agent-loadgen makes those runtime outcomes controllable:

- `behavior.tool_probability` samples one tool call.
- `behavior.parallel_tool_probability` samples concurrent calls.
- `tools.classes[].weight` controls the tool mix.
- `tools.parallel_count` controls parallel fanout.
- Tool latency, result size, and error probability control the continuation's timing and KV growth.
- `tools.retry_probability` approximates the rate at which failed work produces another attempt.

Codex itself does not expose these probabilities. They should come from traces for fidelity runs. Direct overrides are valuable for tool-heavy, network-heavy, or failure-heavy stress tests.

### Empirical preset calibration

A 2026-08-18 analysis of 496 Claude Code sessions and real Codex sessions in [SWE-chat](https://huggingface.co/datasets/SALT-NLP/SWE-chat) found that tool batching is primarily a harness/model behavior rather than a coding-domain behavior:

- Claude Code placed only 15 of 42,784 tool calls in parallel batches, about 0.04%.
- Codex placed roughly 55–65% of tool calls in overlapping batches, with concurrent `exec_command` clusters accounting for much of that behavior.
- Call-weighted parallelism was much larger than time-weighted parallelism. Codex's parallel tool-time share ranged from single digits to roughly 27% across samples and counting rules.

The built-in presets now preserve their previous total tool-phase probability while changing the serial/parallel split. Claude Code uses `tool_probability = 0.62` and `parallel_tool_probability = 0.0`. Codex uses `0.39` and `0.19`; with the default uniform parallel count of two to four calls, the expected parallel-call share is about 59%. OpenCode retains its earlier `0.48` and `0.08` split because this study did not provide an OpenCode calibration.

These defaults are evidence-based starting points, not universal constants. The reported Codex result is sensitive to tool-class inclusion, especially shell execution, and model choice. Fit both action probabilities and tool-class weights from the target workload. Use `scenario.json.tool_parallelism` to check the sampled call-weighted and time-weighted results instead of inferring either from the TOML probabilities.

## Reasoning

Reasoning is not the same as `trajectory.think_time_ms`. For supported models, Codex requests encrypted reasoning content and a reasoning summary. It persists the opaque reasoning item for later requests while omitting raw reasoning text.

The current generator has one `trajectory.output_tokens` distribution and carries that entire synthetic assistant segment forward. It does not separately represent:

- Reasoning output tokens generated during the request.
- Visible assistant output tokens.
- The opaque reasoning item carried into later requests.

This is a known fidelity gap. A future profile should split visible and reasoning geometry while keeping agent- and provider-specific carry-forward behavior. Until then, calibrate `output_tokens` to the total decode load and account for the history-shape approximation when drawing KV-capacity conclusions.

## Compaction

In the studied Codex snapshot, automatic compaction:

- Uses a threshold no greater than 90% of the model context window.
- Checks before a new user turn.
- Checks after a successful model request when another follow-up is required.
- Defers compaction after a terminal response until later work arrives.
- Uses remote `/responses/compact` for the OpenAI provider and local model summarization for other providers.

Local compaction retains recent real user messages under a bounded budget and appends a summary. Remote compaction returns an opaque replacement history that Codex filters before reuse.

Agent-loadgen keeps the full compaction surface because it is useful for calibrating other agents, alternative policies, and failure tests:

- `trigger_fraction`, summary sizes, and retained tokens control normal synthetic KV-window replacement.
- Abort probability, retry probability, attempt limits, and abort timing exercise physical-attempt semantics.

For the studied Codex behavior, set `compaction.trigger_fraction = 0.90`. The built-in generator preset currently uses `0.78`, so the preset is not an exact Codex source default. Summary and replacement-history sizes still require captured traffic because source policy alone does not determine their realized token counts.

Compaction aborts and duplicate attempts are fault scenarios. Do not interpret nonzero values as ordinary Codex behavior without trace evidence.

## Subagents

Codex spawning is asynchronous. A spawn creates an independent child thread and returns without waiting for it. Waiting is a separate tool action. Multiple child spawns form a swarm; there is no distinct swarm runtime object.

Children inherit most parent execution settings, with optional role, model, reasoning-effort, and history-fork overrides. The studied snapshot supports fresh context, full-history forks, and recent-turn forks. It defaults to six open child threads and nesting depth one.

The current generator approximates these behaviors with:

- `behavior.subagent_probability`: one child.
- `behavior.swarm_probability` and `subagents.fanout`: several children.
- `subagents.blocking_probability`: parent waits for all direct children versus continuing independently.
- `subagents.spawn_delay_ms`: spawn and synthetic join timing.
- `subagents.turns`: child trajectory length.
- `subagents.max_depth`: recursive depth.

For the studied Codex default, set `subagents.max_depth = 1`. The generator preset currently uses depth two to exercise recursive traffic.

The approximation deliberately exposes useful stress controls, but it does not yet model exact Codex mailbox delivery, explicit wait calls, child close/reuse, live-open-thread exhaustion, or fresh/full/recent fork selection. See [Subagents and swarms](subagents.md) for the implemented graph contract.

## Retry behavior

Codex provider transport owns model-request retry budgets and backoff. Tool failure does not automatically imply a fixed retry; the model can observe the failure and decide what to do next.

Agent-loadgen's tool and compaction retry probabilities are therefore outcome and fault controls. Use trace-derived values for representative traffic. Use elevated values intentionally for retry storms and idempotency testing.

## Profile guidance

Use the controls at different confidence levels:

| Goal | Configuration approach |
|---|---|
| Reproduce one captured run | Use `replay`; do not infer distributions. |
| Simulate observed Codex traffic | Fit token, action, timing, reasoning, compaction, and subagent distributions from several native traces. |
| Approximate Codex from source only | Use the Codex preset, set compaction to 90% and subagent depth to one, and label the remaining distributions as assumptions. |
| Stress a serving feature | Override the relevant probabilities, fanout, sizes, delays, or fault attempts directly. |
| Compare routing or cache policy | Keep the profile and seed identical across treatments and verify the token path. |

Every run records the fully resolved configuration and its digest in `scenario.json`. Preserve that artifact with benchmark results; the name of a profile alone is not enough to reproduce a run after presets evolve.

## Source index

The study used these Codex paths at snapshot `58ad79b60eeb386cda50663cdc0aaff009dd1493`:

| Behavior | Codex source |
|---|---|
| Continuation loop and compaction boundaries | `codex-rs/core/src/codex.rs:5884`, `codex-rs/core/src/codex.rs:6110`, `codex-rs/core/src/codex.rs:6190` |
| Responses request construction | `codex-rs/core/src/client.rs:787` |
| Model context and 90% compact limit | `codex-rs/protocol/src/openai_models.rs:279` |
| Local compaction | `codex-rs/core/src/compact.rs:50` |
| Remote replacement-history filtering | `codex-rs/core/src/compact_remote.rs:169` |
| History and reasoning retention | `codex-rs/core/src/context_manager/history.rs:95`, `codex-rs/protocol/src/models.rs:780` |
| Agent limits | `codex-rs/core/src/config/mod.rs:125` |
| Spawn and fork options | `codex-rs/core/src/tools/handlers/multi_agents_v2/spawn.rs:45` |
| Explicit waiting | `codex-rs/core/src/tools/handlers/multi_agents_v2/wait.rs:20` |

Source behavior can change. Treat this document as a pinned study, then refresh it when calibrating against a materially different Codex version.
