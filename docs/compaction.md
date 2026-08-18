# Compaction

The generator models compaction as a logical operation that can contain multiple physical requests. The operation changes later KV shape once, even when the client aborts or retries an attempt.

## Trigger

The planner checks context size before each model turn. It starts compaction when the current context reaches this threshold:

```text
context_window_tokens * compaction.trigger_fraction
```

The size check uses block-quantized synthetic tokens. Compaction does not use model output or a live engine signal to select its trigger.

## Logical operation and physical attempts

One operation has a stable `operation_id` and `phase`. Physical attempts reuse that identity and increment `attempt`.

```text
logical operation
  attempt 1  no_mutation_aborted   optional
  attempt 2  apply_once            required
  attempt 3  duplicate_noop        optional
```

The default abort and retry probabilities are zero. In that configuration, one logical operation produces one physical summary request.

`compaction.max_attempts` limits physical attempts. The planner also respects the remaining `limits.max_nodes` capacity.

## Attempt semantics

`no_mutation_aborted` represents a client request that stops waiting after the sampled `abort_after_ms`. The next attempt keeps the same operation ID.

`apply_once` is the successful attempt. It supplies the logical summary size used by later synthetic requests.

`duplicate_noop` represents a duplicate physical request after success. It must not change the planned context a second time.

All compaction requests omit a forced output budget. `compaction.summary_output_tokens` controls retained synthetic summary size. It does not force the target response length.

## KV-window change

The planner adds a synthetic compaction instruction to the request input. After the successful attempt, it builds the next context from:

```text
stable prefix | summary blocks | retained recent blocks
```

The stable prefix contains the global system blocks, tool-catalog blocks, root repository blocks, and session-environment blocks. `compaction.retained_recent_tokens` controls the recent non-stable suffix.

The operation changes the planned context once. Aborted and duplicate attempts do not apply another window change.

## Dependencies and timing

Physical attempts form a serial chain. Each attempt becomes ready after the prior attempt completes. A normal model turn depends on the last physical attempt.

An aborted attempt releases its successor after the client cancels its wait. A duplicate attempt delays the next model turn because it remains part of the planned chain.

## Agent metadata

Codex compaction attempts send `x-codex-turn-metadata`. The value contains the request kind and logical compaction metadata. Other agent adapters keep the canonical session lineage headers but do not add this Codex header.

Current Dynamo request traces do not project the opaque Codex metadata into `AgentContext`. Captured-trace comparison can still verify the resulting KV topology.

## Artifacts

`scenario.json` records:

- `compaction_operations` with the stable operation ID, attempt nodes, applied attempt, and expected apply count.
- Per-node `compaction_attempt` metadata with phase, attempt number, expected effect, and abort delay.
- Per-node `window_epoch`, which increases after a successful logical compaction.

`requests.jsonl` records the same operation and attempt identity with the observed HTTP result.

## Proof boundary

A client abort proves only that the load generator canceled its wait. It does not prove that the router or engine avoided a cache mutation.

Use request-scoped engine telemetry to prove cache effects. Join that telemetry after the run with `join-telemetry`. Cache policy remains outside agent-loadgen.

## Profile controls

- `compaction.enabled`: enables compaction planning.
- `compaction.trigger_fraction`: context-window fraction that starts compaction.
- `compaction.summary_input_tokens`: size of the synthetic compaction instruction.
- `compaction.summary_output_tokens`: size of the retained synthetic summary.
- `compaction.retained_recent_tokens`: recent context retained after compaction.
- `compaction.abort_probability`: probability of one aborted first attempt.
- `compaction.retry_probability`: probability of one duplicate attempt after success.
- `compaction.max_attempts`: maximum physical attempts for one operation.
- `compaction.abort_after_ms`: delay before a planned client abort.

See [Generator configuration](generator.md) for profile syntax and distributions.
