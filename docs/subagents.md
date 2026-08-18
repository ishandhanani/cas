# Subagents and swarms

The generator models subagents as separate sessions in one preplanned dependency graph. This preserves lineage, concurrency, and KV-prefix relationships without running a real coding agent.

## Supported patterns

The current planner supports:

- One child spawned from a parent model turn.
- A swarm of sibling children spawned from one parent turn.
- Recursive children up to `subagents.max_depth`.
- Blocking joins that wait for all direct children.
- Non-blocking delegation that lets parent and child sessions overlap.

Each child has an independent trajectory. It can produce text turns, call tools, compact its context, and spawn more children.

## Planning

The seeded planner selects a `subagent` or `swarm` action for a parent node. The complete graph exists before execution. Model output does not change the number of children or their behavior.

For one child:

```text
parent model response
        |
        +-- sampled spawn delay --> child first request
```

For a swarm, each child receives a separate sampled spawn delay:

```text
parent model response
        |
        +-- delay A --> child A
        +-- delay B --> child B
        +-- delay C --> child C
```

`subagents.fanout` controls the requested swarm size. The planner reduces the fanout when `limits.max_sessions` has less capacity.

## Blocking and non-blocking delegation

A blocking parent waits for the last model response from every direct child. The parent successor also receives a sampled join delay.

```text
child A last response --+
child B last response --+--> join delay --> parent next request
child C last response --+
```

A non-blocking parent does not depend on child completion. Its next request depends on the spawn turn and receives a sampled delay. The children continue in their own sessions.

```text
parent spawn response --+--> sampled delay --> parent next request
                       |
                       +--> child sessions continue independently
```

Non-blocking children can outlive the parent session. They can also overlap with the next root task in the same root-agent slot.

## Session lineage

Each child receives a unique `session_id` and the parent `session_id` as `parent_session_id`. The selected agent adapter maps this lineage to Claude Code, Codex, or OpenCode headers.

`scenario.json` records every generated session with its depth and root-agent slot. The spawning parent node records direct children in `spawned_session_ids`.

## KV-prefix shape

All sessions share the global system and tool-catalog blocks. Sessions in one root tree also use the same root-scoped repository block labels. Their overlapping repository prefix is equal. Each session then receives unique environment, user, assistant, and tool-result blocks.

```text
global system | global tools | root repository | session environment | session history
<------ shared globally -----> <--- root tree --> <------ unique ------>
```

The repository token count is sampled when each session is planned. Therefore, root and child repository segments can have different lengths. Equal overlapping blocks still use the same labels.

## Concurrency

`load.concurrent_agents` limits root-agent slots. It does not limit child sessions. A root slot can have one active root task while several descendants also run.

The runtime limit `--max-in-flight` caps live HTTP requests across roots and children. Blocking and non-blocking graphs can both reach this cap.

## Profile controls

- `behavior.subagent_probability`: probability of one child action.
- `behavior.swarm_probability`: probability of one swarm action.
- `subagents.max_depth`: maximum recursive depth.
- `subagents.turns`: model turns for each child.
- `subagents.fanout`: child count for a swarm.
- `subagents.spawn_delay_ms`: child spawn delay and parent continuation or join delay.
- `subagents.blocking_probability`: probability that a parent waits for all direct children.
- `limits.max_sessions`: maximum total root and child sessions.

See [Generator configuration](generator.md) for profile syntax and distributions.

## Current limits

The generator does not model:

- Specialized child roles or instructions.
- Different profiles for different children.
- Child-to-child messages.
- Voting, consensus, or result ranking.
- Work stealing or a persistent worker pool.
- Cancellation of non-blocking children.
- Runtime spawning based on model output.

These features require new graph semantics. They are not hidden behind profile fields.
