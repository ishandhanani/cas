// SPDX-License-Identifier: Apache-2.0

use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result, bail};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use serde::Serialize;
use sha2::{Digest, Sha256};

use super::config::{GENERATOR_SCHEMA_VERSION, ResolvedGeneratorConfig, ToolClass};
use crate::trace::{AgentContext, TraceManifest, TraceRequest};

#[derive(Debug, Clone, Serialize)]
pub struct GeneratedScenario {
    pub schema_version: u32,
    pub profile_digest_sha256: String,
    pub scenario_digest_sha256: String,
    pub config: ResolvedGeneratorConfig,
    pub sessions: Vec<GeneratedSession>,
    pub nodes: Vec<GeneratedNode>,
    pub trace_manifest: TraceManifest,
}

#[derive(Debug, Clone, Serialize)]
pub struct GeneratedSession {
    pub session_id: String,
    pub parent_session_id: Option<String>,
    pub depth: usize,
    pub root_agent_slot: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct GeneratedNode {
    pub node_id: String,
    pub action: String,
    pub dependencies: Vec<usize>,
    pub delay_after_dependencies_ms: u64,
    pub root_arrival_ms: Option<u64>,
    pub window_epoch: usize,
    pub tool_events: Vec<GeneratedToolEvent>,
    pub spawned_session_ids: Vec<String>,
    pub request: TraceRequest,
}

#[derive(Debug, Clone, Serialize)]
pub struct GeneratedToolEvent {
    pub class: String,
    pub latency_ms: u64,
    pub result_tokens: u64,
    pub failed: bool,
    pub retried: bool,
}

impl GeneratedScenario {
    pub fn generate(config: ResolvedGeneratorConfig) -> Result<Self> {
        Planner::new(config)?.generate()
    }
}

struct Planner {
    config: ResolvedGeneratorConfig,
    rng: StdRng,
    shared_prefix: Vec<u64>,
    sessions: Vec<GeneratedSession>,
    nodes: Vec<GeneratedNode>,
    labels: BTreeMap<String, u64>,
    label_owners: BTreeMap<u64, String>,
    total_input_tokens: u64,
}

struct SessionState {
    session_id: String,
    parent_session_id: Option<String>,
    blocks: Vec<u64>,
    stable_blocks: usize,
    window_epoch: usize,
}

struct SessionLaunch {
    parent_session_id: Option<String>,
    depth: usize,
    first_dependencies: Vec<usize>,
    first_delay_ms: u64,
    root_arrival_ms: Option<u64>,
    lineage_root_sequence: usize,
    root_agent_slot: usize,
}

struct NodeSpec<'a> {
    dependencies: Vec<usize>,
    delay_after_dependencies_ms: u64,
    root_arrival_ms: Option<u64>,
    action: &'a str,
    input_trigger: &'a str,
    output_tokens: u64,
    compaction: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Copy)]
enum NextAction {
    Text,
    Tool,
    ParallelTools,
    Subagent,
    Swarm,
    Complete,
}

impl NextAction {
    fn name(self) -> &'static str {
        match self {
            Self::Text => "text",
            Self::Tool => "tool",
            Self::ParallelTools => "parallel_tools",
            Self::Subagent => "subagent",
            Self::Swarm => "swarm",
            Self::Complete => "complete",
        }
    }
}

impl Planner {
    fn new(config: ResolvedGeneratorConfig) -> Result<Self> {
        config.validate()?;
        let rng = StdRng::seed_from_u64(config.seed);
        let mut planner = Self {
            config,
            rng,
            shared_prefix: Vec::new(),
            sessions: Vec::new(),
            nodes: Vec::new(),
            labels: BTreeMap::new(),
            label_owners: BTreeMap::new(),
            total_input_tokens: 0,
        };
        let system_tokens = planner
            .config
            .system_prefix_tokens
            .sample(&mut planner.rng)?;
        let tool_tokens = planner
            .config
            .tool_catalog_tokens
            .sample(&mut planner.rng)?;
        planner.shared_prefix = planner.segment("shared:system", system_tokens)?;
        let tool_prefix = planner.segment("shared:tools", tool_tokens)?;
        planner.shared_prefix.extend(tool_prefix);
        Ok(planner)
    }

    fn generate(mut self) -> Result<GeneratedScenario> {
        let mut slot_finals = vec![None; self.config.concurrent_agents];
        for root in 0..self.config.root_sessions {
            let slot = root % self.config.concurrent_agents;
            let (dependencies, delay_ms, root_arrival_ms) = match slot_finals[slot] {
                Some(previous_final) => (
                    vec![previous_final],
                    self.config.restart_delay_ms.sample(&mut self.rng)?,
                    None,
                ),
                None => {
                    let slot = u64::try_from(slot).context("agent slot does not fit u64")?;
                    let arrival_ms = slot
                        .checked_mul(self.config.startup_interval_ms)
                        .context("agent startup timestamp overflow")?;
                    (Vec::new(), 0, Some(arrival_ms))
                }
            };
            let final_node = self
                .generate_session(SessionLaunch {
                    parent_session_id: None,
                    depth: 0,
                    first_dependencies: dependencies,
                    first_delay_ms: delay_ms,
                    root_arrival_ms,
                    lineage_root_sequence: root,
                    root_agent_slot: slot,
                })?
                .with_context(|| format!("generation limits prevented root session {root}"))?;
            slot_finals[slot] = Some(final_node);
        }
        let profile_bytes = serde_json::to_vec(&self.config)?;
        let profile_digest_sha256 = hex::encode(Sha256::digest(&profile_bytes));
        let trace_manifest = self.trace_manifest(profile_digest_sha256.clone())?;
        let scenario_bytes = serde_json::to_vec(&(
            &profile_digest_sha256,
            &self.sessions,
            &self.nodes,
            &trace_manifest,
        ))?;
        let scenario_digest_sha256 = hex::encode(Sha256::digest(scenario_bytes));
        Ok(GeneratedScenario {
            schema_version: GENERATOR_SCHEMA_VERSION,
            profile_digest_sha256,
            scenario_digest_sha256,
            config: self.config,
            sessions: self.sessions,
            nodes: self.nodes,
            trace_manifest,
        })
    }

    fn generate_session(&mut self, launch: SessionLaunch) -> Result<Option<usize>> {
        let SessionLaunch {
            parent_session_id,
            depth,
            first_dependencies,
            first_delay_ms,
            root_arrival_ms,
            lineage_root_sequence,
            root_agent_slot,
        } = launch;
        if self.sessions.len() >= self.config.max_sessions
            || self.nodes.len() >= self.config.max_nodes
        {
            return Ok(None);
        }
        let session_id = format!(
            "generated-{:016x}-session-{}",
            self.config.seed,
            self.sessions.len()
        );
        self.sessions.push(GeneratedSession {
            session_id: session_id.clone(),
            parent_session_id: parent_session_id.clone(),
            depth,
            root_agent_slot,
        });
        let repository_tokens = self.config.repository_tokens.sample(&mut self.rng)?;
        let session_tokens = self.config.session_tokens.sample(&mut self.rng)?;
        let mut blocks = self.shared_prefix.clone();
        blocks.extend(self.segment(
            &format!("root:{lineage_root_sequence}:repository"),
            repository_tokens,
        )?);
        blocks.extend(self.segment(&format!("session:{session_id}:environment"), session_tokens)?);
        let stable_blocks = blocks.len();
        let mut state = SessionState {
            session_id,
            parent_session_id,
            blocks,
            stable_blocks,
            window_epoch: 0,
        };
        let user_tokens = self.config.user_tokens.sample(&mut self.rng)?;
        state
            .blocks
            .extend(self.segment(&format!("session:{}:user:0", state.session_id), user_tokens)?);

        let turns = if depth == 0 {
            self.config.turns.sample(&mut self.rng)?
        } else {
            self.config.subagent_turns.sample(&mut self.rng)?
        };
        let turns = usize::try_from(turns).context("turn count does not fit usize")?;
        let minimum_turns = turns.min(2);
        let mut dependencies = first_dependencies;
        let mut delay_ms = first_delay_ms;
        let mut next_trigger = "user_message";
        let mut final_node = None;

        for turn in 0..turns {
            if self.nodes.len() >= self.config.max_nodes {
                break;
            }
            if self.compaction_due(&state) {
                let original_blocks = state.blocks.len();
                let summary_prompt_tokens =
                    self.config.summary_input_tokens.sample(&mut self.rng)?;
                let summary_prompt = self.segment(
                    &format!("session:{}:compaction-prompt:{turn}", state.session_id),
                    summary_prompt_tokens,
                )?;
                state.blocks.extend(summary_prompt);
                let compaction_output = self.config.summary_output_tokens.sample(&mut self.rng)?;
                let compaction = serde_json::json!({
                    "phase": "pre_turn",
                    "window_epoch": state.window_epoch,
                    "pre_tokens": state.blocks.len() * self.config.block_size,
                });
                let node = self.push_request(
                    &state,
                    NodeSpec {
                        dependencies,
                        delay_after_dependencies_ms: delay_ms,
                        root_arrival_ms: root_arrival_ms.filter(|_| final_node.is_none()),
                        action: "compaction",
                        input_trigger: "other",
                        output_tokens: compaction_output,
                        compaction: Some(compaction),
                    },
                )?;
                final_node = Some(node);
                dependencies = vec![node];
                delay_ms = 0;
                state.blocks.truncate(original_blocks);
                self.compact_state(&mut state, turn, compaction_output)?;
                next_trigger = "other";
            }

            let output_tokens = self.config.output_tokens.sample(&mut self.rng)?;
            let action = if turn + 1 == turns {
                NextAction::Complete
            } else {
                self.sample_action(depth, turn + 1 >= minimum_turns)
            };
            let node = self.push_request(
                &state,
                NodeSpec {
                    dependencies,
                    delay_after_dependencies_ms: delay_ms,
                    root_arrival_ms: root_arrival_ms.filter(|_| final_node.is_none()),
                    action: action.name(),
                    input_trigger: next_trigger,
                    output_tokens,
                    compaction: None,
                },
            )?;
            final_node = Some(node);
            let assistant_tokens = output_tokens;
            state.blocks.extend(self.segment(
                &format!("session:{}:assistant:{turn}", state.session_id),
                assistant_tokens,
            )?);

            if !matches!(action, NextAction::Complete)
                && self.rng.random::<f64>() < self.config.background_request_probability
                && self.nodes.len() < self.config.max_nodes
            {
                let background_output = self.config.output_tokens.sample(&mut self.rng)?.min(256);
                self.push_request(
                    &state,
                    NodeSpec {
                        dependencies: vec![node],
                        delay_after_dependencies_ms: 0,
                        root_arrival_ms: None,
                        action: "background",
                        input_trigger: "other",
                        output_tokens: background_output,
                        compaction: None,
                    },
                )?;
            }

            match action {
                NextAction::Complete => break,
                NextAction::Text => {
                    let user_tokens = self.config.user_tokens.sample(&mut self.rng)?;
                    state.blocks.extend(self.segment(
                        &format!("session:{}:user:{}", state.session_id, turn + 1),
                        user_tokens,
                    )?);
                    dependencies = vec![node];
                    delay_ms = self.config.think_time_ms.sample(&mut self.rng)?;
                    next_trigger = "user_message";
                }
                NextAction::Tool | NextAction::ParallelTools => {
                    let count = if matches!(action, NextAction::ParallelTools) {
                        usize::try_from(self.config.parallel_count.sample(&mut self.rng)?)
                            .context("parallel tool count does not fit usize")?
                            .max(2)
                    } else {
                        1
                    };
                    let mut max_latency = 0;
                    let mut tool_events = Vec::with_capacity(count);
                    for tool_index in 0..count {
                        let class = self.sample_tool()?.clone();
                        let mut latency = class.latency_ms.sample(&mut self.rng)?;
                        let mut result_tokens = class.result_tokens.sample(&mut self.rng)?;
                        let failed = self.rng.random::<f64>() < class.error_probability;
                        let retried =
                            failed && self.rng.random::<f64>() < self.config.retry_probability;
                        if retried {
                            latency =
                                latency.saturating_add(class.latency_ms.sample(&mut self.rng)?);
                            result_tokens = result_tokens
                                .saturating_add(class.result_tokens.sample(&mut self.rng)?);
                        }
                        max_latency = max_latency.max(latency);
                        state.blocks.extend(self.segment(
                            &format!(
                                "session:{}:tool:{turn}:{tool_index}:{}:{failed}",
                                state.session_id, class.name
                            ),
                            result_tokens,
                        )?);
                        tool_events.push(GeneratedToolEvent {
                            class: class.name,
                            latency_ms: latency,
                            result_tokens,
                            failed,
                            retried,
                        });
                    }
                    self.nodes[node].tool_events = tool_events;
                    dependencies = vec![node];
                    delay_ms = max_latency;
                    next_trigger = "tool_result";
                }
                NextAction::Subagent | NextAction::Swarm => {
                    let requested = if matches!(action, NextAction::Swarm) {
                        usize::try_from(self.config.fanout.sample(&mut self.rng)?)
                            .context("subagent fanout does not fit usize")?
                            .max(2)
                    } else {
                        1
                    };
                    let available = self.config.max_sessions.saturating_sub(self.sessions.len());
                    let child_count = requested.min(available);
                    let blocking = self.rng.random::<f64>() < self.config.blocking_probability;
                    let mut child_finals = Vec::new();
                    let mut spawned_session_ids = Vec::new();
                    for _ in 0..child_count {
                        let spawn_delay = self.config.spawn_delay_ms.sample(&mut self.rng)?;
                        let child_session_index = self.sessions.len();
                        if let Some(child_final) = self.generate_session(SessionLaunch {
                            parent_session_id: Some(state.session_id.clone()),
                            depth: depth + 1,
                            first_dependencies: vec![node],
                            first_delay_ms: spawn_delay,
                            root_arrival_ms: None,
                            lineage_root_sequence,
                            root_agent_slot,
                        })? {
                            child_finals.push(child_final);
                            spawned_session_ids
                                .push(self.sessions[child_session_index].session_id.clone());
                        }
                    }
                    self.nodes[node].spawned_session_ids = spawned_session_ids;
                    let join_tokens = self.config.tool_result_tokens.sample(&mut self.rng)?;
                    state.blocks.extend(self.segment(
                        &format!("session:{}:join:{turn}", state.session_id),
                        join_tokens,
                    )?);
                    dependencies = if blocking && !child_finals.is_empty() {
                        child_finals
                    } else {
                        vec![node]
                    };
                    delay_ms = self.config.spawn_delay_ms.sample(&mut self.rng)?;
                    next_trigger = "tool_result";
                }
            }
        }
        Ok(final_node)
    }

    fn sample_action(&mut self, depth: usize, allow_completion: bool) -> NextAction {
        let value = self.rng.random::<f64>();
        let mut boundary = self.config.tool_probability;
        if value < boundary {
            return NextAction::Tool;
        }
        boundary += self.config.parallel_tool_probability;
        if value < boundary {
            return NextAction::ParallelTools;
        }
        if depth < self.config.max_depth {
            boundary += self.config.subagent_probability;
            if value < boundary {
                return NextAction::Subagent;
            }
            boundary += self.config.swarm_probability;
            if value < boundary {
                return NextAction::Swarm;
            }
        }
        boundary += if allow_completion {
            self.config.completion_probability
        } else {
            0.0
        };
        if allow_completion && value < boundary {
            NextAction::Complete
        } else {
            NextAction::Text
        }
    }

    fn sample_tool(&mut self) -> Result<&ToolClass> {
        let total = self
            .config
            .tool_classes
            .iter()
            .map(|class| class.weight)
            .sum::<f64>();
        let mut value = self.rng.random::<f64>() * total;
        for class in &self.config.tool_classes {
            if value < class.weight {
                return Ok(class);
            }
            value -= class.weight;
        }
        self.config
            .tool_classes
            .last()
            .context("tool class list is empty")
    }

    fn compaction_due(&self, state: &SessionState) -> bool {
        self.config.compaction_enabled
            && state.blocks.len() * self.config.block_size
                >= (self.config.context_window_tokens as f64
                    * self.config.compaction_trigger_fraction) as usize
    }

    fn compact_state(
        &mut self,
        state: &mut SessionState,
        turn: usize,
        summary_tokens: u64,
    ) -> Result<()> {
        let retain_blocks = self
            .config
            .retained_recent_tokens
            .div_ceil(self.config.block_size)
            .min(state.blocks.len().saturating_sub(state.stable_blocks));
        let recent = state.blocks[state.blocks.len() - retain_blocks..].to_vec();
        state.blocks.truncate(state.stable_blocks);
        state.blocks.extend(self.segment(
            &format!("session:{}:summary:{turn}", state.session_id),
            summary_tokens,
        )?);
        state.blocks.extend(recent);
        state.window_epoch += 1;
        Ok(())
    }

    fn push_request(&mut self, state: &SessionState, spec: NodeSpec<'_>) -> Result<usize> {
        if self.nodes.len() >= self.config.max_nodes {
            bail!("generated scenario reached max_nodes");
        }
        if spec.dependencies.is_empty() != spec.root_arrival_ms.is_some() {
            bail!("each generated request must have dependencies or one root arrival");
        }
        if spec
            .dependencies
            .iter()
            .any(|dependency| *dependency >= self.nodes.len())
        {
            bail!("generated request has a forward or missing dependency");
        }
        let input_tokens = state
            .blocks
            .len()
            .checked_mul(self.config.block_size)
            .context("generated input length overflow")?;
        self.total_input_tokens = self
            .total_input_tokens
            .checked_add(input_tokens as u64)
            .context("generated total input length overflow")?;
        if self.total_input_tokens > self.config.max_total_input_tokens {
            bail!(
                "generated scenario exceeds max_total_input_tokens ({})",
                self.config.max_total_input_tokens
            );
        }
        let output_tokens = u32::try_from(spec.output_tokens)
            .context("generated output token count does not fit u32")?;
        if output_tokens == 0 {
            bail!("generated output token count must be greater than zero");
        }
        let ordinal = self.nodes.len();
        let node_id = format!("node-{ordinal}");
        let request = TraceRequest {
            ordinal,
            source_request_id: format!("generated-{:016x}-{ordinal}", self.config.seed),
            source_x_request_id: None,
            source_model: None,
            input_tokens,
            output_tokens,
            request_received_ms: spec.root_arrival_ms.unwrap_or(0),
            trace_block_size: self.config.block_size,
            input_sequence_hashes: state.blocks.clone(),
            agent_context: Some(AgentContext {
                session_id: state.session_id.clone(),
                parent_session_id: state.parent_session_id.clone(),
                session_final: (spec.action == "complete").then_some(true),
                compaction: spec.compaction,
                input_trigger: Some(spec.input_trigger.to_string()),
            }),
        };
        self.nodes.push(GeneratedNode {
            node_id,
            action: spec.action.to_string(),
            dependencies: spec.dependencies,
            delay_after_dependencies_ms: spec.delay_after_dependencies_ms,
            root_arrival_ms: spec.root_arrival_ms,
            window_epoch: state.window_epoch,
            tool_events: Vec::new(),
            spawned_session_ids: Vec::new(),
            request,
        });
        Ok(ordinal)
    }

    fn segment(&mut self, scope: &str, tokens: u64) -> Result<Vec<u64>> {
        let tokens = usize::try_from(tokens).context("segment token count does not fit usize")?;
        let blocks = tokens.div_ceil(self.config.block_size);
        (0..blocks)
            .map(|block| self.label(&format!("{scope}:block:{block}")))
            .collect()
    }

    fn label(&mut self, owner: &str) -> Result<u64> {
        if let Some(label) = self.labels.get(owner) {
            return Ok(*label);
        }
        for nonce in 0_u64.. {
            let mut digest = Sha256::new();
            digest.update(b"agent-loadgen/generated-segment/v1");
            digest.update(self.config.seed.to_le_bytes());
            digest.update(owner.as_bytes());
            digest.update(nonce.to_le_bytes());
            let bytes: [u8; 8] = digest.finalize()[..8]
                .try_into()
                .expect("the digest prefix is eight bytes");
            let label = u64::from_le_bytes(bytes);
            match self.label_owners.get(&label) {
                None => {
                    self.labels.insert(owner.to_string(), label);
                    self.label_owners.insert(label, owner.to_string());
                    return Ok(label);
                }
                Some(existing) if existing == owner => return Ok(label),
                Some(_) => continue,
            }
        }
        unreachable!("u64 nonce space is not exhausted")
    }

    fn trace_manifest(&self, digest: String) -> Result<TraceManifest> {
        if self.nodes.is_empty() {
            bail!("generated scenario contains no requests");
        }
        let hashes = self
            .nodes
            .iter()
            .flat_map(|node| node.request.input_sequence_hashes.iter().copied())
            .collect::<BTreeSet<_>>();
        let input_tokens = self.nodes.iter().try_fold(0_u64, |total, node| {
            total
                .checked_add(node.request.input_tokens as u64)
                .context("generated input token total overflow")
        })?;
        let output_tokens = self.nodes.iter().try_fold(0_u64, |total, node| {
            total
                .checked_add(node.request.output_tokens as u64)
                .context("generated output token total overflow")
        })?;
        let last_root_arrival = self
            .nodes
            .iter()
            .filter_map(|node| node.root_arrival_ms)
            .max()
            .unwrap_or(0);
        Ok(TraceManifest {
            request_count: self.nodes.len(),
            zero_output_requests: 0,
            session_count: self.sessions.len(),
            requests_with_agent_context: self.nodes.len(),
            first_request_received_ms: 0,
            last_request_received_ms: last_root_arrival,
            duration_ms: last_root_arrival,
            input_tokens,
            output_tokens,
            distinct_sequence_hashes: hashes.len(),
            trace_block_size: self.config.block_size,
            source_digest_sha256: digest,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent::AgentKind;
    use crate::scenario::config::UIntDistribution;

    fn config(agent: AgentKind) -> ResolvedGeneratorConfig {
        let mut config = ResolvedGeneratorConfig::preset(agent, 42);
        config.root_sessions = 3;
        config.concurrent_agents = 3;
        config.turns = UIntDistribution::fixed(4);
        config.subagent_turns = UIntDistribution::fixed(2);
        config.system_prefix_tokens = UIntDistribution::fixed(64);
        config.tool_catalog_tokens = UIntDistribution::fixed(64);
        config.repository_tokens = UIntDistribution::fixed(32);
        config.session_tokens = UIntDistribution::fixed(16);
        config.user_tokens = UIntDistribution::fixed(16);
        config.output_tokens = UIntDistribution::fixed(8);
        config.context_window_tokens = 512;
        config.compaction_trigger_fraction = 0.75;
        config.max_nodes = 500;
        config.max_sessions = 100;
        config.max_total_input_tokens = 1_000_000;
        config
    }

    #[test]
    fn generation_is_seed_deterministic() {
        let first = GeneratedScenario::generate(config(AgentKind::Codex)).unwrap();
        let second = GeneratedScenario::generate(config(AgentKind::Codex)).unwrap();
        assert_eq!(first.scenario_digest_sha256, second.scenario_digest_sha256);
        assert_eq!(first.nodes.len(), second.nodes.len());
    }

    #[test]
    fn graph_dependencies_only_point_backward() {
        let scenario = GeneratedScenario::generate(config(AgentKind::ClaudeCode)).unwrap();
        assert!(scenario.nodes.iter().enumerate().all(|(ordinal, node)| {
            node.dependencies
                .iter()
                .all(|dependency| *dependency < ordinal)
        }));
        assert!(scenario.nodes.iter().any(|node| node.action == "tool"));
        assert!(
            scenario
                .nodes
                .iter()
                .filter(|node| node.action == "tool")
                .all(|node| !node.tool_events.is_empty())
        );
    }

    #[test]
    fn profile_preserves_global_prefix_sharing() {
        let scenario = GeneratedScenario::generate(config(AgentKind::Opencode)).unwrap();
        let roots = scenario
            .nodes
            .iter()
            .filter(|node| node.root_arrival_ms.is_some())
            .collect::<Vec<_>>();
        assert_eq!(roots.len(), 3);
        assert_eq!(
            &roots[0].request.input_sequence_hashes[..8],
            &roots[1].request.input_sequence_hashes[..8]
        );
    }

    #[test]
    fn compaction_rewrites_a_window_and_emits_metadata() {
        let mut config = config(AgentKind::Codex);
        config.root_sessions = 1;
        config.concurrent_agents = 1;
        config.context_window_tokens = 160;
        config.compaction_trigger_fraction = 0.75;
        let scenario = GeneratedScenario::generate(config).unwrap();
        let compactions = scenario
            .nodes
            .iter()
            .filter(|node| node.action == "compaction")
            .collect::<Vec<_>>();
        assert!(!compactions.is_empty());
        assert!(compactions.iter().all(|node| {
            node.request
                .agent_context
                .as_ref()
                .and_then(|context| context.compaction.as_ref())
                .is_some()
        }));
        assert!(scenario.nodes.iter().any(|node| node.window_epoch > 0));
    }

    #[test]
    fn blocking_swarm_joins_all_children() {
        let mut config = config(AgentKind::ClaudeCode);
        config.root_sessions = 1;
        config.concurrent_agents = 1;
        config.turns = UIntDistribution::fixed(2);
        config.subagent_turns = UIntDistribution::fixed(1);
        config.tool_probability = 0.0;
        config.parallel_tool_probability = 0.0;
        config.subagent_probability = 0.0;
        config.swarm_probability = 1.0;
        config.completion_probability = 0.0;
        config.background_request_probability = 0.0;
        config.fanout = UIntDistribution::fixed(3);
        config.blocking_probability = 1.0;
        config.compaction_enabled = false;
        let scenario = GeneratedScenario::generate(config).unwrap();
        assert_eq!(scenario.sessions.len(), 4);
        let root_completion = scenario
            .nodes
            .iter()
            .find(|node| {
                node.action == "complete"
                    && node
                        .request
                        .agent_context
                        .as_ref()
                        .unwrap()
                        .parent_session_id
                        .is_none()
            })
            .unwrap();
        assert_eq!(root_completion.dependencies.len(), 3);
        let swarm = scenario
            .nodes
            .iter()
            .find(|node| node.action == "swarm")
            .unwrap();
        assert_eq!(swarm.spawned_session_ids.len(), 3);
        assert!(scenario.sessions.iter().skip(1).all(|session| {
            session.parent_session_id.as_deref() == Some(scenario.sessions[0].session_id.as_str())
        }));
    }

    #[test]
    fn closed_loop_slot_restarts_after_its_previous_root_completes() {
        let mut config = config(AgentKind::Codex);
        config.concurrent_agents = 1;
        config.turns = UIntDistribution::fixed(1);
        config.tool_probability = 0.0;
        config.parallel_tool_probability = 0.0;
        config.subagent_probability = 0.0;
        config.swarm_probability = 0.0;
        config.completion_probability = 0.0;
        config.background_request_probability = 0.0;
        config.compaction_enabled = false;
        config.restart_delay_ms = UIntDistribution::fixed(17);

        let scenario = GeneratedScenario::generate(config).unwrap();
        assert_eq!(scenario.nodes.len(), 3);
        assert_eq!(scenario.nodes[0].root_arrival_ms, Some(0));
        assert!(scenario.nodes[0].dependencies.is_empty());
        assert_eq!(scenario.nodes[1].root_arrival_ms, None);
        assert_eq!(scenario.nodes[1].dependencies, vec![0]);
        assert_eq!(scenario.nodes[1].delay_after_dependencies_ms, 17);
        assert_eq!(scenario.nodes[2].dependencies, vec![1]);
        assert!(scenario.nodes.iter().all(|node| {
            node.request
                .agent_context
                .as_ref()
                .and_then(|context| context.session_final)
                == Some(true)
        }));
        assert!(
            scenario
                .sessions
                .iter()
                .all(|session| session.root_agent_slot == 0)
        );
    }
}
