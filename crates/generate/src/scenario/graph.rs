// SPDX-License-Identifier: Apache-2.0

//! Graphviz artifacts for generated scenario plans.

use std::collections::BTreeMap;
use std::fmt::Write;
use std::fs;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{Context, Result, bail};

use super::model::{GeneratedNode, GeneratedScenario, GeneratedSession};

/// Graph artifacts written beside a generated scenario.
#[derive(Debug, Clone)]
pub struct PlanGraphArtifacts {
    pub dot_path: PathBuf,
    pub svg_path: Option<PathBuf>,
}

/// Write a Graphviz DOT graph and, when Graphviz is installed, its SVG rendering.
pub fn write_plan_graph(
    output_dir: &Path,
    scenario: &GeneratedScenario,
) -> Result<PlanGraphArtifacts> {
    let dot_path = output_dir.join("plan.dot");
    fs::write(&dot_path, render_plan_dot(scenario)?)
        .with_context(|| format!("failed to write plan graph {}", dot_path.display()))?;

    let svg_path = output_dir.join("plan.svg");
    let svg_path = match Command::new("dot")
        .args(["-Tsvg", "-o"])
        .arg(&svg_path)
        .arg(&dot_path)
        .status()
    {
        Ok(status) if status.success() => Some(svg_path),
        Ok(status) => bail!("Graphviz dot failed with status {status}"),
        Err(error) if error.kind() == ErrorKind::NotFound => None,
        Err(error) => return Err(error).context("failed to execute Graphviz dot"),
    };
    Ok(PlanGraphArtifacts { dot_path, svg_path })
}

/// Render the plan as Graphviz DOT.
///
/// The graph is a causal overview, not a wall-clock forecast: it includes the
/// sampled client-side delay before each request, while target model-service
/// time remains a runtime-dependent closed-loop value.
pub fn render_plan_dot(scenario: &GeneratedScenario) -> Result<String> {
    let sessions = scenario
        .sessions
        .iter()
        .map(|session| (session.session_id.as_str(), session))
        .collect::<BTreeMap<_, _>>();
    let mut children = BTreeMap::<&str, Vec<&GeneratedSession>>::new();
    let mut roots = Vec::new();
    for session in &scenario.sessions {
        if let Some(parent) = session.parent_session_id.as_deref() {
            children.entry(parent).or_default().push(session);
        } else {
            roots.push(session);
        }
    }
    roots.sort_by_key(|session| session.top_level_session_index);
    for descendants in children.values_mut() {
        descendants.sort_by_key(|session| session.session_id.as_str());
    }

    let mut nodes_by_session = BTreeMap::<&str, Vec<(usize, &GeneratedNode)>>::new();
    for (ordinal, node) in scenario.nodes.iter().enumerate() {
        let session_id = node
            .request
            .agent_context
            .as_ref()
            .with_context(|| format!("generated node {ordinal} is missing agent context"))?
            .session_id
            .as_str();
        if !sessions.contains_key(session_id) {
            bail!("generated node {ordinal} references an unknown session");
        }
        nodes_by_session
            .entry(session_id)
            .or_default()
            .push((ordinal, node));
    }

    let mut dot = String::new();
    writeln!(dot, "digraph agent_loadgen_plan {{")?;
    writeln!(
        dot,
        "  graph [rankdir=LR, compound=true, newrank=true, pad=0.25, nodesep=0.35, ranksep=0.75, fontname=\"Helvetica\"];"
    )?;
    writeln!(
        dot,
        "  node [shape=box, style=\"rounded,filled\", fontname=\"Helvetica\", fontsize=10, margin=\"0.08,0.05\", color=\"#4b5563\"];"
    )?;
    writeln!(
        dot,
        "  edge [fontname=\"Helvetica\", fontsize=9, color=\"#6b7280\", arrowsize=0.7];"
    )?;
    writeln!(
        dot,
        "  label=\"agent-loadgen generated plan\\n{} top-level session trees · {} concurrent streams · {} child sessions · {} model requests\";",
        scenario.session_topology.configured_top_level_sessions,
        scenario.session_topology.configured_concurrent_sessions,
        scenario.session_topology.generated_subagent_sessions,
        scenario.nodes.len()
    )?;
    writeln!(dot, "  labelloc=t;")?;
    writeln!(dot, "  fontsize=18;")?;
    writeln!(dot)?;

    for root in roots {
        writeln!(
            dot,
            "  subgraph cluster_tree_{} {{",
            root.top_level_session_index
        )?;
        writeln!(
            dot,
            "    label=\"top-level session tree {} · stream {}\";",
            root.top_level_session_index, root.top_level_stream_slot
        )?;
        writeln!(
            dot,
            "    color=\"#94a3b8\"; style=\"rounded,dashed\"; penwidth=1.25;"
        )?;
        write_session_cluster(&mut dot, root, &children, &nodes_by_session, 2)?;
        writeln!(dot, "  }}")?;
    }

    for (ordinal, node) in scenario.nodes.iter().enumerate() {
        let dependencies = node
            .dependencies
            .iter()
            .map(|dependency| {
                let source = scenario.nodes.get(*dependency).with_context(|| {
                    format!("generated node {ordinal} has missing dependency {dependency}")
                })?;
                Ok((*dependency, edge_relation(source, node, &sessions)?))
            })
            .collect::<Result<Vec<_>>>()?;
        if dependencies.len() > 1
            && dependencies
                .iter()
                .all(|(_, relation)| *relation == "blocking join")
        {
            write_blocking_join(
                &mut dot,
                ordinal,
                &dependencies,
                node.delay_after_dependencies_ms,
            )?;
        } else {
            for (dependency, relation) in dependencies {
                let label = edge_label(relation, node.delay_after_dependencies_ms);
                writeln!(dot, "  n{dependency} -> n{ordinal} [{label}];")?;
            }
        }
    }
    writeln!(dot, "}}")?;
    Ok(dot)
}

fn write_blocking_join(
    dot: &mut String,
    target_ordinal: usize,
    dependencies: &[(usize, &'static str)],
    delay_ms: u64,
) -> Result<()> {
    let join_id = format!("join_{target_ordinal}");
    writeln!(
        dot,
        "  {join_id} [shape=diamond, style=\"filled\", label=\"join all {} children\", fillcolor=\"#ede9fe\", color=\"#7c3aed\", fontname=\"Helvetica\", fontsize=9, margin=\"0.04,0.02\"];",
        dependencies.len()
    )?;
    for (dependency, _) in dependencies {
        writeln!(
            dot,
            "  n{dependency} -> {join_id} [label=\"child complete\", color=\"#7c3aed\"];"
        )?;
    }
    let label = edge_label("join", delay_ms);
    writeln!(dot, "  {join_id} -> n{target_ordinal} [{label}];")?;
    Ok(())
}

fn write_session_cluster(
    dot: &mut String,
    session: &GeneratedSession,
    children: &BTreeMap<&str, Vec<&GeneratedSession>>,
    nodes_by_session: &BTreeMap<&str, Vec<(usize, &GeneratedNode)>>,
    indent: usize,
) -> Result<()> {
    let padding = " ".repeat(indent);
    writeln!(
        dot,
        "{padding}subgraph cluster_session_{} {{",
        session_cluster_id(session)
    )?;
    let kind = if session.parent_session_id.is_some() {
        "subagent session"
    } else {
        "parent session"
    };
    writeln!(
        dot,
        "{padding}  label=\"{kind} · depth {} · {}\";",
        session.depth,
        dot_escape(&session.session_id)
    )?;
    writeln!(
        dot,
        "{padding}  color=\"#cbd5e1\"; style=\"rounded\"; penwidth=1;"
    )?;
    if let Some(nodes) = nodes_by_session.get(session.session_id.as_str()) {
        for (ordinal, node) in nodes {
            writeln!(
                dot,
                "{padding}  n{ordinal} [{}];",
                node_attributes(*ordinal, node)?
            )?;
        }
    }
    if let Some(descendants) = children.get(session.session_id.as_str()) {
        for child in descendants {
            write_session_cluster(dot, child, children, nodes_by_session, indent + 2)?;
        }
    }
    writeln!(dot, "{padding}}}")?;
    Ok(())
}

fn node_attributes(ordinal: usize, node: &GeneratedNode) -> Result<String> {
    let mut lines = vec![format!(
        "node {ordinal} · {}",
        node.action.replace('_', " ")
    )];
    lines.push(format!(
        "ISL {} · OSL {}",
        node.request.input_tokens, node.request.output_tokens
    ));
    if let Some(arrival_ms) = node.initial_arrival_ms {
        lines.push(format!("initial arrival {arrival_ms} ms"));
    }
    if !node.tool_events.is_empty() {
        lines.push(format!("{} tool calls", node.tool_events.len()));
    }
    if !node.spawned_session_ids.is_empty() {
        lines.push(format!(
            "spawns {} child sessions",
            node.spawned_session_ids.len()
        ));
    }
    if node.compaction_attempt.is_some() {
        lines.push("compaction attempt".to_string());
    }
    Ok(format!(
        "label=\"{}\", fillcolor=\"{}\"",
        dot_escape(&lines.join("\n")),
        action_color(&node.action)
    ))
}

fn edge_relation(
    source: &GeneratedNode,
    target: &GeneratedNode,
    sessions: &BTreeMap<&str, &GeneratedSession>,
) -> Result<&'static str> {
    let source_session = source
        .request
        .agent_context
        .as_ref()
        .context("generated dependency source is missing agent context")?;
    let target_session = target
        .request
        .agent_context
        .as_ref()
        .context("generated dependency target is missing agent context")?;
    if source_session.session_id == target_session.session_id {
        if matches!(source.action.as_str(), "subagent" | "swarm") {
            return Ok("parent continues");
        }
        return Ok("continue");
    }
    if target_session.parent_session_id.as_deref() == Some(source_session.session_id.as_str()) {
        return Ok("spawn");
    }
    if source_session.parent_session_id.as_deref() == Some(target_session.session_id.as_str()) {
        return Ok("blocking join");
    }
    let source_session_metadata = sessions
        .get(source_session.session_id.as_str())
        .context("generated dependency source references an unknown session")?;
    let target_session_metadata = sessions
        .get(target_session.session_id.as_str())
        .context("generated dependency target references an unknown session")?;
    if source_session_metadata.parent_session_id.is_none()
        && target_session_metadata.parent_session_id.is_none()
        && source_session_metadata.top_level_stream_slot
            == target_session_metadata.top_level_stream_slot
    {
        return Ok("stream restart");
    }
    Ok("cross-session")
}

fn edge_label(relation: &str, delay_ms: u64) -> String {
    let mut attributes = vec![format!("label=\"{relation}")];
    if delay_ms > 0 {
        attributes[0].push_str(&format!(" + {delay_ms} ms"));
    }
    attributes[0].push('"');
    if relation == "spawn" {
        attributes.push("color=\"#0f766e\"".to_string());
    } else if matches!(relation, "blocking join" | "join") {
        attributes.push("color=\"#7c3aed\"".to_string());
    }
    attributes.join(", ")
}

fn session_cluster_id(session: &GeneratedSession) -> String {
    format!(
        "{}_{}",
        session.top_level_session_index,
        session.session_id.rsplit('-').next().unwrap_or("session")
    )
}

fn dot_escape(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

fn action_color(action: &str) -> &'static str {
    match action {
        "text" => "#dbeafe",
        "tool" => "#ffedd5",
        "parallel_tools" => "#fee2e2",
        "subagent" => "#ccfbf1",
        "swarm" => "#dcfce7",
        "complete" => "#f3e8ff",
        "compaction_attempt" => "#fef3c7",
        _ => "#e5e7eb",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scenario::config::UIntDistribution;
    use crate::scenario::{GeneratedScenario, GeneratorConfig, ResolvedGeneratorConfig};
    use agent_loadgen_core::AgentKind;

    fn swarm_config(blocking_probability: f64) -> ResolvedGeneratorConfig {
        let mut config = ResolvedGeneratorConfig::preset(AgentKind::Codex, 7);
        config.num_sessions = 1;
        config.concurrent_sessions = 1;
        config.turns = UIntDistribution::fixed(2);
        config.subagent_turns = UIntDistribution::fixed(1);
        config.tool_probability = 0.0;
        config.parallel_tool_probability = 0.0;
        config.subagent_probability = 0.0;
        config.swarm_probability = 1.0;
        config.completion_probability = 0.0;
        config.fanout = UIntDistribution::fixed(2);
        config.blocking_probability = blocking_probability;
        config.compaction_enabled = false;
        config
    }

    #[test]
    fn graph_marks_spawn_and_blocking_join() {
        let scenario = GeneratedScenario::generate(swarm_config(1.0)).unwrap();

        let dot = render_plan_dot(&scenario).unwrap();
        assert!(dot.contains("subgraph cluster_tree_0"));
        assert!(dot.contains("subagent session"));
        assert!(dot.contains("label=\"spawn"));
        assert!(dot.contains("shape=diamond"));
        assert!(dot.contains("label=\"join all 2 children\""));
        assert!(dot.contains("label=\"join"));
        assert!(!dot.contains("label=\"blocking join +"));
    }

    #[test]
    fn graph_marks_non_blocking_parent_continuation() {
        let scenario = GeneratedScenario::generate(swarm_config(0.0)).unwrap();
        assert!(
            render_plan_dot(&scenario)
                .unwrap()
                .contains("label=\"parent continues")
        );
    }

    #[test]
    fn graph_marks_top_level_stream_restart() {
        let mut config = swarm_config(0.0);
        config.num_sessions = 2;
        config.turns = UIntDistribution::fixed(1);
        config.swarm_probability = 0.0;
        let scenario = GeneratedScenario::generate(config).unwrap();

        assert!(
            render_plan_dot(&scenario)
                .unwrap()
                .contains("label=\"stream restart")
        );
    }

    #[test]
    fn graph_escapes_dot_labels() {
        assert_eq!(
            dot_escape("line \"one\"\nline two"),
            "line \\\"one\\\"\\nline two"
        );
    }

    #[test]
    fn graph_requires_current_profile_schema() {
        let config = toml::from_str::<GeneratorConfig>(
            r#"
                schema_version = 4
                agent = "codex"
            "#,
        )
        .unwrap();
        let scenario = GeneratedScenario::generate(config.resolve().unwrap()).unwrap();
        assert!(
            render_plan_dot(&scenario)
                .unwrap()
                .contains("agent-loadgen generated plan")
        );
    }
}
