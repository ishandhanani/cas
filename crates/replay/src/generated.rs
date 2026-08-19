// SPDX-License-Identifier: Apache-2.0

use std::fs;
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use uuid::Uuid;

use super::artifacts::{SummaryIdentity, summarize, write_json};
use super::causal::{CausalWorkload, execute_causally};
use super::request::{
    build_http_client, normalize_target, prepare_open_file_limit, prepare_request, validate_options,
};
use super::{PreparedRequest, ReplayOptions, RequestExecution, RunSummary, TrafficKind};
use crate::token_shape::TokenDictionary;
use agent_loadgen_generate::scenario::{GeneratedScenario, write_plan_graph};

pub async fn run_generated_scenario(
    scenario: &GeneratedScenario,
    dictionary: TokenDictionary,
    options: ReplayOptions,
) -> Result<RunSummary> {
    validate_options(&options)?;
    if options.agent != scenario.config.agent {
        bail!("generated scenario agent does not match the request header adapter");
    }
    prepare_open_file_limit(options.max_in_flight)?;
    fs::create_dir_all(&options.output_dir).with_context(|| {
        format!(
            "failed to create output directory {}",
            options.output_dir.display()
        )
    })?;
    write_json(&options.output_dir.join("scenario.json"), scenario)?;
    write_plan_graph(&options.output_dir, scenario)?;

    let client = build_http_client(&options)?;
    let target = normalize_target(&options.target);
    let run_id = Uuid::new_v4().to_string();
    let token_manifest = dictionary.manifest().clone();
    let options = Arc::new(options);
    let workload = GeneratedWorkload {
        request_factory: GeneratedRequestFactory {
            scenario,
            dictionary: &dictionary,
            client: client.clone(),
            target: target.clone(),
            run_id: run_id.clone(),
            options: Arc::clone(&options),
        },
    };
    let execution = execute_causally(&workload, client, &target, &options).await?;
    let mut summary = summarize(
        SummaryIdentity {
            traffic_kind: TrafficKind::SyntheticKvShape,
            run_id,
            target: &target,
        },
        &scenario.trace_manifest,
        &token_manifest,
        &options,
        &execution.accumulator,
        execution.elapsed,
    );
    summary.session_topology = Some(scenario.session_topology.clone());
    write_json(&options.output_dir.join("run.json"), &summary)?;
    Ok(summary)
}

struct GeneratedWorkload<'a> {
    request_factory: GeneratedRequestFactory<'a>,
}

impl CausalWorkload for GeneratedWorkload<'_> {
    fn node_count(&self) -> usize {
        self.request_factory.scenario.nodes.len()
    }

    fn dependencies(&self, ordinal: usize) -> &[usize] {
        &self.request_factory.scenario.nodes[ordinal].dependencies
    }

    fn initial_arrival_ms(&self, ordinal: usize) -> Option<u64> {
        self.request_factory.scenario.nodes[ordinal].initial_arrival_ms
    }

    fn delay_after_dependencies_ms(&self, ordinal: usize) -> u64 {
        self.request_factory.scenario.nodes[ordinal].delay_after_dependencies_ms
    }

    fn prepare(&self, ordinal: usize) -> Result<PreparedRequest> {
        self.request_factory.prepare(ordinal)
    }

    fn name(&self) -> &'static str {
        "generated scenario"
    }
}

struct GeneratedRequestFactory<'a> {
    scenario: &'a GeneratedScenario,
    dictionary: &'a TokenDictionary,
    client: reqwest::Client,
    target: String,
    run_id: String,
    options: Arc<ReplayOptions>,
}

impl GeneratedRequestFactory<'_> {
    fn prepare(&self, ordinal: usize) -> Result<PreparedRequest> {
        let node = self
            .scenario
            .nodes
            .get(ordinal)
            .with_context(|| format!("generated node {ordinal} is missing"))?;
        prepare_request(
            &self.client,
            &self.target,
            &self.run_id,
            &self.options,
            self.dictionary,
            node.request.clone(),
            RequestExecution {
                output_budget_tokens: node.output_budget_tokens,
                compaction_attempt: node.compaction_attempt.clone(),
            },
        )
    }
}
