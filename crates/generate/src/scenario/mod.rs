// SPDX-License-Identifier: Apache-2.0

//! Seeded coding-agent traffic plans.

mod config;
mod distribution;
mod graph;
mod model;
mod plan;
mod tool_parallelism;

pub use config::{GeneratorConfig, ResolvedGeneratorConfig};
pub use graph::{PlanGraphArtifacts, write_plan_graph};
pub use model::{
    CompactionExpectedEffect, GeneratedCompactionAttempt, GeneratedCompactionOperation,
    GeneratedNode, GeneratedScenario, GeneratedSession, GeneratedSessionTopology,
    GeneratedToolEvent, GeneratedToolParallelism,
};
