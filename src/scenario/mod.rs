// SPDX-License-Identifier: Apache-2.0

//! Seeded coding-agent traffic plans.

mod config;
mod plan;

pub use config::{GeneratorConfig, ResolvedGeneratorConfig};
pub use plan::{
    CompactionExpectedEffect, GeneratedCompactionAttempt, GeneratedCompactionOperation,
    GeneratedNode, GeneratedNodeKind, GeneratedScenario, GeneratedSession, GeneratedToolEvent,
};
