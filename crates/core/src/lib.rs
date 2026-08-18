// SPDX-License-Identifier: Apache-2.0

//! Shared workload contracts for agent-loadgen crates.

pub mod agent;
pub mod contracts;
pub mod scheduler;

pub use agent::{AgentKind, agent_headers, is_managed_header};
pub use contracts::{AgentContext, Percentiles, TraceManifest, TraceRequest, percentiles};
