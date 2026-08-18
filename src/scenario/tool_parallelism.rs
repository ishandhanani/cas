// SPDX-License-Identifier: Apache-2.0

use anyhow::{Context, Result};

use super::model::{GeneratedNode, GeneratedToolParallelism};

pub(super) fn summarize_tool_parallelism(
    nodes: &[GeneratedNode],
) -> Result<GeneratedToolParallelism> {
    let mut tool_phases = 0;
    let mut parallel_tool_phases = 0;
    let mut tool_calls = 0;
    let mut parallel_tool_calls = 0;
    let mut tool_work_ms = 0_u64;
    let mut tool_wall_ms = 0_u64;
    let mut parallel_wall_ms = 0_u64;

    for node in nodes.iter().filter(|node| !node.tool_events.is_empty()) {
        tool_phases += 1;
        tool_calls += node.tool_events.len();
        let parallel = node.tool_events.len() > 1;
        if parallel {
            parallel_tool_phases += 1;
            parallel_tool_calls += node.tool_events.len();
        }

        let mut latencies = node
            .tool_events
            .iter()
            .map(|event| event.latency_ms)
            .collect::<Vec<_>>();
        latencies.sort_unstable_by(|left, right| right.cmp(left));
        for latency in &latencies {
            tool_work_ms = tool_work_ms
                .checked_add(*latency)
                .context("generated tool work time overflow")?;
        }
        tool_wall_ms = tool_wall_ms
            .checked_add(latencies[0])
            .context("generated tool wall time overflow")?;
        if parallel {
            // Generated calls in one parallel phase start together. The
            // second-longest duration is therefore the interval with at
            // least two active calls.
            parallel_wall_ms = parallel_wall_ms
                .checked_add(latencies[1])
                .context("generated parallel tool wall time overflow")?;
        }
    }

    Ok(GeneratedToolParallelism {
        tool_phases,
        parallel_tool_phases,
        tool_calls,
        parallel_tool_calls,
        parallel_call_fraction: fraction(parallel_tool_calls, tool_calls),
        tool_work_ms,
        tool_wall_ms,
        parallel_wall_ms,
        parallel_wall_time_fraction: ratio(parallel_wall_ms, tool_wall_ms),
        work_to_wall_ratio: ratio(tool_work_ms, tool_wall_ms),
    })
}

fn fraction(numerator: usize, denominator: usize) -> f64 {
    if denominator == 0 {
        0.0
    } else {
        numerator as f64 / denominator as f64
    }
}

fn ratio(numerator: u64, denominator: u64) -> f64 {
    if denominator == 0 {
        0.0
    } else {
        numerator as f64 / denominator as f64
    }
}
