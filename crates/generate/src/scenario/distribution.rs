// SPDX-License-Identifier: Apache-2.0

use anyhow::{Context, Result, bail};
use rand::Rng;
use rand::rngs::StdRng;
use rand_distr::{Distribution, LogNormal};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum UIntDistribution {
    Fixed {
        value: u64,
    },
    Uniform {
        min: u64,
        max: u64,
    },
    LogNormal {
        median: f64,
        sigma: f64,
        min: u64,
        max: u64,
    },
}

impl UIntDistribution {
    pub(super) fn fixed(value: u64) -> Self {
        Self::Fixed { value }
    }

    pub(super) fn uniform(min: u64, max: u64) -> Self {
        Self::Uniform { min, max }
    }

    pub(super) fn log_normal(median: f64, sigma: f64, min: u64, max: u64) -> Self {
        Self::LogNormal {
            median,
            sigma,
            min,
            max,
        }
    }

    pub(super) fn validate(&self, name: &str) -> Result<()> {
        match self {
            Self::Fixed { .. } => Ok(()),
            Self::Uniform { min, max } => {
                if min > max {
                    bail!("{name} uniform minimum exceeds its maximum");
                }
                Ok(())
            }
            Self::LogNormal {
                median,
                sigma,
                min,
                max,
            } => {
                if !median.is_finite() || *median <= 0.0 {
                    bail!("{name} log-normal median must be positive and finite");
                }
                if !sigma.is_finite() || *sigma <= 0.0 {
                    bail!("{name} log-normal sigma must be positive and finite");
                }
                if min > max {
                    bail!("{name} log-normal minimum exceeds its maximum");
                }
                Ok(())
            }
        }
    }

    pub(super) fn sample(&self, rng: &mut StdRng) -> Result<u64> {
        match self {
            Self::Fixed { value } => Ok(*value),
            Self::Uniform { min, max } => Ok(rng.random_range(*min..=*max)),
            Self::LogNormal {
                median,
                sigma,
                min,
                max,
            } => {
                let distribution = LogNormal::new(median.ln(), *sigma)
                    .context("invalid log-normal distribution")?;
                let value = distribution.sample(rng).round();
                Ok(value.clamp(*min as f64, *max as f64) as u64)
            }
        }
    }

    pub(super) fn bounds(&self) -> (u64, u64) {
        match self {
            Self::Fixed { value } => (*value, *value),
            Self::Uniform { min, max } | Self::LogNormal { min, max, .. } => (*min, *max),
        }
    }
}
