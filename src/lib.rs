//! xLB: a streaming-first load balancer for AI inference gateways.
//!
//! xLB occupies the HAProxy role in front of gateway instances: connection
//! management, scheduling, health checks, timeouts and transparent forwarding.
//! It forwards bytes, not tokens. Model routing, billing, prompt rewriting and
//! protocol translation stay in the backends.

#![forbid(unsafe_code)]

pub mod backend;
pub mod config;
pub mod duration;
pub mod health;
pub mod metrics;
pub mod pool;
pub mod proxy;
pub mod server;
