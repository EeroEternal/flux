//! Flux: a streaming-first AI load balancer.
//!
//! Flux sits in front of one or more AI gateway instances and forwards
//! long-lived, non-idempotent streaming requests with minimal added latency.
//! It deliberately does **not** implement model routing, billing, prompt
//! rewriting or protocol translation. See the README for the design rationale.

#![forbid(unsafe_code)]

pub mod backend;
pub mod config;
pub mod duration;
pub mod health;
pub mod metrics;
pub mod pool;
pub mod proxy;
pub mod server;
