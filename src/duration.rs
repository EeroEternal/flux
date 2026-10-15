//! Human-readable duration parsing and serde support.

use serde::{Deserialize, Deserializer};
use std::time::Duration;

/// Deserialize a duration from a human readable string such as `"5s"`, `"600s"`,
/// `"2m"`, `"1h"` or a bare number of seconds.
pub fn deserialize<'de, D>(deserializer: D) -> Result<Duration, D::Error>
where
    D: Deserializer<'de>,
{
    let raw = String::deserialize(deserializer)?;
    parse(&raw).map_err(serde::de::Error::custom)
}

/// Deserialize an optional duration from a human readable string.
pub fn deserialize_opt<'de, D>(deserializer: D) -> Result<Option<Duration>, D::Error>
where
    D: Deserializer<'de>,
{
    let raw = Option::<String>::deserialize(deserializer)?;
    match raw {
        Some(s) => parse(&s).map(Some).map_err(serde::de::Error::custom),
        None => Ok(None),
    }
}

/// Parse a human readable duration.
pub fn parse(input: &str) -> Result<Duration, String> {
    let s = input.trim();
    if s.is_empty() {
        return Err("empty duration".to_string());
    }
    let split = s
        .find(|c: char| !(c.is_ascii_digit() || c == '.' || c == '-' || c == '+'))
        .unwrap_or(s.len());
    let (num, unit) = (&s[..split], s[split..].trim());
    let value: f64 = num
        .parse()
        .map_err(|_| format!("invalid duration value: {input:?}"))?;
    if value < 0.0 {
        return Err(format!("duration must not be negative: {input:?}"));
    }
    let multiplier = match unit {
        "ns" => 1e-9,
        "us" | "µs" => 1e-6,
        "ms" => 1e-3,
        "" | "s" | "sec" | "secs" => 1.0,
        "m" | "min" | "mins" => 60.0,
        "h" | "hr" | "hrs" => 3600.0,
        "d" | "day" | "days" => 86400.0,
        other => return Err(format!("unknown duration unit: {other:?}")),
    };
    Ok(Duration::from_secs_f64(value * multiplier))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_common_units() {
        assert_eq!(parse("5s").unwrap(), Duration::from_secs(5));
        assert_eq!(parse("600s").unwrap(), Duration::from_secs(600));
        assert_eq!(parse("2m").unwrap(), Duration::from_secs(120));
        assert_eq!(parse("1h").unwrap(), Duration::from_secs(3600));
        assert_eq!(parse("250ms").unwrap(), Duration::from_millis(250));
        assert_eq!(parse("1.5s").unwrap(), Duration::from_millis(1500));
        assert_eq!(parse("30").unwrap(), Duration::from_secs(30));
    }

    #[test]
    fn rejects_garbage() {
        assert!(parse("").is_err());
        assert!(parse("5lightyears").is_err());
    }
}
