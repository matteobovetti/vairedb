/// Engine-specific error message prefixes stripped before surfacing to clients.
const PREFIXES_TO_STRIP: &[&str] = &[
    // DataFusion
    "External error: ",
    "Arrow error: ",
    "Internal error: ",
    "Execution error: ",
    "Schema error: ",
    "Plan error: ",
    "Not Implemented: ",
    "Resources exhausted: ",
    "Configuration error: ",
    // DuckDB
    "DuckDB error: ",
    "Catalog Error: ",
    "Parser Error: ",
    "Binder Error: ",
    "Conversion Error: ",
    "IO Error: ",
    "Runtime Error: ",
    "Invalid Input Error: ",
    "Constraint Error: ",
    "Out of Range Error: ",
    // CoreError thiserror prefixes
    "engine error: ",
    "shard not found: ",
    "write conflict: ",
    "type mismatch: ",
    "write queue error: ",
    // Core write queue
    "write execution failed: ",
];

/// Scrub an internal error message into a client-safe string.
///
/// Repeatedly strips known engine prefixes (DataFusion, DuckDB, `CoreError`),
/// unwraps Ballista "failed on executor" wrappers, and replaces any `http://`
/// URLs with `[node]` so node addresses never leak to clients.
pub fn sanitize_message(raw: &str) -> String {
    let mut msg = raw.to_string();

    loop {
        let before = msg.len();
        for prefix in PREFIXES_TO_STRIP {
            if let Some(stripped) = msg.strip_prefix(prefix) {
                msg = stripped.to_string();
            }
        }
        if msg.len() == before {
            break;
        }
    }

    // Strip Ballista executor task failure patterns containing URLs
    if let Some(idx) = msg.find("failed on executor")
        && let Some(colon_idx) = msg[idx..].find(": ")
    {
        msg = msg[idx + colon_idx + 2..].to_string();
    }

    // Scrub any remaining http:// URLs (executor addresses, node endpoints)
    while let Some(start) = msg.find("http://") {
        let end = msg[start..]
            .find(|c: char| c.is_whitespace())
            .map(|i| start + i)
            .unwrap_or(msg.len());
        msg = format!("{}[node]{}", &msg[..start], &msg[end..]);
    }

    msg
}

#[cfg(test)]
mod tests {
    use super::sanitize_message;

    #[test]
    fn test_sanitize_strips_core_error_prefixes() {
        let msg =
            "engine error: write execution failed: IO Error: /data/core.duckdb: Permission denied";
        let result = sanitize_message(msg);
        assert!(!result.contains("engine error"));
        assert!(!result.contains("write execution failed"));
        assert!(!result.contains("IO Error"));
    }

    #[test]
    fn test_sanitize_strips_ballista_executor_url() {
        let msg = "Task 3 failed on executor http://192.168.1.5:50051: query failed on shard 'orders_shard0'";
        let result = sanitize_message(msg);
        assert!(!result.contains("192.168.1.5"));
        assert!(!result.contains("http://"));
        assert!(result.contains("query failed on shard"));
    }

    #[test]
    fn test_sanitize_strips_http_urls() {
        let msg = "connection to http://10.0.0.1:50041 failed";
        let result = sanitize_message(msg);
        assert!(!result.contains("10.0.0.1"));
        assert!(!result.contains("http://"));
    }

    #[test]
    fn test_sanitize_strips_constraint_error_prefix() {
        let msg = "Constraint Error: NOT NULL constraint failed for column 'id'";
        let result = sanitize_message(msg);
        assert!(!result.contains("Constraint Error"));
        assert!(result.contains("NOT NULL constraint failed"));
    }
}
