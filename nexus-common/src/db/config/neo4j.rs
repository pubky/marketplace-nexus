use serde::{Deserialize, Serialize};

use super::super::connection_url::ConnectionUrl;

pub const NEO4J_URI: &str = "bolt://localhost:7687";
pub const NEO4J_USER: &str = "neo4j";
pub const NEO4J_PASS: &str = "12345678";
// Create temporal struct to wrap database config
#[derive(Clone, Serialize, Deserialize, Eq, PartialEq)]
pub struct Neo4JConfig {
    pub uri: ConnectionUrl,

    #[serde(default = "default_neo4j_user")]
    pub user: String,

    pub password: String,

    /// Slow-query warning threshold in milliseconds.
    /// `Some(ms)` — emit a warning for queries exceeding `ms` milliseconds.
    /// `Some(0)`  — warn on every query (useful for debugging).
    /// `None`     — disable slow-query warnings.
    /// Defaults to `None` (disabled).
    #[serde(default)]
    pub slow_query_logging_threshold_ms: Option<u64>,

    /// Include the full cypher (with interpolated params) in slow-query warnings.
    /// Useful for debugging but verbose. Defaults to false.
    #[serde(default)]
    pub slow_query_logging_include_cypher: bool,
}

/// The password never appears in diagnostics: this config is printed whole by
/// the startup `debug!` lines of the API and the watcher.
impl std::fmt::Debug for Neo4JConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Neo4JConfig")
            .field("uri", &self.uri)
            .field("user", &self.user)
            .field("password", &"<redacted>")
            .field(
                "slow_query_logging_threshold_ms",
                &self.slow_query_logging_threshold_ms,
            )
            .field(
                "slow_query_logging_include_cypher",
                &self.slow_query_logging_include_cypher,
            )
            .finish()
    }
}

fn default_neo4j_user() -> String {
    String::from("neo4j")
}

impl Default for Neo4JConfig {
    fn default() -> Self {
        Self {
            uri: ConnectionUrl::from(NEO4J_URI),
            user: String::from(NEO4J_USER),
            password: String::from(NEO4J_PASS),
            slow_query_logging_threshold_ms: None,
            slow_query_logging_include_cypher: false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::config::DatabaseConfig;

    #[test]
    fn debug_output_never_contains_the_password() {
        let config = Neo4JConfig {
            password: "s3cret-neo4j-pass".to_string(),
            ..Neo4JConfig::default()
        };
        let rendered = format!("{config:?}");
        assert!(!rendered.contains("s3cret-neo4j-pass"), "{rendered}");
        assert!(rendered.contains("<redacted>"));
        assert!(rendered.contains("neo4j"), "user stays visible: {rendered}");
    }

    #[test]
    fn nested_database_config_debug_does_not_leak_the_password() {
        let mut db = DatabaseConfig::default();
        db.neo4j.password = "s3cret-neo4j-pass".to_string();
        let rendered = format!("{db:?}");
        assert!(!rendered.contains("s3cret-neo4j-pass"), "{rendered}");
        assert_eq!(db.neo4j.password, "s3cret-neo4j-pass");
    }
}
