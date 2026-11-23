use crate::{db::DatabaseConfig, get_files_dir_pathbuf};
use pubky_app_specs::PubkyId;
use serde::{Deserialize, Deserializer, Serialize};
use std::{fmt::Debug, path::PathBuf};

use super::{file::validate_and_expand_path, Level, LOG_LEVEL};

const DEFAULT_MAX_CONCURRENCY: usize = 2;
const DEFAULT_PROCESS_TIMEOUT_SECS: u64 = 20;

const fn default_max_concurrency() -> usize {
    DEFAULT_MAX_CONCURRENCY
}

const fn default_process_timeout_secs() -> u64 {
    DEFAULT_PROCESS_TIMEOUT_SECS
}

fn deserialize_max_concurrency<'de, D>(deserializer: D) -> Result<usize, D::Error>
where
    D: Deserializer<'de>,
{
    let value = usize::deserialize(deserializer)?;
    if value == 0 {
        return Err(serde::de::Error::custom(
            "stack.media.max_concurrency must be greater than 0",
        ));
    }
    Ok(value)
}

fn deserialize_process_timeout_secs<'de, D>(deserializer: D) -> Result<u64, D::Error>
where
    D: Deserializer<'de>,
{
    let value = u64::deserialize(deserializer)?;
    if value == 0 {
        return Err(serde::de::Error::custom(
            "stack.media.process_timeout_secs must be greater than 0",
        ));
    }
    Ok(value)
}

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub struct MediaConfig {
    #[serde(
        default = "default_max_concurrency",
        deserialize_with = "deserialize_max_concurrency"
    )]
    pub max_concurrency: usize,
    #[serde(
        default = "default_process_timeout_secs",
        deserialize_with = "deserialize_process_timeout_secs"
    )]
    pub process_timeout_secs: u64,
}

impl Default for MediaConfig {
    fn default() -> Self {
        Self {
            max_concurrency: DEFAULT_MAX_CONCURRENCY,
            process_timeout_secs: DEFAULT_PROCESS_TIMEOUT_SECS,
        }
    }
}

fn deserialize_and_expand<'de, D>(deserializer: D) -> Result<PathBuf, D::Error>
where
    D: Deserializer<'de>,
{
    let path: PathBuf = Deserialize::deserialize(deserializer)?;
    validate_and_expand_path(path).map_err(serde::de::Error::custom)
}

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub struct StackConfig {
    pub log_level: Level,
    #[serde(deserialize_with = "deserialize_and_expand")]
    pub files_path: PathBuf,
    pub otlp_endpoint: Option<String>,
    pub db: DatabaseConfig,
    /// Homeservers this indexer may watch. When non-empty, the watcher polls only these, and a
    /// homeserver discovered through a follow, tag, mention, review or `/v0/bootstrap` is added only
    /// if it is listed. Empty (the default) allows every homeserver.
    #[serde(default)]
    pub allowed_homeservers: Vec<PubkyId>,
    #[serde(default)]
    pub media: MediaConfig,
}

/// Utility function
pub fn default_stack() -> StackConfig {
    StackConfig::default()
}

impl Default for StackConfig {
    fn default() -> Self {
        Self {
            log_level: LOG_LEVEL,
            files_path: get_files_dir_pathbuf(),
            otlp_endpoint: None,
            db: DatabaseConfig::default(),
            allowed_homeservers: Vec::new(),
            media: MediaConfig::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const STACK_WITHOUT_MEDIA: &str = r#"
        log_level = "info"
        files_path = "."
        [db]
        redis = "redis://localhost:6379"
        [db.neo4j]
        uri = "bolt://localhost:7687"
        password = "password"
    "#;

    #[test]
    fn media_defaults_when_the_table_is_absent() {
        let config: StackConfig = toml::from_str(STACK_WITHOUT_MEDIA).expect("config parses");
        assert_eq!(config.media, MediaConfig::default());
        assert_eq!(config.media.max_concurrency, 2);
        assert_eq!(config.media.process_timeout_secs, 20);
    }

    #[test]
    fn media_rejects_zero_concurrency() {
        let error = toml::from_str::<MediaConfig>("max_concurrency = 0")
            .expect_err("zero concurrency is rejected");
        assert!(error.to_string().contains("must be greater than 0"));

        let input =
            STACK_WITHOUT_MEDIA.replace("[db.neo4j]", "[media]\nmax_concurrency = 0\n[db.neo4j]");
        let error = toml::from_str::<StackConfig>(&input).expect_err("config is rejected");
        assert!(error.to_string().contains("must be greater than 0"));
    }

    #[test]
    fn media_rejects_zero_process_timeout() {
        let error = toml::from_str::<MediaConfig>("process_timeout_secs = 0")
            .expect_err("zero process timeout is rejected");
        assert!(error.to_string().contains("must be greater than 0"));

        let input = STACK_WITHOUT_MEDIA.replace(
            "[db.neo4j]",
            "[media]\nprocess_timeout_secs = 0\n[db.neo4j]",
        );
        let error = toml::from_str::<StackConfig>(&input).expect_err("config is rejected");
        assert!(error.to_string().contains("must be greater than 0"));
    }
}
