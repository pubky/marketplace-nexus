use crate::{db::DatabaseConfig, get_files_dir_pathbuf};
use pubky_app_specs::PubkyId;
use serde::{Deserialize, Deserializer, Serialize};
use std::{fmt::Debug, path::PathBuf};

use super::{file::validate_and_expand_path, Level, LOG_LEVEL};

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
        }
    }
}
