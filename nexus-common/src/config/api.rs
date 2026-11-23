use std::net::{IpAddr, Ipv4Addr};
use std::{fmt::Debug, net::SocketAddr};

use super::file::ConfigLoader;
use super::{default_stack, DaemonConfig, StackConfig};
use crate::types::DynError;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

pub const NAME: &str = "nexus.api";

pub const DEFAULT_LOCAL_IP: [u8; 4] = [127, 0, 0, 1];
pub const DEFAULT_ICANN_LOCAL_PORT: u16 = 8080;
pub const DEFAULT_PUBKY_LOCAL_PORT: u16 = 8081;
pub const DEFAULT_REQUEST_TIMEOUT_SECS: u64 = 30;
pub const DEFAULT_MAX_BODY_SIZE_BYTES: usize = 1024 * 1024;

const fn default_request_timeout_secs() -> u64 {
    DEFAULT_REQUEST_TIMEOUT_SECS
}

const fn default_max_body_size_bytes() -> usize {
    DEFAULT_MAX_BODY_SIZE_BYTES
}

fn deserialize_request_timeout_secs<'de, D>(deserializer: D) -> Result<u64, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = u64::deserialize(deserializer)?;
    if value == 0 {
        return Err(serde::de::Error::custom(
            "api.request_timeout_secs must be greater than 0",
        ));
    }
    Ok(value)
}

/// Configuration settings for the Nexus Watcher service
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiConfig {
    pub name: String,
    pub public_ip: IpAddr,
    pub public_addr: SocketAddr,
    pub pubky_listen_socket: SocketAddr,
    #[serde(
        default = "default_request_timeout_secs",
        deserialize_with = "deserialize_request_timeout_secs"
    )]
    pub request_timeout_secs: u64,
    #[serde(default = "default_max_body_size_bytes")]
    pub max_body_size_bytes: usize,
    #[serde(default = "default_stack")]
    pub stack: StackConfig,
}

impl Default for ApiConfig {
    fn default() -> Self {
        Self {
            name: String::from(NAME),
            public_ip: IpAddr::V4(Ipv4Addr::LOCALHOST),
            public_addr: SocketAddr::from((DEFAULT_LOCAL_IP, DEFAULT_ICANN_LOCAL_PORT)),
            pubky_listen_socket: SocketAddr::from((DEFAULT_LOCAL_IP, DEFAULT_PUBKY_LOCAL_PORT)),
            request_timeout_secs: DEFAULT_REQUEST_TIMEOUT_SECS,
            max_body_size_bytes: DEFAULT_MAX_BODY_SIZE_BYTES,
            stack: StackConfig::default(),
        }
    }
}

impl ApiConfig {
    pub fn validate(&self) -> Result<(), DynError> {
        if self.request_timeout_secs == 0 {
            return Err("api.request_timeout_secs must be greater than 0".into());
        }
        if self.stack.media.process_timeout_secs == 0 {
            return Err("stack.media.process_timeout_secs must be greater than 0".into());
        }
        if self.stack.media.process_timeout_secs >= self.request_timeout_secs {
            return Err(format!(
                "stack.media.process_timeout_secs ({}) must be less than api.request_timeout_secs ({})",
                self.stack.media.process_timeout_secs, self.request_timeout_secs
            )
            .into());
        }
        Ok(())
    }
}

/// Converts a [`DaemonConfig`] into an [`ApiConfig`], extracting only the API-related settings
/// and the shared application stack
impl From<DaemonConfig> for ApiConfig {
    fn from(daemon_config: DaemonConfig) -> Self {
        ApiConfig {
            stack: daemon_config.stack,
            ..daemon_config.api
        }
    }
}

#[async_trait]
impl ConfigLoader<ApiConfig> for ApiConfig {
    fn try_from_str(value: &str) -> Result<ApiConfig, DynError> {
        let config: ApiConfig = toml::from_str(value)?;
        config.validate()?;
        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config_with_timeouts(request_timeout_secs: u64, process_timeout_secs: u64) -> String {
        format!(
            r#"
            name = "nexus.api"
            public_ip = "127.0.0.1"
            public_addr = "127.0.0.1:8080"
            pubky_listen_socket = "127.0.0.1:8081"
            request_timeout_secs = {request_timeout_secs}
            [stack]
            log_level = "info"
            files_path = "."
            [stack.media]
            process_timeout_secs = {process_timeout_secs}
            [stack.db]
            redis = "redis://localhost:6379"
            [stack.db.neo4j]
            uri = "bolt://localhost:7687"
            password = "password"
            "#
        )
    }

    #[test]
    fn request_limits_default_when_absent() {
        let config = ApiConfig::try_from_str(
            r#"
            name = "nexus.api"
            public_ip = "127.0.0.1"
            public_addr = "127.0.0.1:8080"
            pubky_listen_socket = "127.0.0.1:8081"
            "#,
        )
        .expect("config parses");
        assert_eq!(config.request_timeout_secs, DEFAULT_REQUEST_TIMEOUT_SECS);
        assert_eq!(config.max_body_size_bytes, DEFAULT_MAX_BODY_SIZE_BYTES);
        assert_eq!(config.stack.media.process_timeout_secs, 20);
    }

    #[test]
    fn request_timeout_rejects_zero() {
        let error = ApiConfig::try_from_str(&config_with_timeouts(0, 1))
            .expect_err("zero request timeout is rejected");
        assert!(error
            .to_string()
            .contains("api.request_timeout_secs must be greater than 0"));
    }

    #[test]
    fn media_timeout_equal_to_request_timeout_is_rejected() {
        let error = ApiConfig::try_from_str(&config_with_timeouts(20, 20))
            .expect_err("equal timeouts are rejected");
        assert!(error
            .to_string()
            .contains("must be less than api.request_timeout_secs"));
    }

    #[test]
    fn media_timeout_greater_than_request_timeout_is_rejected() {
        let error = ApiConfig::try_from_str(&config_with_timeouts(20, 21))
            .expect_err("a longer process timeout is rejected");
        assert!(error
            .to_string()
            .contains("must be less than api.request_timeout_secs"));
    }
}
