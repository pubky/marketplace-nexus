use std::path::PathBuf;

use nexus_common::{file::default_config_dir_path, types::DynError, ApiConfig, DaemonConfig};
use pubky::pkarr::{self, Keypair};

#[derive(Debug, Clone)]
pub struct ApiContext {
    pub(crate) api_config: ApiConfig,
    pub(crate) keypair: pkarr::Keypair,
    pub(crate) pkarr_client: pkarr::Client,
}

pub struct ApiContextBuilder {
    api_config: Option<ApiConfig>,
    config_dir: PathBuf,
    pkarr_builder: Option<pkarr::ClientBuilder>,
}

impl ApiContextBuilder {
    pub fn from_default_config_dir() -> Self {
        Self::from_config_dir(default_config_dir_path())
    }

    pub fn from_config_dir(config_dir: PathBuf) -> Self {
        Self {
            api_config: None,
            config_dir: config_dir.clone(),
            pkarr_builder: None,
        }
    }

    /// Sets a custom [ApiConfig], overriding the one that may be derived from a config file in the given dir
    pub fn api_config(mut self, api_config: ApiConfig) -> Self {
        self.api_config = Some(api_config);

        self
    }

    pub fn pkarr_builder(mut self, pkarr_builder: pkarr::ClientBuilder) -> Self {
        self.pkarr_builder = Some(pkarr_builder);

        self
    }

    pub async fn try_build(&self) -> Result<ApiContext, DynError> {
        // Ensure path to config dir exists, regardless of how the builder was initialized
        std::fs::create_dir_all(self.config_dir.clone())?;

        let api_config = match &self.api_config {
            None => {
                let dc = DaemonConfig::read_or_create_config_file(self.config_dir.clone()).await?;
                ApiConfig::from(dc)
            }
            Some(ac) => ac.clone(),
        };
        api_config.validate()?;

        let pkarr_builder = self.pkarr_builder.clone().unwrap_or_default();
        let pkarr_client = pkarr_builder.build()?;

        let keypair = self.read_or_create_keypair()?;

        Ok(ApiContext {
            api_config,
            keypair,
            pkarr_client,
        })
    }

    /// Reads the secret file. Creates a new secret file if it doesn't exist.
    fn read_or_create_keypair(&self) -> Result<Keypair, DynError> {
        let secret_file_path = self.config_dir.join("secret");

        if !secret_file_path.exists() {
            Keypair::random().write_secret_key_file(&secret_file_path)?;
        }

        let keypair =
            Keypair::from_secret_key_file(&secret_file_path).map_err(|e| e.to_string())?;

        Ok(keypair)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn builder_rejects_invalid_timeout_relationship() {
        for process_timeout_secs in [20, 21] {
            let temp_dir = tempfile::TempDir::new_in(".").unwrap();
            let api_config = ApiConfig {
                request_timeout_secs: 20,
                stack: nexus_common::StackConfig {
                    media: nexus_common::MediaConfig {
                        process_timeout_secs,
                        ..Default::default()
                    },
                    ..Default::default()
                },
                ..Default::default()
            };
            let error = match ApiContextBuilder::from_config_dir(temp_dir.path().to_path_buf())
                .api_config(api_config)
                .try_build()
                .await
            {
                Ok(_) => panic!("invalid timeouts are rejected"),
                Err(error) => error,
            };
            assert!(error
                .to_string()
                .contains("must be less than api.request_timeout_secs"));
        }
    }
}
