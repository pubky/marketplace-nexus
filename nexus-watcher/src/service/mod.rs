mod constants;
mod poll_backoff;
mod processor;
mod processor_runner;
mod stats;
mod traits;

/// Module exports
pub use constants::{
    POLL_TIMEOUT_SECS, PROCESSING_TIMEOUT_SECS, RATE_LIMIT_BACKOFF_SECS, WATCHER_CONFIG_FILE_NAME,
};
use nexus_common::types::DynError;
pub use poll_backoff::HomeserverPollBackoff;
pub use processor::EventProcessor;
pub use processor_runner::{order_homeservers, EventProcessorRunner};
pub use traits::{TEventProcessor, TEventProcessorRunner};

use crate::NexusWatcherBuilder;
use nexus_common::file::ConfigLoader;
use nexus_common::models::homeserver::{Homeserver, HomeserverAllowlist};
use nexus_common::utils::create_shutdown_rx;
use nexus_common::{DaemonConfig, WatcherConfig};
use pubky_app_specs::PubkyId;
use std::path::PathBuf;
use tokio::sync::watch::Receiver;
use tokio::time::Duration;
use tracing::{debug, error, info};

/// A watcher whose default homeserver the allowlist refuses would poll nothing; fail at start instead.
pub fn check_default_homeserver_allowed(
    default_homeserver: &PubkyId,
    allowlist: &HomeserverAllowlist,
) -> Result<(), DynError> {
    if allowlist.allows(default_homeserver.as_ref()) {
        return Ok(());
    }
    Err(format!(
        "the default homeserver {default_homeserver} is not in allowed_homeservers ({allowlist})"
    )
    .into())
}

pub struct NexusWatcher {}

impl NexusWatcher {
    /// Creates a new instance with default configuration
    pub fn builder() -> NexusWatcherBuilder {
        NexusWatcherBuilder::default()
    }

    /// Loads the [WatcherConfig] from [WATCHER_CONFIG_FILE_NAME] in the given path and starts the Nexus Watcher.
    ///
    /// If no [WatcherConfig] file is found, it defaults to [NexusWatcher::start_from_daemon].
    ///
    /// ### Arguments
    ///
    /// - `config_dir`: the directory where the config file is expected to be
    /// - `shutdown_rx`: optional shutdown signal. If none is provided, a default one will be created, listening for Ctrl-C.
    pub async fn start_from_path(
        config_dir: PathBuf,
        shutdown_rx: Option<Receiver<bool>>,
    ) -> Result<(), DynError> {
        let shutdown_rx = shutdown_rx.unwrap_or_else(create_shutdown_rx);

        match WatcherConfig::load(config_dir.join(WATCHER_CONFIG_FILE_NAME)).await {
            Ok(config) => NexusWatcherBuilder(config).start(Some(shutdown_rx)).await,
            Err(_) => NexusWatcher::start_from_daemon(config_dir, Some(shutdown_rx)).await,
        }
    }

    /// Derives the [WatcherConfig] from [DaemonConfig] (nexusd service config), loads it and starts the Watcher.
    ///
    /// If a [DaemonConfig] is not found, a new one is created in the given path with the default contents.
    ///
    /// ### Arguments
    ///
    /// - `config_dir`: the directory where the config file is expected to be
    /// - `shutdown_rx`: optional shutdown signal. If none is provided, a default one will be created, listening for Ctrl-C.
    pub async fn start_from_daemon(
        config_dir: PathBuf,
        shutdown_rx: Option<Receiver<bool>>,
    ) -> Result<(), DynError> {
        let daemon_config = DaemonConfig::read_or_create_config_file(config_dir).await?;
        let watcher_config = WatcherConfig::from(daemon_config);
        NexusWatcherBuilder(watcher_config).start(shutdown_rx).await
    }

    pub async fn start(
        mut shutdown_rx: Receiver<bool>,
        config: WatcherConfig,
    ) -> Result<(), DynError> {
        debug!(?config, "Running NexusWatcher with ");

        let config_hs = PubkyId::try_from(config.homeserver.as_ref())?;
        check_default_homeserver_allowed(&config_hs, HomeserverAllowlist::current())?;
        Homeserver::persist_if_unknown(config_hs).await?;

        let mut interval = tokio::time::interval(Duration::from_millis(config.watcher_sleep));
        let ev_processor_runner = EventProcessorRunner::from_config(&config, shutdown_rx.clone());

        loop {
            tokio::select! {
                _ = shutdown_rx.changed() => {
                    info!("SIGINT received, exiting Nexus Watcher loop");
                    break;
                }
                _ = interval.tick() => {
                    debug!("Indexing homeservers…");
                    _ = ev_processor_runner
                        .run_all()
                        .await
                        .inspect_err(|e| error!("Failed to start event processors run: {e}"));
                }
            }
        }
        info!("Nexus Watcher shut down gracefully");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::check_default_homeserver_allowed;
    use nexus_common::models::homeserver::HomeserverAllowlist;
    use pubky_app_specs::PubkyId;

    const PROD: &str = "8um71us3fyw6h8wbcxb5ar3rwusy1a6u49956ikzojg3gcwd1dty";
    const STAGING: &str = "ufibwbmed6jeq9k4p583go95wofakh9fwpp4k734trq79pd9u1uy";

    #[test]
    fn default_homeserver_must_be_allowed() {
        let default = PubkyId::try_from(STAGING).unwrap();
        assert!(
            check_default_homeserver_allowed(&default, &HomeserverAllowlist::default()).is_ok()
        );
        assert!(
            check_default_homeserver_allowed(&default, &HomeserverAllowlist::new([STAGING]))
                .is_ok()
        );
        let refused = check_default_homeserver_allowed(&default, &HomeserverAllowlist::new([PROD]));
        assert!(refused.unwrap_err().to_string().contains(STAGING));
    }
}
