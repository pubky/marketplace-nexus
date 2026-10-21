use crate::events::Moderation;
use crate::service::traits::{TEventProcessor, TEventProcessorRunner};
use crate::service::{processor::EventProcessor, HomeserverPollBackoff};
use nexus_common::models::homeserver::{Homeserver, HomeserverAllowlist};
use nexus_common::types::DynError;
use nexus_common::WatcherConfig;
use pubky_app_specs::PubkyId;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::watch::Receiver;

pub struct EventProcessorRunner {
    /// See [WatcherConfig::events_limit]
    pub limit: u32,
    /// See [WatcherConfig::monitored_homeservers_limit]
    pub monitored_homeservers_limit: usize,
    pub files_path: PathBuf,
    pub tracer_name: String,
    pub moderation: Arc<Moderation>,
    pub shutdown_rx: Receiver<bool>,
    /// See [WatcherConfig::homeserver]
    pub default_homeserver: PubkyId,
    pub poll_backoff: Arc<HomeserverPollBackoff>,
}

impl EventProcessorRunner {
    /// Creates a new instance from the provided configuration
    pub fn from_config(config: &WatcherConfig, shutdown_rx: Receiver<bool>) -> Self {
        Self {
            limit: config.events_limit,
            monitored_homeservers_limit: config.monitored_homeservers_limit,
            files_path: config.stack.files_path.clone(),
            tracer_name: config.name.clone(),
            moderation: Arc::new(Moderation {
                id: config.moderation_id.clone(),
                tags: config.moderated_tags.clone(),
                review_exclude_tags: config.review_exclude_tags.clone(),
            }),
            shutdown_rx,
            default_homeserver: config.homeserver.clone(),
            poll_backoff: Arc::new(HomeserverPollBackoff::default()),
        }
    }
}

/// Polling order: the known homeservers the allowlist permits (a homeserver already in the graph
/// from before the allowlist was set is skipped too), with the default first.
pub fn order_homeservers(
    mut hs_ids: Vec<String>,
    default_homeserver: &str,
    allowlist: &HomeserverAllowlist,
) -> Vec<String> {
    hs_ids.retain(|hs_id| allowlist.allows(hs_id));
    if let Some(default_pos) = hs_ids.iter().position(|hs_id| hs_id == default_homeserver) {
        let default_hs = hs_ids.remove(default_pos);
        hs_ids.insert(0, default_hs);
    }
    hs_ids
}

#[async_trait::async_trait]
impl TEventProcessorRunner for EventProcessorRunner {
    fn shutdown_rx(&self) -> Receiver<bool> {
        self.shutdown_rx.clone()
    }

    fn default_homeserver(&self) -> &str {
        &self.default_homeserver
    }

    fn monitored_homeservers_limit(&self) -> usize {
        self.monitored_homeservers_limit
    }

    async fn homeservers_by_priority(&self) -> Result<Vec<String>, DynError> {
        Ok(order_homeservers(
            Homeserver::get_all_from_graph().await?,
            self.default_homeserver(),
            HomeserverAllowlist::current(),
        ))
    }

    async fn pre_run_all(&self) -> Result<Vec<String>, DynError> {
        let hs_ids = self.homeservers_by_priority().await?;
        Ok(self
            .poll_backoff
            .eligible(&hs_ids, self.monitored_homeservers_limit))
    }

    /// Creates and returns a new event processor instance for the specified homeserver
    async fn build(&self, homeserver_id: String) -> Result<Arc<dyn TEventProcessor>, DynError> {
        let homeserver_id = PubkyId::try_from(&homeserver_id)?;
        let homeserver = Homeserver::get_by_id(homeserver_id)
            .await?
            .ok_or("Homeserver not found")?;

        // Create a new event processor instance with the specified homeserver
        Ok(Arc::new(EventProcessor {
            homeserver,
            limit: self.limit,
            files_path: self.files_path.clone(),
            tracer_name: self.tracer_name.clone(),
            moderation: self.moderation.clone(),
            shutdown_rx: self.shutdown_rx.clone(),
            poll_backoff: self.poll_backoff.clone(),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::order_homeservers;
    use nexus_common::models::homeserver::HomeserverAllowlist;

    fn ids(values: &[&str]) -> Vec<String> {
        values.iter().map(|v| v.to_string()).collect()
    }

    #[test]
    fn empty_allowlist_polls_every_known_homeserver_default_first() {
        let ordered =
            order_homeservers(ids(&["a", "b", "c"]), "b", &HomeserverAllowlist::default());
        assert_eq!(ordered, ids(&["b", "a", "c"]));
    }

    #[test]
    fn restricted_allowlist_drops_known_homeservers_it_does_not_list() {
        let allowlist = HomeserverAllowlist::new(["a", "c"]);
        let ordered = order_homeservers(ids(&["a", "b", "c"]), "c", &allowlist);
        assert_eq!(ordered, ids(&["c", "a"]));
    }

    #[test]
    fn restricted_allowlist_can_leave_nothing_to_poll() {
        let allowlist = HomeserverAllowlist::new(["z"]);
        assert!(order_homeservers(ids(&["a", "b"]), "a", &allowlist).is_empty());
    }
}
