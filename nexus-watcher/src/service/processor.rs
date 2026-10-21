use nexus_common::models::event::{Event, EventProcessorError};

use crate::events::handle;
use crate::events::retry::event::RetryEvent;
use crate::events::Moderation;
use crate::service::traits::TEventProcessor;
use crate::service::{HomeserverPollBackoff, POLL_TIMEOUT_SECS, RATE_LIMIT_BACKOFF_SECS};
use nexus_common::db::PubkyConnector;
use nexus_common::models::homeserver::Homeserver;
use opentelemetry::trace::{FutureExt, Span, TraceContextExt, Tracer};
use opentelemetry::{global, Context, KeyValue};
use pubky::{Method, PublicKey};
use pubky_app_specs::PubkyId;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch::Receiver;
use tracing::{debug, error, info, warn};

pub struct EventProcessor {
    pub homeserver: Homeserver,
    /// See [WatcherConfig::events_limit]
    pub limit: u32,
    pub files_path: PathBuf,
    pub tracer_name: String,
    pub moderation: Arc<Moderation>,
    pub shutdown_rx: Receiver<bool>,
    pub poll_backoff: Arc<HomeserverPollBackoff>,
}

#[async_trait::async_trait]
impl TEventProcessor for EventProcessor {
    fn get_homeserver_id(&self) -> PubkyId {
        self.homeserver.id.clone()
    }

    async fn run_internal(self: Arc<Self>) -> Result<(), EventProcessorError> {
        let maybe_event_lines = {
            let tracer = global::tracer(self.tracer_name.clone());
            let span = tracer.start("Polling Events");
            let cx = Context::new().with_span(span);
            self.poll_events()
                .with_context(cx)
                .await
                .inspect_err(|e| error!("Error polling events: {e:?}"))?
        };

        match maybe_event_lines {
            None => debug!("No new events"),
            Some(event_lines) => {
                info!("Processing {} event lines", event_lines.len());
                self.process_event_lines(event_lines).await?;
            }
        }

        Ok(())
    }
}

impl EventProcessor {
    /// Polls new events from the homeserver.
    ///
    /// It sends a GET request to the homeserver's events endpoint
    /// using the current cursor and a specified limit. It retrieves new event
    /// URIs in a newline-separated format, processes it into a vector of strings,
    /// and returns the result.
    async fn poll_events(&self) -> Result<Option<Vec<String>>, EventProcessorError> {
        debug!("Polling new events from homeserver");

        let response_text = {
            let pubky = PubkyConnector::get()?;
            let url = format!(
                "https://{}/events/?cursor={}&limit={}",
                self.homeserver.id, self.homeserver.cursor, self.limit
            );

            // Hard deadline on the poll: pkarr resolution or a connection to a
            // dead homeserver can otherwise hang until the processor-level
            // timeout (an hour, sized for large batch processing), starving
            // every other homeserver's processor in the meantime. Observed in
            // production on 2026-08-21: the watcher went silent for 70 minutes
            // on unreachable homeservers.
            let fetch = async {
                let response = pubky
                    .client()
                    .request(Method::GET, &url)
                    .send()
                    .await
                    .map_err(|e| EventProcessorError::client_error(e.to_string()))?;

                response
                    .text()
                    .await
                    .map_err(|e| EventProcessorError::client_error(e.to_string()))
            };
            tokio::time::timeout(std::time::Duration::from_secs(POLL_TIMEOUT_SECS), fetch)
                .await
                .map_err(|_| {
                    EventProcessorError::client_error(format!(
                        "events poll timed out after {POLL_TIMEOUT_SECS}s"
                    ))
                })??
        };

        if Self::defer_rate_limited_feed(
            &response_text,
            self.homeserver.id.as_ref(),
            &self.poll_backoff,
        ) {
            return Ok(None);
        }

        let lines: Vec<String> = response_text.trim().lines().map(String::from).collect();
        debug!("Homeserver response lines {:?}", lines);

        if lines.is_empty() || (lines.len() == 1 && lines[0].is_empty()) {
            return Ok(None);
        }

        Ok(Some(lines))
    }

    fn is_rate_limit_response(response_text: &str) -> bool {
        response_text
            .trim()
            .eq_ignore_ascii_case("rate limit exceeded")
    }

    fn defer_rate_limited_feed(
        response_text: &str,
        homeserver_id: &str,
        poll_backoff: &HomeserverPollBackoff,
    ) -> bool {
        if !Self::is_rate_limit_response(response_text) {
            return false;
        }

        warn!(
            retry_after_secs = RATE_LIMIT_BACKOFF_SECS,
            homeserver = homeserver_id,
            "Homeserver rate-limited events poll; preserving cursor and deferring only this feed"
        );
        poll_backoff.defer(homeserver_id, Duration::from_secs(RATE_LIMIT_BACKOFF_SECS));
        true
    }

    /// Processes a batch of event lines retrieved from the homeserver.
    ///
    /// This function iterates over a vector of event URIs, handling each line based on its content:
    /// - Lines starting with `cursor:` update the cursor for the homeserver and save it to the index.
    /// - Other lines are parsed into events and processed accordingly. If parsing fails, an error is logged.
    ///
    /// # Parameters
    /// - `lines`: A vector of strings representing event lines retrieved from the homeserver.
    pub async fn process_event_lines(&self, lines: Vec<String>) -> Result<(), EventProcessorError> {
        let mut hosting_by_user: HashMap<PubkyId, UserHosting> = HashMap::new();
        for line in &lines {
            let id = self.homeserver.id.clone();

            if *self.shutdown_rx.borrow() {
                debug!("Shutdown detected while processing HS {id}, exiting event processing loop");
                return Ok(());
            }

            if let Some(cursor) = line.strip_prefix("cursor: ") {
                info!("Received cursor for the next request: {cursor}");
                match Homeserver::try_from_cursor(id, cursor) {
                    Ok(hs) => hs.put_to_index().await?,
                    Err(e) => warn!("{e}"),
                }
            } else {
                let maybe_event = Event::parse_event(line, self.files_path.clone())
                    .inspect_err(|e| error!("{e}"))
                    .unwrap_or(None);

                if let Some(event) = maybe_event {
                    let user_id = &event.parsed_uri.user_id;
                    let hosting = match hosting_by_user.get(user_id) {
                        Some(hosting) => *hosting,
                        None => {
                            let hosting = self.user_hosting(user_id).await?;
                            hosting_by_user.insert(user_id.clone(), hosting);
                            hosting
                        }
                    };
                    if hosting != UserHosting::Here {
                        warn!(
                            homeserver = %id,
                            user_id = %user_id,
                            hosting = ?hosting,
                            "Skipping event of a user this homeserver does not host: {}",
                            event.uri
                        );
                        continue;
                    }

                    let tracer = global::tracer(self.tracer_name.clone());
                    let mut span = tracer.start(event.parsed_uri.resource.to_string());
                    span.set_attribute(KeyValue::new("event.uri", event.uri.clone()));
                    span.set_attribute(KeyValue::new("event.type", event.event_type.to_string()));
                    span.set_attribute(KeyValue::new(
                        "event.user_id",
                        event.parsed_uri.user_id.to_string(),
                    ));
                    span.set_attribute(KeyValue::new(
                        "event.resource_id",
                        event.parsed_uri.resource.id().unwrap_or("".to_string()),
                    ));
                    let cx = Context::new().with_span(span);
                    debug!("Processing event: {:?}", event);
                    self.handle_event(&event).with_context(cx).await?;
                }
            }
        }

        Ok(())
    }

    /// Where the user's own Pkarr record says they are hosted, relative to this homeserver.
    ///
    /// A homeserver's event feed is only authoritative for the users it hosts: any homeserver
    /// can list a line for any user's path, so a line for a user hosted elsewhere is not theirs.
    async fn user_hosting(&self, user_id: &PubkyId) -> Result<UserHosting, EventProcessorError> {
        let Ok(user_pk) = user_id.parse::<PublicKey>() else {
            return Ok(UserHosting::Unresolved);
        };
        let pubky = PubkyConnector::get()?;
        let resolved = pubky.get_homeserver_of(&user_pk).await.map(|hs| hs.z32());
        Ok(UserHosting::classify(
            resolved.as_deref(),
            self.homeserver.id.as_ref(),
        ))
    }

    /// Processes an event and track the fail event it if necessary
    /// # Parameters:
    /// - `event`: The event to be processed
    async fn handle_event(&self, event: &Event) -> Result<(), EventProcessorError> {
        if let Err(e) = handle(event, self.moderation.clone()).await {
            if let Some((index_key, retry_event)) = extract_retry_event_info(event, e) {
                error!("{}, {}", retry_event.error_type, index_key);
                if let Err(err) = retry_event.put_to_index(index_key).await {
                    error!("Failed to put event to retry index: {}", err);
                }
            }
        }
        Ok(())
    }
}

/// A user's homeserver, as their Pkarr record publishes it, relative to the polled homeserver.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UserHosting {
    /// The user's record names the polled homeserver.
    Here,
    /// The user's record names another homeserver.
    Elsewhere,
    /// No homeserver could be resolved for the user. Their records can't be fetched either, so
    /// their PUTs could not be applied; a DEL is not taken on the polled homeserver's word.
    Unresolved,
}

impl UserHosting {
    pub fn classify(resolved_homeserver: Option<&str>, polled_homeserver: &str) -> Self {
        match resolved_homeserver {
            Some(hs) if hs == polled_homeserver => UserHosting::Here,
            Some(_) => UserHosting::Elsewhere,
            None => UserHosting::Unresolved,
        }
    }
}

/// Extracts retry-related information from an event and its associated error
///
/// # Parameters
/// - `event`: Reference to the event for which retry information is being extracted
/// - `error`: Determines whether the event is eligible for a retry or should be discarded
fn extract_retry_event_info(
    event: &Event,
    error: EventProcessorError,
) -> Option<(String, RetryEvent)> {
    let retry_event = match error {
        EventProcessorError::InvalidEventLine(ref message) => {
            error!("{}", message);
            return None;
        }
        _ => RetryEvent::new(error),
    };

    // Generate a compress index to save in the cache
    let index = match RetryEvent::generate_index_key(&event.uri) {
        Some(retry_index) => retry_index,
        None => {
            return None;
        }
    };
    Some((format!("{}:{}", event.event_type, index), retry_event))
}

#[cfg(test)]
mod tests {
    use super::{EventProcessor, HomeserverPollBackoff, UserHosting};

    #[test]
    fn events_are_only_applied_for_users_hosted_on_the_polled_homeserver() {
        assert_eq!(
            UserHosting::classify(Some("polled"), "polled"),
            UserHosting::Here
        );
        assert_eq!(
            UserHosting::classify(Some("other"), "polled"),
            UserHosting::Elsewhere
        );
        assert_eq!(
            UserHosting::classify(None, "polled"),
            UserHosting::Unresolved
        );
    }

    #[test]
    fn recognizes_plain_text_events_rate_limit_response() {
        assert!(EventProcessor::is_rate_limit_response(
            "Rate limit exceeded"
        ));
        assert!(EventProcessor::is_rate_limit_response(
            " \nrate limit exceeded\r\n"
        ));
        assert!(!EventProcessor::is_rate_limit_response(
            "PUT pubky://example\ncursor: 42"
        ));
        assert!(!EventProcessor::is_rate_limit_response(
            "rate limit exceeded; retry later"
        ));
    }

    #[test]
    fn rate_limited_feed_is_deferred_without_blocking_a_healthy_feed() {
        let backoff = HomeserverPollBackoff::default();
        let stored_cursor = "0000000000042";

        assert!(EventProcessor::defer_rate_limited_feed(
            "Rate limit exceeded",
            "rate-limited",
            &backoff
        ));
        assert_eq!(stored_cursor, "0000000000042");
        assert!(!backoff.is_allowed("rate-limited"));
        assert!(backoff.is_allowed("healthy"));
    }
}
