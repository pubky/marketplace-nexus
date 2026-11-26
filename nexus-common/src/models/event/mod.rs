mod errors;

use crate::db::{kv::RedisResult, RedisOps};
use crate::models::user::Epoch;
use pubky_app_specs::{ParsedUri, PubkyId, Resource};
use pubky_social_specs::{ParsedUri as SocialParsedUri, Resource as SocialResource};
use serde::{Deserialize, Serialize};
use std::{fmt, path::PathBuf};
use tracing::{debug, error};

pub use errors::EventProcessorError;

/// Encrypted digital-delivery files. They are not public records, can be tens
/// of megabytes, and Nexus never indexes them, so their events are dropped
/// before any handler can fetch the body.
pub const MARKETPLACE_DELIVERABLES_PATH: &str = "/pub/pubky.app/marketplace/v1/deliverables/";

/// Homeservers list every write in `/events`, including files of other apps
/// such as Paykit. Nexus indexes this app's namespace and the public objects
/// of the social spec.
const PUBKY_APP_PATH: &str = "/pub/pubky.app/";

/// The public namespace of the social spec, every epoch of it. The spec reader
/// judges the epoch segment and the resource.
const SOCIAL_PATH: &str = "/pub/social/";

fn uri_path(uri: &str) -> Option<&str> {
    uri.strip_prefix("pubky://")
        .and_then(|rest| rest.find('/').map(|slash| &rest[slash..]))
}

fn is_marketplace_deliverable(uri: &str) -> bool {
    uri_path(uri).is_some_and(|path| path.starts_with(MARKETPLACE_DELIVERABLES_PATH))
}

fn is_social(uri: &str) -> bool {
    uri_path(uri).is_some_and(|path| path.starts_with(SOCIAL_PATH))
}

fn is_other_app(uri: &str) -> bool {
    uri_path(uri).is_some_and(|path| !path.starts_with(PUBKY_APP_PATH))
}

/// The user and resource of a `/pub/social/` address in the vocabulary of the
/// `pubky.app` reader, for the three public `social/v1` objects the
/// marketplace indexer reads: the profile, a follow and a tag. `None` for a
/// resource it does not index (posts, files, feeds, other kinds, another
/// epoch). The spec parser refuses an address that is not the canonical
/// spelling of its resource.
fn social_v1_address(uri: &str) -> Result<Option<ParsedUri>, EventProcessorError> {
    let parsed = SocialParsedUri::try_from(uri).map_err(|e| {
        EventProcessorError::InvalidEventLine(format!("Cannot parse event URI: {e}"))
    })?;
    let resource = match &parsed.resource {
        SocialResource::User => Resource::User,
        SocialResource::Follow(followee_id) => Resource::Follow(pubky_app_id(followee_id)?),
        SocialResource::Tag(tag_id) => Resource::Tag(tag_id.clone()),
        other => {
            debug!("Skipping a social/v1 resource the marketplace indexer does not read ({other}): {uri}");
            return Ok(None);
        }
    };
    Ok(Some(ParsedUri {
        user_id: pubky_app_id(&parsed.user_id)?,
        resource,
    }))
}

fn pubky_app_id(id: &pubky_social_specs::PubkyId) -> Result<PubkyId, EventProcessorError> {
    PubkyId::try_from(id.as_ref()).map_err(EventProcessorError::InvalidEventLine)
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum EventType {
    Put,
    Del,
}

impl fmt::Display for EventType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let upper_case_str = match self {
            EventType::Put => "PUT",
            EventType::Del => "DEL",
        };
        write!(f, "{upper_case_str}")
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event {
    pub uri: String,
    pub event_type: EventType,
    /// The user and resource the event addresses. A `social/v1` address is
    /// named in the same vocabulary as a `pubky.app` one (its profile is
    /// [`Resource::User`], a follow [`Resource::Follow`], a tag
    /// [`Resource::Tag`]); [`Event::epoch`] says which reader its record takes.
    pub parsed_uri: ParsedUri,
    /// The spec epoch the address is written in.
    pub epoch: Epoch,
    pub files_path: PathBuf,
    event_line: String,
}

impl RedisOps for Event {}

impl AsRef<[String]> for Event {
    fn as_ref(&self) -> &[String] {
        std::slice::from_ref(&self.event_line)
    }
}

impl Event {
    /// Parse event based on event line returned by homeservers' /events endpoint.
    /// - line - event line string
    /// - files_path - path to the directory where files are stored on nexus
    pub fn parse_event(
        line: &str,
        files_path: PathBuf,
    ) -> Result<Option<Self>, EventProcessorError> {
        debug!("New event: {}", line);
        let parts: Vec<&str> = line.split(' ').collect();
        if parts.len() != 2 {
            return Err(EventProcessorError::InvalidEventLine(format!(
                "Malformed event line, {line}"
            )));
        }

        let event_type = match parts[0] {
            "PUT" => Ok(EventType::Put),
            "DEL" => Ok(EventType::Del),
            other => Err(EventProcessorError::InvalidEventLine(format!(
                "Unknown event type: {other}"
            ))),
        }?;

        // Validate and parse the URI using pubky-app-specs
        let uri = parts[1].to_string();
        if is_marketplace_deliverable(&uri) {
            debug!("Skipping {event_type} for a marketplace deliverable");
            return Ok(None);
        }
        if is_social(&uri) {
            let Some(parsed_uri) = social_v1_address(&uri)? else {
                return Ok(None);
            };
            return Ok(Some(Event {
                uri,
                event_type,
                parsed_uri,
                epoch: Epoch::V1,
                files_path,
                event_line: line.to_string(),
            }));
        }
        if is_other_app(&uri) {
            debug!("Skipping {event_type} for another app: {uri}");
            return Ok(None);
        }
        let parsed_uri = ParsedUri::try_from(uri.as_str()).map_err(|e| {
            EventProcessorError::InvalidEventLine(format!("Cannot parse event URI: {e}"))
        })?;

        match parsed_uri.resource {
            // Unknown resource
            Resource::Unknown => {
                return Err(EventProcessorError::InvalidEventLine(format!(
                    "Unknown resource in URI: {uri}"
                )))
            }
            // Known resources not handled by Nexus
            Resource::LastRead | Resource::Feed(_) | Resource::Blob(_) => return Ok(None),
            _ => (),
        };

        let event_line = line.to_string();

        Ok(Some(Event {
            uri,
            event_type,
            parsed_uri,
            epoch: Epoch::V0,
            files_path,
            event_line,
        }))
    }

    /// Stores event line in Redis as part of the events list.
    pub async fn store_event(&self) -> RedisResult<()> {
        self.put_index_list(&["Events"]).await
    }

    pub async fn get_events_from_redis(
        cursor: Option<u64>,
        limit: usize,
    ) -> RedisResult<(Vec<String>, u64)> {
        let start = cursor.unwrap_or(0);
        // Clamp to usize::MAX: on 32-bit targets u64 can exceed usize; the LRANGE
        // would return empty results for such a large index either way.
        let start_u = usize::try_from(start).unwrap_or(usize::MAX);
        let result = Event::try_from_index_list(&["Events"], Some(start_u), Some(limit)).await;

        let events = match result {
            Ok(r) => r.unwrap_or_default(),
            Err(error) => {
                error!("IndexReadFailed: Failed to read from list due to Redis error: {error}");
                return Err(error);
            }
        };

        let next_cursor = start + events.len() as u64;

        Ok((events, next_cursor))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const USER: &str = "operrr8wsbpr3ue9d4qj41ge1kcc6r7fdiy6o3ugjrrhi4y77rdo";

    fn line(kind: &str, path: &str) -> String {
        format!("{kind} pubky://{USER}{path}")
    }

    #[test]
    fn deliverable_put_skipped_without_body_read() {
        for path in [
            "/pub/pubky.app/marketplace/v1/deliverables/0f6c2b1e9d7a4c3b/1",
            "/pub/pubky.app/marketplace/v1/deliverables/0f6c2b1e9d7a4c3b/12",
            "/pub/pubky.app/marketplace/v1/deliverables/x",
        ] {
            let parsed = Event::parse_event(&line("PUT", path), PathBuf::new())
                .expect("a deliverable PUT is not an error");
            assert!(parsed.is_none(), "{path} yields no event to handle");
        }
    }

    #[test]
    fn deliverable_del_skipped() {
        let parsed = Event::parse_event(
            &line(
                "DEL",
                "/pub/pubky.app/marketplace/v1/deliverables/0f6c2b1e9d7a4c3b/1",
            ),
            PathBuf::new(),
        )
        .expect("a deliverable DEL is not an error");
        assert!(parsed.is_none());
    }

    #[test]
    fn deliverable_skip_is_prefix_exact() {
        let listing = Event::parse_event(
            &line("PUT", "/pub/pubky.app/marketplace/v1/listings/abc"),
            PathBuf::new(),
        )
        .expect("listing parses")
        .expect("listing is handled");
        assert_eq!(listing.parsed_uri.resource, Resource::Listing("abc".into()));

        for path in [
            "/pub/pubky.app/marketplace/v1/deliverablesx/abc/1",
            "/pub/pubky.app/marketplace/v1/media/abc",
        ] {
            assert!(
                matches!(
                    Event::parse_event(&line("PUT", path), PathBuf::new()),
                    Err(EventProcessorError::InvalidEventLine(_))
                ),
                "{path} keeps the unknown-resource refusal"
            );
        }
        assert!(!is_marketplace_deliverable(
            "https://example.com/pub/pubky.app/marketplace/v1/deliverables/a/1"
        ));
    }

    #[test]
    fn other_app_events_skipped() {
        for kind in ["PUT", "DEL"] {
            for path in [
                "/pub/paykit/v0/shared-state.bin",
                "/pub/paykit/v0/shared-state-writes/3cd20c5c24e8524e",
                "/pub/other.app/marketplace/v1/deliverables/abc/1",
                "/pub/pubky.appx/posts/0033SSE3B1FQ0",
            ] {
                let parsed = Event::parse_event(&line(kind, path), PathBuf::new())
                    .expect("another app's event is not an error");
                assert!(parsed.is_none(), "{kind} {path} yields no event to handle");
            }
        }
    }

    #[test]
    fn unknown_pubky_app_resource_still_refused() {
        assert!(matches!(
            Event::parse_event(&line("PUT", "/pub/pubky.app/unknown/abc"), PathBuf::new()),
            Err(EventProcessorError::InvalidEventLine(_))
        ));
    }

    const OTHER: &str = "8pinxxgqs41n4aididenw5apqp1urfmzdztr8jt4abrkdn435ewo";
    const TAG_ID: &str = "8Z8CWH8NVYQY39ZEBFGKQWWEKG";

    #[test]
    fn social_v1_profile_follow_and_tag_are_events_of_epoch_v1() {
        let cases = [
            ("/pub/social/v1/profile.json".to_string(), Resource::User),
            (
                format!("/pub/social/v1/follows/{OTHER}.json"),
                Resource::Follow(PubkyId::try_from(OTHER).unwrap()),
            ),
            (
                format!("/pub/social/v1/tags/{TAG_ID}.json"),
                Resource::Tag(TAG_ID.to_string()),
            ),
        ];
        for kind in ["PUT", "DEL"] {
            for (path, resource) in &cases {
                let event = Event::parse_event(&line(kind, path), PathBuf::new())
                    .expect("a social/v1 event parses")
                    .expect("a social/v1 profile, follow or tag is handled");
                assert_eq!(event.epoch, Epoch::V1, "{kind} {path}");
                assert_eq!(&event.parsed_uri.resource, resource, "{kind} {path}");
                assert_eq!(event.parsed_uri.user_id.to_string(), USER);
            }
        }
    }

    #[test]
    fn pubky_app_events_are_of_epoch_v0() {
        let event = Event::parse_event(&line("PUT", "/pub/pubky.app/profile.json"), PathBuf::new())
            .unwrap()
            .unwrap();
        assert_eq!(event.epoch, Epoch::V0);
        assert_eq!(event.parsed_uri.resource, Resource::User);
    }

    /// Posts, files and feeds are served by the social indexer; other epochs, an epoch-less
    /// path and a malformed leaf name no object this indexer reads.
    #[test]
    fn social_resources_the_marketplace_indexer_does_not_read_are_skipped() {
        for kind in ["PUT", "DEL"] {
            for path in [
                "/pub/social/v1/posts/0032SSN7Q4EVG/0032SSN7Q4EVJ.json".to_string(),
                format!("/pub/social/v1/files/{TAG_ID}.png"),
                format!("/pub/social/v1/feeds/{TAG_ID}.json"),
                "/pub/social/v1/Profile.json".to_string(),
                format!("/pub/social/v1/follows/{OTHER}"),
                format!("/pub/social/v1/tags/{}.json", TAG_ID.to_lowercase()),
                "/pub/social/v2/profile.json".to_string(),
                "/pub/social/profile.json".to_string(),
            ] {
                let parsed = Event::parse_event(&line(kind, &path), PathBuf::new())
                    .expect("a social path the indexer does not read is not an error");
                assert!(parsed.is_none(), "{kind} {path} yields no event to handle");
            }
        }
    }

    #[test]
    fn private_paths_are_never_events() {
        for kind in ["PUT", "DEL"] {
            for path in [
                format!("/priv/social/v1/mutes/{OTHER}.json"),
                format!("/priv/social/v1/feeds/{TAG_ID}.json"),
                "/priv/app.pubky/v1/last_read.json".to_string(),
                "/priv/pubky.app/marketplace/v2/s/abc".to_string(),
            ] {
                let parsed = Event::parse_event(&line(kind, &path), PathBuf::new())
                    .expect("a private path is not an error");
                assert!(parsed.is_none(), "{kind} {path} yields no event to handle");
            }
        }
    }

    #[test]
    fn non_canonical_social_v1_addresses_are_refused() {
        for path in [
            "/pub/social/v1/profile.json/".to_string(),
            "/pub/social/v1//profile.json".to_string(),
            "/pub/social/v1/%70rofile.json".to_string(),
            format!("/pub/social/v1/tags/{TAG_ID}.json?x=1"),
        ] {
            assert!(
                matches!(
                    Event::parse_event(&line("PUT", &path), PathBuf::new()),
                    Err(EventProcessorError::InvalidEventLine(_))
                ),
                "{path}"
            );
        }
    }
}
