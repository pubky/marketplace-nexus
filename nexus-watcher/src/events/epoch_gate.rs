//! The epoch gate: a user's social data is read through one spec epoch, the one their `epoch`
//! names, while their marketplace records are read in every epoch.
//!
//! The contract is the one `pubky-nexus` runs (its `events/gate.rs`):
//!
//! - A user at `v0` is read through `pubky.app`. Their `social/v1` writes are the migrator's
//!   copies of objects already indexed from `pubky.app`, and are ignored, except the PUT of their
//!   `social/v1/profile.json`: the migrator writes the profile last, so its PUT ends the
//!   migration and flips the user to `v1`.
//! - A user at `v1` is read through `social/v1` only. Their `pubky.app` social writes, PUT or
//!   DEL, are ignored: a client that deletes a migrated object deletes it in both epochs, so the
//!   `social/v1` DEL is the one that counts.
//!
//! The marketplace records under `/pub/pubky.app/marketplace/` have no `social/v1` form; they
//! are handled for a user at either epoch.
//!
//! A flip that fails leaves the user at `v0` and waits in the retry queue
//! ([`crate::events::retry`]), retried without limit while they are at `v0`. While it waits,
//! the user's `social/v1` writes are not the migrator's copies but their own edits, or the
//! moderator's takedowns and lifts: they are deferred behind the flip and replayed once it
//! settles, as in `pubky-nexus`. Unlike there, a waiting user's `pubky.app` DEL is handled at
//! once: this watcher's flip moves no content a DEL could race, only ids it rewrites in place.

use crate::events::epoch_metrics;
use crate::events::retry::queue::RetryQueue;
use nexus_common::models::event::{Event, EventProcessorError, EventType};
use nexus_common::models::user::Epoch;
use pubky_app_specs::Resource;

/// What the gate does with an event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Gate {
    /// The event goes on to its handler.
    Handle,
    /// A `v0` user's `social/v1` profile PUT: the user moves to `v1` (see
    /// [`crate::events::flip`]) with this profile.
    Flip,
    /// The event is skipped: no fetch, no stored event line, no error.
    Ignore,
    /// A `v0` user's `social/v1` write while their flip waits in the retry queue: held there
    /// until the flip settles, then run through the gate again.
    Defer,
}

/// The address of `user_id`'s `social/v1` profile: where their flip waits in the retry queue.
pub fn flip_profile_uri(user_id: &str) -> String {
    pubky_social_specs::user_uri_builder(user_id.to_string())
}

/// Whether `event`, decided `gate` for a user at `user_epoch`, waits for that user's flip when
/// one is queued: a `v0` user's `social/v1` write. Their profile never waits: its PUT is the
/// flip itself, and its DEL would take the flip's place at the same address in the queue.
pub fn waits_for_flip(event: &Event, gate: Gate, user_epoch: Epoch) -> bool {
    gate == Gate::Ignore
        && user_epoch == Epoch::V0
        && event.epoch == Epoch::V1
        && event.parsed_uri.resource != Resource::User
}

/// Whether `resource`, at a `pubky.app` address, is a marketplace record. Exhaustive, so a
/// resource the specs crate adds later must be placed on one side.
fn is_marketplace(resource: &Resource) -> bool {
    match resource {
        Resource::Shop
        | Resource::Listing(_)
        | Resource::Drop(_)
        | Resource::MarketplaceReview(_)
        | Resource::ReviewResponse(_)
        | Resource::AttestorProfile
        | Resource::AttestorAnnotation(_) => true,
        Resource::User
        | Resource::Post(_)
        | Resource::Follow(_)
        | Resource::Mute(_)
        | Resource::Bookmark(_)
        | Resource::Tag(_)
        | Resource::File(_)
        | Resource::Blob(_)
        | Resource::Feed(_)
        | Resource::LastRead
        | Resource::Unknown => false,
    }
}

/// The gate's decision for `event` of a user at `user_epoch`. Pure: the caller reads the epoch.
pub fn decide(event: &Event, user_epoch: Epoch) -> Gate {
    if event.epoch == Epoch::V0 && is_marketplace(&event.parsed_uri.resource) {
        return Gate::Handle;
    }
    match (event.epoch, user_epoch) {
        (Epoch::V0, Epoch::V0) | (Epoch::V1, Epoch::V1) => Gate::Handle,
        (Epoch::V0, Epoch::V1) => Gate::Ignore,
        (Epoch::V1, Epoch::V0)
            if event.event_type == EventType::Put
                && event.parsed_uri.resource == Resource::User =>
        {
            Gate::Flip
        }
        (Epoch::V1, Epoch::V0) => Gate::Ignore,
    }
}

/// The gate's decision for `event`, by its user's epoch in the graph and, for an event that
/// [waits for a flip](waits_for_flip), whether `retry_queue` holds that flip. A marketplace
/// record is decided without a read.
pub async fn check(event: &Event, retry_queue: &RetryQueue) -> Result<Gate, EventProcessorError> {
    if event.epoch == Epoch::V0 && is_marketplace(&event.parsed_uri.resource) {
        return Ok(Gate::Handle);
    }
    let user_epoch = Epoch::get_from_graph(&event.parsed_uri.user_id).await?;
    let gate = decide(event, user_epoch);
    if waits_for_flip(event, gate, user_epoch) {
        let flip_uri = flip_profile_uri(&event.parsed_uri.user_id);
        if retry_queue.flip_queued(&flip_uri).await? {
            epoch_metrics::record_deferred(event, &flip_uri);
            return Ok(Gate::Defer);
        }
    }
    if gate == Gate::Ignore {
        epoch_metrics::record_gated(event, user_epoch);
    }
    Ok(gate)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    const USER: &str = "operrr8wsbpr3ue9d4qj41ge1kcc6r7fdiy6o3ugjrrhi4y77rdo";
    const OTHER: &str = "8pinxxgqs41n4aididenw5apqp1urfmzdztr8jt4abrkdn435ewo";
    const TAG_ID: &str = "8Z8CWH8NVYQY39ZEBFGKQWWEKG";

    fn event(line: &str) -> Event {
        Event::parse_event(line, PathBuf::new())
            .unwrap_or_else(|e| panic!("{line}: {e}"))
            .unwrap_or_else(|| panic!("{line}: skipped"))
    }

    fn lines(paths: &[String]) -> Vec<String> {
        ["PUT", "DEL"]
            .iter()
            .flat_map(|kind| {
                paths
                    .iter()
                    .map(move |path| format!("{kind} pubky://{USER}{path}"))
            })
            .collect()
    }

    fn pubky_app_social_lines() -> Vec<String> {
        lines(&[
            "/pub/pubky.app/profile.json".to_string(),
            "/pub/pubky.app/posts/0032SSN7Q4EVG".to_string(),
            format!("/pub/pubky.app/follows/{OTHER}"),
            format!("/pub/pubky.app/tags/{TAG_ID}"),
            "/pub/pubky.app/files/0032SSN7Q4EVG".to_string(),
        ])
    }

    fn marketplace_lines() -> Vec<String> {
        lines(&[
            "/pub/pubky.app/marketplace/v1/shop.json".to_string(),
            "/pub/pubky.app/marketplace/v1/listings/0032SSN7Q4EVG".to_string(),
            "/pub/pubky.app/marketplace/v1/drops/0032SSN7Q4EVG".to_string(),
        ])
    }

    fn social_v1_lines() -> Vec<String> {
        lines(&[
            "/pub/social/v1/profile.json".to_string(),
            format!("/pub/social/v1/follows/{OTHER}.json"),
            format!("/pub/social/v1/tags/{TAG_ID}.json"),
        ])
    }

    #[test]
    fn a_v0_user_is_read_through_pubky_app() {
        for line in pubky_app_social_lines() {
            assert_eq!(decide(&event(&line), Epoch::V0), Gate::Handle, "{line}");
        }
    }

    #[test]
    fn a_v1_user_has_pubky_app_social_events_ignored() {
        for line in pubky_app_social_lines() {
            assert_eq!(decide(&event(&line), Epoch::V1), Gate::Ignore, "{line}");
        }
    }

    #[test]
    fn marketplace_records_are_handled_in_every_epoch() {
        for line in marketplace_lines() {
            for epoch in [Epoch::V0, Epoch::V1] {
                assert_eq!(
                    decide(&event(&line), epoch),
                    Gate::Handle,
                    "{line} at {epoch}"
                );
            }
        }
    }

    /// All but the profile PUT, which flips the user.
    #[test]
    fn a_v0_user_has_social_v1_events_ignored_but_the_profile_put_flips() {
        let profile_put = format!("PUT pubky://{USER}/pub/social/v1/profile.json");
        for line in social_v1_lines() {
            let expected = if line == profile_put {
                Gate::Flip
            } else {
                Gate::Ignore
            };
            assert_eq!(decide(&event(&line), Epoch::V0), expected, "{line}");
        }
    }

    #[test]
    fn a_v1_user_has_social_v1_events_handled() {
        for line in social_v1_lines() {
            assert_eq!(decide(&event(&line), Epoch::V1), Gate::Handle, "{line}");
        }
    }

    /// Only a `v0` user's `social/v1` writes other than their profile wait for a queued flip:
    /// never a `pubky.app` event, a marketplace record, a profile event or a `v1` user's event.
    #[test]
    fn only_a_v0_users_social_v1_writes_but_the_profile_wait_for_a_flip() {
        let profile_lines = [
            format!("PUT pubky://{USER}/pub/social/v1/profile.json"),
            format!("DEL pubky://{USER}/pub/social/v1/profile.json"),
        ];
        for line in social_v1_lines() {
            let event = event(&line);
            let waits = waits_for_flip(&event, decide(&event, Epoch::V0), Epoch::V0);
            assert_eq!(waits, !profile_lines.contains(&line), "{line} at v0");
            assert!(
                !waits_for_flip(&event, decide(&event, Epoch::V1), Epoch::V1),
                "{line} at v1"
            );
        }
        for line in pubky_app_social_lines()
            .into_iter()
            .chain(marketplace_lines())
        {
            let event = event(&line);
            for epoch in [Epoch::V0, Epoch::V1] {
                assert!(
                    !waits_for_flip(&event, decide(&event, epoch), epoch),
                    "{line} at {epoch}"
                );
            }
        }
    }

    #[test]
    fn the_flip_waits_at_the_address_of_the_users_social_v1_profile() {
        let line = format!("PUT pubky://{USER}/pub/social/v1/profile.json");
        assert_eq!(flip_profile_uri(USER), event(&line).uri);
    }

    #[test]
    fn only_the_social_v1_profile_put_flips() {
        let line = format!("PUT pubky://{USER}/pub/pubky.app/profile.json");
        assert_eq!(decide(&event(&line), Epoch::V0), Gate::Handle);
        let line = format!("PUT pubky://{USER}/pub/social/v1/profile.json");
        assert_eq!(
            decide(&event(&line), Epoch::V1),
            Gate::Handle,
            "a v1 user's profile edit"
        );
    }
}
