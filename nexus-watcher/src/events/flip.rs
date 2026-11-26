//! The flip: a user moves from `pubky.app` (`v0`) to `social/v1` (`v1`) when the watcher reads
//! their `/pub/social/v1/profile.json`, the last file the migrator writes.
//!
//! Before the epoch is written, the graph records whose id hashes an address the migrator
//! rewrote are moved to the id the user's `social/v1` copy carries, so the user's `social/v1`
//! untag or the moderator's `social/v1` tag deletion finds them:
//!
//! | Record the user wrote | `pubky.app` id hashes | Id after the flip |
//! |---|---|---|
//! | `TAGGED` edge on a user | `pubky.app/profile.json` | hash of `social/v1/profile.json` |
//! | `TAGGED` edge on a user | bare `pubky://<pk>` | kept, the migrator keeps the target |
//! | `TAGGED` edge on a post | any post address | hash of the versionless `social/v1` post |
//! | `TAGGED` edge on a listing or a shop | the marketplace address | kept, the migrator keeps it |
//! | `ModeratedUser`, `ExcludedReviewer` marker | `pubky.app/profile.json` | hash of `social/v1/profile.json` |
//! | Listing and review markers | the marketplace address | kept |
//!
//! Follows are keyed by the two users and need no rewrite. The user's edges are read in keyset
//! pages and their markers in one read; every rewrite and the epoch are then written by one
//! statement, one transaction, so a flip that fails leaves nothing half rewritten: the user
//! stays at `v0` with their `pubky.app` ids, and the retry queue runs it again
//! ([`flip_and_settle`]).

use crate::events::epoch_gate::flip_profile_uri;
use crate::events::epoch_metrics::{self, RetryOutcome};
use crate::events::handlers;
use crate::events::remote_body::{fetch_record_capped, MAX_RECORD_BYTES};
use crate::events::retry::queue::{RetryKind, RetryQueue};
use crate::events::social_v1;
use nexus_common::db::queries::social_v1::FlipRewrite;
use nexus_common::db::{fetch_all_rows_from_graph, fetch_key_from_graph, queries, GraphError};
use nexus_common::models::event::{Event, EventProcessorError};
use nexus_common::models::user::{Epoch, UserDetails};
use pubky_app_specs::traits::HashId as _;
use pubky_app_specs::{ParsedUri, PubkyAppTag, PubkyId, Resource};
use pubky_social_specs::traits::HashId as _;
use pubky_social_specs::PubkySocialTag;
use std::collections::HashSet;
use tracing::{debug, info, warn};

/// `TAGGED` edges read per keyset page.
const EDGE_BATCH: usize = 500;

/// Runs [`flip`] for `event` and settles the user's flip in `retry_queue`.
///
/// A flip that leaves the user at `v0` for a reason that can pass is a
/// [`EventProcessorError::FlipFailed`], which the caller queues: the user's `social/v1` writes
/// are deferred behind it. Otherwise the flip is settled, whatever `flip` returned: the user
/// moved to `v1`, or had no profile to flip with, or one the reader refuses for good. Their
/// queued flip, if any, is removed and the writes deferred behind it are woken, to run through
/// the gate again: at `v1` they are handled, at `v0` they are ignored as before.
///
/// A failed epoch read after the flip leaves it undecided, and is a `FlipFailed` too: the retry
/// reads the epoch again, and a user found at `v1` settles there.
pub async fn flip_and_settle(
    event: &Event,
    retry_queue: &RetryQueue,
) -> Result<(), EventProcessorError> {
    let result = flip(event).await;
    let user_id = &event.parsed_uri.user_id;
    let landed = match Epoch::get_from_graph(user_id).await {
        Ok(epoch) => epoch == Epoch::V1,
        Err(read_error) => {
            let cause = result.err().unwrap_or_else(|| read_error.into());
            return Err(EventProcessorError::FlipFailed(Box::new(cause)));
        }
    };
    match result {
        Err(error) if !landed && !error.is_permanent() => {
            Err(EventProcessorError::FlipFailed(Box::new(error)))
        }
        result => {
            let flip_uri = flip_profile_uri(user_id);
            let woken = retry_queue.resolve_flip(&flip_uri).await?;
            if woken > 0 {
                info!(%user_id, %flip_uri, woken, landed, "Flip settled; woke the writes deferred behind it");
                epoch_metrics::record_retry(RetryKind::Deferred, RetryOutcome::Woken, woken as u64);
            }
            result
        }
    }
}

/// Flips the user of `event`, a `social/v1` profile PUT of a user at `v0`.
///
/// A user already in the graph moves first, as in `pubky-nexus`: their records are migrated
/// and the epoch written before the profile is fetched, so a profile that is gone or fails the
/// reader leaves them at `v1` with the profile they had. A user new to the graph has no node to
/// hold an epoch, so a valid profile creates the node, then the epoch is written; a missing or
/// invalid one leaves them out of the graph.
pub async fn flip(event: &Event) -> Result<(), EventProcessorError> {
    let user_id = &event.parsed_uri.user_id;
    if Epoch::get_from_graph_if_present(user_id).await?.is_some() {
        move_to_v1(user_id).await?;
        return put_profile(event).await;
    }
    let Some(blob) = fetch_record_capped(&event.uri, MAX_RECORD_BYTES).await? else {
        debug!("Profile {} is already gone; no user to flip", event.uri);
        return Ok(());
    };
    let profile = social_v1::read_profile(&blob)?;
    handlers::user::sync_put_details(UserDetails::from_social_v1(profile, user_id)).await?;
    Epoch::set_v1_in_graph(user_id).await?;
    info!(%user_id, "User flipped to v1");
    Ok(())
}

/// Indexes the `social/v1` profile of `event`, a profile PUT of a user at `v1`.
pub async fn put_profile(event: &Event) -> Result<(), EventProcessorError> {
    let Some(blob) = fetch_record_capped(&event.uri, MAX_RECORD_BYTES).await? else {
        debug!("Profile {} is already gone from its homeserver", event.uri);
        return Ok(());
    };
    let profile = social_v1::read_profile(&blob)?;
    handlers::user::sync_put_details(UserDetails::from_social_v1(
        profile,
        &event.parsed_uri.user_id,
    ))
    .await
}

/// Migrates what the graph holds for `user_id` at `pubky.app` to its `social/v1` form and
/// writes the epoch, in one transaction. The user's node must exist.
pub async fn move_to_v1(user_id: &PubkyId) -> Result<(), EventProcessorError> {
    let (moderated_users, excluded_reviewers) = marker_changes(user_id).await?;
    let rewrite = FlipRewrite {
        edges: tag_edge_changes(user_id).await?,
        moderated_users,
        excluded_reviewers,
    };
    let changed =
        fetch_key_from_graph::<i64>(queries::social_v1::flip_to_v1(user_id, rewrite), "changed")
            .await?;
    if changed.unwrap_or(0) == 0 {
        return Err(EventProcessorError::generic(format!(
            "User {user_id} is not in the graph; its epoch cannot move to v1"
        )));
    }
    info!(%user_id, "User flipped to v1");
    Ok(())
}

/// One `TAGGED` edge on a user or a post, as the page reads it.
#[derive(Debug, Clone, PartialEq, Eq)]
struct StoredTag {
    id: String,
    label: String,
    target: EdgeTarget,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum EdgeTarget {
    User(String),
    /// A post; `author_id` is `None` when it has no `AUTHORED` author.
    Post {
        author_id: Option<String>,
        post_id: String,
    },
}

/// `[edge element id, id it holds, new id]` for every edge of `user_id` on a user or a post
/// whose id the flip changes, read in keyset pages of [`EDGE_BATCH`].
async fn tag_edge_changes(user_id: &PubkyId) -> Result<Vec<Vec<String>>, EventProcessorError> {
    let mut changes = Vec::new();
    let mut after = String::new();
    loop {
        let rows = fetch_all_rows_from_graph(queries::social_v1::tagger_social_edges_after(
            user_id, &after, EDGE_BATCH,
        ))
        .await?;
        let Some(last) = rows.last() else {
            return Ok(changes);
        };
        after = last.get::<String>("edge").map_err(GraphError::from)?;
        for row in &rows {
            let required = |key: &str| row.get::<String>(key).map_err(GraphError::from);
            let target = match required("kind")?.as_str() {
                "user" => EdgeTarget::User(required("target_id")?),
                _ => EdgeTarget::Post {
                    author_id: row
                        .get::<Option<String>>("author_id")
                        .map_err(GraphError::from)?,
                    post_id: required("target_id")?,
                },
            };
            let tag = StoredTag {
                id: required("id")?,
                label: required("label")?,
                target,
            };
            if let Some(new_id) = flipped_edge_id(user_id, &tag) {
                changes.push(vec![required("edge")?, tag.id, new_id]);
            }
        }
    }
}

/// The id `tag`, written by `owner`, takes after the flip; `None` when it keeps its id.
fn flipped_edge_id(owner: &PubkyId, tag: &StoredTag) -> Option<String> {
    match &tag.target {
        EdgeTarget::User(user_id) => {
            let v1_id = social_tag_id(&social_profile_uri(user_id), &tag.label);
            if tag.id == pubky_app_tag_id(&pubky_app_profile_uri(user_id), &tag.label) {
                Some(v1_id)
            } else {
                if tag.id != v1_id
                    && tag.id != pubky_app_tag_id(&bare_user_uri(user_id), &tag.label)
                {
                    warn!(
                        user_id = %owner,
                        tag_id = %tag.id,
                        label = %tag.label,
                        "Tag id on user {user_id} kept as it is: no spelling of the user reproduces it"
                    );
                }
                None
            }
        }
        EdgeTarget::Post {
            author_id: Some(author_id),
            post_id,
        } => {
            let v1_id = social_tag_id(
                &pubky_social_specs::post_uri_builder(author_id.clone(), post_id.clone()),
                &tag.label,
            );
            (tag.id != v1_id).then_some(v1_id)
        }
        EdgeTarget::Post {
            author_id: None,
            post_id,
        } => {
            warn!(
                user_id = %owner,
                tag_id = %tag.id,
                label = %tag.label,
                "Tag id on post {post_id} kept as it is: the post has no author"
            );
            None
        }
    }
}

/// The `ModeratedUser` and the `ExcludedReviewer` rows of [`FlipRewrite`] for the markers
/// `moderator_id`'s tags wrote.
async fn marker_changes(
    moderator_id: &PubkyId,
) -> Result<(Vec<Vec<String>>, Vec<Vec<String>>), EventProcessorError> {
    let rows =
        fetch_all_rows_from_graph(queries::social_v1::moderator_user_markers(moderator_id)).await?;
    let mut moderated_users = Vec::new();
    let mut excluded_reviewers = Vec::new();
    for row in &rows {
        let required = |key: &str| row.get::<String>(key).map_err(GraphError::from);
        let (tag_id, uri, label) = (required("tag_id")?, required("uri")?, required("label")?);
        let Some((new_id, new_uri)) = flipped_marker(&tag_id, &uri, &label) else {
            continue;
        };
        let change = (tag_id, new_id, new_uri);
        match required("kind")?.as_str() {
            "ModeratedUser" => moderated_users.push(change),
            _ => excluded_reviewers.push(change),
        }
    }
    Ok((
        marker_rows(moderated_users),
        marker_rows(excluded_reviewers),
    ))
}

/// `[old tag id, new tag id, new uri, "move" or "drop"]` for each `(old, new, uri)` marker
/// change: the first change to a new tag id moves its marker there, every later one drops its
/// marker, as one statement cannot move two markers to one `(moderator_id, tag_id)`.
fn marker_rows(changes: Vec<(String, String, String)>) -> Vec<Vec<String>> {
    let mut taken = HashSet::new();
    changes
        .into_iter()
        .map(|(old, new, uri)| {
            let action = if taken.insert(new.clone()) {
                "move"
            } else {
                "drop"
            };
            vec![old, new, uri, action.to_string()]
        })
        .collect()
}

/// The tag id and uri a user marker takes after its moderator's flip: the moderator's tag on a
/// user's `pubky.app` profile was rewritten by the migrator to the user's `social/v1` profile.
/// `None` for a marker that keeps its id: a bare `pubky://<pk>` or a `social/v1` target.
fn flipped_marker(tag_id: &str, uri: &str, label: &str) -> Option<(String, String)> {
    let parsed = ParsedUri::try_from(uri).ok()?;
    if parsed.resource != Resource::User || uri == bare_user_uri(&parsed.user_id) {
        return None;
    }
    if tag_id != pubky_app_tag_id(uri, label) {
        return None;
    }
    let new_uri = social_profile_uri(&parsed.user_id);
    Some((social_tag_id(&new_uri, label), new_uri))
}

fn pubky_app_profile_uri(user_id: &str) -> String {
    pubky_app_specs::user_uri_builder(user_id.to_string())
}

fn social_profile_uri(user_id: &str) -> String {
    pubky_social_specs::user_uri_builder(user_id.to_string())
}

fn bare_user_uri(user_id: &str) -> String {
    format!("pubky://{user_id}")
}

/// The id of a `pubky.app` tag file on `target` with `label`.
fn pubky_app_tag_id(target: &str, label: &str) -> String {
    PubkyAppTag {
        uri: target.to_string(),
        label: label.to_string(),
        created_at: 0,
    }
    .create_id()
}

/// The id of a `social/v1` tag file on `target` with `label`.
fn social_tag_id(target: &str, label: &str) -> String {
    PubkySocialTag {
        uri: target.to_string(),
        label: label.to_string(),
        created_at: 0,
        extra: Default::default(),
    }
    .create_id()
}

#[cfg(test)]
mod tests {
    use super::*;

    const OWNER: &str = "operrr8wsbpr3ue9d4qj41ge1kcc6r7fdiy6o3ugjrrhi4y77rdo";
    const OTHER: &str = "8pinxxgqs41n4aididenw5apqp1urfmzdztr8jt4abrkdn435ewo";
    const POST_ID: &str = "0032SSN7Q4EVG";
    const LABEL: &str = "trusted";

    fn owner() -> PubkyId {
        PubkyId::try_from(OWNER).unwrap()
    }

    fn user_edge(id: String) -> StoredTag {
        StoredTag {
            id,
            label: LABEL.to_string(),
            target: EdgeTarget::User(OTHER.to_string()),
        }
    }

    fn post_edge(id: String, author_id: Option<&str>) -> StoredTag {
        StoredTag {
            id,
            label: LABEL.to_string(),
            target: EdgeTarget::Post {
                author_id: author_id.map(str::to_string),
                post_id: POST_ID.to_string(),
            },
        }
    }

    #[test]
    fn a_profile_tag_takes_the_id_of_the_social_v1_profile() {
        let tag = user_edge(pubky_app_tag_id(&pubky_app_profile_uri(OTHER), LABEL));
        assert_eq!(
            flipped_edge_id(&owner(), &tag),
            Some(social_tag_id(&social_profile_uri(OTHER), LABEL))
        );
    }

    #[test]
    fn a_bare_user_tag_and_an_already_flipped_one_keep_their_id() {
        for id in [
            pubky_app_tag_id(&bare_user_uri(OTHER), LABEL),
            social_tag_id(&social_profile_uri(OTHER), LABEL),
        ] {
            assert_eq!(
                flipped_edge_id(&owner(), &user_edge(id.clone())),
                None,
                "{id}"
            );
        }
    }

    #[test]
    fn a_post_tag_takes_the_id_of_the_versionless_social_v1_post() {
        let v1_id = social_tag_id(
            &pubky_social_specs::post_uri_builder(OTHER.into(), POST_ID.into()),
            LABEL,
        );
        let v0_id = pubky_app_tag_id(
            &pubky_app_specs::post_uri_builder(OTHER.into(), POST_ID.into()),
            LABEL,
        );
        assert_ne!(v0_id, v1_id);
        assert_eq!(
            flipped_edge_id(&owner(), &post_edge(v0_id, Some(OTHER))),
            Some(v1_id.clone())
        );
        assert_eq!(
            flipped_edge_id(&owner(), &post_edge(v1_id, Some(OTHER))),
            None
        );
    }

    #[test]
    fn a_post_tag_without_an_author_keeps_its_id() {
        let id = pubky_app_tag_id(
            &pubky_app_specs::post_uri_builder(OTHER.into(), POST_ID.into()),
            LABEL,
        );
        assert_eq!(flipped_edge_id(&owner(), &post_edge(id, None)), None);
    }

    #[test]
    fn a_marker_on_a_pubky_app_profile_moves_to_the_social_v1_profile() {
        let uri = pubky_app_profile_uri(OTHER);
        let id = pubky_app_tag_id(&uri, LABEL);
        let new_uri = social_profile_uri(OTHER);
        assert_eq!(
            flipped_marker(&id, &uri, LABEL),
            Some((social_tag_id(&new_uri, LABEL), new_uri))
        );
    }

    #[test]
    fn markers_on_other_targets_or_already_moved_keep_their_id() {
        let bare = bare_user_uri(OTHER);
        assert_eq!(
            flipped_marker(&pubky_app_tag_id(&bare, LABEL), &bare, LABEL),
            None
        );
        let v1 = social_profile_uri(OTHER);
        assert_eq!(flipped_marker(&social_tag_id(&v1, LABEL), &v1, LABEL), None);
        let listing = pubky_app_specs::listing_uri_builder(OTHER.into(), POST_ID.into());
        assert_eq!(
            flipped_marker(&pubky_app_tag_id(&listing, LABEL), &listing, LABEL),
            None
        );
    }

    #[test]
    fn only_the_first_marker_moving_to_a_tag_id_moves_and_the_rest_drop() {
        let change = |old: &str, new: &str| (old.to_string(), new.to_string(), "uri".to_string());
        let row = |old: &str, new: &str, action: &str| {
            vec![old, new, "uri", action]
                .into_iter()
                .map(str::to_string)
                .collect::<Vec<_>>()
        };
        assert_eq!(
            marker_rows(vec![change("a", "x"), change("b", "y"), change("c", "x")]),
            vec![
                row("a", "x", "move"),
                row("b", "y", "move"),
                row("c", "x", "drop")
            ]
        );
        assert!(marker_rows(Vec::new()).is_empty());
    }

    /// The tag file the migrator writes for the `pubky.app` tag on `target`, read back as the
    /// id in its `social/v1` path. The `pubky.app` id is the one the watcher stored on the edge.
    fn migrated_tag_id(target: &str) -> (String, String) {
        let tag = PubkyAppTag {
            uri: target.to_string(),
            label: LABEL.to_string(),
            created_at: 1_700_000_000_000,
        };
        let v0_id = tag.create_id();
        let bytes = serde_json::to_vec(&tag).unwrap();
        let ctx = pubky_social_specs::migrate::MigrationCtx::new(
            pubky_social_specs::PubkyId::try_from(OWNER).unwrap(),
        );
        let migrated = pubky_social_specs::migrate::transform(
            &format!("pub/pubky.app/tags/{v0_id}"),
            &bytes,
            &ctx,
        )
        .unwrap_or_else(|skipped| panic!("the migrator wrote no file for {target}: {skipped}"));
        let [(path, _)] = migrated.writes.as_slice() else {
            panic!(
                "the migrator wrote {} files for {target}",
                migrated.writes.len()
            );
        };
        let v1_id = path
            .strip_prefix("pub/social/v1/tags/")
            .and_then(|rest| rest.strip_suffix(".json"))
            .unwrap_or_else(|| panic!("not a social/v1 tag path: {path}"));
        (v0_id, v1_id.to_string())
    }

    /// The agreement with the migrator: for every target a `pubky.app` tag of this index can
    /// name, the id the flip leaves on the edge is the id of the file the migrator writes for
    /// that tag, so the user's `social/v1` untag of the copy finds the edge.
    #[test]
    fn every_flipped_edge_takes_the_id_of_the_migrators_copy() {
        let user = || EdgeTarget::User(OTHER.to_string());
        let post_of = |author: &str| EdgeTarget::Post {
            author_id: Some(author.to_string()),
            post_id: POST_ID.to_string(),
        };
        let cases = [
            (pubky_app_profile_uri(OTHER), Some(user())),
            (bare_user_uri(OTHER), Some(user())),
            (
                pubky_app_specs::post_uri_builder(OTHER.into(), POST_ID.into()),
                Some(post_of(OTHER)),
            ),
            (
                pubky_app_specs::post_uri_builder(OWNER.into(), POST_ID.into()),
                Some(post_of(OWNER)),
            ),
            (
                pubky_app_specs::listing_uri_builder(OTHER.into(), POST_ID.into()),
                None,
            ),
            (pubky_app_specs::shop_uri_builder(OTHER.into()), None),
            (
                pubky_app_specs::marketplace_review_uri_builder(OTHER.into(), "0".repeat(32)),
                None,
            ),
        ];
        for (target, edge_target) in cases {
            let (v0_id, migrated_id) = migrated_tag_id(&target);
            let flipped = match edge_target {
                Some(target) => flipped_edge_id(
                    &owner(),
                    &StoredTag {
                        id: v0_id.clone(),
                        label: LABEL.to_string(),
                        target,
                    },
                )
                .unwrap_or(v0_id),
                None => v0_id,
            };
            assert_eq!(flipped, migrated_id, "{target}");
        }
    }

    /// The same agreement for a moderator's user markers: the takedown on a `pubky.app`
    /// profile moves to the id of the migrator's copy, and one on a bare key keeps it.
    #[test]
    fn every_flipped_marker_takes_the_id_of_the_migrators_copy() {
        for uri in [pubky_app_profile_uri(OTHER), bare_user_uri(OTHER)] {
            let (v0_id, migrated_id) = migrated_tag_id(&uri);
            let flipped = flipped_marker(&v0_id, &uri, LABEL).map_or(v0_id, |(id, _)| id);
            assert_eq!(flipped, migrated_id, "{uri}");
        }
    }

    /// The migrator keeps a listing, shop or review address, so the tag file it writes there
    /// has the id the `pubky.app` one had: those records need no rewrite.
    #[test]
    fn a_marketplace_target_has_one_id_in_both_epochs() {
        for uri in [
            pubky_app_specs::listing_uri_builder(OTHER.into(), POST_ID.into()),
            pubky_app_specs::shop_uri_builder(OTHER.into()),
            pubky_app_specs::marketplace_review_uri_builder(OTHER.into(), "0".repeat(32)),
        ] {
            assert_eq!(
                pubky_app_tag_id(&uri, LABEL),
                social_tag_id(&uri, LABEL),
                "{uri}"
            );
        }
    }
}
