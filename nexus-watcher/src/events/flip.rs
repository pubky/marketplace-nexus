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
//! Follows are keyed by the two users and need no rewrite. Every step pages or batches through
//! the user's records and reruns to the same result, so a flip that fails part way leaves the
//! user at `v0` and their next `social/v1` profile PUT runs it again.

use crate::events::handlers;
use crate::events::remote_body::{fetch_record_capped, MAX_RECORD_BYTES};
use crate::events::social_v1;
use nexus_common::db::{fetch_all_rows_from_graph, fetch_key_from_graph, queries, GraphError};
use nexus_common::models::event::{Event, EventProcessorError};
use nexus_common::models::user::{Epoch, UserDetails};
use pubky_app_specs::traits::HashId as _;
use pubky_app_specs::{ParsedUri, PubkyAppTag, PubkyId, Resource};
use pubky_social_specs::traits::HashId as _;
use pubky_social_specs::PubkySocialTag;
use tracing::{debug, info, warn};

/// `TAGGED` edges read and rewritten per batch.
const EDGE_BATCH: usize = 500;

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

/// Migrates what the graph holds for `user_id` at `pubky.app` to its `social/v1` form, then
/// writes the epoch. The user's node must exist.
pub async fn move_to_v1(user_id: &PubkyId) -> Result<(), EventProcessorError> {
    rekey_tag_edges(user_id).await?;
    rekey_moderation_markers(user_id).await?;
    Epoch::set_v1_in_graph(user_id).await?;
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

async fn rekey_tag_edges(user_id: &PubkyId) -> Result<(), EventProcessorError> {
    let mut after = String::new();
    loop {
        let rows = fetch_all_rows_from_graph(queries::social_v1::tagger_social_edges_after(
            user_id, &after, EDGE_BATCH,
        ))
        .await?;
        let Some(last) = rows.last() else {
            return Ok(());
        };
        after = last.get::<String>("edge").map_err(GraphError::from)?;
        let mut changes = Vec::new();
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
                changes.push(vec![required("edge")?, new_id]);
            }
        }
        if !changes.is_empty() {
            fetch_key_from_graph::<i64>(
                queries::social_v1::set_tagger_edge_ids(user_id, changes),
                "changed",
            )
            .await?;
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

async fn rekey_moderation_markers(moderator_id: &PubkyId) -> Result<(), EventProcessorError> {
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
        let change = vec![tag_id, new_id, new_uri];
        match required("kind")?.as_str() {
            "ModeratedUser" => moderated_users.push(change),
            _ => excluded_reviewers.push(change),
        }
    }
    if !moderated_users.is_empty() {
        nexus_common::db::exec_single_row(queries::social_v1::rekey_moderated_user_markers(
            moderator_id,
            moderated_users,
        ))
        .await?;
    }
    if !excluded_reviewers.is_empty() {
        nexus_common::db::exec_single_row(queries::social_v1::rekey_excluded_reviewer_markers(
            moderator_id,
            excluded_reviewers,
        ))
        .await?;
    }
    Ok(())
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
