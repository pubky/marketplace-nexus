//! Two-epoch Social v1 indexing against a test homeserver, Neo4j and Redis: users read through
//! `pubky.app` until their `social/v1/profile.json` flips them, then through `social/v1` only,
//! while their marketplace records index in every epoch.

mod flip_retry;
mod flip_transaction;
mod follows;
mod invalid;
mod migration;
mod migrator_copies;
mod moderation;
mod new_seller;
mod tags;
mod tombstones;

use crate::event_processor::utils::default_moderation_tests;
use crate::event_processor::utils::watcher::{HomeserverHashIdPath, HomeserverPath, WatcherTest};
use anyhow::Result;
use chrono::Utc;
use nexus_common::db::fetch_key_from_graph;
use nexus_common::db::graph::Query;
use nexus_common::models::event::EventProcessorError;
use nexus_common::models::user::{UserDetails, USER_DELETED_SENTINEL};
use nexus_watcher::events::Moderation;
use pubky::{Keypair, ResourcePath};
use pubky_app_specs::traits::HasIdPath as _;
use pubky_app_specs::{user_uri_builder, PubkyAppFollow, PubkyAppTag, PubkyAppUser, PubkyId};
use pubky_social_specs::traits::{HasIdPath, HasPath, HashId};
use pubky_social_specs::{PubkySocialFollow, PubkySocialTag, PubkySocialUser};
use std::sync::Arc;

pub(super) fn social_profile(name: &str) -> PubkySocialUser {
    PubkySocialUser::new(name.to_string(), None, None, None, None)
}

pub(super) fn profile_path() -> ResourcePath {
    PubkySocialUser::create_path().parse().unwrap()
}

pub(super) fn follow_path(followee_id: &str) -> ResourcePath {
    PubkySocialFollow::create_path(followee_id).parse().unwrap()
}

/// A `social/v1` tag as the spec builder makes it: the label folded, the target as given.
pub(super) fn social_tag(target: &str, label: &str) -> PubkySocialTag {
    PubkySocialTag::new(target.to_string(), label.to_string())
}

pub(super) fn tag_path(tag: &PubkySocialTag) -> ResourcePath {
    PubkySocialTag::create_path(&tag.create_id())
        .parse()
        .unwrap()
}

/// The event line of a PUT or DEL at `path` of `user_id`'s storage.
pub(super) fn event_line(kind: &str, user_id: &str, path: &ResourcePath) -> String {
    format!("{kind} pubky://{user_id}{path}")
}

/// Registers `keypair` on the test homeserver and writes only a `social/v1` profile.
pub(super) async fn create_social_user(
    test: &mut WatcherTest,
    keypair: &Keypair,
    name: &str,
) -> Result<String> {
    test.register_user(keypair).await?;
    test.put(keypair, &profile_path(), social_profile(name))
        .await?;
    Ok(keypair.public_key().to_z32())
}

pub(super) async fn follow(
    test: &mut WatcherTest,
    follower: &Keypair,
    followee_id: &str,
) -> Result<ResourcePath> {
    let path = follow_path(followee_id);
    test.put(follower, &path, PubkySocialFollow::new()).await?;
    Ok(path)
}

pub(super) async fn tag(
    test: &mut WatcherTest,
    tagger: &Keypair,
    target: &str,
    label: &str,
) -> Result<(PubkySocialTag, ResourcePath)> {
    let tag = social_tag(target, label);
    let path = tag_path(&tag);
    test.put(tagger, &path, &tag).await?;
    Ok((tag, path))
}

/// The `epoch` stored on the user's node; `None` when the node has none or is missing.
pub(super) async fn graph_epoch(user_id: &str) -> Result<Option<String>> {
    let query = Query::new(
        "test_user_epoch",
        "MATCH (u:User {id: $id}) RETURN u.epoch AS epoch",
    )
    .param("id", user_id);
    Ok(fetch_key_from_graph::<Option<String>>(query, "epoch")
        .await?
        .flatten())
}

/// Whether the user has a node in the graph.
pub(super) async fn graph_user_exists(user_id: &str) -> Result<bool> {
    let query = Query::new(
        "test_user_exists",
        "OPTIONAL MATCH (u:User {id: $id}) RETURN u IS NOT NULL AS present",
    )
    .param("id", user_id);
    Ok(fetch_key_from_graph::<bool>(query, "present")
        .await?
        .unwrap_or(false))
}

/// Whether the graph holds `follower_id` following `followee_id`.
pub(super) async fn follows_edge(follower_id: &str, followee_id: &str) -> Result<bool> {
    let query = Query::new(
        "test_follows_edge",
        "RETURN EXISTS((:User {id: $follower})-[:FOLLOWS]->(:User {id: $followee})) AS exist",
    )
    .param("follower", follower_id)
    .param("followee", followee_id);
    Ok(fetch_key_from_graph::<bool>(query, "exist")
        .await?
        .unwrap_or(false))
}

/// The ids of the `TAGGED` edges `tagger_id` holds with `label`.
pub(super) async fn tag_edge_ids(tagger_id: &str, label: &str) -> Result<Vec<String>> {
    let query = Query::new(
        "test_tag_edge_ids",
        "MATCH (:User {id: $tagger_id})-[t:TAGGED {label: $label}]->()
         RETURN collect(t.id) AS ids",
    )
    .param("tagger_id", tagger_id)
    .param("label", label);
    Ok(fetch_key_from_graph::<Vec<String>>(query, "ids")
        .await?
        .unwrap_or_default())
}

/// Installs a moderator of this test's own on its event processor runner, with the labels
/// of the default test moderation. The shared test moderator stays at `v0` for every other
/// test; a test that flips its moderator to `v1` must not move that one.
pub(super) fn own_moderator(test: &mut WatcherTest) -> (Keypair, Arc<Moderation>) {
    let keypair = Keypair::random();
    let defaults = default_moderation_tests();
    let moderation = Arc::new(Moderation {
        id: PubkyId::try_from(keypair.public_key().to_z32().as_str()).unwrap(),
        tags: defaults.tags,
        review_exclude_tags: defaults.review_exclude_tags,
    });
    test.event_processor_runner.moderation = moderation.clone();
    (keypair, moderation)
}

/// Asserts the user is a deleted user at `v1`: their node stays, with its epoch, and their
/// profile is the `[DELETED]` tombstone.
pub(super) async fn assert_v1_tombstone(user_id: &str) -> Result<()> {
    assert!(
        graph_user_exists(user_id).await?,
        "the node of {user_id} stays"
    );
    assert_eq!(graph_epoch(user_id).await?.as_deref(), Some("v1"));
    let details = UserDetails::get_by_id(user_id)
        .await?
        .expect("the tombstone profile is indexed");
    assert_eq!(details.name, USER_DELETED_SENTINEL);
    Ok(())
}

/// Writes, then deletes, what a client that predates the migration writes for `user_kp` under
/// `pubky.app`: a profile, a follow of `other_id` and a tag on `other_id`'s profile. Asserts
/// none is read: the user stays the `v1` tombstone.
pub(super) async fn assert_stale_pubky_app_writes_ignored(
    test: &mut WatcherTest,
    user_kp: &Keypair,
    other_id: &str,
) -> Result<()> {
    let user_id = user_kp.public_key().to_z32();
    let label = format!("stale{}", Utc::now().timestamp_micros() % 1_000_000);
    test.put(
        user_kp,
        &PubkyAppUser::hs_path(),
        PubkyAppUser {
            name: "Watcher:SocialV1:StaleClient".to_string(),
            bio: None,
            image: None,
            links: None,
            status: None,
        },
    )
    .await?;
    let follow_path: ResourcePath = PubkyAppFollow::create_path(other_id).parse()?;
    test.put(
        user_kp,
        &follow_path,
        PubkyAppFollow {
            created_at: Utc::now().timestamp_millis(),
        },
    )
    .await?;
    let stale_tag = PubkyAppTag {
        uri: user_uri_builder(other_id.to_string()),
        label: label.clone(),
        created_at: Utc::now().timestamp_millis(),
    };
    test.put(user_kp, &stale_tag.hs_path(), &stale_tag).await?;
    assert_v1_tombstone(&user_id).await?;
    assert!(!follows_edge(&user_id, other_id).await?);
    assert!(tag_edge_ids(&user_id, &label).await?.is_empty());

    test.del(user_kp, &stale_tag.hs_path()).await?;
    test.del(user_kp, &follow_path).await?;
    test.del(user_kp, &PubkyAppUser::hs_path()).await?;
    assert_v1_tombstone(&user_id).await
}

/// Asserts `result` is the error whose message starts with `kind` (`SpecValidation`,
/// `UnsupportedTarget`).
pub(super) fn assert_refused(result: Result<(), EventProcessorError>, kind: &str, what: &str) {
    match result {
        Err(error) => assert!(
            error.to_string().starts_with(&format!("{kind}:")),
            "{what}: expected {kind}, got {error}"
        ),
        Ok(()) => panic!("{what}: expected {kind}, the event was accepted"),
    }
}
