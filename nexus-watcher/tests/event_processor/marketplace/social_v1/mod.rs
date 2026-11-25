//! Two-epoch Social v1 indexing against a test homeserver, Neo4j and Redis: users read through
//! `pubky.app` until their `social/v1/profile.json` flips them, then through `social/v1` only,
//! while their marketplace records index in every epoch.

mod follows;
mod invalid;
mod migration;
mod moderation;
mod new_seller;
mod tags;

use crate::event_processor::utils::default_moderation_tests;
use crate::event_processor::utils::watcher::WatcherTest;
use anyhow::Result;
use nexus_common::db::fetch_key_from_graph;
use nexus_common::db::graph::Query;
use nexus_common::models::event::EventProcessorError;
use nexus_watcher::events::Moderation;
use pubky::{Keypair, ResourcePath};
use pubky_app_specs::PubkyId;
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
