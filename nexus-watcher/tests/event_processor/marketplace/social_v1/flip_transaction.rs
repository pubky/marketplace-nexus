use super::super::moderation::{seller_profile, MODERATED_LABEL};
use super::{graph_epoch, own_moderator, profile_path, social_profile, social_tag, tag};
use crate::event_processor::utils::watcher::{HomeserverHashIdPath, WatcherTest};
use anyhow::Result;
use chrono::Utc;
use nexus_common::db::graph::Query;
use nexus_common::db::queries::social_v1::{flip_to_v1, FlipRewrite};
use nexus_common::db::{exec_single_row, fetch_all_rows_from_graph, fetch_key_from_graph};
use nexus_common::models::marketplace::ModeratedUser;
use pubky::Keypair;
use pubky_app_specs::traits::HashId;
use pubky_app_specs::{user_uri_builder, PubkyAppTag, PubkyId};
use pubky_social_specs::traits::HashId as _;
use std::collections::BTreeMap;

/// More `TAGGED` edges than one keyset page of the flip reads.
const EDGES_PAST_ONE_PAGE: usize = 1_100;

/// The id the watcher stored for a `pubky.app` tag on `target_id`'s `pubky.app` profile.
fn pubky_app_profile_tag_id(target_id: &str, label: &str) -> String {
    PubkyAppTag {
        uri: user_uri_builder(target_id.to_string()),
        label: label.to_string(),
        created_at: 0,
    }
    .create_id()
}

/// The id of the `social/v1` tag on `target_id`'s `social/v1` profile.
fn social_profile_tag_id(target_id: &str, label: &str) -> String {
    social_tag(
        &pubky_social_specs::user_uri_builder(target_id.to_string()),
        label,
    )
    .create_id()
}

/// `count` labels unique to one test run, with the prefix they share.
fn run_labels(prefix: &str, count: usize) -> (String, Vec<String>) {
    let run = format!("{prefix}{}x", Utc::now().timestamp_micros() % 1_000_000);
    let labels = (0..count).map(|i| format!("{run}{i}")).collect();
    (run, labels)
}

/// Writes one `TAGGED` edge from `tagger_id` to `target_id` per `[id, label]` row, as the
/// tag PUT query writes it.
async fn seed_user_tags(tagger_id: &str, target_id: &str, rows: Vec<Vec<String>>) -> Result<()> {
    let query = Query::new(
        "test_seed_user_tags",
        "MATCH (tagger:User {id: $tagger_id}), (target:User {id: $target_id})
         UNWIND $rows AS row
         CREATE (tagger)-[:TAGGED {id: row[0], label: row[1], indexed_at: 0}]->(target)",
    )
    .param("tagger_id", tagger_id)
    .param("target_id", target_id)
    .param("rows", rows);
    exec_single_row(query).await?;
    Ok(())
}

/// The id of each of `tagger_id`'s `TAGGED` edges whose label starts with `prefix`, by label.
async fn edge_ids_by_label(tagger_id: &str, prefix: &str) -> Result<BTreeMap<String, String>> {
    let query = Query::new(
        "test_edge_ids_by_label",
        "MATCH (:User {id: $tagger_id})-[t:TAGGED]->()
         WHERE t.label STARTS WITH $prefix
         RETURN t.label AS label, t.id AS id",
    )
    .param("tagger_id", tagger_id)
    .param("prefix", prefix);
    let mut ids = BTreeMap::new();
    for row in fetch_all_rows_from_graph(query).await? {
        ids.insert(row.get::<String>("label")?, row.get::<String>("id")?);
    }
    Ok(ids)
}

async fn remove_seeded_tags(tagger_id: &str, prefix: &str) -> Result<()> {
    let query = Query::new(
        "test_remove_seeded_tags",
        "MATCH (:User {id: $tagger_id})-[t:TAGGED]->()
         WHERE t.label STARTS WITH $prefix
         DELETE t",
    )
    .param("tagger_id", tagger_id)
    .param("prefix", prefix);
    exec_single_row(query).await?;
    Ok(())
}

/// The tag ids of the `ModeratedUser` markers `moderator_id` holds on `user_id`, sorted.
async fn user_marker_tag_ids(moderator_id: &str, user_id: &str) -> Result<Vec<String>> {
    let query = Query::new(
        "test_user_marker_tag_ids",
        "MATCH (m:ModeratedUser {moderator_id: $moderator_id, user_id: $user_id})
         WITH m.tag_id AS tag_id ORDER BY tag_id
         RETURN collect(tag_id) AS ids",
    )
    .param("moderator_id", moderator_id)
    .param("user_id", user_id);
    Ok(fetch_key_from_graph::<Vec<String>>(query, "ids")
        .await?
        .unwrap_or_default())
}

async fn remove_user_markers(moderator_id: &str) -> Result<()> {
    let query = Query::new(
        "test_remove_user_markers",
        "MATCH (m:ModeratedUser {moderator_id: $moderator_id}) DETACH DELETE m",
    )
    .param("moderator_id", moderator_id);
    exec_single_row(query).await?;
    Ok(())
}

/// A user with more edges than one keyset page flips with every edge on a user's profile
/// moved to the id of its `social/v1` copy: the pages are all read before the one statement
/// that writes them and the epoch.
#[tokio_shared_rt::test(shared)]
async fn a_flip_rekeys_more_than_one_page_of_edges() -> Result<()> {
    let mut test = WatcherTest::setup().await?;
    let seller_kp = Keypair::random();
    let seller_id = test
        .create_user(&seller_kp, &seller_profile("Watcher:SocialV1:ManyTags"))
        .await?;
    let friend_kp = Keypair::random();
    let friend_id = test
        .create_user(&friend_kp, &seller_profile("Watcher:SocialV1:ManyTagged"))
        .await?;
    let (prefix, labels) = run_labels("bulk", EDGES_PAST_ONE_PAGE);
    let rows = labels
        .iter()
        .map(|label| vec![pubky_app_profile_tag_id(&friend_id, label), label.clone()])
        .collect();
    seed_user_tags(&seller_id, &friend_id, rows).await?;
    assert_eq!(
        edge_ids_by_label(&seller_id, &prefix).await?.len(),
        EDGES_PAST_ONE_PAGE
    );

    test.put(
        &seller_kp,
        &profile_path(),
        social_profile("Watcher:SocialV1:ManyTagsFlipped"),
    )
    .await?;
    assert_eq!(graph_epoch(&seller_id).await?.as_deref(), Some("v1"));
    let expected: BTreeMap<String, String> = labels
        .iter()
        .map(|label| (label.clone(), social_profile_tag_id(&friend_id, label)))
        .collect();
    assert_eq!(edge_ids_by_label(&seller_id, &prefix).await?, expected);

    remove_seeded_tags(&seller_id, &prefix).await?;
    test.del(&seller_kp, &profile_path()).await?;
    test.cleanup_user(&friend_kp).await?;
    Ok(())
}

/// The flip statement writes all or nothing: when one of its marker moves breaks the
/// `(moderator_id, tag_id)` uniqueness, the edge ids it already set and the epoch are rolled
/// back with it. Marking the second move `drop` lets the same statement through.
#[tokio_shared_rt::test(shared)]
async fn a_failed_flip_statement_writes_nothing() -> Result<()> {
    let mut test = WatcherTest::setup().await?;
    let user_kp = Keypair::random();
    let user_id = test
        .create_user(&user_kp, &seller_profile("Watcher:SocialV1:FailedFlip"))
        .await?;
    let friend_kp = Keypair::random();
    let friend_id = test
        .create_user(
            &friend_kp,
            &seller_profile("Watcher:SocialV1:FailedFlipFriend"),
        )
        .await?;
    let (prefix, labels) = run_labels("atomic", 3);
    seed_user_tags(
        &user_id,
        &friend_id,
        labels
            .iter()
            .map(|label| vec![pubky_app_profile_tag_id(&friend_id, label), label.clone()])
            .collect(),
    )
    .await?;
    let before = edge_ids_by_label(&user_id, &prefix).await?;
    let edges: Vec<Vec<String>> = fetch_all_rows_from_graph(
        Query::new(
            "test_seeded_edges",
            "MATCH (:User {id: $user_id})-[t:TAGGED]->()
             WHERE t.label STARTS WITH $prefix
             RETURN elementId(t) AS edge, t.id AS id, t.label AS label",
        )
        .param("user_id", user_id.as_str())
        .param("prefix", prefix.as_str()),
    )
    .await?
    .iter()
    .map(|row| -> Result<Vec<String>> {
        let label = row.get::<String>("label")?;
        Ok(vec![
            row.get::<String>("edge")?,
            row.get::<String>("id")?,
            social_profile_tag_id(&friend_id, &label),
        ])
    })
    .collect::<Result<_>>()?;
    let new_uri = pubky_social_specs::user_uri_builder(friend_id.clone());
    let new_marker_id = social_profile_tag_id(&friend_id, MODERATED_LABEL);
    for old in ["old-marker-a", "old-marker-b"] {
        ModeratedUser::put(
            &friend_id,
            &user_uri_builder(friend_id.clone()),
            &user_id,
            old,
            MODERATED_LABEL,
            0,
        )
        .await?;
    }
    let rewrite = |second: &str| FlipRewrite {
        edges: edges.clone(),
        moderated_users: vec![
            vec![
                "old-marker-a".to_string(),
                new_marker_id.clone(),
                new_uri.clone(),
                "move".to_string(),
            ],
            vec![
                "old-marker-b".to_string(),
                new_marker_id.clone(),
                new_uri.clone(),
                second.to_string(),
            ],
        ],
        excluded_reviewers: Vec::new(),
    };
    let failed =
        fetch_key_from_graph::<i64>(flip_to_v1(&user_id, rewrite("move")), "changed").await;
    assert!(
        failed.is_err(),
        "two markers moved to one tag id: {failed:?}"
    );
    assert_eq!(edge_ids_by_label(&user_id, &prefix).await?, before);
    assert_eq!(
        user_marker_tag_ids(&user_id, &friend_id).await?,
        ["old-marker-a", "old-marker-b"]
    );
    assert_eq!(graph_epoch(&user_id).await?, None);

    let changed =
        fetch_key_from_graph::<i64>(flip_to_v1(&user_id, rewrite("drop")), "changed").await?;
    assert_eq!(changed, Some(1));
    let expected: BTreeMap<String, String> = labels
        .iter()
        .map(|label| (label.clone(), social_profile_tag_id(&friend_id, label)))
        .collect();
    assert_eq!(edge_ids_by_label(&user_id, &prefix).await?, expected);
    assert_eq!(
        user_marker_tag_ids(&user_id, &friend_id).await?,
        [new_marker_id]
    );
    assert_eq!(graph_epoch(&user_id).await?.as_deref(), Some("v1"));

    remove_user_markers(&user_id).await?;
    remove_seeded_tags(&user_id, &prefix).await?;
    test.cleanup_user(&user_kp).await?;
    test.cleanup_user(&friend_kp).await?;
    Ok(())
}

/// A user left half moved, as a flip of an earlier build that wrote page by page could leave
/// them (some edges and a marker at their `social/v1` id, the rest at their `pubky.app` id, no
/// epoch), is finished by the flip, and running the flip again changes nothing.
#[tokio_shared_rt::test(shared)]
async fn a_flip_over_a_partial_flip_finishes_it_and_reruns_to_the_same_graph() -> Result<()> {
    let mut test = WatcherTest::setup().await?;
    let user_kp = Keypair::random();
    let user_id = test
        .create_user(&user_kp, &seller_profile("Watcher:SocialV1:PartialFlip"))
        .await?;
    let friend_kp = Keypair::random();
    let friend_id = test
        .create_user(
            &friend_kp,
            &seller_profile("Watcher:SocialV1:PartialFriend"),
        )
        .await?;
    let (prefix, labels) = run_labels("partial", 6);
    let rows = labels
        .iter()
        .enumerate()
        .map(|(i, label)| {
            let id = if i % 2 == 0 {
                social_profile_tag_id(&friend_id, label)
            } else {
                pubky_app_profile_tag_id(&friend_id, label)
            };
            vec![id, label.clone()]
        })
        .collect();
    seed_user_tags(&user_id, &friend_id, rows).await?;
    let pubky_app_uri = user_uri_builder(friend_id.clone());
    let moved_label = MODERATED_LABEL;
    ModeratedUser::put(
        &friend_id,
        &pubky_social_specs::user_uri_builder(friend_id.clone()),
        &user_id,
        &social_profile_tag_id(&friend_id, moved_label),
        moved_label,
        0,
    )
    .await?;
    ModeratedUser::put(
        &friend_id,
        &pubky_app_uri,
        &user_id,
        &pubky_app_profile_tag_id(&friend_id, "review_fraud"),
        "review_fraud",
        0,
    )
    .await?;
    assert_eq!(graph_epoch(&user_id).await?, None);

    let user = PubkyId::try_from(user_id.as_str()).map_err(anyhow::Error::msg)?;
    nexus_watcher::events::flip::move_to_v1(&user).await?;
    let expected: BTreeMap<String, String> = labels
        .iter()
        .map(|label| (label.clone(), social_profile_tag_id(&friend_id, label)))
        .collect();
    let mut expected_markers = vec![
        social_profile_tag_id(&friend_id, moved_label),
        social_profile_tag_id(&friend_id, "review_fraud"),
    ];
    expected_markers.sort();
    assert_eq!(edge_ids_by_label(&user_id, &prefix).await?, expected);
    assert_eq!(
        user_marker_tag_ids(&user_id, &friend_id).await?,
        expected_markers
    );
    assert_eq!(graph_epoch(&user_id).await?.as_deref(), Some("v1"));

    nexus_watcher::events::flip::move_to_v1(&user).await?;
    assert_eq!(edge_ids_by_label(&user_id, &prefix).await?, expected);
    assert_eq!(
        user_marker_tag_ids(&user_id, &friend_id).await?,
        expected_markers
    );
    assert_eq!(graph_epoch(&user_id).await?.as_deref(), Some("v1"));

    remove_user_markers(&user_id).await?;
    remove_seeded_tags(&user_id, &prefix).await?;
    test.cleanup_user(&user_kp).await?;
    test.cleanup_user(&friend_kp).await?;
    Ok(())
}

/// A moderator at `v0` who hid a user twice, through the user's `pubky.app` profile and through
/// a `pubky.app` tag on their `social/v1` profile, holds two markers; the second already has the
/// id the first moves to. The moderator's flip keeps one marker under that id, the user stays
/// hidden, and the DEL of the `social/v1` copy releases them.
#[tokio_shared_rt::test(shared)]
async fn colliding_takedowns_keep_one_marker_across_the_moderators_flip() -> Result<()> {
    let mut test = WatcherTest::setup().await?;
    let (moderator_kp, _) = own_moderator(&mut test);
    let moderator_id = test
        .create_user(
            &moderator_kp,
            &seller_profile("Watcher:SocialV1:CollidingModerator"),
        )
        .await?;
    let user_kp = Keypair::random();
    let user_id = test
        .create_user(
            &user_kp,
            &seller_profile("Watcher:SocialV1:CollidingTarget"),
        )
        .await?;
    let social_uri = pubky_social_specs::user_uri_builder(user_id.clone());
    let v1_marker_id = social_profile_tag_id(&user_id, MODERATED_LABEL);

    for uri in [user_uri_builder(user_id.clone()), social_uri.clone()] {
        let takedown = PubkyAppTag {
            uri,
            label: MODERATED_LABEL.to_string(),
            created_at: Utc::now().timestamp_millis(),
        };
        test.put(&moderator_kp, &takedown.hs_path(), &takedown)
            .await?;
    }
    let mut both = vec![
        pubky_app_profile_tag_id(&user_id, MODERATED_LABEL),
        v1_marker_id.clone(),
    ];
    both.sort();
    assert_eq!(user_marker_tag_ids(&moderator_id, &user_id).await?, both);
    assert!(ModeratedUser::is_moderated(&user_id).await?);

    let (_, v1_takedown_path) = tag(&mut test, &moderator_kp, &social_uri, MODERATED_LABEL).await?;
    test.put(
        &moderator_kp,
        &profile_path(),
        social_profile("Watcher:SocialV1:CollidingModeratorV1"),
    )
    .await?;
    assert_eq!(graph_epoch(&moderator_id).await?.as_deref(), Some("v1"));
    assert_eq!(
        user_marker_tag_ids(&moderator_id, &user_id).await?,
        [v1_marker_id]
    );
    assert!(ModeratedUser::is_moderated(&user_id).await?);

    test.del(&moderator_kp, &v1_takedown_path).await?;
    assert!(user_marker_tag_ids(&moderator_id, &user_id)
        .await?
        .is_empty());
    assert!(!ModeratedUser::is_moderated(&user_id).await?);

    test.cleanup_user(&user_kp).await?;
    test.del(&moderator_kp, &profile_path()).await?;
    Ok(())
}
