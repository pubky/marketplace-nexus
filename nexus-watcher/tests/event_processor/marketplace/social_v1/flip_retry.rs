//! A flip that fails waits in the retry queue and is retried until it lands; the user's
//! `social/v1` writes in the meantime are deferred behind it and applied once it does. Run
//! against the test homeserver, Neo4j and Redis, the queue under this test's own prefix.
//!
//! The flip is made to fail by a `TAGGED` edge of the user's whose id is a number, which the
//! flip's edge read refuses; deleting the edge, as an operator repairing the graph would, lets the
//! next retry land.

use super::super::moderation::{assert_hidden, assert_listed, seller_profile, MODERATED_LABEL};
use super::super::utils::{test_listing, test_shop};
use super::{create_social_user, event_line, follow_path, follows_edge, graph_epoch};
use super::{own_moderator, profile_path};
use super::{social_profile, social_tag, tag, tag_edge_ids, tag_path};
use crate::event_processor::utils::watcher::{generate_post_id, HomeserverHashIdPath, WatcherTest};
use anyhow::Result;
use chrono::Utc;
use nexus_common::db::graph::Query;
use nexus_common::db::{exec_single_row, fetch_key_from_graph};
use nexus_common::models::event::Event;
use nexus_common::models::marketplace::ModeratedUser;
use nexus_common::models::user::UserDetails;
use nexus_watcher::events::epoch_gate::flip_profile_uri;
use nexus_watcher::events::retry::processor::RetryPassStats;
use nexus_watcher::events::retry::queue::{QueueSizes, RetryEntry, RetryKind};
use pubky::{Keypair, ResourcePath};
use pubky_app_specs::traits::HasPath as _;
use pubky_app_specs::{
    listing_uri_builder, user_uri_builder, PubkyAppListingCondition, PubkyAppShop, PubkyAppTag,
};
use pubky_social_specs::traits::HashId as _;
use pubky_social_specs::PubkySocialFollow;
use std::path::PathBuf;

const HOUR_MS: i64 = 3_600_000;

/// Writes a `TAGGED` edge from `tagger_id` to `target_id` whose id is a number: the flip's edge
/// read refuses it, so every flip of `tagger_id` fails while it is there.
async fn break_flip(tagger_id: &str, target_id: &str) -> Result<()> {
    let query = Query::new(
        "test_break_flip",
        "MATCH (tagger:User {id: $tagger_id}), (target:User {id: $target_id})
         CREATE (tagger)-[t:TAGGED {id: 0, label: 'broken', indexed_at: 0, test_broken: true}]->(target)
         RETURN count(t) AS created",
    )
    .param("tagger_id", tagger_id)
    .param("target_id", target_id);
    let created = fetch_key_from_graph::<i64>(query, "created").await?;
    assert_eq!(created, Some(1), "both users are in the graph");
    Ok(())
}

/// Deletes the edges [`break_flip`] wrote.
async fn repair_flip(tagger_id: &str) -> Result<()> {
    let query = Query::new(
        "test_repair_flip",
        "MATCH (:User {id: $tagger_id})-[t:TAGGED {test_broken: true}]->() DELETE t",
    )
    .param("tagger_id", tagger_id);
    exec_single_row(query).await?;
    Ok(())
}

/// One retry pass of the test's runner at `now`.
async fn retry_pass(test: &WatcherTest, now: i64) -> Result<RetryPassStats> {
    Ok(test
        .event_processor_runner
        .retry_processor()
        .run_pass(now)
        .await?)
}

async fn queued_flip(test: &WatcherTest, user_id: &str) -> Result<Option<RetryEntry>> {
    Ok(test
        .event_processor_runner
        .retry_queue
        .get(&flip_profile_uri(user_id))
        .await?)
}

async fn parked(test: &WatcherTest, user_id: &str) -> Result<usize> {
    Ok(test
        .event_processor_runner
        .retry_queue
        .parked_count(user_id)
        .await?)
}

/// Parks `event` as a write deferred behind its user's flip and settles the flip, waking it: the
/// queue as it stands once the flip has landed and before the write is replayed.
async fn wake_deferred(test: &WatcherTest, event: &Event) -> Result<()> {
    let queue = &test.event_processor_runner.retry_queue;
    assert!(queue.park(&RetryEntry::deferred(event), 10).await?);
    let flip_uri = flip_profile_uri(&event.parsed_uri.user_id);
    assert_eq!(queue.resolve_flip(&flip_uri).await?, 1);
    Ok(())
}

async fn sizes(test: &WatcherTest) -> Result<QueueSizes> {
    Ok(test.event_processor_runner.retry_queue.sizes().await?)
}

/// Writes `user_kp`'s `social/v1` profile, whose flip fails, and asserts it waits in the queue
/// after one failed attempt, the user still at `v0`. Returns the entry.
async fn fail_flip(test: &mut WatcherTest, user_kp: &Keypair, name: &str) -> Result<RetryEntry> {
    let user_id = user_kp.public_key().to_z32();
    test.put(user_kp, &profile_path(), social_profile(name))
        .await?;
    assert_eq!(graph_epoch(&user_id).await?, None, "the flip did not land");
    let entry = queued_flip(test, &user_id)
        .await?
        .expect("the failed flip waits in the retry queue");
    assert_eq!(entry.kind, RetryKind::Flip);
    assert_eq!(entry.attempts, 1);
    Ok(entry)
}

/// The moderator's own flip fails. While it waits, its `social/v1` takedown of a listing and
/// its deletion of the `social/v1` copy of a user takedown are deferred, not ignored: the
/// listing stays listed and the user hidden. A retry that fails again changes nothing and
/// comes once per backoff. Once the graph is repaired, the next retry flips the moderator and
/// the same pass applies both: the listing is taken down and the user restored.
#[tokio_shared_rt::test(shared)]
async fn a_moderators_failed_flip_retries_and_then_applies_its_deferred_takedowns() -> Result<()> {
    let mut test = WatcherTest::setup().await?;
    let (moderator_kp, _) = own_moderator(&mut test);
    let moderator_id = test
        .create_user(
            &moderator_kp,
            &seller_profile("Watcher:FlipRetry:Moderator"),
        )
        .await?;

    let seller_kp = Keypair::random();
    let seller_id = test
        .create_user(&seller_kp, &seller_profile("Watcher:FlipRetry:Seller"))
        .await?;
    let shop_path: ResourcePath = PubkyAppShop::create_path().parse()?;
    test.put(&seller_kp, &shop_path, &test_shop(&seller_id))
        .await?;
    let listing = test_listing(
        &seller_id,
        "Watcher:FlipRetry:Listing",
        "fashion",
        PubkyAppListingCondition::New,
        8_000,
    );
    let (listing_id, listing_path) = test.create_listing(&seller_kp, &listing).await?;
    assert_listed(&seller_id, &listing_id).await;

    let user_kp = Keypair::random();
    let user_id = test
        .create_user(&user_kp, &seller_profile("Watcher:FlipRetry:HiddenUser"))
        .await?;
    let takedown = PubkyAppTag {
        uri: user_uri_builder(user_id.clone()),
        label: MODERATED_LABEL.to_string(),
        created_at: Utc::now().timestamp_millis(),
    };
    test.put(&moderator_kp, &takedown.hs_path(), &takedown)
        .await?;
    assert!(ModeratedUser::is_moderated(&user_id).await?);

    // The migrator's copy of the user takedown, written before the profile: ignored at v0.
    let user_social_uri = pubky_social_specs::user_uri_builder(user_id.clone());
    let (_, takedown_copy_path) =
        tag(&mut test, &moderator_kp, &user_social_uri, MODERATED_LABEL).await?;
    assert!(ModeratedUser::is_moderated(&user_id).await?);

    break_flip(&moderator_id, &seller_id).await?;
    let queued = fail_flip(&mut test, &moderator_kp, "Watcher:FlipRetry:ModeratorV1").await?;

    let listing_uri = listing_uri_builder(seller_id.clone(), listing_id.clone());
    tag(&mut test, &moderator_kp, &listing_uri, MODERATED_LABEL).await?;
    test.del(&moderator_kp, &takedown_copy_path).await?;
    assert_listed(&seller_id, &listing_id).await;
    assert!(ModeratedUser::is_moderated(&user_id).await?);
    assert_eq!(parked(&test, &moderator_id).await?, 2);
    assert_eq!(sizes(&test).await?, QueueSizes { due: 1, parked: 2 });

    // Not due yet: nothing is attempted.
    let early = retry_pass(&test, queued.next_retry_at - 1).await?;
    assert_eq!(early, RetryPassStats::default());

    // Due, and fails again: one attempt, scheduled after the doubled backoff.
    let now = queued.next_retry_at;
    let failed = retry_pass(&test, now).await?;
    assert_eq!(
        failed,
        RetryPassStats {
            attempted: 1,
            rescheduled: 1,
            ..RetryPassStats::default()
        }
    );
    let rescheduled = queued_flip(&test, &moderator_id).await?.unwrap();
    assert_eq!(rescheduled.attempts, 2);
    assert_eq!(rescheduled.next_retry_at, now + 20_000);
    assert_eq!(retry_pass(&test, now).await?, RetryPassStats::default());
    assert_eq!(graph_epoch(&moderator_id).await?, None);
    assert_listed(&seller_id, &listing_id).await;
    assert!(ModeratedUser::is_moderated(&user_id).await?);
    assert_eq!(sizes(&test).await?, QueueSizes { due: 1, parked: 2 });

    repair_flip(&moderator_id).await?;
    let landed = retry_pass(&test, rescheduled.next_retry_at).await?;
    assert_eq!(landed.attempted, 3, "the flip and the two writes it woke");
    assert_eq!(landed.applied, 3);
    assert_eq!(landed.rescheduled + landed.dropped + landed.superseded, 0);
    assert_eq!(graph_epoch(&moderator_id).await?.as_deref(), Some("v1"));
    assert_hidden(&seller_id, &listing_id).await;
    assert!(
        !ModeratedUser::is_moderated(&user_id).await?,
        "deleting the social/v1 copy lifted the takedown placed through pubky.app"
    );
    assert_eq!(sizes(&test).await?, QueueSizes { due: 0, parked: 0 });

    test.cleanup_user(&user_kp).await?;
    test.del(&seller_kp, &listing_path).await?;
    test.del(&seller_kp, &shop_path).await?;
    test.cleanup_user(&seller_kp).await?;
    test.del(&moderator_kp, &profile_path()).await?;
    Ok(())
}

/// A user's flip fails; their `social/v1` follow, tag and untag in the meantime are deferred,
/// one entry per address, the untag replacing its tag. The flip lands on the retry after the
/// repair and the same pass applies them: the follow and the kept tag are indexed, the removed
/// tag is not, and the profile is the `social/v1` one.
#[tokio_shared_rt::test(shared)]
async fn a_users_failed_flip_retries_and_then_applies_their_deferred_writes() -> Result<()> {
    let mut test = WatcherTest::setup().await?;
    let user_kp = Keypair::random();
    let user_id = test
        .create_user(&user_kp, &seller_profile("Watcher:FlipRetry:User"))
        .await?;
    let friend_kp = Keypair::random();
    let friend_id = test
        .create_user(&friend_kp, &seller_profile("Watcher:FlipRetry:Friend"))
        .await?;
    let run = Utc::now().timestamp_micros() % 1_000_000;
    let (kept, removed) = (format!("kept{run}"), format!("removed{run}"));

    break_flip(&user_id, &friend_id).await?;
    let queued = fail_flip(&mut test, &user_kp, "Watcher:FlipRetry:UserV1").await?;

    let friend_social_uri = pubky_social_specs::user_uri_builder(friend_id.clone());
    test.put(&user_kp, &follow_path(&friend_id), PubkySocialFollow::new())
        .await?;
    let (kept_tag, _) = tag(&mut test, &user_kp, &friend_social_uri, &kept).await?;
    let (_, removed_path) = tag(&mut test, &user_kp, &friend_social_uri, &removed).await?;
    test.del(&user_kp, &removed_path).await?;
    assert!(!follows_edge(&user_id, &friend_id).await?);
    assert!(tag_edge_ids(&user_id, &kept).await?.is_empty());
    assert_eq!(parked(&test, &user_id).await?, 3);

    let failed = retry_pass(&test, queued.next_retry_at).await?;
    assert_eq!((failed.attempted, failed.rescheduled), (1, 1));
    assert_eq!(parked(&test, &user_id).await?, 3);
    assert!(!follows_edge(&user_id, &friend_id).await?);

    repair_flip(&user_id).await?;
    let retry_at = queued_flip(&test, &user_id).await?.unwrap().next_retry_at;
    let landed = retry_pass(&test, retry_at).await?;
    assert_eq!(landed.attempted, 4, "the flip and the three writes it woke");
    assert_eq!(landed.applied, 3, "the flip, the follow and the kept tag");
    assert_eq!(
        landed.dropped, 1,
        "the untag of a tag never indexed has nothing to remove"
    );
    assert_eq!(landed.rescheduled + landed.superseded, 0);
    assert_eq!(graph_epoch(&user_id).await?.as_deref(), Some("v1"));
    assert!(follows_edge(&user_id, &friend_id).await?);
    assert_eq!(tag_edge_ids(&user_id, &kept).await?, [kept_tag.create_id()]);
    assert!(tag_edge_ids(&user_id, &removed).await?.is_empty());
    let details = UserDetails::get_by_id(&user_id).await?.unwrap();
    assert_eq!(details.name, "Watcher:FlipRetry:UserV1");
    assert_eq!(sizes(&test).await?, QueueSizes { due: 0, parked: 0 });

    test.del(&user_kp, &tag_path(&kept_tag)).await?;
    test.del(&user_kp, &follow_path(&friend_id)).await?;
    test.del(&user_kp, &profile_path()).await?;
    test.cleanup_user(&friend_kp).await?;
    Ok(())
}

/// A flip that keeps failing is attempted once per pass that finds it due, never twice in one
/// pass, its backoff doubling to the cap, one entry all along. The user's parked writes stop at
/// the policy's cap, and once the flip lands a write that keeps failing is replayed up to the
/// replay limit and dropped: the queue ends empty.
#[tokio_shared_rt::test(shared)]
async fn retries_are_bounded_and_never_spin() -> Result<()> {
    let mut test = WatcherTest::setup().await?;
    test.event_processor_runner.retry_policy.max_parked_per_user = 2;
    test.event_processor_runner.retry_policy.max_replays = 2;
    let policy = test.event_processor_runner.retry_policy;
    let user_kp = Keypair::random();
    let user_id = test
        .create_user(&user_kp, &seller_profile("Watcher:FlipRetry:Bounded"))
        .await?;
    let friend_kp = Keypair::random();
    let friend_id = test
        .create_user(
            &friend_kp,
            &seller_profile("Watcher:FlipRetry:BoundedFriend"),
        )
        .await?;

    break_flip(&user_id, &friend_id).await?;
    let mut entry = fail_flip(&mut test, &user_kp, "Watcher:FlipRetry:BoundedV1").await?;

    // Tags on posts the friend never wrote: each is a dependency no replay finds.
    let mut missing_post_tags = Vec::new();
    for label in ["one", "two", "three"] {
        let post_uri = pubky_social_specs::post_uri_builder(friend_id.clone(), generate_post_id());
        let (_, path) = tag(&mut test, &user_kp, &post_uri, label).await?;
        missing_post_tags.push(path);
    }
    assert_eq!(
        parked(&test, &user_id).await?,
        2,
        "the third write is over the cap"
    );

    let mut backoffs = Vec::new();
    for _ in 0..12 {
        let now = entry.next_retry_at;
        let pass = retry_pass(&test, now).await?;
        assert_eq!((pass.attempted, pass.rescheduled), (1, 1));
        assert_eq!(
            retry_pass(&test, now).await?.attempted,
            0,
            "never twice in one pass"
        );
        let next = queued_flip(&test, &user_id).await?.unwrap();
        assert_eq!(next.attempts, entry.attempts + 1);
        backoffs.push(next.next_retry_at - now);
        assert_eq!(sizes(&test).await?, QueueSizes { due: 1, parked: 2 });
        entry = next;
    }
    assert_eq!(entry.attempts, 13);
    assert_eq!(
        backoffs,
        [
            20_000, 40_000, 80_000, 160_000, 320_000, 640_000, 1_280_000, 2_560_000, HOUR_MS,
            HOUR_MS, HOUR_MS, HOUR_MS
        ]
    );

    repair_flip(&user_id).await?;
    let landed = retry_pass(&test, entry.next_retry_at).await?;
    assert_eq!(landed.attempted, 3, "the flip and the two parked writes");
    assert_eq!((landed.applied, landed.rescheduled), (1, 2));
    assert_eq!(graph_epoch(&user_id).await?.as_deref(), Some("v1"));
    assert_eq!(sizes(&test).await?, QueueSizes { due: 2, parked: 0 });

    let replay_at = entry.next_retry_at + policy.backoff_ms(RetryKind::Deferred, 1);
    let exhausted = retry_pass(&test, replay_at).await?;
    assert_eq!((exhausted.attempted, exhausted.dropped), (2, 2));
    assert_eq!(sizes(&test).await?, QueueSizes { due: 0, parked: 0 });
    assert_eq!(
        retry_pass(&test, replay_at + 10 * HOUR_MS).await?.attempted,
        0
    );

    for path in &missing_post_tags {
        test.del(&user_kp, path).await?;
    }
    test.del(&user_kp, &profile_path()).await?;
    test.cleanup_user(&friend_kp).await?;
    Ok(())
}

/// Once the flip lands the user's writes are handled as they arrive, so a woken untag replayed
/// later can be older than the tag the user wrote again since: it is dropped and the tag kept.
/// Once the record is gone from the homeserver, the same untag applies.
#[tokio_shared_rt::test(shared)]
async fn a_woken_untag_older_than_the_tag_written_again_is_dropped() -> Result<()> {
    let mut test = WatcherTest::setup().await?;
    let user_kp = Keypair::random();
    let user_id = create_social_user(&mut test, &user_kp, "Watcher:FlipRetry:Rewrite").await?;
    let friend_kp = Keypair::random();
    let friend_id =
        create_social_user(&mut test, &friend_kp, "Watcher:FlipRetry:RewriteFriend").await?;
    let label = format!("again{}", Utc::now().timestamp_micros() % 1_000_000);
    let friend_social_uri = pubky_social_specs::user_uri_builder(friend_id.clone());
    let (written_again, path) = tag(&mut test, &user_kp, &friend_social_uri, &label).await?;
    assert_eq!(
        tag_edge_ids(&user_id, &label).await?,
        [written_again.create_id()]
    );

    let untag = Event::parse_event(&event_line("DEL", &user_id, &path), PathBuf::new())?
        .expect("a DEL line of a social/v1 tag");

    wake_deferred(&test, &untag).await?;
    let stale = retry_pass(&test, Utc::now().timestamp_millis()).await?;
    assert_eq!((stale.attempted, stale.dropped, stale.applied), (1, 1, 0));
    assert_eq!(
        tag_edge_ids(&user_id, &label).await?,
        [written_again.create_id()],
        "the older untag left the tag written again"
    );
    assert_eq!(sizes(&test).await?, QueueSizes { due: 0, parked: 0 });

    test.ensure_event_processing = false;
    test.del(&user_kp, &path).await?;
    test.ensure_event_processing = true;
    wake_deferred(&test, &untag).await?;
    let current = retry_pass(&test, Utc::now().timestamp_millis()).await?;
    assert_eq!((current.attempted, current.applied), (1, 1));
    assert!(tag_edge_ids(&user_id, &label).await?.is_empty());
    assert_eq!(sizes(&test).await?, QueueSizes { due: 0, parked: 0 });

    test.del(&user_kp, &profile_path()).await?;
    test.del(&friend_kp, &profile_path()).await?;
    Ok(())
}

/// Woken writes whose user has a flip queued anew are parked again under the same per-user cap
/// as a first park: one is parked again, the one over the cap is dropped. When that flip then
/// settles without landing, the user left at `v0`, the woken writes meet the gate's ignore and
/// are counted as ignored, not applied.
#[tokio_shared_rt::test(shared)]
async fn a_write_parked_again_obeys_the_cap_and_one_left_at_v0_counts_as_ignored() -> Result<()> {
    let mut test = WatcherTest::setup().await?;
    test.event_processor_runner.retry_policy.max_parked_per_user = 2;
    let user_kp = Keypair::random();
    let user_id = test
        .create_user(&user_kp, &seller_profile("Watcher:FlipRetry:Repark"))
        .await?;
    let friend_kp = Keypair::random();
    let friend_id = test
        .create_user(
            &friend_kp,
            &seller_profile("Watcher:FlipRetry:ReparkFriend"),
        )
        .await?;
    let friend_social_uri = pubky_social_specs::user_uri_builder(friend_id.clone());
    let run = Utc::now().timestamp_micros() % 1_000_000;
    let mut writes = Vec::new();
    for label in ["first", "second", "third"] {
        let tag = social_tag(&friend_social_uri, &format!("{label}{run}"));
        let line = event_line("PUT", &user_id, &tag_path(&tag));
        writes.push(Event::parse_event(&line, PathBuf::new())?.expect("a social/v1 tag PUT"));
    }
    let profile_line = event_line("PUT", &user_id, &profile_path());
    let profile = Event::parse_event(&profile_line, PathBuf::new())?.expect("a profile PUT");
    let flip_uri = flip_profile_uri(&user_id);
    let queue = test.event_processor_runner.retry_queue.clone();
    let now = Utc::now().timestamp_millis();

    assert!(queue.park(&RetryEntry::deferred(&writes[0]), 2).await?);
    assert!(queue.park(&RetryEntry::deferred(&writes[1]), 2).await?);
    assert_eq!(queue.resolve_flip(&flip_uri).await?, 2);
    queue
        .queue_flip(&RetryEntry::failed_flip(&profile, now + HOUR_MS))
        .await?;
    assert!(queue.park(&RetryEntry::deferred(&writes[2]), 2).await?);
    assert_eq!(sizes(&test).await?, QueueSizes { due: 3, parked: 1 });

    let reparked = retry_pass(&test, now).await?;
    assert_eq!(
        reparked,
        RetryPassStats {
            attempted: 2,
            reparked: 1,
            dropped: 1,
            ..RetryPassStats::default()
        },
        "the first parked again, the second over the cap"
    );
    assert_eq!(parked(&test, &user_id).await?, 2);
    assert_eq!(sizes(&test).await?, QueueSizes { due: 1, parked: 2 });

    assert_eq!(queue.resolve_flip(&flip_uri).await?, 2);
    let ignored = retry_pass(&test, now).await?;
    assert_eq!(
        ignored,
        RetryPassStats {
            attempted: 2,
            ignored: 2,
            ..RetryPassStats::default()
        }
    );
    assert_eq!(graph_epoch(&user_id).await?, None);
    assert_eq!(sizes(&test).await?, QueueSizes { due: 0, parked: 0 });

    test.cleanup_user(&user_kp).await?;
    test.cleanup_user(&friend_kp).await?;
    Ok(())
}

/// Before the profile, with no flip queued, a `v0` user's `social/v1` writes are the migrator's
/// copies: ignored, never parked.
#[tokio_shared_rt::test(shared)]
async fn copies_before_the_profile_are_ignored_and_never_parked() -> Result<()> {
    let mut test = WatcherTest::setup().await?;
    let user_kp = Keypair::random();
    let user_id = test
        .create_user(&user_kp, &seller_profile("Watcher:FlipRetry:Copies"))
        .await?;
    let friend_kp = Keypair::random();
    let friend_id = test
        .create_user(
            &friend_kp,
            &seller_profile("Watcher:FlipRetry:CopiesFriend"),
        )
        .await?;
    let label = format!("copy{}", Utc::now().timestamp_micros() % 1_000_000);
    let copy = social_tag(
        &pubky_social_specs::user_uri_builder(friend_id.clone()),
        &label,
    );
    test.put(&user_kp, &tag_path(&copy), &copy).await?;
    assert_eq!(sizes(&test).await?, QueueSizes { due: 0, parked: 0 });
    assert!(tag_edge_ids(&user_id, &label).await?.is_empty());

    test.del(&user_kp, &tag_path(&copy)).await?;
    test.cleanup_user(&user_kp).await?;
    test.cleanup_user(&friend_kp).await?;
    Ok(())
}
