//! The throttled review backfill jobs against a real homeserver: they give the
//! same graph as the one-shot migrations, once, resumably, and never resurrect
//! a review the event feed deleted after the backfill read it.

use super::review_trust_buckets::{buyer, listing_summary, market, Market, Order};
use super::utils::{attestor_pubky, test_attestor_key};
use crate::event_processor::utils::watcher::WatcherTest;
use anyhow::Result;
use ed25519_dalek::SigningKey;
use nexus_common::db::graph::Query;
use nexus_common::db::{exec_single_row, RedisOps};
use nexus_common::models::backfill_run::{BackfillRun, BackfillStatus};
use nexus_common::models::marketplace::{ReputationSummary, ReviewDetails, ReviewTouch};
use nexus_watcher::backfill::gate::{GateConfig, Unthrottled};
use nexus_watcher::backfill::jobs::{BackfillJob, DiscoveryJob, TrustBucketsJob};
use nexus_watcher::backfill::run::{
    run_job, BackfillReport, Outcome, ProgressEvent, ProgressSink, RunOptions, EXIT_OK,
};
use nexus_watcher::events::handlers::review::{
    index_review, ingest_read_review, ingest_review_through, read_review, settle_after_write,
    ReviewIngest,
};
use pubky::Keypair;
use pubky_app_specs::{PubkyAppReviewRole, PubkyId};
use std::collections::BTreeMap;
use std::time::Duration;

struct Silent;

impl ProgressSink for Silent {
    fn item_finished(&self, _event: &ProgressEvent<'_>) {}
}

fn options() -> RunOptions {
    RunOptions {
        gate: GateConfig {
            rate: 20.0,
            backoff_initial: Duration::from_millis(5),
            backoff_max: Duration::from_millis(20),
            jitter: 0.0,
            ..GateConfig::default()
        },
        aggregate_pause: Duration::ZERO,
        ..RunOptions::default()
    }
}

fn unique(name: &str) -> String {
    format!(
        "test-{name}-{}",
        &Keypair::random().public_key().to_z32()[..8]
    )
}

async fn run(job: &dyn BackfillJob, options: RunOptions) -> BackfillReport {
    run_job(job, options, &Silent).await.unwrap()
}

/// The index before the attestation time was kept: no `iat` or weight on the
/// edge or in the details, and an aggregate without buckets.
async fn strip_attestation_time(market: &Market, reviewer_id: &str, review_id: &str) -> Result<()> {
    exec_single_row(
        Query::new(
            "test_remove_attestation_time",
            "MATCH (:User {id: $reviewer_id})-[r:REVIEWED {review_id: $review_id}]->(:User)
             REMOVE r.iat, r.price_weight",
        )
        .param("reviewer_id", reviewer_id)
        .param("review_id", review_id),
    )
    .await?;
    let mut details = ReviewDetails::get_from_index(reviewer_id, review_id)
        .await?
        .expect("indexed");
    details.iat = None;
    details.price_weight = None;
    details
        .put_index_json(&[reviewer_id, review_id], None, None)
        .await?;
    ReputationSummary::recompute_subject(
        &market.seller_id,
        PubkyAppReviewRole::BuyerReviewingSeller,
    )
    .await?;
    ReputationSummary::recompute_listing(&market.seller_id, &market.listing_id).await?;
    Ok(())
}

async fn seller_summary(seller_id: &str) -> Option<ReputationSummary> {
    ReputationSummary::get_by_subject(seller_id, PubkyAppReviewRole::BuyerReviewingSeller)
        .await
        .unwrap()
}

async fn indexed(reviewer_id: &str, review_id: &str) -> Option<ReviewDetails> {
    ReviewDetails::get_from_index(reviewer_id, review_id)
        .await
        .unwrap()
}

/// A reviewer with `orders` priced, attested reviews of the market, indexed
/// through the feed and then stripped to the pre-`iat` state.
struct Stripped {
    reviewers: Vec<(Keypair, String)>,
    reviews: Vec<(String, String)>,
    paths: Vec<pubky::ResourcePath>,
}

async fn stripped_market(
    test: &mut WatcherTest,
    market: &Market,
    attestor: &SigningKey,
    name: &str,
    orders: &[(u64, i64, i64, &str)],
) -> Result<Stripped> {
    let mut reviewers = Vec::new();
    let mut reviews = Vec::new();
    let mut paths = Vec::new();
    for (n, (order, iat, overall, price)) in orders.iter().enumerate() {
        let (keypair, id) = buyer(test, &format!("{name}:Buyer{n}")).await?;
        let mut order = Order::new(attestor, *order, *iat, *overall);
        if !price.is_empty() {
            order = order.priced(price);
        }
        let review = order.review(market, &id);
        let (review_id, path) = test.create_review(&keypair, &review).await?;
        reviews.push((id.clone(), review_id));
        paths.push(path);
        reviewers.push((keypair, id));
    }
    for (reviewer_id, review_id) in &reviews {
        strip_attestation_time(market, reviewer_id, review_id).await?;
    }
    Ok(Stripped {
        reviewers,
        reviews,
        paths,
    })
}

fn reviewer_ids(stripped: &Stripped) -> Vec<String> {
    stripped.reviews.iter().map(|(id, _)| id.clone()).collect()
}

async fn finish(test: &mut WatcherTest, market: &Market, stripped: Stripped) -> Result<()> {
    for ((keypair, _), path) in stripped.reviewers.iter().zip(&stripped.paths) {
        test.del(keypair, path).await?;
        test.cleanup_user(keypair).await?;
    }
    test.cleanup_user(&market.seller_kp).await?;
    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn the_trust_bucket_job_gives_the_migrations_graph_once() -> Result<()> {
    let mut test = WatcherTest::setup().await?;
    let market = market(&mut test, "Watcher:ReviewJob:Buckets").await?;
    let attestor = test_attestor_key();
    let attestor_id = attestor_pubky(&attestor);
    let stripped = stripped_market(
        &mut test,
        &market,
        &attestor,
        "Watcher:ReviewJob:Buckets",
        &[
            (901, 1_790_000_901, 4, "USD:5"),
            (902, 1_790_000_902, 2, ""),
        ],
    )
    .await?;
    assert!(seller_summary(&market.seller_id)
        .await
        .unwrap()
        .buckets
        .is_empty());

    let job = TrustBucketsJob::new()
        .run_key(unique("buckets"))
        .only_reviewers(reviewer_ids(&stripped));
    let report = run(&job, options()).await;
    assert_eq!(report.outcome, Outcome::Done);
    assert_eq!(report.exit_code(), EXIT_OK);
    assert_eq!(report.state.status, BackfillStatus::Done);
    assert_eq!(report.state.counters.items, 2);
    assert_eq!(report.state.counters.requests, 2, "one GET per review");
    assert_eq!(report.state.counters.refetched, 2);
    assert_eq!(report.state.counters.failed, 0);

    let (first, second) = (&stripped.reviews[0], &stripped.reviews[1]);
    let first = indexed(&first.0, &first.1).await.unwrap();
    let second = indexed(&second.0, &second.1).await.unwrap();
    assert_eq!(
        (first.iat, first.price_weight),
        (Some(1_790_000_901), Some(4))
    );
    assert_eq!(
        (second.iat, second.price_weight),
        (Some(1_790_000_902), Some(1))
    );
    let summary = seller_summary(&market.seller_id).await.unwrap();
    let bucket = &summary.buckets[&attestor_id];
    assert_eq!(
        (bucket.buyers, bucket.orders, bucket.weight_total),
        (2, 2, 5)
    );
    assert_eq!(summary.count, 2, "no review is counted twice");
    let listing = listing_summary(&market).await;
    assert_eq!(
        listing.buckets[&attestor_id].first_iat,
        Some(1_790_000_901),
        "the listing aggregate is recomputed with its buckets too"
    );

    // Once only: a second run sends nothing and changes nothing.
    let again = run(&job, options()).await;
    assert_eq!(again.outcome, Outcome::AlreadyDone);
    assert_eq!(again.state.counters, report.state.counters);

    // Even forced, nothing is left without an `iat`: no request, same graph.
    let forced = run(
        &job,
        RunOptions {
            force_rerun: true,
            ..options()
        },
    )
    .await;
    assert_eq!(forced.outcome, Outcome::Done);
    assert_eq!(forced.state.counters.items, 0);
    assert_eq!(forced.state.counters.requests, 0);
    let after = seller_summary(&market.seller_id).await.unwrap();
    assert_eq!(after.count, summary.count);
    assert_eq!(after.buckets, summary.buckets);

    BackfillRun::delete(job.id()).await?;
    finish(&mut test, &market, stripped).await
}

#[tokio_shared_rt::test(shared)]
async fn an_interrupted_trust_bucket_run_ends_where_an_uninterrupted_one_does() -> Result<()> {
    let mut test = WatcherTest::setup().await?;
    let attestor = test_attestor_key();
    let attestor_id = attestor_pubky(&attestor);
    let orders = [
        (911, 1_790_000_911, 5, "USD:4"),
        (912, 1_790_000_912, 3, "USD:2"),
        (913, 1_790_000_913, 1, ""),
    ];
    let market_a = market(&mut test, "Watcher:ReviewJob:Interrupted").await?;
    let market_b = market(&mut test, "Watcher:ReviewJob:Straight").await?;
    let a = stripped_market(
        &mut test,
        &market_a,
        &attestor,
        "Watcher:ReviewJob:Interrupted",
        &orders,
    )
    .await?;
    let b = stripped_market(
        &mut test,
        &market_b,
        &attestor,
        "Watcher:ReviewJob:Straight",
        &[
            (921, 1_790_000_911, 5, "USD:4"),
            (922, 1_790_000_912, 3, "USD:2"),
            (923, 1_790_000_913, 1, ""),
        ],
    )
    .await?;

    // Interrupted: one request, then the budget pauses the run.
    let interrupted = TrustBucketsJob::new()
        .run_key(unique("interrupted"))
        .only_reviewers(reviewer_ids(&a));
    let first = run(
        &interrupted,
        RunOptions {
            gate: GateConfig {
                max_requests: Some(1),
                ..options().gate
            },
            ..options()
        },
    )
    .await;
    assert!(matches!(first.outcome, Outcome::Paused(_)), "{first:?}");
    assert_eq!(first.state.counters.items, 1);
    assert_eq!(first.state.counters.requests, 1);

    let second = run(&interrupted, options()).await;
    assert_eq!(second.outcome, Outcome::Done);
    assert_eq!(
        second.state.counters.requests, 3,
        "the resume never asks again for the review the first run finished"
    );
    assert_eq!(second.state.counters.items, 3);

    // Uninterrupted twin.
    let straight = TrustBucketsJob::new()
        .run_key(unique("straight"))
        .only_reviewers(reviewer_ids(&b));
    let whole = run(&straight, options()).await;
    assert_eq!(whole.outcome, Outcome::Done);
    assert_eq!(whole.state.counters.requests, 3);

    let summary_a = seller_summary(&market_a.seller_id).await.unwrap();
    let summary_b = seller_summary(&market_b.seller_id).await.unwrap();
    assert_eq!(summary_a.count, 3);
    assert_eq!(summary_a.count, summary_b.count);
    assert_eq!(summary_a.histogram, summary_b.histogram);
    assert_eq!(summary_a.buckets, summary_b.buckets);
    assert_eq!(summary_a.buckets[&attestor_id].buyers, 3);
    for (reviewer, review) in &a.reviews {
        let details = indexed(reviewer, review).await.unwrap();
        assert!(details.iat.is_some() && details.price_weight.is_some());
    }

    BackfillRun::delete(interrupted.id()).await?;
    BackfillRun::delete(straight.id()).await?;
    finish(&mut test, &market_a, a).await?;
    finish(&mut test, &market_b, b).await
}

#[tokio_shared_rt::test(shared)]
async fn a_review_that_left_its_homeserver_is_counted_gone_and_not_retried() -> Result<()> {
    let mut test = WatcherTest::setup().await?;
    let market = market(&mut test, "Watcher:ReviewJob:Gone").await?;
    let attestor = test_attestor_key();
    let stripped = stripped_market(
        &mut test,
        &market,
        &attestor,
        "Watcher:ReviewJob:Gone",
        &[(931, 1_790_000_931, 4, "USD:3")],
    )
    .await?;
    // The record leaves the homeserver without the feed seeing it.
    let mut quiet = test.remove_event_processing().await;
    quiet
        .del(&stripped.reviewers[0].0, &stripped.paths[0])
        .await?;

    let job = TrustBucketsJob::new()
        .run_key(unique("gone"))
        .only_reviewers(reviewer_ids(&stripped));
    let report = run(&job, options()).await;
    assert_eq!(report.outcome, Outcome::Done);
    assert_eq!(report.exit_code(), EXIT_OK);
    assert_eq!(report.state.counters.gone, 1);
    assert_eq!(report.state.counters.requests, 1);
    assert_eq!(report.state.counters.failed, 0);
    let (reviewer, review) = &stripped.reviews[0];
    let details = indexed(reviewer, review).await.unwrap();
    assert_eq!(details.iat, None, "a gone review keeps no iat");

    let again = run(&job, options()).await;
    assert_eq!(again.outcome, Outcome::AlreadyDone);
    assert_eq!(again.state.counters.requests, 1);

    BackfillRun::delete(job.id()).await?;
    del_review(reviewer, review).await?;
    let mut test = quiet;
    test.cleanup_user(&stripped.reviewers[0].0).await?;
    test.cleanup_user(&market.seller_kp).await?;
    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn an_unreachable_reviewer_stays_out_of_the_buckets_and_is_not_retried() -> Result<()> {
    let mut test = WatcherTest::setup().await?;
    let p = published(&mut test, "Watcher:ReviewJob:Unreachable", 961).await?;
    let attestor_id = attestor_pubky(&test_attestor_key());
    let counted = seller_summary(&p.market.seller_id).await.unwrap();
    assert_eq!(counted.buckets[&attestor_id].buyers, 1);

    // A verified review indexed before the `iat` was kept, whose reviewer's
    // homeserver cannot be reached.
    let ghost_id = Keypair::random().public_key().to_z32();
    exec_single_row(
        Query::new(
            "test_ghost_reviewer",
            "MERGE (:User {id: $id, name: 'ghost', indexed_at: 0})",
        )
        .param("id", ghost_id.as_str()),
    )
    .await?;
    let mut ghost = indexed(&p.buyer_id, &p.review_id).await.unwrap();
    ghost.reviewer_id = ghost_id.clone();
    ghost.review_id = format!("ghost{}", &ghost_id[..8]);
    ghost.order_ref = Some(format!("{:064x}", 962));
    ghost.iat = None;
    ghost.price_weight = None;
    ghost.put_to_graph().await?;
    ghost.put_to_index(false).await?;
    ReputationSummary::recompute_subject(
        &p.market.seller_id,
        PubkyAppReviewRole::BuyerReviewingSeller,
    )
    .await?;
    assert_eq!(seller_summary(&p.market.seller_id).await.unwrap().count, 2);

    let job = TrustBucketsJob::new()
        .run_key(unique("unreachable"))
        .only_reviewers(vec![ghost_id.clone()]);
    let report = run(&job, options()).await;
    assert_eq!(report.outcome, Outcome::Done);
    assert_eq!(report.exit_code(), EXIT_OK);
    assert_eq!(report.state.counters.unreachable, 1);
    assert_eq!(report.state.counters.refetched, 0);
    assert_eq!(report.state.counters.failed, 0);
    assert_eq!(report.state.counters.requests, 1);

    // Never over-counted: the unreachable review is no bucketed voice.
    let after = seller_summary(&p.market.seller_id).await.unwrap();
    assert_eq!(after.buckets, counted.buckets);
    assert_eq!(
        indexed(&ghost_id, &ghost.review_id).await.unwrap().iat,
        None
    );

    // Not retried by a resumed or repeated run of the same job.
    let again = run(&job, options()).await;
    assert_eq!(again.outcome, Outcome::AlreadyDone);
    assert_eq!(again.state.counters.requests, 1);

    BackfillRun::delete(job.id()).await?;
    ghost.delete().await?;
    exec_single_row(
        Query::new(
            "test_delete_ghost_reviewer",
            "MATCH (u:User {id: $id}) DETACH DELETE u",
        )
        .param("id", ghost_id.as_str()),
    )
    .await?;
    test.del(&p.buyer_kp, &p.path).await?;
    tear_down(&mut test, p).await
}

#[tokio_shared_rt::test(shared)]
async fn the_discovery_job_indexes_unseen_reviews_once_and_counts_each_outcome() -> Result<()> {
    let mut test = WatcherTest::setup().await?;
    let market = market(&mut test, "Watcher:ReviewJob:Discovery").await?;
    let attestor = test_attestor_key();
    let (buyer_kp, buyer_id) = buyer(&mut test, "Watcher:ReviewJob:Discovery:Buyer").await?;
    let (empty_kp, empty_id) = buyer(&mut test, "Watcher:ReviewJob:Discovery:Empty").await?;
    let ghost_id = Keypair::random().public_key().to_z32();
    exec_single_row(
        Query::new(
            "test_user_without_homeserver",
            "MERGE (:User {id: $id, name: 'ghost', indexed_at: 0})",
        )
        .param("id", ghost_id.as_str()),
    )
    .await?;

    // The review is published while event processing is off: the record is on
    // the homeserver but the feed never delivered it to the index.
    let mut test = test.remove_event_processing().await;
    let review = Order::new(&attestor, 941, 1_790_000_941, 5).review(&market, &buyer_id);
    let (review_id, path) = test.create_review(&buyer_kp, &review).await?;
    assert!(indexed(&buyer_id, &review_id).await.is_none());

    let job = DiscoveryJob::new()
        .run_key(unique("discovery"))
        .only_users(vec![buyer_id.clone(), empty_id.clone(), ghost_id.clone()]);
    let report = run(&job, options()).await;
    assert_eq!(report.outcome, Outcome::Done);
    assert_eq!(report.exit_code(), EXIT_OK);
    let counters = &report.state.counters;
    assert_eq!(counters.items, 3);
    assert_eq!(counters.refetched, 1, "the unseen review is indexed");
    assert_eq!(counters.gone, 1, "the user without a directory is gone");
    assert_eq!(counters.unreachable, 1, "the ghost has no homeserver");
    assert_eq!(counters.failed, 0);
    let details = indexed(&buyer_id, &review_id).await.expect("indexed");
    assert_eq!(details.iat, Some(1_790_000_941));
    assert!(
        details.verified,
        "the attestation verifies during the backfill ingest"
    );
    assert_eq!(details.rating_overall, 5);
    assert_eq!(details.subject_id, market.seller_id);
    assert_eq!(seller_summary(&market.seller_id).await.unwrap().count, 1);

    // Once only: nothing is asked again, so the gone and unreachable users
    // are not retried either.
    let again = run(&job, options()).await;
    assert_eq!(again.outcome, Outcome::AlreadyDone);
    assert_eq!(again.state.counters, report.state.counters);

    // A forced re-walk finds the review indexed: no second ingest and no
    // double count.
    let forced = run(
        &job,
        RunOptions {
            force_rerun: true,
            ..options()
        },
    )
    .await;
    assert_eq!(forced.outcome, Outcome::Done);
    assert_eq!(forced.state.counters.refetched, 0);
    assert_eq!(seller_summary(&market.seller_id).await.unwrap().count, 1);

    exec_single_row(
        Query::new(
            "test_delete_ghost",
            "MATCH (u:User {id: $id}) DETACH DELETE u",
        )
        .param("id", ghost_id.as_str()),
    )
    .await?;
    BackfillRun::delete(job.id()).await?;
    test.del(&buyer_kp, &path).await?;
    del_review(&buyer_id, &review_id).await?;
    test.cleanup_user(&buyer_kp).await?;
    test.cleanup_user(&empty_kp).await?;
    test.cleanup_user(&market.seller_kp).await?;
    Ok(())
}

/// Removes a review from the index as the feed would, for tests that ran with
/// event processing off.
async fn del_review(reviewer_id: &str, review_id: &str) -> Result<()> {
    if indexed(reviewer_id, review_id).await.is_none() {
        return Ok(());
    }
    let reviewer = PubkyId::try_from(reviewer_id).map_err(|e| anyhow::anyhow!("{e}"))?;
    nexus_watcher::events::handlers::review::del(reviewer, review_id.to_string())
        .await
        .map_err(|e| anyhow::anyhow!("{e:?}"))
}

/// One review indexed by the feed, with the record still on its homeserver.
struct Published {
    market: Market,
    buyer_kp: Keypair,
    buyer_id: String,
    review_id: String,
    path: pubky::ResourcePath,
    review: pubky_app_specs::PubkyAppMarketplaceReview,
}

async fn published(test: &mut WatcherTest, name: &str, order: u64) -> Result<Published> {
    let market = market(test, name).await?;
    let (buyer_kp, buyer_id) = buyer(test, &format!("{name}:Buyer")).await?;
    let attestor = test_attestor_key();
    let review =
        Order::new(&attestor, order, 1_790_001_000 + order as i64, 4).review(&market, &buyer_id);
    let (review_id, path) = test.create_review(&buyer_kp, &review).await?;
    let mut review = review;
    review.review_id = review_id.clone();
    assert!(indexed(&buyer_id, &review_id).await.is_some());
    Ok(Published {
        market,
        buyer_kp,
        buyer_id,
        review_id,
        path,
        review,
    })
}

async fn tear_down(test: &mut WatcherTest, published: Published) -> Result<()> {
    test.cleanup_user(&published.buyer_kp).await?;
    test.cleanup_user(&published.market.seller_kp).await?;
    Ok(())
}

/// A pause so the read's clock reading is later than every earlier feed
/// mutation of the review, whatever the clock's resolution.
async fn tick() {
    tokio::time::sleep(Duration::from_millis(5)).await;
}

#[tokio_shared_rt::test(shared)]
async fn a_review_deleted_after_the_backfill_read_it_does_not_come_back() -> Result<()> {
    let mut test = WatcherTest::setup().await?;
    let p = published(&mut test, "Watcher:ReviewJob:DeleteAfterRead", 951).await?;

    // The backfill reads the record while the review still exists...
    tick().await;
    let read = read_review(&Unthrottled, &p.buyer_id, &p.review_id)
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    assert!(read.record.is_some());

    // ...the reviewer then deletes it and the feed removes it from the index...
    test.del(&p.buyer_kp, &p.path).await?;
    assert!(indexed(&p.buyer_id, &p.review_id).await.is_none());

    // ...and only now does the backfill try to write what it read.
    let ended = ingest_read_review(read)
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    assert_eq!(ended, ReviewIngest::Gone);
    assert!(
        indexed(&p.buyer_id, &p.review_id).await.is_none(),
        "the deleted review must not be indexed again"
    );
    assert!(seller_summary(&p.market.seller_id).await.is_none());

    tear_down(&mut test, p).await
}

#[tokio_shared_rt::test(shared)]
async fn a_delete_that_lands_during_the_backfill_write_is_undone_not_resurrected() -> Result<()> {
    let mut test = WatcherTest::setup().await?;
    let p = published(&mut test, "Watcher:ReviewJob:DeleteDuringWrite", 952).await?;

    tick().await;
    let read = read_review(&Unthrottled, &p.buyer_id, &p.review_id)
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    let record = read
        .record
        .clone()
        .expect("the record is on the homeserver");
    // The check before the write finds nothing: the delete has not begun.
    assert!(
        ReviewTouch::since(&p.buyer_id, &p.review_id, read_started(&read))
            .await?
            .is_none()
    );

    // The delete runs entirely between that check and the end of the write:
    // it finds the review indexed and removes it...
    test.del(&p.buyer_kp, &p.path).await?;
    assert!(indexed(&p.buyer_id, &p.review_id).await.is_none());

    // ...and the backfill's write, which began before it, lands afterwards.
    let reviewer = PubkyId::try_from(p.buyer_id.as_str()).map_err(|e| anyhow::anyhow!("{e}"))?;
    index_review(record, reviewer, p.review_id.clone())
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    assert!(
        indexed(&p.buyer_id, &p.review_id).await.is_some(),
        "the write alone resurrects the review: the check after it must undo that"
    );

    let ended = settle_after_write(&read)
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    assert_eq!(ended, ReviewIngest::Gone);
    assert!(
        indexed(&p.buyer_id, &p.review_id).await.is_none(),
        "the resurrected review is removed again"
    );
    assert!(seller_summary(&p.market.seller_id).await.is_none());

    tear_down(&mut test, p).await
}

/// The clock reading the read took before its request.
fn read_started(read: &nexus_watcher::events::handlers::review::ReviewRead) -> i64 {
    read.started_ms()
}

#[tokio_shared_rt::test(shared)]
async fn a_review_edited_after_the_backfill_read_it_is_read_again_never_overwritten() -> Result<()>
{
    let mut test = WatcherTest::setup().await?;
    let p = published(&mut test, "Watcher:ReviewJob:EditAfterRead", 953).await?;
    assert_eq!(
        indexed(&p.buyer_id, &p.review_id)
            .await
            .unwrap()
            .rating_overall,
        4
    );

    tick().await;
    let read = read_review(&Unthrottled, &p.buyer_id, &p.review_id)
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    let mut edited = p.review.clone();
    edited.ratings.overall = 2;
    test.put(&p.buyer_kp, &p.path, &edited).await?;
    assert_eq!(
        indexed(&p.buyer_id, &p.review_id)
            .await
            .unwrap()
            .rating_overall,
        2
    );

    let ended = ingest_read_review(read)
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    assert_eq!(ended, ReviewIngest::Changed);
    assert_eq!(
        indexed(&p.buyer_id, &p.review_id)
            .await
            .unwrap()
            .rating_overall,
        2,
        "the stale record must not overwrite the edit"
    );

    // A fresh read ingests the current record.
    let ended = ingest_review_through(&Unthrottled, &p.buyer_id, &p.review_id)
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    assert_eq!(ended, ReviewIngest::Ingested);
    assert_eq!(
        indexed(&p.buyer_id, &p.review_id)
            .await
            .unwrap()
            .rating_overall,
        2
    );

    tear_down(&mut test, p).await
}

#[tokio_shared_rt::test(shared)]
async fn a_review_nobody_touched_is_ingested_by_the_fenced_path() -> Result<()> {
    let mut test = WatcherTest::setup().await?;
    let p = published(&mut test, "Watcher:ReviewJob:Untouched", 954).await?;
    let before = BTreeMap::from([(
        p.review_id.clone(),
        indexed(&p.buyer_id, &p.review_id)
            .await
            .unwrap()
            .rating_overall,
    )]);

    tick().await;
    let ended = ingest_review_through(&Unthrottled, &p.buyer_id, &p.review_id)
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    assert_eq!(ended, ReviewIngest::Ingested);
    assert_eq!(
        indexed(&p.buyer_id, &p.review_id)
            .await
            .unwrap()
            .rating_overall,
        before[&p.review_id]
    );
    assert_eq!(seller_summary(&p.market.seller_id).await.unwrap().count, 1);

    test.del(&p.buyer_kp, &p.path).await?;
    tear_down(&mut test, p).await
}
