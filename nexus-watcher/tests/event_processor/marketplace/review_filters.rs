use super::review_trust_buckets::{buyer, market, Order};
use super::utils::test_attestor_key;
use crate::event_processor::utils::watcher::{HomeserverHashIdPath, WatcherTest};
use anyhow::Result;
use chrono::Utc;
use nexus_common::models::marketplace::{
    ReviewReach, ReviewStream, ReviewStreamFilter, ReviewStreamScope, ReviewerFilter,
    DEFAULT_WOT_DEPTH,
};
use nexus_common::types::Pagination;
use pubky_app_specs::{user_uri_builder, PubkyAppReviewRole, PubkyAppTag};

/// The reviewer ids a viewer's filtered seller stream lists, newest first.
async fn reviewers(
    seller_id: &str,
    viewer_id: &str,
    reach: Option<ReviewReach>,
    tagged_by_viewer: &[&str],
) -> Result<Vec<String>> {
    let filter = ReviewStreamFilter {
        reviewers: ReviewerFilter {
            viewer_id: Some(viewer_id.to_string()),
            reach,
            depth: DEFAULT_WOT_DEPTH,
            tagged_by_viewer: tagged_by_viewer.iter().map(|l| l.to_string()).collect(),
        },
        ..Default::default()
    };
    let scope = ReviewStreamScope::Subject {
        subject_id: seller_id.to_string(),
        role: PubkyAppReviewRole::BuyerReviewingSeller,
    };
    let stream = ReviewStream::get_filtered(&scope, &filter, Pagination::default()).await?;
    Ok(stream
        .reviews
        .into_iter()
        .map(|view| view.review.reviewer_id)
        .collect())
}

/// Follows and user tags published on the watched homeserver are the edges
/// the review filters read: they land as the events replay, and a deleted
/// follow or tag stops matching.
#[tokio_shared_rt::test(shared)]
async fn homeserver_follows_and_tags_drive_the_review_filters() -> Result<()> {
    let mut test = WatcherTest::setup().await?;
    let market = market(&mut test, "Watcher:ReviewFilters").await?;
    let (viewer_kp, viewer_id) = buyer(&mut test, "Watcher:ReviewFilters:Viewer").await?;
    let (followed_kp, followed_id) = buyer(&mut test, "Watcher:ReviewFilters:Followed").await?;
    let (tagged_kp, tagged_id) = buyer(&mut test, "Watcher:ReviewFilters:Tagged").await?;
    let (other_kp, other_id) = buyer(&mut test, "Watcher:ReviewFilters:Other").await?;
    let attestor = test_attestor_key();

    let mut reviews = Vec::new();
    for (i, (kp, id)) in [
        (&followed_kp, &followed_id),
        (&tagged_kp, &tagged_id),
        (&other_kp, &other_id),
    ]
    .into_iter()
    .enumerate()
    {
        let order = Order::new(&attestor, 900 + i as u64, 1_790_009_000 + i as i64, 4);
        let (_, path) = test.create_review(kp, &order.review(&market, id)).await?;
        reviews.push((kp, path));
    }

    let follow_path = test.create_follow(&viewer_kp, &followed_id).await?;
    let tag = PubkyAppTag {
        uri: user_uri_builder(tagged_id.clone()),
        label: "trusted".to_string(),
        created_at: Utc::now().timestamp_millis(),
    };
    test.put(&viewer_kp, &tag.hs_path(), tag.clone()).await?;
    // Another user's tag with the same label never counts for the viewer.
    let other_tag = PubkyAppTag {
        uri: user_uri_builder(other_id.clone()),
        label: "trusted".to_string(),
        created_at: Utc::now().timestamp_millis(),
    };
    test.put(&followed_kp, &other_tag.hs_path(), other_tag.clone())
        .await?;

    let seller = &market.seller_id;
    assert_eq!(
        reviewers(seller, &viewer_id, Some(ReviewReach::Following), &[]).await?,
        [followed_id.as_str()]
    );
    assert_eq!(
        reviewers(seller, &viewer_id, None, &["trusted"]).await?,
        [tagged_id.as_str()]
    );
    // `followed` now follows `other`: two hops from the viewer.
    test.create_follow(&followed_kp, &other_id).await?;
    assert_eq!(
        reviewers(seller, &viewer_id, Some(ReviewReach::Wot), &[]).await?,
        [other_id.as_str(), followed_id.as_str()]
    );

    test.del(&viewer_kp, &follow_path).await?;
    test.del(&viewer_kp, &tag.hs_path()).await?;
    assert!(reviewers(seller, &viewer_id, Some(ReviewReach::Wot), &[])
        .await?
        .is_empty());
    assert!(reviewers(seller, &viewer_id, None, &["trusted"])
        .await?
        .is_empty());

    for (kp, path) in reviews {
        test.del(kp, &path).await?;
    }
    for kp in [&viewer_kp, &followed_kp, &tagged_kp, &other_kp] {
        test.cleanup_user(kp).await?;
    }
    test.cleanup_user(&market.seller_kp).await?;
    Ok(())
}
