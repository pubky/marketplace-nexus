use super::super::review_trust_buckets::{buyer, market, Order};
use super::super::utils::test_attestor_key;
use super::{create_social_user, follow, follows_edge, tag};
use crate::event_processor::users::utils::find_user_counts;
use crate::event_processor::utils::watcher::WatcherTest;
use anyhow::Result;
use nexus_common::models::marketplace::{
    ReviewReach, ReviewStream, ReviewStreamFilter, ReviewStreamScope, ReviewerFilter,
    DEFAULT_WOT_DEPTH,
};
use nexus_common::types::Pagination;
use pubky::Keypair;
use pubky_app_specs::PubkyAppReviewRole;

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

/// A Social v1 follow and its DEL are the follow and the unfollow of a `pubky.app` follow:
/// the edge, the counts and friendship.
#[tokio_shared_rt::test(shared)]
async fn social_v1_follows_and_unfollows_update_the_graph() -> Result<()> {
    let mut test = WatcherTest::setup().await?;
    let alice_kp = Keypair::random();
    let alice_id = create_social_user(&mut test, &alice_kp, "Watcher:SocialV1:Alice").await?;
    let bob_kp = Keypair::random();
    let bob_id = create_social_user(&mut test, &bob_kp, "Watcher:SocialV1:Bob").await?;

    let alice_follows_bob = follow(&mut test, &alice_kp, &bob_id).await?;
    assert!(follows_edge(&alice_id, &bob_id).await?);
    assert_eq!(find_user_counts(&alice_id).await.following, 1);
    assert_eq!(find_user_counts(&bob_id).await.followers, 1);

    let bob_follows_alice = follow(&mut test, &bob_kp, &alice_id).await?;
    assert_eq!(find_user_counts(&alice_id).await.friends, 1);
    assert_eq!(find_user_counts(&bob_id).await.friends, 1);

    test.del(&alice_kp, &alice_follows_bob).await?;
    assert!(!follows_edge(&alice_id, &bob_id).await?);
    let alice = find_user_counts(&alice_id).await;
    assert_eq!((alice.following, alice.followers, alice.friends), (0, 1, 0));
    let bob = find_user_counts(&bob_id).await;
    assert_eq!((bob.following, bob.followers, bob.friends), (1, 0, 0));

    test.del(&bob_kp, &bob_follows_alice).await?;
    assert!(!follows_edge(&bob_id, &alice_id).await?);
    test.del(&alice_kp, &super::profile_path()).await?;
    test.del(&bob_kp, &super::profile_path()).await?;
    Ok(())
}

/// The follows and user tags a Social v1 viewer publishes are the edges the review filters
/// read: `reach=following|friends|wot` and `tagged_by_viewer` match them, and their DELs stop
/// matching.
#[tokio_shared_rt::test(shared)]
async fn social_v1_follows_and_tags_drive_the_review_filters() -> Result<()> {
    let mut test = WatcherTest::setup().await?;
    let market = market(&mut test, "Watcher:SocialV1:ReviewFilters").await?;
    let viewer_kp = Keypair::random();
    let viewer_id = create_social_user(
        &mut test,
        &viewer_kp,
        "Watcher:SocialV1:ReviewFilters:Viewer",
    )
    .await?;
    let (followed_kp, followed_id) =
        buyer(&mut test, "Watcher:SocialV1:ReviewFilters:Followed").await?;
    let (tagged_kp, tagged_id) = buyer(&mut test, "Watcher:SocialV1:ReviewFilters:Tagged").await?;
    let (other_kp, other_id) = buyer(&mut test, "Watcher:SocialV1:ReviewFilters:Other").await?;
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
        let order = Order::new(&attestor, 9_100 + i as u64, 1_790_019_000 + i as i64, 4);
        let (_, path) = test.create_review(kp, &order.review(&market, id)).await?;
        reviews.push((kp, path));
    }
    let seller = &market.seller_id;

    let follow_path = follow(&mut test, &viewer_kp, &followed_id).await?;
    let (_, tag_path) = tag(
        &mut test,
        &viewer_kp,
        &pubky_social_specs::user_uri_builder(tagged_id.clone()),
        "trusted",
    )
    .await?;
    assert_eq!(
        reviewers(seller, &viewer_id, Some(ReviewReach::Following), &[]).await?,
        [followed_id.as_str()]
    );
    assert!(
        reviewers(seller, &viewer_id, Some(ReviewReach::Friends), &[])
            .await?
            .is_empty()
    );
    assert_eq!(
        reviewers(seller, &viewer_id, None, &["trusted"]).await?,
        [tagged_id.as_str()]
    );

    // `followed` follows the viewer back through pubky.app: friends across epochs
    let followed_back = test.create_follow(&followed_kp, &viewer_id).await?;
    assert_eq!(
        reviewers(seller, &viewer_id, Some(ReviewReach::Friends), &[]).await?,
        [followed_id.as_str()]
    );
    // `followed` follows `other`: two hops from the viewer
    let followed_other = test.create_follow(&followed_kp, &other_id).await?;
    assert_eq!(
        reviewers(seller, &viewer_id, Some(ReviewReach::Wot), &[]).await?,
        [other_id.as_str(), followed_id.as_str()]
    );

    test.del(&viewer_kp, &follow_path).await?;
    test.del(&viewer_kp, &tag_path).await?;
    assert!(reviewers(seller, &viewer_id, Some(ReviewReach::Wot), &[])
        .await?
        .is_empty());
    assert!(reviewers(seller, &viewer_id, None, &["trusted"])
        .await?
        .is_empty());

    test.del(&followed_kp, &followed_back).await?;
    test.del(&followed_kp, &followed_other).await?;
    for (kp, path) in reviews {
        test.del(kp, &path).await?;
    }
    for kp in [&followed_kp, &tagged_kp, &other_kp] {
        test.cleanup_user(kp).await?;
    }
    test.del(&viewer_kp, &super::profile_path()).await?;
    test.cleanup_user(&market.seller_kp).await?;
    Ok(())
}
