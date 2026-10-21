use super::moderation::{moderator_keypair, seller_profile};
use super::utils::{
    attestor_pubky, sign_attestation, test_attestation_claims, test_attestor_key, test_listing,
    test_review, test_shop,
};
use crate::event_processor::utils::watcher::{HomeserverHashIdPath, WatcherTest};
use anyhow::Result;
use chrono::Utc;
use nexus_common::db::graph::Query;
use nexus_common::db::{exec_single_row, queries, RedisOps};
use nexus_common::models::marketplace::{ReputationSummary, ReviewDetails, ReviewStream};
use nexus_common::types::Pagination;
use nexus_watcher::events::handlers::review::recompute_per_item_counting;
use pubky::{Keypair, ResourcePath};
use pubky_app_specs::{
    marketplace_review_uri_builder, traits::HasPath, PubkyAppListingCondition,
    PubkyAppMarketplaceReview, PubkyAppReviewRole, PubkyAppShop, PubkyAppTag,
};

/// A seller with a shop and two listings, A and B.
struct Market {
    seller_kp: Keypair,
    seller_id: String,
    listing_a: String,
    listing_b: String,
}

async fn market(test: &mut WatcherTest, name: &str) -> Result<Market> {
    let seller_kp = Keypair::random();
    let seller_id = test
        .create_user(&seller_kp, &seller_profile(&format!("{name}:Seller")))
        .await?;
    let shop_path: ResourcePath = PubkyAppShop::create_path().parse()?;
    test.put(&seller_kp, &shop_path, &test_shop(&seller_id))
        .await?;
    let mut listings = Vec::new();
    for title in ["Item A", "Item B"] {
        let listing = test_listing(
            &seller_id,
            title,
            "electronics",
            PubkyAppListingCondition::Excellent,
            15_000,
        );
        listings.push(test.create_listing(&seller_kp, &listing).await?.0);
    }
    let listing_b = listings.pop().unwrap();
    let listing_a = listings.pop().unwrap();
    Ok(Market {
        seller_kp,
        seller_id,
        listing_a,
        listing_b,
    })
}

/// A buyer's review of one item of an attested order: the order's attestation
/// bound to that listing. `scoped` records carry `orderRef`; legacy records
/// (published before per-order reviews) do not.
fn item_review(
    market: &Market,
    buyer_id: &str,
    listing_id: &str,
    order_seed: char,
    overall: i64,
    scoped: bool,
) -> PubkyAppMarketplaceReview {
    let attestor = test_attestor_key();
    let claims = test_attestation_claims(
        &attestor_pubky(&attestor),
        buyer_id,
        &market.seller_id,
        &market.seller_id,
        listing_id,
        order_seed,
    );
    let mut review = test_review(
        buyer_id,
        &market.seller_id,
        &market.seller_id,
        listing_id,
        overall,
        "Item review.",
        &sign_attestation(&claims, &attestor),
    );
    if scoped {
        review.order_ref = Some(claims.order_ref);
    }
    review
}

async fn summary_of_seller(seller_id: &str) -> ReputationSummary {
    ReputationSummary::get_by_subject(seller_id, PubkyAppReviewRole::BuyerReviewingSeller)
        .await
        .unwrap()
        .expect("the seller aggregate exists")
}

/// Asserts the supersession flags and the aggregates for the indexed subset
/// of (legacy A rated 5, scoped A rated 3, scoped B rated 1).
async fn assert_item_counts(
    market: &Market,
    buyer_id: &str,
    ids: &[Option<String>; 3],
    legacy_hidden: bool,
    order: &[usize],
) {
    for (index, review_id) in ids.iter().enumerate() {
        let Some(review_id) = review_id else { continue };
        let superseded = ReviewDetails::get_from_index(buyer_id, review_id)
            .await
            .unwrap()
            .expect("every record stays indexed")
            .superseded;
        let expected = index == 0 && legacy_hidden;
        assert_eq!(superseded, expected, "{order:?}: record {index} superseded");
    }
    let ratings = [5.0, 3.0, 1.0];
    let counted: Vec<usize> = (0..3)
        .filter(|index| ids[*index].is_some() && !(*index == 0 && legacy_hidden))
        .collect();
    let seller = summary_of_seller(&market.seller_id).await;
    assert_eq!(
        seller.count,
        counted.len() as u64,
        "{order:?}: one count per item"
    );
    let sum: f64 = counted.iter().map(|index| ratings[*index]).sum();
    assert_eq!(seller.avg, sum / counted.len() as f64, "{order:?}");
    let listing_a: Vec<usize> = counted.iter().copied().filter(|index| *index < 2).collect();
    match ReputationSummary::get_by_listing(&market.seller_id, &market.listing_a)
        .await
        .unwrap()
    {
        Some(summary) => {
            assert_eq!(summary.count, listing_a.len() as u64, "{order:?}");
            assert_eq!(summary.avg, ratings[listing_a[0]], "{order:?}");
        }
        None => assert!(listing_a.is_empty(), "{order:?}"),
    }
    let listing_b = ReputationSummary::get_by_listing(&market.seller_id, &market.listing_b)
        .await
        .unwrap();
    assert_eq!(
        listing_b.map(|summary| summary.count).unwrap_or(0),
        u64::from(ids[2].is_some()),
        "{order:?}"
    );
}

fn permutations(items: &[usize]) -> Vec<Vec<usize>> {
    if items.len() <= 1 {
        return vec![items.to_vec()];
    }
    let mut all = Vec::new();
    for (index, item) in items.iter().enumerate() {
        let mut rest = items.to_vec();
        rest.remove(index);
        for mut tail in permutations(&rest) {
            tail.insert(0, *item);
            all.push(tail);
        }
    }
    all
}

#[tokio_shared_rt::test(shared)]
async fn only_the_same_items_scoped_record_supersedes_a_legacy_review_in_any_arrival_order(
) -> Result<()> {
    let mut test = WatcherTest::setup().await?;
    let market = market(&mut test, "Watcher:ReviewItemTwin").await?;

    for order in permutations(&[0, 1, 2]) {
        // One buyer and one attested order per arrival order.
        let buyer_kp = Keypair::random();
        let buyer_id = test
            .create_user(
                &buyer_kp,
                &seller_profile(&format!("Watcher:ReviewItemTwin:Buyer{order:?}")),
            )
            .await?;
        let records = [
            item_review(&market, &buyer_id, &market.listing_a, 'c', 5, false),
            item_review(&market, &buyer_id, &market.listing_a, 'c', 3, true),
            item_review(&market, &buyer_id, &market.listing_b, 'c', 1, true),
        ];
        let mut ids: [Option<String>; 3] = [None, None, None];
        let mut paths: [Option<ResourcePath>; 3] = [None, None, None];
        for index in &order {
            let (review_id, path) = test.create_review(&buyer_kp, &records[*index]).await?;
            ids[*index] = Some(review_id);
            paths[*index] = Some(path);
            // After every arrival: A's legacy review is superseded exactly
            // when scoped A is indexed, whatever scoped B does.
            let legacy_hidden = ids[1].is_some();
            assert_item_counts(&market, &buyer_id, &ids, legacy_hidden, &order).await;
        }

        // Deleting scoped A brings A's legacy review back while scoped B stays.
        test.del(&buyer_kp, paths[1].as_ref().unwrap()).await?;
        ids[1] = None;
        assert_item_counts(&market, &buyer_id, &ids, false, &order).await;

        for path in [&paths[0], &paths[2]] {
            test.del(&buyer_kp, path.as_ref().unwrap()).await?;
        }
        test.cleanup_user(&buyer_kp).await?;
    }

    test.cleanup_user(&market.seller_kp).await?;
    Ok(())
}

/// An order-scoped item review carrying the order's seller rating.
#[allow(clippy::too_many_arguments)]
fn rated_item(
    market: &Market,
    buyer_id: &str,
    listing_id: &str,
    order_seed: char,
    overall: i64,
    shipping: i64,
    communication: i64,
    updated_at: &str,
) -> PubkyAppMarketplaceReview {
    let mut review = item_review(market, buyer_id, listing_id, order_seed, overall, true);
    review.ratings.shipping = Some(shipping);
    review.ratings.communication = Some(communication);
    review.updated_at = updated_at.to_string();
    review
}

async fn assert_seller_fields(seller_id: &str, count: u64, shipping: f64, communication: f64) {
    let seller = summary_of_seller(seller_id).await;
    assert_eq!(seller.count, count, "every item review counts its stars");
    assert_eq!(seller.avg_shipping, Some(shipping));
    assert_eq!(seller.avg_communication, Some(communication));
}

#[tokio_shared_rt::test(shared)]
async fn seller_fields_count_once_per_order_and_move_when_an_item_review_is_deleted() -> Result<()>
{
    let mut test = WatcherTest::setup().await?;
    let market = market(&mut test, "Watcher:ReviewSellerFields").await?;
    let buyer_kp = Keypair::random();
    let buyer_id = test
        .create_user(
            &buyer_kp,
            &seller_profile("Watcher:ReviewSellerFields:Buyer"),
        )
        .await?;
    let other_kp = Keypair::random();
    let other_id = test
        .create_user(
            &other_kp,
            &seller_profile("Watcher:ReviewSellerFields:Other"),
        )
        .await?;

    // One order of two items. Item B's copy of the seller rating is the
    // latest, so it is the one that counts.
    let item_a = rated_item(
        &market,
        &buyer_id,
        &market.listing_a,
        'd',
        4,
        5,
        5,
        "2026-08-20T12:00:00Z",
    );
    let (item_a_id, item_a_path) = test.create_review(&buyer_kp, &item_a).await?;
    let item_b = rated_item(
        &market,
        &buyer_id,
        &market.listing_b,
        'd',
        2,
        2,
        1,
        "2026-08-20T13:00:00Z",
    );
    let (item_b_id, item_b_path) = test.create_review(&buyer_kp, &item_b).await?;
    // Another buyer's one-item order.
    let other = rated_item(
        &market,
        &other_id,
        &market.listing_a,
        'e',
        5,
        4,
        3,
        "2026-08-20T12:00:00Z",
    );
    let (_other_id, other_path) = test.create_review(&other_kp, &other).await?;

    assert_seller_fields(&market.seller_id, 3, 3.0, 2.0).await;
    // A listing holds one review per order, so its averages use every copy.
    let listing_a = ReputationSummary::get_by_listing(&market.seller_id, &market.listing_a)
        .await?
        .expect("the listing aggregate exists");
    assert_eq!(listing_a.avg_shipping, Some(4.5));

    // Deleting item B's review moves the order's seller rating to item A.
    test.del(&buyer_kp, &item_b_path).await?;
    assert_seller_fields(&market.seller_id, 2, 4.5, 4.0).await;

    // A later copy takes it back.
    let mut item_b_again = item_b.clone();
    item_b_again.ratings.shipping = Some(1);
    item_b_again.updated_at = "2026-08-20T14:00:00Z".to_string();
    let (item_b_again_id, _path) = test.create_review(&buyer_kp, &item_b_again).await?;
    assert_eq!(item_b_again_id, item_b_id);
    assert_seller_fields(&market.seller_id, 3, 2.5, 2.0).await;

    // An edit of item A that is now the latest moves it again.
    let mut item_a_edit = item_a.clone();
    item_a_edit.revision = 2;
    item_a_edit.ratings.shipping = Some(3);
    item_a_edit.updated_at = "2026-08-20T15:00:00Z".to_string();
    let (item_a_edit_id, _path) = test.create_review(&buyer_kp, &item_a_edit).await?;
    assert_eq!(item_a_edit_id, item_a_id);
    assert_seller_fields(&market.seller_id, 3, 3.5, 4.0).await;

    // A copy moderation excludes never carries the seller rating.
    let moderator_kp = moderator_keypair().await;
    test.create_user(
        &moderator_kp,
        &seller_profile("Watcher:ReviewSellerFields:Moderator"),
    )
    .await?;
    let exclusion = PubkyAppTag {
        uri: marketplace_review_uri_builder(buyer_id.clone(), item_a_id.clone()),
        label: "review_fraud".to_string(),
        created_at: Utc::now().timestamp_millis(),
    };
    test.put(&moderator_kp, &exclusion.hs_path(), &exclusion)
        .await?;
    assert_seller_fields(&market.seller_id, 2, 2.5, 2.0).await;
    test.del(&moderator_kp, &exclusion.hs_path()).await?;
    assert_seller_fields(&market.seller_id, 3, 3.5, 4.0).await;

    test.del(&buyer_kp, &item_a_path).await?;
    test.del(&buyer_kp, &item_b_path).await?;
    test.del(&other_kp, &other_path).await?;
    test.cleanup_user(&buyer_kp).await?;
    test.cleanup_user(&other_kp).await?;
    test.cleanup_user(&market.seller_kp).await?;
    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn copies_updated_at_the_same_time_count_the_lowest_review_id() -> Result<()> {
    let mut test = WatcherTest::setup().await?;
    let market = market(&mut test, "Watcher:ReviewSellerTie").await?;
    let buyer_kp = Keypair::random();
    let buyer_id = test
        .create_user(&buyer_kp, &seller_profile("Watcher:ReviewSellerTie:Buyer"))
        .await?;
    let same_time = "2026-08-20T12:00:00Z";
    let item_a = rated_item(
        &market,
        &buyer_id,
        &market.listing_a,
        'f',
        5,
        5,
        4,
        same_time,
    );
    let item_b = rated_item(
        &market,
        &buyer_id,
        &market.listing_b,
        'f',
        5,
        1,
        2,
        same_time,
    );
    let (item_a_id, item_a_path) = test.create_review(&buyer_kp, &item_a).await?;
    let (item_b_id, item_b_path) = test.create_review(&buyer_kp, &item_b).await?;

    let (shipping, communication) = if item_a_id < item_b_id {
        (5.0, 4.0)
    } else {
        (1.0, 2.0)
    };
    assert_seller_fields(&market.seller_id, 2, shipping, communication).await;

    // Another buyer attested under the same orderRef, with a later copy,
    // rates the seller once more and leaves the first buyer's copy counted.
    let other_kp = Keypair::random();
    let other_id = test
        .create_user(&other_kp, &seller_profile("Watcher:ReviewSellerTie:Other"))
        .await?;
    let other = rated_item(
        &market,
        &other_id,
        &market.listing_a,
        'f',
        5,
        3,
        4,
        "2026-08-20T13:00:00Z",
    );
    let (_other_id, other_path) = test.create_review(&other_kp, &other).await?;
    assert_seller_fields(
        &market.seller_id,
        3,
        (shipping + 3.0) / 2.0,
        (communication + 4.0) / 2.0,
    )
    .await;

    test.del(&other_kp, &other_path).await?;
    test.del(&buyer_kp, &item_a_path).await?;
    test.del(&buyer_kp, &item_b_path).await?;
    test.cleanup_user(&other_kp).await?;
    test.cleanup_user(&buyer_kp).await?;
    test.cleanup_user(&market.seller_kp).await?;
    Ok(())
}

/// Puts a review back in the state an index without per-item counting left
/// it in: superseded (by another item's scoped review of its order) or not,
/// and with no seller-fields flag.
async fn restore_pre_per_item_state(
    buyer_id: &str,
    review_id: &str,
    superseded: bool,
) -> Result<()> {
    exec_single_row(queries::put::set_review_superseded(
        buyer_id, review_id, superseded,
    ))
    .await?;
    exec_single_row(
        Query::new(
            "test_remove_seller_fields_flag",
            "MATCH (:User {id: $reviewer_id})-[r:REVIEWED {review_id: $review_id}]->(:User)
             REMOVE r.seller_fields_counted",
        )
        .param("reviewer_id", buyer_id)
        .param("review_id", review_id),
    )
    .await?;
    let mut details = ReviewDetails::get_from_index(buyer_id, review_id)
        .await?
        .expect("indexed review");
    details.superseded = superseded;
    details
        .put_index_json(&[buyer_id, review_id], None, None)
        .await?;
    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn the_one_shot_recompute_recounts_reviews_indexed_before_per_item_counting() -> Result<()> {
    let mut test = WatcherTest::setup().await?;
    let market = market(&mut test, "Watcher:ReviewPerItemRecompute").await?;
    let buyer_kp = Keypair::random();
    let buyer_id = test
        .create_user(
            &buyer_kp,
            &seller_profile("Watcher:ReviewPerItemRecompute:Buyer"),
        )
        .await?;

    // Legacy item A and scoped item B of one order, both with a seller rating.
    let mut legacy_a = item_review(&market, &buyer_id, &market.listing_a, '9', 4, false);
    legacy_a.ratings.shipping = Some(5);
    legacy_a.ratings.communication = Some(5);
    let (legacy_a_id, legacy_a_path) = test.create_review(&buyer_kp, &legacy_a).await?;
    let scoped_b = rated_item(
        &market,
        &buyer_id,
        &market.listing_b,
        '9',
        2,
        1,
        3,
        "2026-08-20T13:00:00Z",
    );
    let (scoped_b_id, scoped_b_path) = test.create_review(&buyer_kp, &scoped_b).await?;

    // The index before this release: scoped B superseded legacy A, and no
    // edge carries the seller-fields flag.
    restore_pre_per_item_state(&buyer_id, &legacy_a_id, true).await?;
    restore_pre_per_item_state(&buyer_id, &scoped_b_id, false).await?;
    ReputationSummary::recompute_subject(
        &market.seller_id,
        PubkyAppReviewRole::BuyerReviewingSeller,
    )
    .await?;
    assert_eq!(summary_of_seller(&market.seller_id).await.count, 1);

    let summary = recompute_per_item_counting().await?;
    assert!(summary.reviews >= 2);
    assert!(summary.orders >= 1);

    assert!(
        !ReviewDetails::get_from_index(&buyer_id, &legacy_a_id)
            .await?
            .expect("indexed")
            .superseded,
        "another item's scoped review no longer hides legacy A"
    );
    let listed =
        ReviewStream::get_by_listing(&market.seller_id, &market.listing_a, Pagination::default())
            .await?
            .expect("listing A lists its legacy review");
    assert_eq!(listed.0[0].review.review_id, legacy_a_id);
    // Both items count; the order's seller rating counts once, from scoped B
    // (legacy A's updatedAt is earlier).
    assert_seller_fields(&market.seller_id, 2, 1.0, 3.0).await;
    let listing_a = ReputationSummary::get_by_listing(&market.seller_id, &market.listing_a)
        .await?
        .expect("listing A aggregate");
    assert_eq!(listing_a.count, 1);

    // Idempotent.
    recompute_per_item_counting().await?;
    assert_seller_fields(&market.seller_id, 2, 1.0, 3.0).await;

    test.del(&buyer_kp, &legacy_a_path).await?;
    test.del(&buyer_kp, &scoped_b_path).await?;
    test.cleanup_user(&buyer_kp).await?;
    test.cleanup_user(&market.seller_kp).await?;
    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn an_empty_text_item_review_counts_like_any_other_and_oversized_text_is_rejected(
) -> Result<()> {
    use pubky_app_specs::traits::{HasIdPath, HashId};

    let mut test = WatcherTest::setup().await?;
    let market = market(&mut test, "Watcher:ReviewEmptyText").await?;
    let buyer_kp = Keypair::random();
    let buyer_id = test
        .create_user(&buyer_kp, &seller_profile("Watcher:ReviewEmptyText:Buyer"))
        .await?;

    // Item A: a rating with no text at all (0 characters).
    let mut empty = item_review(&market, &buyer_id, &market.listing_a, 'f', 4, true);
    empty.text = String::new();
    let (empty_id, empty_path) = test.create_review(&buyer_kp, &empty).await?;
    let indexed = ReviewDetails::get_from_index(&buyer_id, &empty_id)
        .await?
        .expect("an empty-text review is indexed");
    assert_eq!(indexed.text, "");
    assert!(indexed.verified);
    assert!(indexed.order_scoped);
    assert!(!indexed.superseded);
    assert_eq!(summary_of_seller(&market.seller_id).await.count, 1);
    let listing_a = ReputationSummary::get_by_listing(&market.seller_id, &market.listing_a)
        .await?
        .expect("the listing aggregate exists");
    assert_eq!((listing_a.count, listing_a.avg), (1, 4.0));

    // Item B of the same order with 5,001 characters: specs refuses it, so
    // nothing is indexed and no aggregate moves.
    let mut oversized = item_review(&market, &buyer_id, &market.listing_b, 'f', 2, true);
    oversized.text = "a".repeat(5_001);
    let oversized_id = oversized.create_id();
    oversized.review_id = oversized_id.clone();
    let oversized_path: ResourcePath =
        PubkyAppMarketplaceReview::create_path(&oversized_id).parse()?;
    let _ = test.put(&buyer_kp, &oversized_path, &oversized).await;
    assert!(
        ReviewDetails::get_from_index(&buyer_id, &oversized_id)
            .await?
            .is_none(),
        "5,001 characters stay rejected"
    );
    assert_eq!(summary_of_seller(&market.seller_id).await.count, 1);
    assert!(
        ReputationSummary::get_by_listing(&market.seller_id, &market.listing_b)
            .await?
            .is_none(),
        "the rejected review is not counted for its listing"
    );

    // The same review with exactly 5,000 characters is accepted and counted
    // per item next to the empty-text one.
    let mut longest = item_review(&market, &buyer_id, &market.listing_b, 'f', 2, true);
    longest.text = "a".repeat(5_000);
    let (longest_id, longest_path) = test.create_review(&buyer_kp, &longest).await?;
    let indexed = ReviewDetails::get_from_index(&buyer_id, &longest_id)
        .await?
        .expect("5,000 characters are accepted");
    assert_eq!(indexed.text.chars().count(), 5_000);
    let seller = summary_of_seller(&market.seller_id).await;
    assert_eq!((seller.count, seller.avg), (2, 3.0));
    let listing_b = ReputationSummary::get_by_listing(&market.seller_id, &market.listing_b)
        .await?
        .expect("the listing aggregate exists");
    assert_eq!((listing_b.count, listing_b.avg), (1, 2.0));

    test.del(&buyer_kp, &empty_path).await?;
    test.del(&buyer_kp, &longest_path).await?;
    test.cleanup_user(&buyer_kp).await?;
    test.cleanup_user(&market.seller_kp).await?;
    Ok(())
}
