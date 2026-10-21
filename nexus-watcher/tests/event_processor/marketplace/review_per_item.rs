use super::moderation::seller_profile;
use super::utils::{
    attestor_pubky, sign_attestation, test_attestation_claims, test_attestor_key, test_listing,
    test_review, test_shop,
};
use crate::event_processor::utils::watcher::WatcherTest;
use anyhow::Result;
use nexus_common::models::marketplace::{ReputationSummary, ReviewDetails};
use pubky::{Keypair, ResourcePath};
use pubky_app_specs::{
    traits::HasPath, PubkyAppListingCondition, PubkyAppMarketplaceReview, PubkyAppReviewRole,
    PubkyAppShop,
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
