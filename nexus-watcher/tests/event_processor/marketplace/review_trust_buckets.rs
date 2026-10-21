use super::moderation::seller_profile;
use super::utils::{
    attestor_pubky, sign_attestation, test_attestation_claims, test_attestor_key, test_listing,
    test_review, test_shop,
};
use crate::event_processor::utils::watcher::WatcherTest;
use anyhow::Result;
use ed25519_dalek::SigningKey;
use nexus_common::db::graph::Query;
use nexus_common::db::{exec_single_row, fetch_row_from_graph, RedisOps};
use nexus_common::models::marketplace::{
    AttestorWindow, ReputationBucket, ReputationSummary, ReviewDetails,
};
use nexus_watcher::events::handlers::review::backfill_trust_buckets;
use pubky::{Keypair, ResourcePath};
use pubky_app_specs::{
    traits::HasPath, PubkyAppListing, PubkyAppListingCondition, PubkyAppListingSale,
    PubkyAppMarketplaceReview, PubkyAppReviewRole, PubkyAppShop,
};

const PRICE_WEIGHT_VECTORS: &str = include_str!("fixtures/price-weight-vectors.json");

/// A second attestor key, for buckets that split by attestor.
fn other_attestor_key() -> SigningKey {
    SigningKey::from_bytes(&[43u8; 32])
}

pub(super) struct Market {
    pub(super) seller_kp: Keypair,
    pub(super) seller_id: String,
    listing: PubkyAppListing,
    pub(super) listing_id: String,
    listing_path: ResourcePath,
}

pub(super) async fn market(test: &mut WatcherTest, name: &str) -> Result<Market> {
    let seller_kp = Keypair::random();
    let seller_id = test
        .create_user(&seller_kp, &seller_profile(&format!("{name}:Seller")))
        .await?;
    let shop_path: ResourcePath = PubkyAppShop::create_path().parse()?;
    test.put(&seller_kp, &shop_path, &test_shop(&seller_id))
        .await?;
    let mut listing = test_listing(
        &seller_id,
        "Trust bucket subject",
        "electronics",
        PubkyAppListingCondition::Excellent,
        15_000,
    );
    let (listing_id, listing_path) = test.create_listing(&seller_kp, &listing).await?;
    listing.listing_id = listing_id.clone();
    Ok(Market {
        seller_kp,
        seller_id,
        listing,
        listing_id,
        listing_path,
    })
}

pub(super) async fn buyer(test: &mut WatcherTest, name: &str) -> Result<(Keypair, String)> {
    let keypair = Keypair::random();
    let id = test.create_user(&keypair, &seller_profile(name)).await?;
    Ok((keypair, id))
}

/// An order-scoped buyer review of the market's listing, attested by
/// `attestor` for order number `order` at `iat` with `price_decade`.
#[derive(Clone)]
pub(super) struct Order<'a> {
    attestor: &'a SigningKey,
    order: u64,
    iat: i64,
    price_decade: Option<&'a str>,
    overall: i64,
    shipping: Option<i64>,
    communication: Option<i64>,
}

impl<'a> Order<'a> {
    pub(super) fn new(attestor: &'a SigningKey, order: u64, iat: i64, overall: i64) -> Self {
        Order {
            attestor,
            order,
            iat,
            price_decade: None,
            overall,
            shipping: None,
            communication: None,
        }
    }

    fn priced(mut self, price_decade: &'a str) -> Self {
        self.price_decade = Some(price_decade);
        self
    }

    fn seller(mut self, shipping: Option<i64>, communication: Option<i64>) -> Self {
        self.shipping = shipping;
        self.communication = communication;
        self
    }

    pub(super) fn review(&self, market: &Market, buyer_id: &str) -> PubkyAppMarketplaceReview {
        let mut claims = test_attestation_claims(
            &attestor_pubky(self.attestor),
            buyer_id,
            &market.seller_id,
            &market.seller_id,
            &market.listing_id,
            '0',
        );
        claims.order_ref = format!("{:064x}", self.order);
        claims.iat = self.iat;
        claims.price_decade = self.price_decade.map(str::to_string);
        let mut review = test_review(
            buyer_id,
            &market.seller_id,
            &market.seller_id,
            &market.listing_id,
            self.overall,
            "",
            &sign_attestation(&claims, self.attestor),
        );
        review.order_ref = Some(claims.order_ref);
        review.ratings.shipping = self.shipping;
        review.ratings.communication = self.communication;
        review
    }
}

async fn seller_summary(seller_id: &str) -> ReputationSummary {
    ReputationSummary::get_by_subject(seller_id, PubkyAppReviewRole::BuyerReviewingSeller)
        .await
        .unwrap()
        .expect("the seller aggregate exists")
}

async fn listing_summary(market: &Market) -> ReputationSummary {
    ReputationSummary::get_by_listing(&market.seller_id, &market.listing_id)
        .await
        .unwrap()
        .expect("the listing aggregate exists")
}

/// The price weight stored on a review's graph edge.
async fn edge_weight(reviewer_id: &str, review_id: &str) -> Option<i64> {
    let row = fetch_row_from_graph(
        Query::new(
            "test_review_edge_weight",
            "MATCH (:User {id: $reviewer_id})-[r:REVIEWED {review_id: $review_id}]->(:User)
             RETURN r.price_weight AS price_weight",
        )
        .param("reviewer_id", reviewer_id)
        .param("review_id", review_id),
    )
    .await
    .unwrap()
    .expect("the review edge exists");
    row.get("price_weight").unwrap()
}

#[tokio_shared_rt::test(shared)]
async fn one_buyer_with_many_orders_is_one_voice() -> Result<()> {
    let mut test = WatcherTest::setup().await?;
    let market = market(&mut test, "Watcher:ReviewBuckets:ManyOrders").await?;
    let (buyer_kp, buyer_id) = buyer(&mut test, "Watcher:ReviewBuckets:ManyOrders:B1").await?;
    let (other_kp, other_id) = buyer(&mut test, "Watcher:ReviewBuckets:ManyOrders:B2").await?;
    let attestor = test_attestor_key();

    let mut paths = Vec::new();
    for order in [
        Order::new(&attestor, 101, 1_790_000_100, 5).priced("USD:2"),
        Order::new(&attestor, 102, 1_790_000_200, 5).priced("USD:4"),
        Order::new(&attestor, 103, 1_790_000_300, 2),
    ] {
        paths.push((
            &buyer_kp,
            test.create_review(&buyer_kp, &order.review(&market, &buyer_id))
                .await?
                .1,
        ));
    }
    let other = Order::new(&attestor, 104, 1_790_000_400, 1).priced("USD:3");
    paths.push((
        &other_kp,
        test.create_review(&other_kp, &other.review(&market, &other_id))
            .await?
            .1,
    ));

    let summary = seller_summary(&market.seller_id).await;
    assert_eq!(summary.count, 4, "the legacy count keeps every review");
    assert_eq!(summary.unverified_count, 0);
    let bucket = &summary.buckets[&attestor_pubky(&attestor)];
    assert_eq!(bucket.orders, 4);
    assert_eq!(bucket.buyers, 2, "three orders by one buyer are one voice");
    assert_eq!(bucket.sum_buyer_means, 4.0 + 1.0);
    assert_eq!(bucket.histogram, [1, 1, 0, 0, 2]);
    // Buyer 1: weights 1, 3, 1 -> m' = (5 + 15 + 2) / 5 at W = 3; buyer 2:
    // 1 at W = 2.
    assert!((bucket.weighted_sum - (3.0 * 22.0 / 5.0 + 2.0)).abs() < 1e-9);
    assert_eq!(bucket.weight_total, 5);
    assert_eq!(
        (bucket.first_iat, bucket.last_iat),
        (Some(1_790_000_100), Some(1_790_000_400))
    );
    assert_eq!(bucket.seller_buyers, Some(0), "no order rated the seller");

    // The listing aggregate holds the same reviews, without seller figures,
    // and the snippets carry the buckets.
    let listing = listing_summary(&market).await;
    assert_eq!(listing.buckets[&attestor_pubky(&attestor)].buyers, 2);
    assert_eq!(
        listing.buckets[&attestor_pubky(&attestor)].seller_buyers,
        None
    );
    let snippet = ReputationSummary::snippets_by_subjects(&[market.seller_id.as_str()])
        .await?
        .remove(0)
        .expect("the seller snippet exists");
    assert_eq!(snippet.buckets, summary.buckets);
    assert_eq!(snippet.count, 4);

    for (keypair, path) in paths {
        test.del(keypair, &path).await?;
    }
    test.cleanup_user(&buyer_kp).await?;
    test.cleanup_user(&other_kp).await?;
    test.cleanup_user(&market.seller_kp).await?;
    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn a_trust_window_that_cuts_a_bucket_counts_only_what_it_covers() -> Result<()> {
    let mut test = WatcherTest::setup().await?;
    let market = market(&mut test, "Watcher:ReviewBuckets:Window").await?;
    let (b1_kp, b1_id) = buyer(&mut test, "Watcher:ReviewBuckets:Window:B1").await?;
    let (b2_kp, b2_id) = buyer(&mut test, "Watcher:ReviewBuckets:Window:B2").await?;
    let (b3_kp, b3_id) = buyer(&mut test, "Watcher:ReviewBuckets:Window:B3").await?;
    let first = test_attestor_key();
    let second = other_attestor_key();
    let (first_id, second_id) = (attestor_pubky(&first), attestor_pubky(&second));

    let published = [
        (
            &b1_kp,
            Order::new(&first, 201, 1_790_001_000, 5).review(&market, &b1_id),
        ),
        (
            &b2_kp,
            Order::new(&first, 202, 1_790_002_000, 3).review(&market, &b2_id),
        ),
        (
            &b3_kp,
            Order::new(&first, 203, 1_790_003_000, 1).review(&market, &b3_id),
        ),
        (
            &b1_kp,
            Order::new(&second, 204, 1_790_002_500, 1).review(&market, &b1_id),
        ),
    ];
    let mut paths = Vec::new();
    for (keypair, review) in &published {
        paths.push((*keypair, test.create_review(keypair, review).await?.1));
    }

    let summary = seller_summary(&market.seller_id).await;
    let bucket = &summary.buckets[&first_id];
    assert_eq!(
        (bucket.first_iat, bucket.last_iat),
        (Some(1_790_001_000), Some(1_790_003_000)),
        "the bucket's span is what a client checks against its window"
    );
    assert_eq!(summary.buckets[&second_id].buyers, 1);

    // A window that starts inside the first attestor's bucket cuts it: only
    // the covered reviews count.
    let windows = AttestorWindow::parse_list(&format!("{first_id}:1790001500:"))
        .map_err(anyhow::Error::msg)?;
    let scoped = ReputationSummary::scoped_by_subject(
        &market.seller_id,
        PubkyAppReviewRole::BuyerReviewingSeller,
        &windows,
    )
    .await?;
    assert_eq!(scoped.buyers, 2);
    assert_eq!(scoped.sum_buyer_means, 3.0 + 1.0);
    assert_eq!(
        (scoped.first_iat, scoped.last_iat),
        (Some(1_790_002_000), Some(1_790_003_000))
    );

    // Both attestors: buyer 1 counts once, with the mean of both reviews.
    let windows = AttestorWindow::parse_list(&format!(
        "{first_id}::1790002500,{second_id}:1790002000:1790003000"
    ))
    .map_err(anyhow::Error::msg)?;
    let scoped = ReputationSummary::scoped_by_subject(
        &market.seller_id,
        PubkyAppReviewRole::BuyerReviewingSeller,
        &windows,
    )
    .await?;
    assert_eq!(scoped.buyers, 2);
    assert_eq!(scoped.orders, 3);
    assert_eq!(scoped.sum_buyer_means, (5.0 + 1.0) / 2.0 + 3.0);
    let listing =
        ReputationSummary::scoped_by_listing(&market.seller_id, &market.listing_id, &windows)
            .await?;
    assert_eq!(listing.buyers, 2);
    assert_eq!(listing.seller_buyers, None);

    // A window that covers nothing counts nothing.
    let windows = AttestorWindow::parse_list(&format!("{second_id}:1790003000:"))
        .map_err(anyhow::Error::msg)?;
    let scoped = ReputationSummary::scoped_by_subject(
        &market.seller_id,
        PubkyAppReviewRole::BuyerReviewingSeller,
        &windows,
    )
    .await?;
    assert_eq!(
        scoped,
        ReputationBucket {
            seller_buyers: Some(0),
            sum_seller_buyer_means: Some(0.0),
            ..Default::default()
        }
    );

    for (keypair, path) in paths {
        test.del(keypair, &path).await?;
    }
    for keypair in [&b1_kp, &b2_kp, &b3_kp] {
        test.cleanup_user(keypair).await?;
    }
    test.cleanup_user(&market.seller_kp).await?;
    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn every_shared_price_weight_vector_is_the_indexed_weight() -> Result<()> {
    let mut test = WatcherTest::setup().await?;
    let market = market(&mut test, "Watcher:ReviewBuckets:Vectors").await?;
    let (buyer_kp, buyer_id) = buyer(&mut test, "Watcher:ReviewBuckets:Vectors:Buyer").await?;
    let attestor = test_attestor_key();

    let fixture: serde_json::Value = serde_json::from_str(PRICE_WEIGHT_VECTORS)?;
    let mut expected: Vec<(Option<String>, i64)> = Vec::new();
    for vector in fixture["vectors"].as_array().expect("vectors") {
        let decade = vector["priceDecade"].as_str().map(str::to_string);
        let weight = vector["weight"].as_i64().expect("weight");
        match expected.iter().find(|(seen, _)| *seen == decade) {
            Some((_, seen_weight)) => assert_eq!(*seen_weight, weight, "{decade:?}"),
            None => expected.push((decade, weight)),
        }
    }
    assert!(expected.len() > 10, "the fixture covers many decades");

    let mut paths = Vec::new();
    for (index, (decade, weight)) in expected.iter().enumerate() {
        let order = Order {
            price_decade: decade.as_deref(),
            ..Order::new(&attestor, 300 + index as u64, 1_790_000_000, 4)
        };
        let (review_id, path) = test
            .create_review(&buyer_kp, &order.review(&market, &buyer_id))
            .await?;
        let details = ReviewDetails::get_from_index(&buyer_id, &review_id)
            .await?
            .expect("indexed");
        assert!(details.verified, "{decade:?}");
        assert_eq!(details.price_weight, Some(*weight as u8), "{decade:?}");
        assert_eq!(
            edge_weight(&buyer_id, &review_id).await,
            Some(*weight),
            "{decade:?}"
        );
        paths.push(path);
    }
    // One buyer: their bucket weight is their largest weight.
    let largest = expected.iter().map(|(_, weight)| *weight).max().unwrap();
    let summary = seller_summary(&market.seller_id).await;
    assert_eq!(
        summary.buckets[&attestor_pubky(&attestor)].weight_total,
        largest as u64
    );

    for path in paths {
        test.del(&buyer_kp, &path).await?;
    }
    test.cleanup_user(&buyer_kp).await?;
    test.cleanup_user(&market.seller_kp).await?;
    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn an_attestation_without_the_claim_weighs_one_and_keeps_its_iat() -> Result<()> {
    let mut test = WatcherTest::setup().await?;
    let market = market(&mut test, "Watcher:ReviewBuckets:NoClaim").await?;
    let (buyer_kp, buyer_id) = buyer(&mut test, "Watcher:ReviewBuckets:NoClaim:Buyer").await?;
    let attestor = test_attestor_key();

    // A `.14`-era attestation: no price_decade claim.
    let order = Order::new(&attestor, 401, 1_787_000_000, 5);
    let (review_id, path) = test
        .create_review(&buyer_kp, &order.review(&market, &buyer_id))
        .await?;
    let details = ReviewDetails::get_from_index(&buyer_id, &review_id)
        .await?
        .expect("indexed");
    assert_eq!(details.price_weight, Some(1));
    assert_eq!(details.iat, Some(1_787_000_000));
    assert_eq!(
        details.text, "",
        "a rating-only review indexes with empty text"
    );
    assert!(details.verified);
    assert_eq!(edge_weight(&buyer_id, &review_id).await, Some(1));
    let bucket = &seller_summary(&market.seller_id).await.buckets[&attestor_pubky(&attestor)];
    assert_eq!((bucket.weighted_sum, bucket.weight_total), (5.0, 1));

    test.del(&buyer_kp, &path).await?;
    test.cleanup_user(&buyer_kp).await?;
    test.cleanup_user(&market.seller_kp).await?;
    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn a_listing_price_change_never_changes_a_weight() -> Result<()> {
    let mut test = WatcherTest::setup().await?;
    let market = market(&mut test, "Watcher:ReviewBuckets:PriceChange").await?;
    let (buyer_kp, buyer_id) = buyer(&mut test, "Watcher:ReviewBuckets:PriceChange:Buyer").await?;
    let attestor = test_attestor_key();

    // Bought at $150.00: the attestation signs USD:4, weight 3.
    let order = Order::new(&attestor, 501, 1_790_000_000, 4).priced("USD:4");
    let review = order.review(&market, &buyer_id);
    let (review_id, path) = test.create_review(&buyer_kp, &review).await?;
    let before = seller_summary(&market.seller_id).await;
    assert_eq!(before.buckets[&attestor_pubky(&attestor)].weight_total, 3);

    // The seller drops the price to $1.00, then the buyer edits the review.
    let mut cheaper = market.listing.clone();
    if let PubkyAppListingSale::FixedPrice { unit_price, .. } = &mut cheaper.sale {
        unit_price.amount_minor = 100;
    }
    cheaper.revision += 1;
    test.put(&market.seller_kp, &market.listing_path, &cheaper)
        .await?;
    let mut edit = review.clone();
    edit.revision = 2;
    edit.ratings.overall = 5;
    let (edited_id, _) = test.create_review(&buyer_kp, &edit).await?;
    assert_eq!(edited_id, review_id);

    let details = ReviewDetails::get_from_index(&buyer_id, &review_id)
        .await?
        .expect("indexed");
    assert_eq!(details.price_weight, Some(3));
    assert_eq!(edge_weight(&buyer_id, &review_id).await, Some(3));
    let after = seller_summary(&market.seller_id).await;
    let bucket = &after.buckets[&attestor_pubky(&attestor)];
    assert_eq!(bucket.weight_total, 3);
    assert_eq!(bucket.weighted_sum, 3.0 * 5.0);

    test.del(&buyer_kp, &path).await?;
    test.cleanup_user(&buyer_kp).await?;
    test.cleanup_user(&market.seller_kp).await?;
    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn the_seller_score_uses_the_present_field_and_skips_legacy_reviews() -> Result<()> {
    let mut test = WatcherTest::setup().await?;
    let market = market(&mut test, "Watcher:ReviewBuckets:SellerScore").await?;
    let (b1_kp, b1_id) = buyer(&mut test, "Watcher:ReviewBuckets:SellerScore:B1").await?;
    let (b2_kp, b2_id) = buyer(&mut test, "Watcher:ReviewBuckets:SellerScore:B2").await?;
    let (b3_kp, b3_id) = buyer(&mut test, "Watcher:ReviewBuckets:SellerScore:B3").await?;
    let attestor = test_attestor_key();
    let attestor_id = attestor_pubky(&attestor);

    let mut paths = Vec::new();
    // Buyer 1 rated shipping only: their order scores 4.
    let b1 = Order::new(&attestor, 601, 1_790_000_000, 5).seller(Some(4), None);
    paths.push((
        &b1_kp,
        test.create_review(&b1_kp, &b1.review(&market, &b1_id))
            .await?
            .1,
    ));
    // Buyer 2 rated both: (5 + 2) / 2.
    let b2 = Order::new(&attestor, 602, 1_790_000_000, 3).seller(Some(5), Some(2));
    paths.push((
        &b2_kp,
        test.create_review(&b2_kp, &b2.review(&market, &b2_id))
            .await?
            .1,
    ));
    // Buyer 3's review predates the seller questions (a legacy record with
    // neither field): it counts as an item review only.
    let mut legacy = Order::new(&attestor, 603, 1_790_000_000, 2).review(&market, &b3_id);
    legacy.order_ref = None;
    paths.push((&b3_kp, test.create_review(&b3_kp, &legacy).await?.1));

    let summary = seller_summary(&market.seller_id).await;
    let bucket = &summary.buckets[&attestor_id];
    assert_eq!(bucket.buyers, 3);
    assert_eq!(bucket.seller_buyers, Some(2));
    assert_eq!(bucket.sum_seller_buyer_means, Some(4.0 + 3.5));

    // A seller whose only reviews are legacy ones has item stars and no
    // seller rating yet.
    for (keypair, path) in paths.drain(..2) {
        test.del(keypair, &path).await?;
    }
    let summary = seller_summary(&market.seller_id).await;
    let bucket = &summary.buckets[&attestor_id];
    assert_eq!((bucket.buyers, bucket.sum_buyer_means), (1, 2.0));
    assert_eq!(bucket.seller_buyers, Some(0));
    assert_eq!(bucket.sum_seller_buyer_means, Some(0.0));

    for (keypair, path) in paths {
        test.del(keypair, &path).await?;
    }
    for keypair in [&b1_kp, &b2_kp, &b3_kp] {
        test.cleanup_user(keypair).await?;
    }
    test.cleanup_user(&market.seller_kp).await?;
    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn an_unverified_review_is_never_bucketed() -> Result<()> {
    let mut test = WatcherTest::setup().await?;
    let market = market(&mut test, "Watcher:ReviewBuckets:Unverified").await?;
    let (buyer_kp, buyer_id) = buyer(&mut test, "Watcher:ReviewBuckets:Unverified:Buyer").await?;
    let (forged_kp, forged_id) =
        buyer(&mut test, "Watcher:ReviewBuckets:Unverified:Forged").await?;
    let attestor = test_attestor_key();
    let attestor_id = attestor_pubky(&attestor);

    let genuine = Order::new(&attestor, 701, 1_790_000_000, 5);
    let (_, genuine_path) = test
        .create_review(&buyer_kp, &genuine.review(&market, &buyer_id))
        .await?;

    // Claims naming the attestor, signed by another key.
    let mut claims = test_attestation_claims(
        &attestor_id,
        &forged_id,
        &market.seller_id,
        &market.seller_id,
        &market.listing_id,
        '0',
    );
    claims.order_ref = format!("{:064x}", 702);
    let mut forged = test_review(
        &forged_id,
        &market.seller_id,
        &market.seller_id,
        &market.listing_id,
        1,
        "Never bought it.",
        &sign_attestation(&claims, &other_attestor_key()),
    );
    forged.order_ref = Some(claims.order_ref);
    let (forged_review_id, forged_path) = test.create_review(&forged_kp, &forged).await?;
    let details = ReviewDetails::get_from_index(&forged_id, &forged_review_id)
        .await?
        .expect("indexed");
    assert!(!details.verified);
    assert_eq!((details.iat, details.price_weight), (None, None));

    let summary = seller_summary(&market.seller_id).await;
    assert_eq!(summary.count, 2);
    assert_eq!(summary.unverified_count, 1);
    assert_eq!(summary.buckets.len(), 1);
    assert_eq!(summary.buckets[&attestor_id].buyers, 1);
    assert_eq!(summary.buckets[&attestor_id].histogram, [0, 0, 0, 0, 1]);
    let scoped = ReputationSummary::scoped_by_subject(
        &market.seller_id,
        PubkyAppReviewRole::BuyerReviewingSeller,
        &AttestorWindow::parse_list(&attestor_id).map_err(anyhow::Error::msg)?,
    )
    .await?;
    assert_eq!(
        scoped.buyers, 1,
        "a forged claim of a trusted attestor never counts"
    );
    assert_eq!(listing_summary(&market).await.unverified_count, 1);

    test.del(&buyer_kp, &genuine_path).await?;
    test.del(&forged_kp, &forged_path).await?;
    test.cleanup_user(&buyer_kp).await?;
    test.cleanup_user(&forged_kp).await?;
    test.cleanup_user(&market.seller_kp).await?;
    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn the_one_shot_backfill_rereads_reviews_indexed_without_their_iat() -> Result<()> {
    let mut test = WatcherTest::setup().await?;
    let market = market(&mut test, "Watcher:ReviewBuckets:Backfill").await?;
    let (buyer_kp, buyer_id) = buyer(&mut test, "Watcher:ReviewBuckets:Backfill:Buyer").await?;
    let attestor = test_attestor_key();
    let attestor_id = attestor_pubky(&attestor);

    let order = Order::new(&attestor, 801, 1_790_000_800, 4).priced("USD:5");
    let (review_id, path) = test
        .create_review(&buyer_kp, &order.review(&market, &buyer_id))
        .await?;

    // The index before this release: no iat or weight on the edge or in
    // the details, and an aggregate without buckets.
    exec_single_row(
        Query::new(
            "test_remove_attestation_time",
            "MATCH (:User {id: $reviewer_id})-[r:REVIEWED {review_id: $review_id}]->(:User)
             REMOVE r.iat, r.price_weight",
        )
        .param("reviewer_id", buyer_id.as_str())
        .param("review_id", review_id.as_str()),
    )
    .await?;
    let mut details = ReviewDetails::get_from_index(&buyer_id, &review_id)
        .await?
        .expect("indexed");
    details.iat = None;
    details.price_weight = None;
    details
        .put_index_json(&[&buyer_id, &review_id], None, None)
        .await?;
    ReputationSummary::recompute_subject(
        &market.seller_id,
        PubkyAppReviewRole::BuyerReviewingSeller,
    )
    .await?;
    assert!(seller_summary(&market.seller_id).await.buckets.is_empty());

    let summary = backfill_trust_buckets().await?;
    assert!(summary.refetched >= 1);
    assert_eq!(summary.failed, 0);

    let details = ReviewDetails::get_from_index(&buyer_id, &review_id)
        .await?
        .expect("indexed");
    assert_eq!(
        (details.iat, details.price_weight),
        (Some(1_790_000_800), Some(4))
    );
    let bucket = &seller_summary(&market.seller_id).await.buckets[&attestor_id];
    assert_eq!((bucket.buyers, bucket.weight_total), (1, 4));
    assert_eq!(
        listing_summary(&market).await.buckets[&attestor_id].first_iat,
        Some(1_790_000_800)
    );

    // Idempotent: nothing is left to re-read for this review.
    backfill_trust_buckets().await?;
    assert_eq!(
        seller_summary(&market.seller_id).await.buckets[&attestor_id].weight_total,
        4
    );

    test.del(&buyer_kp, &path).await?;
    test.cleanup_user(&buyer_kp).await?;
    test.cleanup_user(&market.seller_kp).await?;
    Ok(())
}
