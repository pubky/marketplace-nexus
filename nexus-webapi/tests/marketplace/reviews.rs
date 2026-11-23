use super::{listing_details, seed_user, shop_details};
use crate::utils::{get_request, invalid_get_request};
use anyhow::Result;
use axum::http::StatusCode;
use nexus_common::db::{exec_single_row, queries};
use nexus_common::models::marketplace::{
    ReputationSummary, ReviewDetails, ReviewModeration, ReviewModerationState,
};
use pubky::Keypair;
use pubky_app_specs::{PubkyAppListingCondition, PubkyAppReviewRole};
use serde_json::Value;

pub(super) const LISTING_ID: &str = "R0AAAAAAAAAAA";

struct Seeded {
    seller_id: String,
    attestor_id: String,
    verified_reviewer: String,
    unverified_reviewer: String,
}

fn random_id() -> String {
    Keypair::random().public_key().to_z32()
}

pub(super) fn review(
    reviewer_id: &str,
    seller_id: &str,
    review_id: &str,
    overall: i64,
    attested: Option<(&str, &str, i64, u8)>,
    indexed_at: i64,
) -> ReviewDetails {
    ReviewDetails {
        review_id: review_id.to_string(),
        uri: format!("pubky://{reviewer_id}/pub/pubky.app/marketplace/v1/reviews/{review_id}"),
        reviewer_id: reviewer_id.to_string(),
        subject_id: seller_id.to_string(),
        listing_owner_id: seller_id.to_string(),
        listing_id: LISTING_ID.to_string(),
        role: PubkyAppReviewRole::BuyerReviewingSeller,
        rating_overall: overall,
        rating_item_accuracy: None,
        rating_shipping: Some(4),
        rating_communication: Some(2),
        text: "Arrived as described.".to_string(),
        verified: attested.is_some(),
        attestor_id: attested.map(|(attestor, ..)| attestor.to_string()),
        order_ref: attested.map(|(_, order_ref, ..)| order_ref.to_string()),
        iat: attested.map(|(_, _, iat, _)| iat),
        price_weight: attested.map(|(.., weight)| weight),
        edited_late: false,
        order_scoped: true,
        superseded: false,
        moderation: None,
        created_at: "2026-08-20T12:00:00Z".to_string(),
        updated_at: "2026-08-20T12:00:00Z".to_string(),
        revision: 1,
        indexed_at,
    }
}

fn listing_review(
    reviewer_id: &str,
    seller_id: &str,
    listing_id: &str,
    overall: i64,
    attested: Option<(&str, &str, i64, u8)>,
    indexed_at: i64,
) -> ReviewDetails {
    let mut details = review(
        reviewer_id,
        seller_id,
        &random_id(),
        overall,
        attested,
        indexed_at,
    );
    details.listing_id = listing_id.to_string();
    details
}

async fn put_graph_reviews(reviews: &[ReviewDetails]) -> Result<()> {
    for details in reviews {
        details.put_to_graph().await?;
    }
    Ok(())
}

async fn recompute_pair(
    seller_id: &str,
    listing_id: &str,
) -> Result<(ReputationSummary, ReputationSummary)> {
    let subject =
        ReputationSummary::recompute_subject(seller_id, PubkyAppReviewRole::BuyerReviewingSeller)
            .await?
            .expect("the subject aggregate exists");
    let listing = ReputationSummary::recompute_listing(seller_id, listing_id)
        .await?
        .expect("the listing aggregate exists");
    Ok((subject, listing))
}

/// Seeds a shop, one listing, one verified review (iat 1790002000, price
/// weight 3) and one unverified review, then recomputes both aggregates.
async fn seed_reviews() -> Result<Seeded> {
    crate::utils::host_url().await;

    let seller_id = Keypair::random().public_key().to_z32();
    let attestor_id = Keypair::random().public_key().to_z32();
    let verified_reviewer = Keypair::random().public_key().to_z32();
    let unverified_reviewer = Keypair::random().public_key().to_z32();
    for user in [&seller_id, &verified_reviewer, &unverified_reviewer] {
        seed_user(user).await?;
    }
    let shop = shop_details(&seller_id);
    shop.put_to_graph().await?;
    shop.put_to_index().await?;
    let category = format!("cat-{}", chrono::Utc::now().timestamp_micros());
    let listing = listing_details(
        &seller_id,
        LISTING_ID,
        "Lamp",
        &category,
        PubkyAppListingCondition::Good,
        4_000,
        chrono::Utc::now().timestamp_millis(),
    );
    listing.put_to_graph().await?;
    listing.put_to_index(false).await?;

    let order_ref = format!("{:064x}", 1);
    let now = chrono::Utc::now().timestamp_millis();
    let reviews = [
        review(
            &verified_reviewer,
            &seller_id,
            "V0AAAAAAAAAAA",
            5,
            Some((&attestor_id, &order_ref, 1_790_002_000, 3)),
            now,
        ),
        review(
            &unverified_reviewer,
            &seller_id,
            "U0AAAAAAAAAAA",
            1,
            None,
            now + 1,
        ),
    ];
    for review in &reviews {
        review.put_to_graph().await?;
        review.put_to_index(false).await?;
    }
    ReviewDetails::resolve_seller_fields(
        &verified_reviewer,
        &attestor_id,
        &order_ref,
        PubkyAppReviewRole::BuyerReviewingSeller,
    )
    .await?;
    ReputationSummary::recompute_subject(&seller_id, PubkyAppReviewRole::BuyerReviewingSeller)
        .await?;
    ReputationSummary::recompute_listing(&seller_id, LISTING_ID).await?;

    Ok(Seeded {
        seller_id,
        attestor_id,
        verified_reviewer,
        unverified_reviewer,
    })
}

fn review_by<'a>(stream: &'a Value, reviewer_id: &str) -> &'a Value {
    &stream
        .as_array()
        .expect("Review stream should be an array")
        .iter()
        .find(|entry| entry["review"]["reviewer_id"] == reviewer_id)
        .expect("The reviewer's review is in the stream")["review"]
}

#[tokio_shared_rt::test(shared)]
async fn test_review_streams_serve_the_attestation_iat_next_to_the_attestor() -> Result<()> {
    let seeded = seed_reviews().await?;

    for route in [
        format!("/v0/shop/{}/reviews", seeded.seller_id),
        format!("/v0/listing/{}/{LISTING_ID}/reviews", seeded.seller_id),
    ] {
        let stream = get_request(&route).await?;
        assert_eq!(stream.as_array().expect("array").len(), 2, "{route}");

        let verified = review_by(&stream, &seeded.verified_reviewer);
        assert_eq!(verified["attestor_id"], seeded.attestor_id.as_str());
        assert_eq!(verified["iat"], 1_790_002_000);
        assert_eq!(verified["price_weight"], 3);

        let unverified = review_by(&stream, &seeded.unverified_reviewer);
        assert_eq!(unverified["verified"], false);
        for field in ["attestor_id", "iat", "price_weight"] {
            assert_eq!(
                unverified.get(field),
                Some(&Value::Null),
                "{route}: {field} is served as null on an unverified review"
            );
        }
    }

    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn test_reputation_serves_buckets_and_scopes_them_by_attestor_windows() -> Result<()> {
    let seeded = seed_reviews().await?;
    let attestor = seeded.attestor_id.as_str();

    // Without `attestors=` the summary carries the buckets and no scoped figure
    let body = get_request(&format!("/v0/shop/{}/reputation", seeded.seller_id)).await?;
    assert_eq!(body["count"], 2);
    assert_eq!(body["verified_count"], 1);
    assert_eq!(body["unverified_count"], 1);
    assert!(body.get("scoped").is_none());
    let bucket = &body["buckets"][attestor];
    assert_eq!(bucket["orders"], 1);
    assert_eq!(bucket["buyers"], 1);
    assert_eq!(bucket["sum_buyer_means"], 5.0);
    assert_eq!(bucket["weighted_sum"], 15.0);
    assert_eq!(bucket["weight_total"], 3);
    assert_eq!(bucket["histogram"], serde_json::json!([0, 0, 0, 0, 1]));
    assert_eq!(bucket["first_iat"], 1_790_002_000);
    assert_eq!(bucket["last_iat"], 1_790_002_000);
    assert_eq!(bucket["seller_buyers"], 1);
    assert_eq!(bucket["sum_seller_buyer_means"], 3.0);
    assert_eq!(body["buckets"].as_object().expect("buckets").len(), 1);

    // A window covering the review scopes it in; one that ends before it does not
    let body = get_request(&format!(
        "/v0/shop/{}/reputation?attestors={attestor}:1790001000:",
        seeded.seller_id
    ))
    .await?;
    assert_eq!(body["scoped"]["buyers"], 1);
    assert_eq!(body["scoped"]["sum_seller_buyer_means"], 3.0);
    let body = get_request(&format!(
        "/v0/shop/{}/reputation?attestors={attestor}::1790002000",
        seeded.seller_id
    ))
    .await?;
    assert_eq!(body["scoped"]["buyers"], 0);
    assert_eq!(body["scoped"]["first_iat"], Value::Null);

    // Listing detail: the snippets carry the buckets, the scoped figures follow `attestors=`
    let detail_route = format!("/v0/listing/{}/{LISTING_ID}", seeded.seller_id);
    let body = get_request(&detail_route).await?;
    assert_eq!(body["reputation"]["buckets"][attestor]["buyers"], 1);
    assert_eq!(body["reputation"]["unverified_count"], 1);
    assert_eq!(body["listing_reputation"]["buckets"][attestor]["orders"], 1);
    assert!(body.get("scoped_reputation").is_none());
    assert!(body.get("scoped_listing_reputation").is_none());

    let body = get_request(&format!("{detail_route}?attestors={attestor}")).await?;
    assert_eq!(body["title"], "Lamp");
    assert_eq!(body["scoped_reputation"]["buyers"], 1);
    assert_eq!(body["scoped_reputation"]["seller_buyers"], 1);
    assert_eq!(body["scoped_listing_reputation"]["buyers"], 1);
    assert!(body["scoped_listing_reputation"]
        .get("seller_buyers")
        .is_none());

    // Malformed lists are rejected on both routes
    for value in [
        String::new(),
        "not-a-pubky".to_string(),
        format!("{attestor}:20:10"),
        format!("{attestor}:x:"),
        vec![attestor; 21].join(","),
    ] {
        invalid_get_request(
            &format!("/v0/shop/{}/reputation?attestors={value}", seeded.seller_id),
            StatusCode::BAD_REQUEST,
        )
        .await?;
        invalid_get_request(
            &format!("{detail_route}?attestors={value}"),
            StatusCode::BAD_REQUEST,
        )
        .await?;
    }

    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn test_same_user_edges_do_not_shape_reputation() -> Result<()> {
    crate::utils::host_url().await;
    let seller = random_id();
    let other_user = random_id();
    let attestor = random_id();
    let listing_id = random_id();
    let order_ref = random_id();
    for user in [&seller, &other_user] {
        seed_user(user).await?;
    }
    let now = chrono::Utc::now().timestamp_millis();
    let reviews = [
        listing_review(&seller, &seller, &listing_id, 1, None, now),
        listing_review(
            &seller,
            &seller,
            &listing_id,
            3,
            Some((&attestor, &order_ref, 1_790_002_000, 2)),
            now + 1,
        ),
        listing_review(&other_user, &seller, &listing_id, 5, None, now + 2),
    ];
    put_graph_reviews(&reviews).await?;

    let (subject, listing) = recompute_pair(&seller, &listing_id).await?;
    for summary in [&subject, &listing] {
        assert_eq!(summary.count, 1);
        assert_eq!(summary.verified_count, 0);
        assert_eq!(summary.unverified_count, 1);
        assert_eq!(summary.avg, 5.0);
        assert!(summary.attestors.is_empty());
        assert!(summary.buckets.is_empty());
    }

    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn test_verified_records_from_different_reviewers_count_separately() -> Result<()> {
    crate::utils::host_url().await;
    let seller = random_id();
    let first_reviewer = random_id();
    let second_reviewer = random_id();
    let attestor = random_id();
    let review_id = random_id();
    for user in [&seller, &first_reviewer, &second_reviewer] {
        seed_user(user).await?;
    }
    let now = chrono::Utc::now().timestamp_millis();
    let reviews = [
        review(
            &first_reviewer,
            &seller,
            &review_id,
            5,
            Some((&attestor, &random_id(), 1_790_002_000, 2)),
            now,
        ),
        review(
            &second_reviewer,
            &seller,
            &review_id,
            3,
            Some((&attestor, &random_id(), 1_790_002_001, 4)),
            now + 1,
        ),
    ];
    put_graph_reviews(&reviews).await?;

    let (subject, listing) = recompute_pair(&seller, LISTING_ID).await?;
    for summary in [&subject, &listing] {
        assert_eq!(summary.count, 2);
        assert_eq!(summary.verified_count, 2);
        assert_eq!(summary.unverified_count, 0);
        assert_eq!(summary.avg, 4.0);
        assert_eq!(summary.histogram, [0, 0, 1, 0, 1]);
        assert_eq!(summary.attestors.get(&attestor), Some(&2));
        let bucket = summary.buckets.get(&attestor).expect("attestor bucket");
        assert_eq!(bucket.orders, 2);
        assert_eq!(bucket.buyers, 2);
        assert_eq!(bucket.sum_buyer_means, 8.0);
        assert_eq!(bucket.histogram, [0, 0, 1, 0, 1]);
    }

    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn test_filtered_streams_leave_out_same_user_edges() -> Result<()> {
    crate::utils::host_url().await;
    let seller = random_id();
    let reviewer = random_id();
    let listing_id = random_id();
    for user in [&seller, &reviewer] {
        seed_user(user).await?;
    }
    let now = chrono::Utc::now().timestamp_millis();
    let reviews = [
        listing_review(&seller, &seller, &listing_id, 1, None, now),
        listing_review(&reviewer, &seller, &listing_id, 5, None, now + 1),
    ];
    put_graph_reviews(&reviews).await?;
    for details in &reviews {
        details.put_to_index(false).await?;
    }

    for route in [
        format!("/v0/shop/{seller}/reviews?sort=newest"),
        format!("/v0/listing/{seller}/{listing_id}/reviews?sort=newest"),
    ] {
        let body = get_request(&route).await?;
        let stream = body["reviews"].as_array().expect("filtered reviews");
        assert_eq!(stream.len(), 1, "{route}");
        assert_eq!(stream[0]["review"]["reviewer_id"], reviewer);
    }

    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn test_unverified_records_collapse_to_latest_per_user_and_listing() -> Result<()> {
    crate::utils::host_url().await;
    let seller = random_id();
    let author = random_id();
    let listing_id = random_id();
    for user in [&seller, &author] {
        seed_user(user).await?;
    }
    let now = chrono::Utc::now().timestamp_millis();
    let mut older = listing_review(&author, &seller, &listing_id, 1, None, now);
    older.created_at = "2026-08-20T12:00:00Z".to_string();
    older.order_ref = Some(random_id());
    let mut tie_a = listing_review(&author, &seller, &listing_id, 3, None, now + 1);
    tie_a.created_at = "2026-08-22T12:00:00Z".to_string();
    tie_a.order_ref = Some(random_id());
    let mut tie_b = listing_review(&author, &seller, &listing_id, 3, None, now + 2);
    tie_b.created_at = "2026-08-22T12:00:00Z".to_string();
    tie_b.order_ref = Some(random_id());
    if tie_a.review_id > tie_b.review_id {
        tie_a.rating_overall = 5;
    } else {
        tie_b.rating_overall = 5;
    }
    put_graph_reviews(&[older, tie_a, tie_b]).await?;

    let (subject, listing) = recompute_pair(&seller, &listing_id).await?;
    for summary in [&subject, &listing] {
        assert_eq!(summary.count, 1);
        assert_eq!(summary.verified_count, 0);
        assert_eq!(summary.unverified_count, 1);
        assert_eq!(summary.avg, 5.0);
        assert_eq!(summary.histogram, [0, 0, 0, 0, 1]);
        assert_eq!(
            summary.last_reviewed_at.as_deref(),
            Some("2026-08-22T12:00:00Z")
        );
    }

    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn test_unverified_records_on_two_listings_count_separately() -> Result<()> {
    crate::utils::host_url().await;
    let seller = random_id();
    let author = random_id();
    let first_listing = random_id();
    let second_listing = random_id();
    for user in [&seller, &author] {
        seed_user(user).await?;
    }
    let now = chrono::Utc::now().timestamp_millis();
    let reviews = [
        listing_review(&author, &seller, &first_listing, 4, None, now),
        listing_review(&author, &seller, &second_listing, 2, None, now + 1),
    ];
    put_graph_reviews(&reviews).await?;

    let (subject, first) = recompute_pair(&seller, &first_listing).await?;
    let second = ReputationSummary::recompute_listing(&seller, &second_listing)
        .await?
        .expect("the second listing aggregate exists");
    assert_eq!(subject.count, 2);
    assert_eq!(subject.unverified_count, 2);
    assert_eq!(subject.avg, 3.0);
    assert_eq!(first.count, 1);
    assert_eq!(first.avg, 4.0);
    assert_eq!(second.count, 1);
    assert_eq!(second.avg, 2.0);

    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn test_filtered_records_do_not_replace_counted_unverified_records() -> Result<()> {
    crate::utils::host_url().await;
    let seller = random_id();
    let author = random_id();
    let first_listing = random_id();
    let second_listing = random_id();
    for user in [&seller, &author] {
        seed_user(user).await?;
    }
    let now = chrono::Utc::now().timestamp_millis();
    let mut kept_first = listing_review(&author, &seller, &first_listing, 4, None, now);
    kept_first.created_at = "2026-08-20T12:00:00Z".to_string();
    let mut superseded = listing_review(&author, &seller, &first_listing, 1, None, now + 1);
    superseded.created_at = "2026-08-22T12:00:00Z".to_string();
    superseded.superseded = true;
    let superseded_id = superseded.review_id.clone();
    let mut kept_second = listing_review(&author, &seller, &second_listing, 2, None, now + 2);
    kept_second.created_at = "2026-08-20T12:00:00Z".to_string();
    let mut excluded = listing_review(&author, &seller, &second_listing, 5, None, now + 3);
    excluded.created_at = "2026-08-22T12:00:00Z".to_string();
    excluded.moderation = Some(ReviewModeration {
        state: ReviewModerationState::Excluded,
        reason: "not_counted".to_string(),
    });
    put_graph_reviews(&[kept_first, superseded, kept_second, excluded]).await?;
    exec_single_row(queries::put::set_review_superseded(
        &author,
        &superseded_id,
        true,
    ))
    .await?;

    let subject =
        ReputationSummary::recompute_subject(&seller, PubkyAppReviewRole::BuyerReviewingSeller)
            .await?
            .expect("the subject aggregate exists");
    let first = ReputationSummary::recompute_listing(&seller, &first_listing)
        .await?
        .expect("the first listing aggregate exists");
    let second = ReputationSummary::recompute_listing(&seller, &second_listing)
        .await?
        .expect("the second listing aggregate exists");
    assert_eq!(subject.count, 2);
    assert_eq!(subject.unverified_count, 2);
    assert_eq!(subject.avg, 3.0);
    assert_eq!(subject.histogram, [0, 1, 0, 1, 0]);
    assert_eq!(first.count, 1);
    assert_eq!(first.avg, 4.0);
    assert_eq!(second.count, 1);
    assert_eq!(second.avg, 2.0);

    Ok(())
}
