use super::{listing_details, seed_user, shop_details};
use crate::utils::{get_request, invalid_get_request};
use anyhow::Result;
use axum::http::StatusCode;
use nexus_common::models::marketplace::{ReputationSummary, ReviewDetails};
use pubky::Keypair;
use pubky_app_specs::{PubkyAppListingCondition, PubkyAppReviewRole};
use serde_json::Value;

const LISTING_ID: &str = "R0AAAAAAAAAAA";

struct Seeded {
    seller_id: String,
    attestor_id: String,
    verified_reviewer: String,
    unverified_reviewer: String,
}

fn review(
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
