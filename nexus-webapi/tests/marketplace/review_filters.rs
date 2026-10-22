use super::reviews::{review, LISTING_ID};
use super::seed_user;
use crate::utils::{get_request, invalid_get_request};
use anyhow::Result;
use axum::http::StatusCode;
use nexus_common::db::{exec_single_row, queries};
use nexus_common::models::marketplace::{ReputationSummary, ReviewDetails};
use pubky::Keypair;
use pubky_app_specs::PubkyAppReviewRole;
use serde_json::Value;
use std::collections::HashMap;

const REVIEW_ID: &str = "F0AAAAAAAAAAA";

/// A seller reviewed by six buyers around one viewer, plus the viewer:
/// - `friend`: viewer and friend follow each other; the viewer tagged them
///   `other-label`.
/// - `followed`: the viewer follows them; another user tagged them `trusted`.
/// - `two_hops`: followed by `followed`; `three_hops`: followed by `two_hops`.
/// - `tagged`: no follow; the viewer tagged them `trusted`.
/// - `unverified`: the viewer follows them; their review is unverified.
struct Graph {
    seller: String,
    viewer: String,
    attestor: String,
    names: HashMap<String, &'static str>,
}

fn key() -> String {
    Keypair::random().public_key().to_z32()
}

async fn follow(follower: &str, followee: &str) -> Result<()> {
    exec_single_row(queries::put::create_follow(follower, followee, 1)).await?;
    Ok(())
}

async fn tag(tagger: &str, tagged: &str, label: &str) -> Result<()> {
    let tag_id = format!("T{}", chrono::Utc::now().timestamp_micros());
    exec_single_row(queries::put::create_user_tag(
        tagger, tagged, &tag_id, label, 1,
    ))
    .await?;
    Ok(())
}

async fn seed_graph() -> Result<Graph> {
    crate::utils::host_url().await;

    let seller = key();
    let viewer = key();
    let attestor = key();
    let other_tagger = key();
    let base = chrono::Utc::now().timestamp_millis();
    // (name, overall, price weight, (shipping, communication)); indexed in
    // this order. `three_hops` is a legacy review without seller fields.
    type SellerFields = (Option<i64>, Option<i64>);
    let buyers: [(&'static str, i64, Option<u8>, SellerFields); 7] = [
        ("friend", 5, Some(1), (Some(5), Some(3))),
        ("followed", 4, Some(3), (Some(4), None)),
        ("two_hops", 3, Some(2), (None, Some(2))),
        ("three_hops", 2, Some(5), (None, None)),
        ("tagged", 1, Some(4), (Some(1), Some(1))),
        ("viewer", 5, Some(2), (Some(5), Some(5))),
        ("unverified", 3, None, (Some(3), Some(3))),
    ];
    let mut ids: HashMap<&'static str, String> = HashMap::new();
    for user in [&seller, &other_tagger] {
        seed_user(user).await?;
    }
    for (i, (name, overall, weight, (shipping, communication))) in buyers.iter().enumerate() {
        let id = if *name == "viewer" {
            viewer.clone()
        } else {
            key()
        };
        seed_user(&id).await?;
        let order_ref = format!("{:064x}", i + 1);
        let iat = 1_790_000_100 + 100 * i as i64;
        let attested = weight.map(|weight| (attestor.as_str(), order_ref.as_str(), iat, weight));
        let mut details = review(&id, &seller, REVIEW_ID, *overall, attested, base + i as i64);
        details.rating_shipping = *shipping;
        details.rating_communication = *communication;
        details.put_to_graph().await?;
        details.put_to_index(false).await?;
        if weight.is_some() {
            ReviewDetails::resolve_seller_fields(
                &id,
                &attestor,
                &order_ref,
                PubkyAppReviewRole::BuyerReviewingSeller,
            )
            .await?;
        }
        ids.insert(name, id);
    }

    follow(&viewer, &ids["friend"]).await?;
    follow(&ids["friend"], &viewer).await?;
    follow(&viewer, &ids["followed"]).await?;
    follow(&viewer, &ids["unverified"]).await?;
    follow(&ids["followed"], &ids["two_hops"]).await?;
    follow(&ids["two_hops"], &ids["three_hops"]).await?;
    tag(&viewer, &ids["friend"], "other-label").await?;
    tag(&viewer, &ids["tagged"], "trusted").await?;
    tag(&other_tagger, &ids["followed"], "trusted").await?;

    ReputationSummary::recompute_subject(&seller, PubkyAppReviewRole::BuyerReviewingSeller).await?;
    ReputationSummary::recompute_listing(&seller, LISTING_ID).await?;

    Ok(Graph {
        seller,
        viewer,
        attestor,
        names: ids.into_iter().map(|(name, id)| (id, name)).collect(),
    })
}

impl Graph {
    fn routes(&self, query: &str) -> [String; 2] {
        [
            format!("/v0/shop/{}/reviews?limit=30&{query}", self.seller),
            format!(
                "/v0/listing/{}/{LISTING_ID}/reviews?limit=30&{query}",
                self.seller
            ),
        ]
    }

    /// The reviewers of a filtered page, by fixture name, in order.
    fn reviewers(&self, body: &Value) -> Vec<&'static str> {
        body["reviews"]
            .as_array()
            .expect("a filtered page")
            .iter()
            .map(|entry| self.names[entry["review"]["reviewer_id"].as_str().unwrap()])
            .collect()
    }

    /// The reviewers and `matched` of a query, the same on both streams.
    async fn get(&self, query: &str) -> Result<(Vec<&'static str>, Value)> {
        let [shop, listing] = self.routes(query);
        let shop = get_request(&shop).await?;
        let listing = get_request(&listing).await?;
        assert_eq!(self.reviewers(&shop), self.reviewers(&listing), "{query}");
        assert_eq!(
            item_figures(&shop["matched"]),
            listing["matched"],
            "{query}"
        );
        Ok((self.reviewers(&shop), item_figures(&shop["matched"])))
    }
}

/// `matched` without the storefront's seller score keys.
fn item_figures(matched: &Value) -> Value {
    serde_json::json!({
        "buyers": matched["buyers"],
        "orders": matched["orders"],
        "avg": matched["avg"],
    })
}

fn matched(buyers: u64, orders: u64, avg: Option<f64>) -> Value {
    serde_json::json!({"buyers": buyers, "orders": orders, "avg": avg})
}

#[tokio_shared_rt::test(shared)]
async fn test_each_reach_keeps_the_viewers_network() -> Result<()> {
    let graph = seed_graph().await?;
    let viewer = &graph.viewer;

    let (reviewers, counted) = graph
        .get(&format!("viewer_id={viewer}&reach=following"))
        .await?;
    assert_eq!(reviewers, ["unverified", "followed", "friend"]);
    assert_eq!(counted, matched(2, 2, Some(4.5)), "unverified never counts");

    let (reviewers, counted) = graph
        .get(&format!("viewer_id={viewer}&reach=friends"))
        .await?;
    assert_eq!(reviewers, ["friend"]);
    assert_eq!(counted, matched(1, 1, Some(5.0)));

    let (reviewers, _) = graph
        .get(&format!("viewer_id={viewer}&reach=wot&depth=1"))
        .await?;
    assert_eq!(reviewers, ["unverified", "followed", "friend"]);
    let (default_depth, _) = graph.get(&format!("viewer_id={viewer}&reach=wot")).await?;
    let (reviewers, counted) = graph
        .get(&format!("viewer_id={viewer}&reach=wot&depth=2"))
        .await?;
    assert_eq!(reviewers, ["unverified", "two_hops", "followed", "friend"]);
    assert_eq!(default_depth, reviewers, "depth defaults to 2");
    assert_eq!(counted, matched(3, 3, Some(4.0)));
    let (reviewers, _) = graph
        .get(&format!("viewer_id={viewer}&reach=wot&depth=3"))
        .await?;
    assert_eq!(
        reviewers,
        ["unverified", "three_hops", "two_hops", "followed", "friend"],
        "the viewer's own review is never in their reach, even through a follow cycle"
    );

    // A viewer the index does not hold reaches nobody.
    let (reviewers, counted) = graph
        .get(&format!("viewer_id={}&reach=following", key()))
        .await?;
    assert!(reviewers.is_empty());
    assert_eq!(counted, matched(0, 0, None));

    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn test_tagged_by_viewer_counts_only_the_viewers_tags() -> Result<()> {
    let graph = seed_graph().await?;
    let viewer = &graph.viewer;

    // `followed` carries `trusted` from another tagger: ignored.
    let (reviewers, counted) = graph
        .get(&format!("viewer_id={viewer}&tagged_by_viewer=trusted"))
        .await?;
    assert_eq!(reviewers, ["tagged"]);
    assert_eq!(counted, matched(1, 1, Some(1.0)));

    let (reviewers, _) = graph
        .get(&format!(
            "viewer_id={viewer}&tagged_by_viewer=Trusted,%20other-label"
        ))
        .await?;
    assert_eq!(reviewers, ["tagged", "friend"], "any label, case-folded");

    let (reviewers, _) = graph
        .get(&format!(
            "viewer_id={viewer}&reach=following&tagged_by_viewer=other-label,trusted"
        ))
        .await?;
    assert_eq!(reviewers, ["friend"], "reach and tags must both hold");

    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn test_each_sort_orders_the_stream() -> Result<()> {
    let graph = seed_graph().await?;

    let (newest, counted) = graph.get("sort=newest").await?;
    assert_eq!(
        newest,
        [
            "unverified",
            "viewer",
            "tagged",
            "three_hops",
            "two_hops",
            "followed",
            "friend"
        ]
    );
    // Six verified buyers: (5 + 4 + 3 + 2 + 1 + 5) / 6.
    assert_eq!(counted, matched(6, 6, Some(20.0 / 6.0)));

    // The unfiltered stream keeps its order and its bare-list shape.
    let plain = get_request(&format!("/v0/shop/{}/reviews?limit=30", graph.seller)).await?;
    let plain: Vec<&str> = plain
        .as_array()
        .expect("the plain stream is a bare list")
        .iter()
        .map(|entry| graph.names[entry["review"]["reviewer_id"].as_str().unwrap()])
        .collect();
    assert_eq!(plain, newest);

    // Price weights 5, 4, 3, then 2 twice (newest first), 1, and none last.
    let (value, _) = graph.get("sort=value").await?;
    assert_eq!(
        value,
        [
            "three_hops",
            "tagged",
            "followed",
            "viewer",
            "two_hops",
            "friend",
            "unverified"
        ]
    );
    let (desc, _) = graph.get("sort=rating_desc").await?;
    assert_eq!(
        desc,
        [
            "viewer",
            "friend",
            "followed",
            "unverified",
            "two_hops",
            "three_hops",
            "tagged"
        ]
    );
    let (asc, _) = graph.get("sort=rating_asc").await?;
    assert_eq!(
        asc,
        [
            "tagged",
            "three_hops",
            "unverified",
            "two_hops",
            "followed",
            "viewer",
            "friend"
        ]
    );

    // Paging walks the sorted matches; `matched` covers all of them.
    let [shop, _] = graph.routes("sort=value&skip=2&limit=2");
    let shop = shop.replace("limit=30&", "");
    let page = get_request(&shop).await?;
    assert_eq!(graph.reviewers(&page), ["followed", "viewer"]);
    assert_eq!(item_figures(&page["matched"]), counted);

    // The price weight sort reads the served weights.
    let weights: Vec<Value> = get_request(&graph.routes("sort=value")[0]).await?["reviews"]
        .as_array()
        .unwrap()
        .iter()
        .map(|entry| entry["review"]["price_weight"].clone())
        .collect();
    assert_eq!(
        weights,
        [5, 4, 3, 2, 2, 1]
            .into_iter()
            .map(Value::from)
            .chain([Value::Null])
            .collect::<Vec<_>>()
    );

    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn test_matched_equals_the_scoped_reputation_of_the_same_scope() -> Result<()> {
    let graph = seed_graph().await?;
    let attestor = &graph.attestor;

    for attestors in [
        attestor.to_string(),
        format!("{attestor}:1790000200:1790000500"),
        format!("{attestor}::1790000200,{attestor}:1790000400:"),
    ] {
        let (reviewers, counted) = graph.get(&format!("attestors={attestors}")).await?;
        assert!(!reviewers.contains(&"unverified"), "unverified drops out");

        let reputation = get_request(&format!(
            "/v0/shop/{}/reputation?attestors={attestors}",
            graph.seller
        ))
        .await?;
        let scoped = &reputation["scoped"];
        let buyers = scoped["buyers"].as_u64().unwrap();
        assert_eq!(counted["buyers"], buyers, "{attestors}");
        assert_eq!(counted["orders"], scoped["orders"], "{attestors}");
        assert_eq!(
            counted["avg"].as_f64(),
            Some(scoped["sum_buyer_means"].as_f64().unwrap() / buyers as f64),
            "{attestors}"
        );

        let windows = nexus_common::models::marketplace::AttestorWindow::parse_list(&attestors)
            .map_err(anyhow::Error::msg)?;
        let listing =
            ReputationSummary::scoped_by_listing(&graph.seller, LISTING_ID, &windows).await?;
        assert_eq!(counted["buyers"], listing.buyers);
        assert_eq!(counted["orders"], listing.orders);
    }

    // A moderation-excluded review is listed but leaves `matched` and the
    // scoped reputation alike.
    let tagged_id = graph
        .names
        .iter()
        .find(|(_, name)| **name == "tagged")
        .map(|(id, _)| id.clone())
        .unwrap();
    exec_single_row(
        nexus_common::db::graph::Query::new(
            "test_exclude_review",
            "MATCH (:User {id: $reviewer_id})-[r:REVIEWED {review_id: $review_id}]->(:User)
             SET r.moderation = 'excluded'",
        )
        .param("reviewer_id", tagged_id.clone())
        .param("review_id", REVIEW_ID),
    )
    .await?;
    let (reviewers, counted) = graph.get(&format!("attestors={attestor}")).await?;
    assert!(reviewers.contains(&"tagged"));
    let scoped = ReputationSummary::scoped_by_subject(
        &graph.seller,
        PubkyAppReviewRole::BuyerReviewingSeller,
        &nexus_common::models::marketplace::AttestorWindow::parse_list(attestor)
            .map_err(anyhow::Error::msg)?,
    )
    .await?;
    assert_eq!(counted["buyers"], scoped.buyers);
    assert_eq!(scoped.buyers, 5);
    assert!(ReviewDetails::get_from_index(&tagged_id, REVIEW_ID)
        .await?
        .is_some());

    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn test_inconsistent_filter_parameters_are_rejected() -> Result<()> {
    let graph = seed_graph().await?;
    let viewer = &graph.viewer;
    for query in [
        "reach=following".to_string(),
        "reach=wot&depth=2".to_string(),
        "tagged_by_viewer=trusted".to_string(),
        format!("viewer_id={viewer}&reach=everyone"),
        format!("viewer_id={viewer}&reach=wot&depth=0"),
        format!("viewer_id={viewer}&reach=wot&depth=4"),
        format!("viewer_id={viewer}&reach=friends&depth=1"),
        format!("viewer_id={viewer}&tagged_by_viewer=a,b,c,d,e,f"),
        "viewer_id=not-a-pubky".to_string(),
        "sort=oldest".to_string(),
        "attestors=".to_string(),
    ] {
        for route in graph.routes(&query) {
            invalid_get_request(&route, StatusCode::BAD_REQUEST).await?;
        }
    }
    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn test_the_storefront_matched_carries_the_rv2_seller_score() -> Result<()> {
    let graph = seed_graph().await?;
    let attestor = &graph.attestor;
    let shop_matched = async |query: &str| -> Result<Value> {
        Ok(get_request(&graph.routes(query)[0]).await?["matched"].clone())
    };

    // Every buyer with a seller rating: friend (5 + 3) / 2, followed 4,
    // two_hops 2, tagged 1, viewer 5; three_hops' legacy review has none,
    // and the unverified review never counts.
    let all = shop_matched("sort=newest").await?;
    assert_eq!(all["seller_buyers"], 5);
    assert_eq!(all["seller_avg"].as_f64(), Some(16.0 / 5.0));
    let following = shop_matched(&format!("viewer_id={}&reach=following", graph.viewer)).await?;
    assert_eq!(following["seller_buyers"], 2);
    assert_eq!(following["seller_avg"].as_f64(), Some(4.0));

    // The scoped seller figure of the same scope is the same score.
    for attestors in [
        attestor.to_string(),
        format!("{attestor}:1790000200:1790000500"),
        format!("{attestor}::1790000200,{attestor}:1790000400:"),
    ] {
        let matched = shop_matched(&format!("attestors={attestors}")).await?;
        let reputation = get_request(&format!(
            "/v0/shop/{}/reputation?attestors={attestors}",
            graph.seller
        ))
        .await?;
        let scoped = &reputation["scoped"];
        let seller_buyers = scoped["seller_buyers"].as_u64().unwrap();
        assert!(seller_buyers > 0, "{attestors}");
        assert_eq!(matched["seller_buyers"], seller_buyers, "{attestors}");
        assert_eq!(
            matched["seller_avg"].as_f64(),
            Some(scoped["sum_seller_buyer_means"].as_f64().unwrap() / seller_buyers as f64),
            "{attestors}"
        );
    }

    // The listing stream carries no seller score.
    let listing = get_request(&graph.routes("sort=newest")[1]).await?;
    assert!(listing["matched"].get("seller_buyers").is_none());
    assert!(listing["matched"].get("seller_avg").is_none());

    // A seller whose only reviews are legacy ones: no seller rating yet.
    let seller = key();
    let buyer = key();
    seed_user(&seller).await?;
    seed_user(&buyer).await?;
    let order_ref = format!("{:064x}", 1);
    let mut legacy = review(
        &buyer,
        &seller,
        REVIEW_ID,
        4,
        Some((attestor, &order_ref, 1_790_000_100, 1)),
        chrono::Utc::now().timestamp_millis(),
    );
    legacy.rating_shipping = None;
    legacy.rating_communication = None;
    legacy.put_to_graph().await?;
    legacy.put_to_index(false).await?;
    ReviewDetails::resolve_seller_fields(
        &buyer,
        attestor,
        &order_ref,
        PubkyAppReviewRole::BuyerReviewingSeller,
    )
    .await?;
    let matched =
        get_request(&format!("/v0/shop/{seller}/reviews?sort=newest")).await?["matched"].clone();
    assert_eq!(matched["buyers"], 1);
    assert_eq!(matched["seller_buyers"], 0);
    assert_eq!(matched.get("seller_avg"), Some(&Value::Null));

    Ok(())
}
