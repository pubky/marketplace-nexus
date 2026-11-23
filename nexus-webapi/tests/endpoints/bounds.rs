use anyhow::Result;
use axum::http::StatusCode;
use nexus_common::db::RedisOps;
use nexus_common::models::notification::{Notification, NotificationBody};
use nexus_common::models::tag::post::TagPost;
use nexus_common::models::tag::traits::TagCollection;
use nexus_common::models::tag::user::TagUser;
use nexus_common::models::tag::TagDetails;
use serde_json::Value;

use crate::tags::user::PUBKY_PEER;
use crate::utils::get_request;

const MAX: usize = usize::MAX;

fn assert_array_len(body: &Value, cap: usize) {
    let items = body.as_array().expect("list response");
    assert!(items.len() <= cap, "len {} exceeds {cap}", items.len());
}

async fn get_list_allowing_missing(path: &str) -> Result<Option<Value>> {
    let url = crate::utils::host_url().await;
    let client = httpc_test::new_client("")?;
    let response = client.do_get(&format!("{url}{path}")).await?;
    assert!(!response.status().is_server_error(), "{path}");
    if response.status() == StatusCode::NOT_FOUND {
        return Ok(None);
    }
    assert_eq!(response.status(), StatusCode::OK, "{path}");
    Ok(Some(response.json_body()?))
}

fn tag_fixtures() -> Vec<TagDetails> {
    (0..101)
        .map(|index| TagDetails {
            label: format!("tag-{index}"),
            taggers: (0..101).map(|tagger| format!("tagger-{tagger}")).collect(),
            taggers_count: 101,
            relationship: false,
        })
        .collect()
}

async fn seed_tag_pages() -> Result<()> {
    crate::utils::host_url().await;
    let tags = tag_fixtures();
    TagUser::put_to_index("bounds_user", None, &tags, false).await?;
    TagPost::put_to_index("bounds_author", Some("bounds_post"), &tags, false).await?;
    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn test_tag_pages_cap_caller_limits() -> Result<()> {
    seed_tag_pages().await?;
    for path in [
        format!("/v0/user/bounds_user/tags?limit_tags={MAX}&limit_taggers={MAX}"),
        format!("/v0/post/bounds_author/bounds_post/tags?limit_tags={MAX}&limit_taggers={MAX}"),
    ] {
        let body = get_request(&path).await?;
        let tags = body.as_array().expect("tag list");
        assert_eq!(tags.len(), 100, "{path}");
        for tag in tags {
            assert_eq!(tag["taggers"].as_array().expect("taggers").len(), 100);
        }
    }
    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn test_tag_skip_and_tagger_pages_are_bounded() -> Result<()> {
    seed_tag_pages().await?;
    for path in [
        format!("/v0/user/{PUBKY_PEER}/tags?skip_tags={MAX}"),
        "/v0/post/bounds_author/bounds_post/tags?skip_tags=18446744073709551615".to_string(),
    ] {
        let body = get_request(&path).await?;
        assert!(body.as_array().expect("tag list").is_empty(), "{path}");
    }

    let body = get_request(&format!("/v0/user/bounds_user/taggers/tag-0?limit={MAX}")).await?;
    assert_eq!(body["users"].as_array().expect("taggers").len(), 100);

    let body = get_request(&format!(
        "/v0/user/bounds_user/taggers/tag-0?limit={MAX}&skip={MAX}"
    ))
    .await?;
    assert_array_len(&body["users"], 100);
    assert!(body["users"].as_array().expect("taggers").is_empty());
    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn test_notification_page_is_bounded() -> Result<()> {
    crate::utils::host_url().await;
    let entries: Vec<(f64, String)> = (0..101)
        .map(|index| {
            let body = NotificationBody::Follow {
                followed_by: format!("follower-{index}"),
            };
            (
                index as f64,
                serde_json::to_string(&body).expect("notification"),
            )
        })
        .collect();
    let entries: Vec<(f64, &str)> = entries
        .iter()
        .map(|(score, body)| (*score, body.as_str()))
        .collect();
    Notification::put_index_sorted_set(
        &["Notification", "bounds_notifications"],
        &entries,
        None,
        None,
    )
    .await?;

    let body = get_request(&format!(
        "/v0/user/bounds_notifications/notifications?limit={MAX}"
    ))
    .await?;
    assert_eq!(body.as_array().expect("notifications").len(), 100);
    let body = get_request(&format!(
        "/v0/user/bounds_notifications/notifications?limit={MAX}&skip={MAX}"
    ))
    .await?;
    assert_array_len(&body, 100);
    assert!(body.as_array().expect("notifications").is_empty());
    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn test_post_and_user_streams_are_bounded() -> Result<()> {
    for (path, cap) in [
        (format!("/v0/stream/posts?skip={MAX}&limit={MAX}"), 30),
        (
            format!("/v0/stream/users?source=most_followed&skip={MAX}&limit={MAX}"),
            20,
        ),
    ] {
        let body = get_request(&path).await?;
        assert_array_len(&body, cap);
        assert!(body.as_array().expect("stream").is_empty(), "{path}");
    }
    for (path, cap) in [
        (
            format!("/v0/stream/posts?kind=short&skip={MAX}&limit={MAX}"),
            30,
        ),
        (
            format!("/v0/stream/users?source=influencers&skip={MAX}&limit={MAX}"),
            20,
        ),
    ] {
        if let Some(body) = get_list_allowing_missing(&path).await? {
            assert_array_len(&body, cap);
            assert!(body.as_array().expect("stream").is_empty(), "{path}");
        }
    }
    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn test_search_and_follow_pages_are_bounded() -> Result<()> {
    for path in [
        format!("/v0/search/users/by_name/Jo?skip={MAX}&limit={MAX}"),
        format!("/v0/search/tags/by_prefix/he?skip={MAX}&limit={MAX}"),
    ] {
        let body = get_request(&path).await?;
        assert_array_len(&body, 1000);
        assert!(
            body.as_array().expect("search results").is_empty(),
            "{path}"
        );
    }
    for relation in ["followers", "following", "friends"] {
        let path = format!("/v0/user/{PUBKY_PEER}/{relation}?skip={MAX}&limit={MAX}");
        let body = get_list_allowing_missing(&path).await?;
        let count = body
            .as_ref()
            .map(|body| body.as_array().expect("follow list").len())
            .unwrap_or(0);
        assert!(count <= 1000, "{path}");
    }
    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn test_marketplace_pages_are_bounded_and_server_remains_ready() -> Result<()> {
    for (path, cap) in [
        (
            format!("/v0/shop/{PUBKY_PEER}/reviews?skip={MAX}&limit={MAX}"),
            30,
        ),
        (format!("/v0/stream/listings?skip={MAX}&limit={MAX}"), 30),
        (format!("/v0/stream/drops?skip={MAX}&limit={MAX}"), 30),
    ] {
        let body = get_request(&path).await?;
        assert_array_len(&body, cap);
        assert!(body.as_array().expect("stream").is_empty(), "{path}");
    }

    let body = get_request(&format!("/v0/user/{PUBKY_PEER}/tags")).await?;
    assert_array_len(&body, 5);
    Ok(())
}
