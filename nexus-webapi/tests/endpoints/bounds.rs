use anyhow::Result;
use axum::http::StatusCode;
use nexus_common::db::graph::Query;
use nexus_common::db::{exec_single_row, RedisOps};
use nexus_common::models::notification::{Notification, NotificationBody};
use nexus_common::models::post::{PostStream, POST_PER_USER_KEY_PARTS};
use nexus_common::models::tag::post::TagPost;
use nexus_common::models::tag::traits::TagCollection;
use nexus_common::models::tag::user::TagUser;
use nexus_common::models::tag::TagDetails;
use serde_json::Value;

use crate::tags::user::PUBKY_PEER;
use crate::utils::get_request;

const MAX: usize = usize::MAX;
const SKIP_BOUND: usize = 10_000;

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
async fn test_redis_stream_skip_above_bound_is_empty() -> Result<()> {
    crate::utils::host_url().await;
    let author = "bounds_redis_skip_author";
    let entries: Vec<(f64, String)> = (0..=SKIP_BOUND)
        .map(|index| (index as f64, format!("post-{index}")))
        .collect();
    let entries: Vec<(f64, &str)> = entries
        .iter()
        .map(|(score, post_id)| (*score, post_id.as_str()))
        .collect();
    let key_parts = [&POST_PER_USER_KEY_PARTS[..], &[author]].concat();
    PostStream::put_index_sorted_set(&key_parts, &entries, None, None).await?;

    let bound_page = get_request(&format!(
        "/v0/stream/posts/keys?author_id={author}&source=author&sorting=timeline&skip={SKIP_BOUND}&limit=1"
    ))
    .await?;
    assert_eq!(
        bound_page["post_keys"].as_array().expect("post keys").len(),
        1
    );
    assert_eq!(bound_page["post_keys"][0], format!("{author}:post-0"));

    let next_page = get_request(&format!(
        "/v0/stream/posts/keys?author_id={author}&source=author&sorting=timeline&skip={}&limit=1",
        SKIP_BOUND + 1
    ))
    .await?;
    assert!(next_page["post_keys"]
        .as_array()
        .expect("post keys")
        .is_empty());
    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn test_graph_stream_skip_above_bound_is_empty() -> Result<()> {
    crate::utils::host_url().await;
    let author = "bounds_graph_skip_author";
    exec_single_row(Query::new(
        "seed_graph_skip_stream",
        "MERGE (author:User {id: 'bounds_graph_skip_author'})
         WITH author
         UNWIND range(0, 10000) AS index
         MERGE (author)-[:AUTHORED]->(post:Post {id: 'bounds-graph-post-' + toString(index)})
         ON CREATE SET post.indexed_at = index
         SET post.kind = 'short', post.content = '', post.attachments = []",
    ))
    .await?;

    let bound_page = get_request(&format!(
        "/v0/stream/posts/keys?author_id={author}&source=author&sorting=timeline&kind=short&skip={SKIP_BOUND}&limit=1"
    ))
    .await;
    let next_page = get_request(&format!(
        "/v0/stream/posts/keys?author_id={author}&source=author&sorting=timeline&kind=short&skip={}&limit=1",
        SKIP_BOUND + 1
    ))
    .await;
    // Other suites compare post streams whose ties follow the graph's scan order.
    exec_single_row(Query::new(
        "remove_graph_skip_stream",
        "MATCH (author:User {id: 'bounds_graph_skip_author'})
         OPTIONAL MATCH (author)-[:AUTHORED]->(post:Post)
         DETACH DELETE post, author",
    ))
    .await?;

    let bound_page = bound_page?;
    assert_eq!(
        bound_page["post_keys"].as_array().expect("post keys").len(),
        1
    );
    assert_eq!(
        bound_page["post_keys"][0],
        format!("{author}:bounds-graph-post-0")
    );
    let next_page = next_page?;
    assert!(next_page["post_keys"]
        .as_array()
        .expect("post keys")
        .is_empty());
    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn test_user_streams_skip_above_bound_is_empty() -> Result<()> {
    let author_id = "emq37ky6fbnaun7q1ris6rx3mqmw3a33so1txfesg9jj3ak9ryoy";
    let post_id = "1A1P4D8C9K0F";
    for route in ["/v0/stream/users", "/v0/stream/users/ids"] {
        let path = format!(
            "{route}?source=post_replies&post_id={post_id}&author_id={author_id}&skip={}",
            SKIP_BOUND + 1
        );
        let body = get_request(&path).await?;
        assert!(body.as_array().expect("user stream").is_empty(), "{path}");
    }
    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn test_tag_page_skip_above_bound_is_empty() -> Result<()> {
    crate::utils::host_url().await;
    let tags: Vec<TagDetails> = (0..=SKIP_BOUND)
        .map(|index| TagDetails {
            label: format!("boundary-{index:05}"),
            taggers: vec!["tagger".into()],
            taggers_count: 1,
            relationship: false,
        })
        .collect();
    TagUser::put_to_index("bounds_skip_tags", None, &tags, false).await?;

    let bound_page = get_request(&format!(
        "/v0/user/bounds_skip_tags/tags?skip_tags={SKIP_BOUND}&limit_tags=1&limit_taggers=1"
    ))
    .await?;
    let bound_tags = bound_page.as_array().expect("tag list");
    assert_eq!(bound_tags.len(), 1);
    assert_eq!(bound_tags[0]["label"], "boundary-00000");

    let next_page = get_request(&format!(
        "/v0/user/bounds_skip_tags/tags?skip_tags={}&limit_tags=1&limit_taggers=1",
        SKIP_BOUND + 1
    ))
    .await?;
    assert!(next_page.as_array().expect("tag list").is_empty());
    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn test_taggers_page_skip_above_bound_is_empty() -> Result<()> {
    crate::utils::host_url().await;
    let tag = TagDetails {
        label: "boundary".into(),
        taggers: (0..=SKIP_BOUND)
            .map(|index| format!("tagger-{index}"))
            .collect(),
        taggers_count: SKIP_BOUND + 1,
        relationship: false,
    };
    TagUser::put_to_index("bounds_skip_taggers", None, &[tag], false).await?;

    let bound_page = get_request(&format!(
        "/v0/user/bounds_skip_taggers/taggers/boundary?skip={SKIP_BOUND}&limit=100"
    ))
    .await?;
    assert_eq!(bound_page["users"].as_array().expect("taggers").len(), 1);

    let next_page = get_request(&format!(
        "/v0/user/bounds_skip_taggers/taggers/boundary?skip={}&limit=100",
        SKIP_BOUND + 1
    ))
    .await?;
    assert!(next_page["users"].as_array().expect("taggers").is_empty());
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
