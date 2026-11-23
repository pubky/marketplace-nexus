use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use axum::body::{to_bytes, Body};
use axum::extract::Path;
use axum::http::header::{
    ACCESS_CONTROL_ALLOW_ORIGIN, CONTENT_LENGTH, CONTENT_TYPE, ORIGIN, RETRY_AFTER,
};
use axum::http::{HeaderValue, Method, Request, StatusCode};
use axum::routing::{get, post};
use axum::Router;
use nexus_common::media::processors::acquire_all_conversion_slots;
use nexus_common::models::{
    file::{FileDetails, FileUrls},
    traits::Collection,
    user::UserDetails,
};
use nexus_common::{get_files_dir_test_pathbuf, ApiConfig};
use nexus_webapi::routes::{app_routes, build_app, AppState};
use pubky::Keypair;
use pubky_app_specs::PubkyId;
use tempfile::TempDir;
use tokio::sync::Mutex;
use tower::ServiceExt;

use crate::utils::server::TestServiceServer;

fn state() -> Result<(TempDir, AppState)> {
    let temp_dir = TempDir::new()?;
    let state = AppState {
        files_path: Arc::new(temp_dir.path().to_path_buf()),
    };
    Ok((temp_dir, state))
}

static MEDIA_ROUTER_TEST_LOCK: Mutex<()> = Mutex::const_new(());

/// The media settings, the database connections and the static file root are
/// process-wide, and the first use sets them. These tests go through the shared test
/// server and its files directory, so the media settings are the defaults and the
/// router gets the default request timeout above them.
async fn media_router_state() -> AppState {
    TestServiceServer::get_test_server().await;
    AppState {
        files_path: Arc::new(get_files_dir_test_pathbuf()),
    }
}

async fn remove_media_file(state: &AppState, file: &FileDetails) -> Result<()> {
    file.delete().await?;
    tokio::fs::remove_dir_all(state.files_path.join(&file.owner_id)).await?;
    Ok(())
}

fn media_router(state: AppState) -> Router {
    build_app(
        app_routes(state.clone()),
        state,
        ApiConfig::default().request_timeout_secs,
        1024 * 1024,
    )
}

async fn index_media_file(
    state: &AppState,
    file_id: &str,
    content_type: &str,
    body: &[u8],
) -> Result<FileDetails> {
    let owner_id = Keypair::random().public_key().to_z32();
    let origin_dir = state.files_path.join(&owner_id).join(file_id);
    tokio::fs::create_dir_all(&origin_dir).await?;
    tokio::fs::write(origin_dir.join("main"), body).await?;
    let file = FileDetails {
        id: file_id.to_string(),
        uri: format!("pubky://{owner_id}/pub/pubky.app/files/{file_id}"),
        owner_id: owner_id.clone(),
        indexed_at: 0,
        created_at: 0,
        src: format!("pubky://{owner_id}/pub/pubky.app/blobs/{file_id}"),
        name: "image.png".to_string(),
        size: body.len() as i64,
        content_type: content_type.to_string(),
        urls: FileUrls {
            main: format!("{owner_id}/{file_id}/main"),
            feed: None,
            small: None,
        },
        metadata: None,
    };
    file.put_to_graph().await?;
    FileDetails::put_to_index(&[&[owner_id.as_str(), file_id]], vec![Some(file.clone())]).await?;
    Ok(file)
}

async fn index_avatar(file: &FileDetails) -> Result<()> {
    let user = UserDetails {
        name: "Router Avatar".to_string(),
        bio: None,
        id: PubkyId::try_from(file.owner_id.as_str()).map_err(anyhow::Error::msg)?,
        links: None,
        status: None,
        image: Some(file.uri.clone()),
        indexed_at: 0,
    };
    user.put_to_graph().await?;
    UserDetails::put_to_index(&[file.owner_id.as_str()], vec![Some(user)]).await?;
    Ok(())
}

async fn png_body() -> Result<Vec<u8>> {
    Ok(tokio::fs::read("tests/files/blobs/SynonymLogo.png").await?)
}

async fn bytes_handler(_body: axum::body::Bytes) -> StatusCode {
    StatusCode::OK
}

async fn sleep_handler(Path(millis): Path<u64>) -> StatusCode {
    tokio::time::sleep(Duration::from_millis(millis)).await;
    StatusCode::OK
}

async fn ok_handler() -> StatusCode {
    StatusCode::OK
}

#[tokio::test]
async fn test_request_body_size_limit() -> Result<()> {
    let (_temp_dir, state) = state()?;
    let app = build_app(app_routes(state.clone()), state, 30, 10);
    let body = vec![0u8; 1000];
    let req = Request::builder()
        .method(Method::POST)
        .uri("/v0/files/by_ids")
        .header(CONTENT_LENGTH, body.len().to_string())
        .body(Body::from(body))?;

    let response = app.oneshot(req).await?;
    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    Ok(())
}

#[tokio::test]
async fn test_request_body_size_limit_without_content_length() -> Result<()> {
    let (_temp_dir, state) = state()?;
    let routes = Router::new().route("/echo", post(bytes_handler));
    let app = build_app(routes, state, 30, 10);
    let req = Request::builder()
        .method(Method::POST)
        .uri("/echo")
        .body(Body::from(vec![0u8; 1000]))?;
    assert!(req.headers().get(CONTENT_LENGTH).is_none());

    let response = app.oneshot(req).await?;
    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    Ok(())
}

#[tokio::test]
async fn test_request_timeout_returns_408() -> Result<()> {
    tokio::time::pause();
    let (_temp_dir, state) = state()?;
    let routes = Router::new().route("/sleep/{millis}", get(sleep_handler));
    let app = build_app(routes, state, 1, 1024 * 1024);
    let req = Request::builder()
        .uri("/sleep/9999999")
        .body(Body::empty())?;
    let task = tokio::spawn(app.oneshot(req));
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(2)).await;

    let response = task.await??;
    assert_eq!(response.status(), StatusCode::REQUEST_TIMEOUT);
    Ok(())
}

#[tokio::test]
async fn test_body_size_limit_response_has_cors_header() -> Result<()> {
    let (_temp_dir, state) = state()?;
    let routes = Router::new().route("/echo", post(ok_handler));
    let app = build_app(routes, state, 30, 10);
    let req = Request::builder()
        .method(Method::POST)
        .uri("/echo")
        .header(ORIGIN, "https://example.com")
        .header(CONTENT_LENGTH, "1000")
        .body(Body::from(vec![0u8; 1000]))?;

    let response = app.oneshot(req).await?;
    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    assert!(response.headers().contains_key(ACCESS_CONTROL_ALLOW_ORIGIN));
    Ok(())
}

#[tokio::test]
async fn test_request_timeout_response_has_cors_header() -> Result<()> {
    tokio::time::pause();
    let (_temp_dir, state) = state()?;
    let routes = Router::new().route("/sleep/{millis}", get(sleep_handler));
    let app = build_app(routes, state, 1, 1024 * 1024);
    let req = Request::builder()
        .uri("/sleep/9999999")
        .header(ORIGIN, "https://example.com")
        .body(Body::empty())?;
    let task = tokio::spawn(app.oneshot(req));
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(2)).await;

    let response = task.await??;
    assert_eq!(response.status(), StatusCode::REQUEST_TIMEOUT);
    assert!(response.headers().contains_key(ACCESS_CONTROL_ALLOW_ORIGIN));
    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn test_file_variant_slots_held_returns_503_with_retry_after() -> Result<()> {
    let _serial = MEDIA_ROUTER_TEST_LOCK.lock().await;
    let state = media_router_state().await;
    let file = index_media_file(&state, "2ZKBUSY000001", "image/png", &png_body().await?).await?;
    let held = acquire_all_conversion_slots().await;
    let response = media_router(state.clone())
        .oneshot(
            Request::builder()
                .uri(format!("/static/files/{}/{}/small", file.owner_id, file.id))
                .body(Body::empty())?,
        )
        .await?;
    drop(held);
    remove_media_file(&state, &file).await?;

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        response.headers().get(RETRY_AFTER).unwrap(),
        HeaderValue::from_static("5")
    );
    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn test_avatar_slots_held_serves_original() -> Result<()> {
    let _serial = MEDIA_ROUTER_TEST_LOCK.lock().await;
    let state = media_router_state().await;
    let png = png_body().await?;
    let file = index_media_file(&state, "2ZKAVATAR0001", "image/png", &png).await?;
    index_avatar(&file).await?;
    let held = acquire_all_conversion_slots().await;
    let response = media_router(state.clone())
        .oneshot(
            Request::builder()
                .uri(format!("/static/avatar/{}", file.owner_id))
                .body(Body::empty())?,
        )
        .await?;
    drop(held);
    let status = response.status();
    let content_type = response
        .headers()
        .get(CONTENT_TYPE)
        .unwrap()
        .to_str()?
        .to_string();
    let body = to_bytes(response.into_body(), png.len() + 1024).await?;
    remove_media_file(&state, &file).await?;
    UserDetails::delete(&file.owner_id).await?;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(content_type, "image/png");
    assert_eq!(body.as_ref(), png.as_slice());
    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn test_file_variant_declared_type_mismatch_returns_400() -> Result<()> {
    let _serial = MEDIA_ROUTER_TEST_LOCK.lock().await;
    let state = media_router_state().await;
    let file = index_media_file(
        &state,
        "2ZKTYPE000001",
        "image/png",
        &[0xFF, 0xD8, 0xFF, 0x00],
    )
    .await?;
    let response = media_router(state.clone())
        .oneshot(
            Request::builder()
                .uri(format!("/static/files/{}/{}/small", file.owner_id, file.id))
                .body(Body::empty())?,
        )
        .await?;
    remove_media_file(&state, &file).await?;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    Ok(())
}
