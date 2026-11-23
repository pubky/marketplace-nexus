use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use axum::body::Body;
use axum::extract::Path;
use axum::http::header::{ACCESS_CONTROL_ALLOW_ORIGIN, CONTENT_LENGTH, ORIGIN};
use axum::http::{Method, Request, StatusCode};
use axum::routing::{get, post};
use axum::Router;
use nexus_webapi::routes::{app_routes, build_app, AppState};
use tempfile::TempDir;
use tower::ServiceExt;

fn state() -> Result<(TempDir, AppState)> {
    let temp_dir = TempDir::new()?;
    let state = AppState {
        files_path: Arc::new(temp_dir.path().to_path_buf()),
    };
    Ok((temp_dir, state))
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
