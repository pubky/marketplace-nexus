use axum::extract::DefaultBodyLimit;
use axum::http::StatusCode;
use axum::Router;
use std::time::Duration;
use std::{path::PathBuf, sync::Arc};
use tower_http::compression::CompressionLayer;
use tower_http::cors::{Any, CorsLayer};
use tower_http::limit::RequestBodyLimitLayer;
use tower_http::timeout::TimeoutLayer;
use utoipa_swagger_ui::SwaggerUi;

pub mod r#static;
pub mod v0;

mod middlewares;

#[derive(Clone)]
pub struct AppState {
    pub files_path: Arc<PathBuf>,
}

pub fn app_routes(state: AppState) -> Router<AppState> {
    let route_static = r#static::router(state.clone());
    let routes_v0 = v0::router(state.clone());

    let route_openapi = SwaggerUi::new("/swagger-ui")
        .url("/api-docs/v0/openapi.json", v0::ApiDoc::merge_docs())
        .url(
            "/api-docs/static/openapi.json",
            r#static::ApiDoc::merge_docs(),
        );

    routes_v0.merge(route_static).merge(route_openapi)
}

pub fn build_app(
    routes: Router<AppState>,
    state: AppState,
    request_timeout_secs: u64,
    max_body_size_bytes: usize,
) -> Router {
    let app = routes.with_state(state);

    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    app.layer(CompressionLayer::new())
        .layer(RequestBodyLimitLayer::new(max_body_size_bytes))
        .layer(DefaultBodyLimit::max(max_body_size_bytes))
        .layer(TimeoutLayer::with_status_code(
            StatusCode::REQUEST_TIMEOUT,
            Duration::from_secs(request_timeout_secs),
        ))
        .layer(cors)
        .layer(axum::middleware::from_fn(
            middlewares::tracing::tracing_middleware,
        ))
}
