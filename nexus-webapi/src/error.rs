use axum::http::header::InvalidHeaderValue;
use axum::http::uri::InvalidUri;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use nexus_common::db::kv::RedisError;
use nexus_common::media::processors::MediaProcessorError;
use nexus_common::models::error::ModelError;
use nexus_common::types::DynError;
use std::io;
use thiserror::Error;
use tracing::error;

pub type Result<T> = core::result::Result<T, Error>;

#[derive(Error, Debug)]
pub enum Error {
    #[error("User not found: {user_id}")]
    UserNotFound { user_id: String },
    #[error("Post not found: {author_id} {post_id}")]
    PostNotFound { author_id: String, post_id: String },
    #[error("Internal server error: {source}")]
    InternalServerError { source: DynError },
    #[error("Service busy, retry shortly")]
    Busy,
    #[error("Bookmarks not found: {user_id}")]
    BookmarksNotFound { user_id: String },
    #[error("Tags not found")]
    TagsNotFound { reach: String },
    #[error("Invalid input: {message}")]
    InvalidInput { message: String },
    #[error("File not found.")]
    FileNotFound {},
    #[error("Tag {tag_id} of {tagger_id} not found")]
    TagNotFound { tag_id: String, tagger_id: String },
    #[error("Shop not found: {seller_id}")]
    ShopNotFound { seller_id: String },
    #[error("Listing not found: {seller_id} {listing_id}")]
    ListingNotFound {
        seller_id: String,
        listing_id: String,
    },
    #[error("Drop not found: {owner_id} {drop_id}")]
    DropNotFound { owner_id: String, drop_id: String },
    #[error("No indexed reviews for subject: {subject_id}")]
    ReputationNotFound { subject_id: String },
    #[error("Attestor profile not found: {attestor_id}")]
    AttestorProfileNotFound { attestor_id: String },
    // Add other custom errors here
}

impl Error {
    pub fn invalid_input(message: &str) -> Self {
        Error::InvalidInput {
            message: message.to_string(),
        }
    }
}

impl From<ModelError> for Error {
    fn from(source: ModelError) -> Self {
        match source {
            ModelError::MediaProcessorError(MediaProcessorError::Busy) => Error::Busy,
            other => Error::InternalServerError {
                source: other.into(),
            },
        }
    }
}

impl From<RedisError> for Error {
    fn from(source: RedisError) -> Self {
        Error::InternalServerError {
            source: source.into(),
        }
    }
}

impl From<DynError> for Error {
    fn from(source: DynError) -> Self {
        Error::InternalServerError { source }
    }
}

impl From<InvalidHeaderValue> for Error {
    fn from(source: InvalidHeaderValue) -> Self {
        Error::InternalServerError {
            source: Box::new(source),
        }
    }
}

impl From<InvalidUri> for Error {
    fn from(source: InvalidUri) -> Self {
        Error::InternalServerError {
            source: Box::new(source),
        }
    }
}

impl From<io::Error> for Error {
    fn from(source: io::Error) -> Self {
        Error::InternalServerError {
            source: Box::new(source),
        }
    }
}

impl IntoResponse for Error {
    fn into_response(self) -> Response {
        // HTTP Status codes
        let status_code = match self {
            Error::UserNotFound { .. } => StatusCode::NOT_FOUND,
            Error::PostNotFound { .. } => StatusCode::NOT_FOUND,
            Error::FileNotFound { .. } => StatusCode::NOT_FOUND,
            Error::BookmarksNotFound { .. } => StatusCode::NOT_FOUND,
            Error::TagsNotFound { .. } => StatusCode::NOT_FOUND,
            Error::InvalidInput { .. } => StatusCode::BAD_REQUEST,
            Error::InternalServerError { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            Error::Busy => StatusCode::SERVICE_UNAVAILABLE,
            Error::TagNotFound { .. } => StatusCode::NOT_FOUND,
            Error::ShopNotFound { .. } => StatusCode::NOT_FOUND,
            Error::ListingNotFound { .. } => StatusCode::NOT_FOUND,
            Error::DropNotFound { .. } => StatusCode::NOT_FOUND,
            Error::ReputationNotFound { .. } => StatusCode::NOT_FOUND,
            Error::AttestorProfileNotFound { .. } => StatusCode::NOT_FOUND,
            // Map other errors to appropriate status codes
        };

        // Logging
        match &self {
            Error::UserNotFound { user_id } => error!("User not found: {}", user_id),
            Error::PostNotFound { author_id, post_id } => {
                error!("Post not found: {} {}", author_id, post_id)
            }
            Error::FileNotFound {} => {
                error!("File not found.")
            }
            Error::BookmarksNotFound { user_id } => {
                error!("Bookmarks not found: {}", user_id)
            }
            Error::TagsNotFound { reach } => {
                error!("Tags not found: {}", reach)
            }
            Error::InvalidInput { message } => {
                error!("Invalid input: {}", message)
            }
            Error::TagNotFound { tag_id, tagger_id } => {
                error!("Tag not found: {} of {}", tag_id, tagger_id)
            }
            Error::ShopNotFound { seller_id } => error!("Shop not found: {}", seller_id),
            Error::ListingNotFound {
                seller_id,
                listing_id,
            } => {
                error!("Listing not found: {} {}", seller_id, listing_id)
            }
            Error::DropNotFound { owner_id, drop_id } => {
                error!("Drop not found: {} {}", owner_id, drop_id)
            }
            Error::ReputationNotFound { subject_id } => {
                error!("No indexed reviews for subject: {}", subject_id)
            }
            Error::AttestorProfileNotFound { attestor_id } => {
                error!("Attestor profile not found: {}", attestor_id)
            }
            Error::InternalServerError { source } => error!("Internal server error: {:?}", source),
            Error::Busy => error!("Service busy: no media conversion slot was free in time"),
        };

        // The cause of an internal failure (Redis, graph, filesystem) is logged
        // above and never sent to the caller.
        let message = match &self {
            Error::InternalServerError { .. } => "Internal server error".to_string(),
            other => other.to_string(),
        };
        let body = serde_json::json!({
            "error": message
        });

        let mut response = (status_code, axum::Json(body)).into_response();
        if matches!(self, Error::Busy) {
            let headers = response.headers_mut();
            headers.insert(
                axum::http::header::RETRY_AFTER,
                axum::http::HeaderValue::from_static("5"),
            );
            headers.insert(
                axum::http::header::CACHE_CONTROL,
                axum::http::HeaderValue::from_static("no-store"),
            );
        }
        response
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn body_of(error: Error) -> (StatusCode, String) {
        let response = error.into_response();
        let status = response.status();
        let bytes = axum::body::to_bytes(response.into_body(), 4096)
            .await
            .unwrap();
        (status, String::from_utf8(bytes.to_vec()).unwrap())
    }

    #[tokio::test]
    async fn internal_errors_do_not_expose_their_cause() {
        let cause = io::Error::other("WRONGTYPE Operation against a key at 10.1.2.3:6379");
        let (status, body) = body_of(Error::InternalServerError {
            source: Box::new(cause),
        })
        .await;
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert!(
            !body.contains("WRONGTYPE") && !body.contains("10.1.2.3"),
            "{body}"
        );
        assert_eq!(body, r#"{"error":"Internal server error"}"#);
    }

    #[tokio::test]
    async fn redis_failures_do_not_expose_their_cause() {
        let (status, body) = body_of(Error::from(RedisError::IoError(
            "connection reset by peer 10.1.2.3:6379".into(),
        )))
        .await;
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert!(!body.contains("10.1.2.3"), "{body}");
    }

    #[tokio::test]
    async fn a_busy_conversion_pool_answers_503_with_retry_after_and_no_store() {
        let error = Error::from(ModelError::MediaProcessorError(MediaProcessorError::Busy));
        let response = error.into_response();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(response.headers()["retry-after"], "5");
        assert_eq!(response.headers()["cache-control"], "no-store");
    }

    #[tokio::test]
    async fn a_failed_conversion_is_still_a_generic_500() {
        let error = Error::from(ModelError::MediaProcessorError(
            MediaProcessorError::command_failed("convert: /srv/files/x/main: bad"),
        ));
        let (status, body) = body_of(error).await;
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert!(!body.contains("/srv"), "{body}");
    }

    #[tokio::test]
    async fn client_errors_keep_their_message() {
        let (status, body) = body_of(Error::invalid_input("limit must be a number")).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(body.contains("limit must be a number"));
    }
}
