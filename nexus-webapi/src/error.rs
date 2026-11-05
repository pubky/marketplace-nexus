use axum::http::header::{InvalidHeaderValue, CACHE_CONTROL, RETRY_AFTER};
use axum::http::uri::InvalidUri;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use nexus_common::db::kv::RedisError;
use nexus_common::media::processors::MediaProcessorError;
use nexus_common::models::error::ModelError;
use nexus_common::types::DynError;
use std::io;
use thiserror::Error;
use tracing::{error, warn};

pub type Result<T> = core::result::Result<T, Error>;

#[derive(Error, Debug)]
pub enum Error {
    #[error("User not found: {user_id}")]
    UserNotFound { user_id: String },
    #[error("Post not found: {author_id} {post_id}")]
    PostNotFound { author_id: String, post_id: String },
    #[error("Internal server error: {source}")]
    InternalServerError { source: DynError },
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
    #[error("Service busy, retry later")]
    ServiceBusy {},
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
            ModelError::MediaProcessorError(MediaProcessorError::Busy) => Error::ServiceBusy {},
            source => Error::InternalServerError {
                source: source.into(),
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
            Error::TagNotFound { .. } => StatusCode::NOT_FOUND,
            Error::ShopNotFound { .. } => StatusCode::NOT_FOUND,
            Error::ListingNotFound { .. } => StatusCode::NOT_FOUND,
            Error::DropNotFound { .. } => StatusCode::NOT_FOUND,
            Error::ReputationNotFound { .. } => StatusCode::NOT_FOUND,
            Error::AttestorProfileNotFound { .. } => StatusCode::NOT_FOUND,
            Error::ServiceBusy {} => StatusCode::SERVICE_UNAVAILABLE,
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
            Error::ServiceBusy {} => warn!("Service busy, retry later"),
        };

        let body = serde_json::json!({
            "error": self.to_string()
        });

        if matches!(self, Error::ServiceBusy {}) {
            let headers = [(CACHE_CONTROL, "no-store"), (RETRY_AFTER, "5")];
            return (status_code, headers, axum::Json(body)).into_response();
        }

        (status_code, axum::Json(body)).into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn response_for(source: MediaProcessorError) -> Response {
        Error::from(ModelError::MediaProcessorError(source)).into_response()
    }

    async fn body_of(response: Response) -> String {
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        String::from_utf8(body.to_vec()).unwrap()
    }

    #[tokio::test]
    async fn processor_failures_answer_500_without_their_details() {
        let secret = "/static/files/secret-path";
        for source in [
            MediaProcessorError::ConversionFailed(format!("cannot read {secret}")),
            MediaProcessorError::command_failed(format!("cannot open {secret}")),
        ] {
            let response = response_for(source);

            assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
            assert!(!response.headers().contains_key(RETRY_AFTER));
            let body = body_of(response).await;
            assert!(!body.contains(secret), "{body}");
        }
    }

    #[tokio::test]
    async fn busy_answers_503_with_retry_headers() {
        let response = response_for(MediaProcessorError::Busy);

        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(response.headers()[CACHE_CONTROL], "no-store");
        assert_eq!(response.headers()[RETRY_AFTER], "5");
        assert!(body_of(response)
            .await
            .contains("Service busy, retry later"));
    }
}
