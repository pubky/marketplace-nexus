use std::path::PathBuf;
use std::sync::OnceLock;

use crate::Result;
use axum::{
    body::Body,
    http::{header::CACHE_CONTROL, HeaderValue, Request, Uri},
    response::Response,
};
use tower_http::services::{fs::ServeFileSystemResponseBody, ServeDir};
use tracing::error;

static SERVE_DIR_INSTANCE: OnceLock<ServeDir> = OnceLock::new();

/// Lets a served file be cached publicly for an hour, in place of any Cache-Control header the
/// response already has. An error, such as a 404 for a variant removed mid-serve, gets no header.
pub fn set_public_cache_control<B>(response: &mut Response<B>) {
    response.headers_mut().remove(CACHE_CONTROL);

    let status = response.status();
    if !status.is_client_error() && !status.is_server_error() {
        response.headers_mut().insert(
            CACHE_CONTROL,
            HeaderValue::from_static("public, max-age=3600"),
        );
    }
}

pub struct PubkyServeDir;

/// Wrapper around ServeDir to serve files from the configured directory
impl PubkyServeDir {
    fn get_serve_dir(file_path: PathBuf) -> ServeDir {
        SERVE_DIR_INSTANCE
            .get_or_init(|| ServeDir::new(file_path))
            .to_owned()
    }

    pub async fn try_call(
        mut request: Request<Body>,
        path: String,
        content_type: String,
        file_path: PathBuf,
    ) -> Result<Response<ServeFileSystemResponseBody>> {
        *request.uri_mut() = path.as_str().parse::<Uri>()?;

        let response_result = Self::get_serve_dir(file_path).try_call(request).await;

        let mut response = match response_result {
            // In case of success, extract the response for content-type header injection
            // is_success() checks for all success codes: 200 OK, but also 206 Partial Content
            // 206 is used for video streaming
            Ok(response) if response.status().is_success() => response,

            // In all other cases, return right away and skip content-type header injection
            Ok(response) => return Ok(response),
            Err(err) => return Err(err.into()),
        };

        // set the content type header
        let content_type_header = content_type
            .parse()
            .inspect_err(|_| error!("Invalid content type header: {}", content_type))?;

        response
            .headers_mut()
            .insert("content-type", content_type_header);
        Ok(response)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::StatusCode;

    /// A response of the given status that already has a Cache-Control header.
    fn response_with_cache_control(status: StatusCode) -> Response<()> {
        let mut response = Response::new(());
        *response.status_mut() = status;
        response
            .headers_mut()
            .insert(CACHE_CONTROL, HeaderValue::from_static("no-cache"));
        response
    }

    #[test]
    fn served_and_unchanged_files_are_cached_publicly() {
        for status in [
            StatusCode::OK,
            StatusCode::PARTIAL_CONTENT,
            StatusCode::NOT_MODIFIED,
        ] {
            let mut response = response_with_cache_control(status);

            set_public_cache_control(&mut response);

            assert_eq!(
                response.headers()[CACHE_CONTROL],
                "public, max-age=3600",
                "{status}"
            );
        }
    }

    #[test]
    fn errors_are_not_cached_publicly() {
        for status in [StatusCode::NOT_FOUND, StatusCode::INTERNAL_SERVER_ERROR] {
            let mut response = response_with_cache_control(status);

            set_public_cache_control(&mut response);

            assert!(!response.headers().contains_key(CACHE_CONTROL), "{status}");
        }
    }
}
