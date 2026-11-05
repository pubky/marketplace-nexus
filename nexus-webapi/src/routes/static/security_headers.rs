//! Response hardening for publisher-supplied files.
//!
//! A publisher chooses the `content_type` of a file record and nexus stores the
//! blob verbatim, so the main variant can be HTML, XHTML or SVG. Served inline
//! from the API origin, such a document would run with that origin's browser
//! authority. Every static response therefore carries `nosniff`, and a response
//! whose type is not known to be inert (or cannot be classified) is sandboxed
//! and offered as a download.

use axum::{
    extract::Request,
    http::{
        header::{CONTENT_DISPOSITION, CONTENT_SECURITY_POLICY, CONTENT_TYPE},
        HeaderName, HeaderValue,
    },
    middleware::Next,
    response::Response,
};

const X_CONTENT_TYPE_OPTIONS: HeaderName = HeaderName::from_static("x-content-type-options");

/// No script, no embedded resources, an opaque origin and no top-level navigation.
const ACTIVE_CONTENT_POLICY: &str = "default-src 'none'; sandbox";

/// The `type/subtype` of a single, well-formed media type, lowercased.
fn media_type_essence(content_type: &str) -> Option<String> {
    if content_type.contains(',') {
        return None;
    }
    let essence = content_type
        .split(';')
        .next()
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    let (kind, subtype) = essence.split_once('/')?;
    let is_token = |part: &str| {
        !part.is_empty()
            && part
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"!#$&-^_.+".contains(&b))
    };
    (is_token(kind) && is_token(subtype)).then_some(essence)
}

/// True when a browser may render a document of this media type inline without
/// giving it script. Anything unrecognised, malformed or ambiguous is not.
pub fn is_inline_safe(content_type: &str) -> bool {
    let Some(essence) = media_type_essence(content_type) else {
        return false;
    };
    let (kind, subtype) = essence.split_once('/').unwrap_or_default();
    match kind {
        "image" => !subtype.ends_with("+xml"),
        "video" | "audio" => true,
        "application" => matches!(subtype, "pdf" | "octet-stream" | "json"),
        "text" => subtype == "plain",
        _ => false,
    }
}

/// Longest download name used. The specs cap file names at 255 characters; the
/// limit keeps the encoded header (three bytes per character, twice) small even
/// for a record that was indexed without that check.
const MAX_DOWNLOAD_NAME_CHARS: usize = 255;

/// Content-Disposition for a download of `name`. A publisher-controlled name may
/// hold quotes, control characters or non-ASCII text, none of which belong in
/// a quoted-string header parameter.
pub fn attachment_disposition(name: &str) -> HeaderValue {
    let name: String = name.chars().take(MAX_DOWNLOAD_NAME_CHARS).collect();
    let name = name.as_str();
    let plain = name
        .chars()
        .all(|c| c.is_ascii() && !c.is_ascii_control() && c != '"' && c != '\\');
    let value = if name.is_empty() {
        "attachment; filename=\"download\"".to_string()
    } else if plain {
        format!("attachment; filename=\"{name}\"")
    } else {
        let fallback: String = name
            .chars()
            .map(|c| {
                if c.is_ascii() && !c.is_ascii_control() && c != '"' && c != '\\' {
                    c
                } else {
                    '_'
                }
            })
            .collect();
        format!(
            "attachment; filename=\"{fallback}\"; filename*=UTF-8''{}",
            percent_encode_attr_chars(name)
        )
    };
    HeaderValue::from_str(&value).unwrap_or_else(|_| HeaderValue::from_static("attachment"))
}

/// RFC 8187 `attr-char` percent-encoding.
fn percent_encode_attr_chars(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z'
            | b'a'..=b'z'
            | b'0'..=b'9'
            | b'!'
            | b'#'
            | b'$'
            | b'&'
            | b'+'
            | b'-'
            | b'.'
            | b'^'
            | b'_'
            | b'`'
            | b'|'
            | b'~' => out.push(byte as char),
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}

/// What a response declares about its own type.
enum DeclaredType {
    Missing,
    /// Empty, repeated, comma-joined or otherwise not a single `type/subtype`.
    Unclassifiable,
    Media(String),
}

fn declared_type(headers: &axum::http::HeaderMap) -> DeclaredType {
    let mut values = headers.get_all(CONTENT_TYPE).iter();
    match (values.next(), values.next()) {
        (None, _) => DeclaredType::Missing,
        (Some(value), None) => match value.to_str() {
            Ok(text) if media_type_essence(text).is_some() => DeclaredType::Media(text.to_string()),
            _ => DeclaredType::Unclassifiable,
        },
        (Some(_), Some(_)) => DeclaredType::Unclassifiable,
    }
}

/// Applies the hardening headers to every response of the static routes.
///
/// A successful response whose declared type is not known to be inert is
/// sandboxed and offered as a download. A type that is absent, empty,
/// repeated or malformed cannot be classified, so it is replaced by
/// `application/octet-stream` instead of being left to the browser's guess.
pub async fn static_security_headers(request: Request, next: Next) -> Response {
    let mut response = next.run(request).await;
    let success = response.status().is_success();
    let declared = declared_type(response.headers());
    let headers = response.headers_mut();

    headers.insert(X_CONTENT_TYPE_OPTIONS, HeaderValue::from_static("nosniff"));

    match declared {
        DeclaredType::Missing if !success => return response,
        DeclaredType::Media(content_type) if is_inline_safe(&content_type) => return response,
        DeclaredType::Missing | DeclaredType::Unclassifiable => {
            headers.insert(
                CONTENT_TYPE,
                HeaderValue::from_static("application/octet-stream"),
            );
        }
        DeclaredType::Media(_) => {}
    }

    headers.insert(
        CONTENT_SECURITY_POLICY,
        HeaderValue::from_static(ACTIVE_CONTENT_POLICY),
    );
    if !headers.contains_key(CONTENT_DISPOSITION) {
        headers.insert(CONTENT_DISPOSITION, HeaderValue::from_static("attachment"));
    }
    response
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{body::Body, http::StatusCode, routing::get, Router};
    use tower::ServiceExt;

    fn app(content_type: &'static str, disposition: Option<&'static str>) -> Router {
        Router::new()
            .route(
                "/file",
                get(move || async move {
                    let mut builder = Response::builder()
                        .status(StatusCode::OK)
                        .header(CONTENT_TYPE, content_type);
                    if let Some(disposition) = disposition {
                        builder = builder.header(CONTENT_DISPOSITION, disposition);
                    }
                    builder
                        .body(Body::from("<script>alert(1)</script>"))
                        .unwrap()
                }),
            )
            .layer(axum::middleware::from_fn(static_security_headers))
    }

    async fn fetch(app: Router) -> Response {
        app.oneshot(Request::builder().uri("/file").body(Body::empty()).unwrap())
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn html_is_sandboxed_and_downloaded() {
        let response = fetch(app("text/html", None)).await;
        let headers = response.headers();
        assert_eq!(headers["x-content-type-options"], "nosniff");
        assert_eq!(headers[CONTENT_SECURITY_POLICY], ACTIVE_CONTENT_POLICY);
        assert_eq!(headers[CONTENT_DISPOSITION], "attachment");
    }

    #[tokio::test]
    async fn svg_with_parameters_and_mixed_case_is_sandboxed() {
        let response = fetch(app("Image/SVG+XML; charset=utf-8", None)).await;
        let headers = response.headers();
        assert_eq!(headers[CONTENT_SECURITY_POLICY], ACTIVE_CONTENT_POLICY);
        assert_eq!(headers[CONTENT_DISPOSITION], "attachment");
    }

    #[tokio::test]
    async fn other_xml_media_types_are_sandboxed() {
        for content_type in [
            "application/xhtml+xml",
            "application/xml",
            "text/xml",
            "application/rss+xml",
        ] {
            let response = fetch(app(content_type, None)).await;
            assert_eq!(
                response.headers()[CONTENT_SECURITY_POLICY],
                ACTIVE_CONTENT_POLICY,
                "{content_type}"
            );
        }
    }

    #[tokio::test]
    async fn an_explicit_download_name_is_kept() {
        let response = fetch(app("text/html", Some("attachment; filename=\"a.html\""))).await;
        assert_eq!(
            response.headers()[CONTENT_DISPOSITION],
            "attachment; filename=\"a.html\""
        );
        assert_eq!(
            response.headers()[CONTENT_SECURITY_POLICY],
            ACTIVE_CONTENT_POLICY
        );
    }

    #[tokio::test]
    async fn media_stays_inline_but_is_never_sniffed() {
        for content_type in ["image/png", "video/mp4", "application/pdf", "image/webp"] {
            let response = fetch(app(content_type, None)).await;
            let headers = response.headers();
            assert_eq!(
                headers["x-content-type-options"], "nosniff",
                "{content_type}"
            );
            assert!(
                !headers.contains_key(CONTENT_SECURITY_POLICY),
                "{content_type}"
            );
            assert!(!headers.contains_key(CONTENT_DISPOSITION), "{content_type}");
        }
    }

    #[tokio::test]
    async fn unknown_types_are_not_rendered_inline() {
        for content_type in [
            "text/css",
            "text/javascript",
            "application/zip",
            "font/woff2",
            "model/gltf+json",
            "multipart/form-data",
        ] {
            let response = fetch(app(content_type, None)).await;
            let headers = response.headers();
            assert_eq!(
                headers[CONTENT_SECURITY_POLICY], ACTIVE_CONTENT_POLICY,
                "{content_type}"
            );
            assert_eq!(headers[CONTENT_DISPOSITION], "attachment", "{content_type}");
            assert_eq!(headers[CONTENT_TYPE], content_type);
        }
    }

    #[tokio::test]
    async fn an_empty_or_malformed_type_becomes_a_sandboxed_download() {
        for content_type in [
            "",
            " ",
            "text/html, text/plain",
            "html",
            "text/",
            "/html",
            "a b/c",
        ] {
            let response = fetch(app(content_type, None)).await;
            let headers = response.headers();
            assert_eq!(
                headers[CONTENT_TYPE], "application/octet-stream",
                "{content_type:?}"
            );
            assert_eq!(
                headers[CONTENT_SECURITY_POLICY], ACTIVE_CONTENT_POLICY,
                "{content_type:?}"
            );
            assert_eq!(
                headers[CONTENT_DISPOSITION], "attachment",
                "{content_type:?}"
            );
            assert_eq!(headers["x-content-type-options"], "nosniff");
        }
    }

    #[tokio::test]
    async fn a_type_with_non_ascii_bytes_becomes_a_sandboxed_download() {
        let app = Router::new()
            .route(
                "/file",
                get(|| async {
                    Response::builder()
                        .header(
                            CONTENT_TYPE,
                            HeaderValue::from_bytes(b"text/html; note=\"\xc3\xa9\"").unwrap(),
                        )
                        .body(Body::from("<script>alert(1)</script>"))
                        .unwrap()
                }),
            )
            .layer(axum::middleware::from_fn(static_security_headers));
        let response = fetch(app).await;
        let headers = response.headers();
        assert_eq!(headers[CONTENT_TYPE], "application/octet-stream");
        assert_eq!(headers[CONTENT_SECURITY_POLICY], ACTIVE_CONTENT_POLICY);
        assert_eq!(headers[CONTENT_DISPOSITION], "attachment");
    }

    #[tokio::test]
    async fn a_repeated_type_header_becomes_a_sandboxed_download() {
        let app = Router::new()
            .route(
                "/file",
                get(|| async {
                    Response::builder()
                        .header(CONTENT_TYPE, "image/png")
                        .header(CONTENT_TYPE, "text/html")
                        .body(Body::from("x"))
                        .unwrap()
                }),
            )
            .layer(axum::middleware::from_fn(static_security_headers));
        let response = fetch(app).await;
        let headers = response.headers();
        assert_eq!(headers.get_all(CONTENT_TYPE).iter().count(), 1);
        assert_eq!(headers[CONTENT_TYPE], "application/octet-stream");
        assert_eq!(headers[CONTENT_SECURITY_POLICY], ACTIVE_CONTENT_POLICY);
    }

    #[tokio::test]
    async fn a_successful_response_with_no_type_becomes_a_sandboxed_download() {
        let app = Router::new()
            .route(
                "/file",
                get(|| async { Response::new(Body::from("<script>alert(1)</script>")) }),
            )
            .layer(axum::middleware::from_fn(static_security_headers));
        let response = fetch(app).await;
        let headers = response.headers();
        assert_eq!(headers[CONTENT_TYPE], "application/octet-stream");
        assert_eq!(headers[CONTENT_SECURITY_POLICY], ACTIVE_CONTENT_POLICY);
        assert_eq!(headers[CONTENT_DISPOSITION], "attachment");
    }

    #[tokio::test]
    async fn a_partial_content_response_is_hardened_like_a_full_one() {
        let app = Router::new()
            .route(
                "/file",
                get(|| async {
                    Response::builder()
                        .status(StatusCode::PARTIAL_CONTENT)
                        .header(CONTENT_TYPE, "text/html")
                        .body(Body::from("<scr"))
                        .unwrap()
                }),
            )
            .layer(axum::middleware::from_fn(static_security_headers));
        let response = fetch(app).await;
        assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
        assert_eq!(
            response.headers()[CONTENT_SECURITY_POLICY],
            ACTIVE_CONTENT_POLICY
        );
    }

    #[tokio::test]
    async fn a_redirect_without_a_type_only_gains_nosniff() {
        let app = Router::new()
            .route(
                "/file",
                get(|| async { axum::response::Redirect::permanent("/elsewhere") }),
            )
            .layer(axum::middleware::from_fn(static_security_headers));
        let response = fetch(app).await;
        let headers = response.headers();
        assert_eq!(headers["x-content-type-options"], "nosniff");
        assert!(!headers.contains_key(CONTENT_TYPE));
        assert!(!headers.contains_key(CONTENT_SECURITY_POLICY));
    }

    #[tokio::test]
    async fn plain_text_and_json_stay_inline() {
        for content_type in ["text/plain; charset=utf-8", "application/json"] {
            let response = fetch(app(content_type, None)).await;
            let headers = response.headers();
            assert!(
                !headers.contains_key(CONTENT_SECURITY_POLICY),
                "{content_type}"
            );
            assert!(!headers.contains_key(CONTENT_DISPOSITION), "{content_type}");
        }
    }

    #[tokio::test]
    async fn error_responses_are_also_marked_nosniff() {
        let app = Router::new()
            .route(
                "/file",
                get(|| async { (StatusCode::NOT_FOUND, "File not found.") }),
            )
            .layer(axum::middleware::from_fn(static_security_headers));
        let response = fetch(app).await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        assert_eq!(response.headers()["x-content-type-options"], "nosniff");
    }

    #[test]
    fn plain_names_keep_the_existing_header_form() {
        assert_eq!(
            attachment_disposition("photo 1.png"),
            "attachment; filename=\"photo 1.png\""
        );
    }

    #[test]
    fn names_with_quotes_controls_or_non_ascii_get_an_encoded_form() {
        let value = attachment_disposition("a\"b\r\nSet-Cookie: x=1.html");
        let text = value.to_str().expect("header must be valid text");
        assert!(!text.contains('\r') && !text.contains('\n'));
        assert!(text
            .starts_with("attachment; filename=\"a_b__Set-Cookie: x=1.html\"; filename*=UTF-8''"));
        assert!(text.contains("%22") && text.contains("%0D%0A"));

        let value = attachment_disposition("café.svg");
        assert_eq!(
            value,
            "attachment; filename=\"caf_.svg\"; filename*=UTF-8''caf%C3%A9.svg"
        );
    }

    #[test]
    fn a_very_long_name_is_cut_before_encoding() {
        let name = "é".repeat(100_000);
        let value = attachment_disposition(&name);
        assert!(value.len() < 4_000, "{}", value.len());
        assert!(value
            .to_str()
            .unwrap()
            .starts_with("attachment; filename=\""));
    }

    #[test]
    fn an_empty_name_falls_back_to_a_generic_download_name() {
        assert_eq!(
            attachment_disposition(""),
            "attachment; filename=\"download\""
        );
    }
}
