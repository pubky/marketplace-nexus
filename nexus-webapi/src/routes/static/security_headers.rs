//! Response hardening for publisher-supplied files.
//!
//! A publisher chooses the `content_type` of a file record and nexus stores the
//! blob verbatim, so the main variant can be HTML, XHTML or SVG. Served inline
//! from the API origin, such a document would run with that origin's browser
//! authority. Every static response therefore carries `nosniff`, and responses
//! whose type can execute script are sandboxed and offered as a download.

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

/// True when a browser would run script from a document of this media type.
pub fn is_active_content_type(content_type: &str) -> bool {
    let essence = content_type
        .split(';')
        .next()
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    matches!(
        essence.as_str(),
        "text/html"
            | "application/xhtml+xml"
            | "image/svg+xml"
            | "text/xml"
            | "application/xml"
            | "text/xsl"
            | "application/xslt+xml"
            | "text/cache-manifest"
    ) || essence.ends_with("+xml")
}

/// Content-Disposition for a download of `name`. A publisher-controlled name may
/// hold quotes, control characters or non-ASCII text, none of which belong in
/// a quoted-string header parameter.
pub fn attachment_disposition(name: &str) -> HeaderValue {
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

/// Applies the hardening headers to every response of the static routes.
pub async fn static_security_headers(request: Request, next: Next) -> Response {
    let mut response = next.run(request).await;
    let headers = response.headers_mut();

    headers.insert(X_CONTENT_TYPE_OPTIONS, HeaderValue::from_static("nosniff"));

    let active = headers
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .is_some_and(is_active_content_type);
    if active {
        headers.insert(
            CONTENT_SECURITY_POLICY,
            HeaderValue::from_static(ACTIVE_CONTENT_POLICY),
        );
        if !headers.contains_key(CONTENT_DISPOSITION) {
            headers.insert(CONTENT_DISPOSITION, HeaderValue::from_static("attachment"));
        }
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
    fn an_empty_name_falls_back_to_a_generic_download_name() {
        assert_eq!(
            attachment_disposition(""),
            "attachment; filename=\"download\""
        );
    }
}
