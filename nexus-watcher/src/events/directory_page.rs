//! Bounded reads of one page of a shallow homeserver directory listing.
//!
//! A homeserver's answer is untrusted: `limit` is only a request to it. A page
//! is read against a byte budget, refused when it has more entries than were
//! asked for, and refused when any entry is not a resource inside the listed
//! directory. Entries are parsed with the SDK's [PubkyResource] parser, the
//! way the SDK's own LIST does.

use pubky::{Method, PubkyHttpClient, PubkyResource, StatusCode};
use reqwest::Url;

use crate::events::remote_body::{event_feed_budget, percent_encode_query_value, read_capped};
use crate::events::EventProcessorError;

/// One page of the shallow listing of `directory` (a `pubky://` directory
/// ending with `/`), at most `limit` entries from after `cursor`, or `None`
/// when the homeserver answers 404.
///
/// The body is read against a byte budget, but there is no deadline here:
/// callers wrap the call in [`within`](crate::events::remote_body::within).
pub async fn fetch_directory_page(
    client: &PubkyHttpClient,
    directory: &PubkyResource,
    limit: u16,
    cursor: Option<&str>,
) -> Result<Option<Vec<PubkyResource>>, EventProcessorError> {
    let mut url = format!(
        "https://_pubky.{}{}?shallow&limit={limit}",
        directory.owner.z32(),
        directory.path.as_str()
    );
    if let Some(cursor) = cursor {
        url.push_str("&cursor=");
        url.push_str(&percent_encode_query_value(cursor));
    }
    let response = client
        .request(Method::GET, &url)
        .send()
        .await
        .map_err(|e| EventProcessorError::client_error(e.to_string()))?;
    read_directory_page(response, directory, limit).await
}

/// The entries of a directory page `response`, or `None` for a 404. Any other
/// non-success status is a client error.
async fn read_directory_page(
    response: reqwest::Response,
    directory: &PubkyResource,
    limit: u16,
) -> Result<Option<Vec<PubkyResource>>, EventProcessorError> {
    let status = response.status();
    if status == StatusCode::NOT_FOUND {
        return Ok(None);
    }
    if !status.is_success() {
        return Err(EventProcessorError::client_error(format!(
            "listing {} answered {status}",
            directory.to_pubky_url()
        )));
    }
    let body = read_capped(
        response,
        event_feed_budget(u32::from(limit)),
        "Directory page",
    )
    .await?;
    parse_directory_page(&String::from_utf8_lossy(&body), directory, limit).map(Some)
}

/// Parses a page with one resource per line, as `pubky://` URLs (or the
/// `pubky<owner>/` form) or `https://_pubky.<owner>/<path>` transport URLs.
/// More than `limit` entries means the homeserver ignored the request, so the
/// page is refused, and so is a page with any entry that is malformed, owned
/// by someone else, or not inside `directory`.
fn parse_directory_page(
    text: &str,
    directory: &PubkyResource,
    limit: u16,
) -> Result<Vec<PubkyResource>, EventProcessorError> {
    let mut entries = Vec::new();
    for line in text.lines().map(str::trim).filter(|line| !line.is_empty()) {
        if entries.len() == usize::from(limit) {
            return Err(EventProcessorError::InvalidEventLine(format!(
                "directory page has more than {limit} entries"
            )));
        }
        let entry = parse_entry(line).map_err(|e| {
            EventProcessorError::InvalidEventLine(format!(
                "directory entry is not a Pubky resource ({e}): {line}"
            ))
        })?;
        let inside = entry.owner == directory.owner
            && entry
                .path
                .as_str()
                .strip_prefix(directory.path.as_str())
                .is_some_and(|rest| !rest.is_empty());
        if !inside {
            return Err(EventProcessorError::InvalidEventLine(format!(
                "directory entry is not inside {}: {line}",
                directory.to_pubky_url()
            )));
        }
        entries.push(entry);
    }
    Ok(entries)
}

/// One entry line, parsed as the SDK's LIST parses it.
fn parse_entry(line: &str) -> Result<PubkyResource, pubky::Error> {
    if line.starts_with("http://") || line.starts_with("https://") {
        PubkyResource::from_transport_url(&Url::parse(line)?)
    } else {
        line.parse()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::events::remote_body::test_server::{serve, write_chunk};
    use crate::events::remote_body::within;
    use pubky::Keypair;
    use std::time::Duration;
    use tokio::io::AsyncWriteExt;

    const DIRECTORY_PATH: &str = "pub/pubky.app/marketplace/v1/reviews/";
    const PAGE: u16 = 500;

    fn directory(owner: &pubky::PublicKey) -> PubkyResource {
        format!("pubky://{}/{DIRECTORY_PATH}", owner.z32())
            .parse()
            .unwrap()
    }

    fn pubky_line(owner: &pubky::PublicKey, path: &str) -> String {
        format!("pubky://{}/{path}", owner.z32())
    }

    fn transport_line(owner: &pubky::PublicKey, path: &str) -> String {
        format!("https://_pubky.{}/{path}", owner.z32())
    }

    fn full_page(line: impl Fn(&str) -> String) -> String {
        (0..PAGE)
            .map(|i| line(&format!("{DIRECTORY_PATH}{i:05}")))
            .collect::<Vec<_>>()
            .join("\n")
    }

    fn assert_refused(result: Result<Vec<PubkyResource>, EventProcessorError>, what: &str) {
        assert!(
            matches!(result, Err(EventProcessorError::InvalidEventLine(_))),
            "{what}: {result:?}"
        );
    }

    async fn respond(head: &'static str, body: String) -> reqwest::Response {
        serve(move |mut stream| async move {
            let head = format!("{head}Content-Length: {}\r\n\r\n", body.len());
            stream.write_all(head.as_bytes()).await.unwrap();
            stream.write_all(body.as_bytes()).await.unwrap();
        })
        .await
    }

    #[tokio::test]
    async fn a_full_page_reads_the_same_in_pubky_and_transport_form() {
        let owner = Keypair::random().public_key();
        let dir = directory(&owner);
        let mut pages = Vec::new();
        for page in [
            full_page(|path| pubky_line(&owner, path)),
            full_page(|path| transport_line(&owner, path)),
        ] {
            let response = respond("HTTP/1.1 200 OK\r\nConnection: close\r\n", page).await;
            let entries = read_directory_page(response, &dir, PAGE)
                .await
                .unwrap()
                .expect("a 200 page has entries");
            pages.push(entries);
        }
        assert_eq!(pages[0], pages[1]);
        assert_eq!(pages[0].len(), PAGE as usize);
        assert_eq!(
            pages[0].last().map(PubkyResource::to_pubky_url),
            Some(pubky_line(&owner, &format!("{DIRECTORY_PATH}00499")))
        );
    }

    #[test]
    fn blank_lines_are_skipped_and_subdirectories_kept() {
        let owner = Keypair::random().public_key();
        let text = format!(
            "{}\n\n  {}  \nhttp://_pubky.{}/{DIRECTORY_PATH}dir/\n",
            pubky_line(&owner, &format!("{DIRECTORY_PATH}a")),
            transport_line(&owner, &format!("{DIRECTORY_PATH}b")),
            owner.z32()
        );
        let urls: Vec<String> = parse_directory_page(&text, &directory(&owner), PAGE)
            .unwrap()
            .iter()
            .map(PubkyResource::to_pubky_url)
            .collect();
        assert_eq!(
            urls,
            [
                pubky_line(&owner, &format!("{DIRECTORY_PATH}a")),
                pubky_line(&owner, &format!("{DIRECTORY_PATH}b")),
                pubky_line(&owner, &format!("{DIRECTORY_PATH}dir/")),
            ]
        );
    }

    #[test]
    fn malformed_entries_are_refused() {
        let owner = Keypair::random().public_key();
        let dir = directory(&owner);
        for line in [
            "garbage".to_string(),
            "ftp://a/b".to_string(),
            format!("https://example.com/{DIRECTORY_PATH}a"),
            format!("pubky://not-a-key/{DIRECTORY_PATH}a"),
            pubky_line(&owner, &format!("{DIRECTORY_PATH}../a")),
        ] {
            assert_refused(parse_directory_page(&line, &dir, PAGE), &line);
        }
    }

    #[test]
    fn entries_of_another_owner_are_refused() {
        let owner = Keypair::random().public_key();
        let other = Keypair::random().public_key();
        let dir = directory(&owner);
        for line in [
            pubky_line(&other, &format!("{DIRECTORY_PATH}a")),
            transport_line(&other, &format!("{DIRECTORY_PATH}a")),
        ] {
            assert_refused(parse_directory_page(&line, &dir, PAGE), &line);
        }
    }

    #[test]
    fn entries_outside_the_directory_are_refused() {
        let owner = Keypair::random().public_key();
        let dir = directory(&owner);
        for path in [
            "pub/pubky.app/marketplace/v1/listings/a".to_string(),
            "pub/pubky.app/marketplace/v1/reviewsx".to_string(),
            DIRECTORY_PATH.to_string(),
        ] {
            let line = pubky_line(&owner, &path);
            assert_refused(parse_directory_page(&line, &dir, PAGE), &line);
        }
    }

    #[test]
    fn more_entries_than_the_limit_are_refused() {
        let owner = Keypair::random().public_key();
        let dir = directory(&owner);
        let text = full_page(|path| pubky_line(&owner, path));
        assert_eq!(
            parse_directory_page(&text, &dir, PAGE).unwrap().len(),
            PAGE as usize
        );
        assert_refused(parse_directory_page(&text, &dir, PAGE - 1), "limit - 1");
    }

    #[tokio::test]
    async fn not_found_is_none_and_a_server_error_is_a_client_error() {
        let dir = directory(&Keypair::random().public_key());
        let response = respond("HTTP/1.1 404 Not Found\r\n", String::new()).await;
        assert_eq!(
            read_directory_page(response, &dir, PAGE).await.unwrap(),
            None
        );

        let response = respond("HTTP/1.1 500 Internal Server Error\r\n", String::new()).await;
        let error = read_directory_page(response, &dir, PAGE)
            .await
            .expect_err("a 500 is not an empty directory");
        assert!(
            matches!(error, EventProcessorError::PubkyClientError(_)),
            "{error:?}"
        );
    }

    #[tokio::test]
    async fn a_declared_length_over_the_budget_is_refused() {
        let dir = directory(&Keypair::random().public_key());
        let response = serve(|mut stream| async move {
            stream
                .write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Length: 104857600\r\nConnection: close\r\n\r\n",
                )
                .await
                .unwrap();
            tokio::time::sleep(Duration::from_secs(30)).await;
        })
        .await;
        let error = within(
            Duration::from_secs(5),
            "test listing",
            read_directory_page(response, &dir, PAGE),
        )
        .await
        .expect_err("a page declared at 100 MiB must not be read");
        assert!(
            matches!(error, EventProcessorError::InvalidEventLine(_)),
            "{error:?}"
        );
    }

    #[tokio::test]
    async fn a_chunked_body_over_the_budget_is_refused() {
        let owner = Keypair::random().public_key();
        let dir = directory(&owner);
        let line = format!("{}\n", pubky_line(&owner, &format!("{DIRECTORY_PATH}a")));
        let response = serve(move |mut stream| async move {
            stream
                .write_all(
                    b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n",
                )
                .await
                .unwrap();
            let block = line.repeat(64 * 1024 / line.len());
            for _ in 0..1_600 {
                if write_chunk(&mut stream, block.as_bytes()).await.is_err() {
                    break;
                }
            }
        })
        .await;
        let error = read_directory_page(response, &dir, PAGE)
            .await
            .expect_err("a streamed page over the budget must be refused");
        assert!(error.to_string().contains("byte limit"), "{error}");
    }

    #[tokio::test]
    async fn a_page_dripping_past_the_deadline_is_a_client_error() {
        let owner = Keypair::random().public_key();
        let dir = directory(&owner);
        let line = format!("{}\n", pubky_line(&owner, &format!("{DIRECTORY_PATH}a")));
        let response = serve(move |mut stream| async move {
            stream
                .write_all(
                    b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n",
                )
                .await
                .unwrap();
            loop {
                if write_chunk(&mut stream, &line.as_bytes()[..1])
                    .await
                    .is_err()
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await;
        let error = within(
            Duration::from_millis(300),
            "test listing",
            read_directory_page(response, &dir, PAGE),
        )
        .await
        .expect_err("a page that never ends must hit the deadline");
        assert!(
            matches!(error, EventProcessorError::PubkyClientError(_)),
            "{error:?}"
        );
        assert!(error.to_string().contains("timed out"), "{error}");
    }
}
