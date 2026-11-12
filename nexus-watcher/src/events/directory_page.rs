//! Bounded reads of one page of a shallow homeserver directory listing.
//!
//! A homeserver's answer is untrusted: `limit` is only a request to it. A page
//! is read against a byte budget and a deadline, refused when it has more
//! entries than were asked for, refused when any entry is not a direct child
//! of the listed directory (a resource, or a subdirectory ending with `/`),
//! and refused when a full page ends where the previous one did. Entries are
//! parsed with the SDK's [PubkyResource] parser, the way the SDK's own LIST
//! does. [DirectoryPager] pages a whole directory, bounded in pages.

use std::collections::HashSet;

use pubky::{Method, PubkyHttpClient, PubkyResource, StatusCode};
use reqwest::Url;

use crate::events::remote_body::{event_feed_budget, read_capped, within, RECORD_FETCH_TIMEOUT};
use crate::events::EventProcessorError;

/// Pages through a shallow directory listing, one request per [Self::next]
/// call, so a caller can take a request permit before each call and settle it
/// from the result.
///
/// At most `max_pages` full pages are read. A homeserver chooses what each
/// page holds, so after the last of them one more page of a single entry is
/// read, only to learn whether the directory ends at the cap.
pub struct DirectoryPager {
    directory: PubkyResource,
    limit: u16,
    max_pages: usize,
    cursor: Option<String>,
    pages: usize,
    seen: HashSet<String>,
    done: bool,
}

/// What one [DirectoryPager::next] call found.
#[derive(Debug, PartialEq, Eq)]
pub enum PageStep {
    /// The entry names of one page that no earlier page of this pager held
    /// (a homeserver may serve the same entries again; a repeat was handled
    /// already). Subdirectories are left out. `last` is set for a short page:
    /// the directory ends there and nothing more is read.
    Names { names: Vec<String>, last: bool },
    /// The homeserver answered 404 before the cap: the directory is not there
    /// (or left during the paging).
    Gone,
    /// The directory ends at the cap: the single-entry page after `max_pages`
    /// full pages was empty or 404. Also what `next` returns, without a
    /// request, once paging has finished.
    End,
    /// The single-entry page after `max_pages` full pages has an entry: the
    /// directory goes on past the cap, and nothing past it is read.
    Truncated,
}

impl DirectoryPager {
    /// A pager over `directory` (a `pubky://` directory ending with `/`) that
    /// reads pages of `limit` entries and at most `max_pages` full pages.
    pub fn new(directory: PubkyResource, limit: u16, max_pages: usize) -> Self {
        DirectoryPager {
            directory,
            limit,
            max_pages,
            cursor: None,
            pages: 0,
            seen: HashSet::new(),
            done: false,
        }
    }

    /// Reads the next page with exactly one request, or returns
    /// [PageStep::End] without one once paging has finished. A failed request
    /// is an error and leaves the pager where it was.
    pub async fn next(
        &mut self,
        client: &PubkyHttpClient,
    ) -> Result<PageStep, EventProcessorError> {
        if self.done {
            return Ok(PageStep::End);
        }
        let (limit, cursor) = self.request();
        let page = fetch_directory_page(client, &self.directory, limit, cursor).await?;
        Ok(self.step(page))
    }

    /// The `limit` and `cursor` of the next request: a single entry once
    /// `max_pages` full pages were read.
    fn request(&self) -> (u16, Option<&str>) {
        let limit = if self.at_cap() { 1 } else { self.limit };
        (limit, self.cursor.as_deref())
    }

    /// Whether the next page is the check page past the cap.
    fn at_cap(&self) -> bool {
        self.pages == self.max_pages
    }

    /// Records the page the last request read (`None` for a 404) and decides
    /// what it means.
    fn step(&mut self, page: Option<Vec<PubkyResource>>) -> PageStep {
        let at_cap = self.at_cap();
        self.done = true;
        let Some(entries) = page else {
            return if at_cap {
                PageStep::End
            } else {
                PageStep::Gone
            };
        };
        if at_cap {
            return if entries.is_empty() {
                PageStep::End
            } else {
                PageStep::Truncated
            };
        }
        self.pages += 1;
        let names = entries
            .iter()
            .filter_map(entry_name)
            .filter(|name| self.seen.insert(name.to_string()))
            .map(str::to_string)
            .collect();
        self.cursor = next_cursor(&entries, self.limit);
        self.done = self.cursor.is_none();
        PageStep::Names {
            names,
            last: self.done,
        }
    }
}

/// One page of the shallow listing of `directory` (a `pubky://` directory
/// ending with `/`), at most `limit` entries from after `cursor`, or `None`
/// when the homeserver answers 404. The request and the whole body share
/// [RECORD_FETCH_TIMEOUT].
///
/// The request goes over the direct Pubky transport, like the events poll.
/// The SDK's ICANN-domain fallback is crate-private in pubky 0.9.3 and its
/// public `get` cannot carry the LIST query, so a homeserver reachable only
/// through that fallback reads as unreachable here.
pub async fn fetch_directory_page(
    client: &PubkyHttpClient,
    directory: &PubkyResource,
    limit: u16,
    cursor: Option<&str>,
) -> Result<Option<Vec<PubkyResource>>, EventProcessorError> {
    let url = page_url(directory, limit, cursor)?;
    within(
        RECORD_FETCH_TIMEOUT,
        &format!("listing {}", directory.to_pubky_url()),
        async {
            let response = client
                .request(Method::GET, &url)
                .send()
                .await
                .map_err(|e| EventProcessorError::client_error(e.to_string()))?;
            read_directory_page(response, directory, limit, cursor).await
        },
    )
    .await
}

/// The LIST URL of one shallow page, with its query built the way the SDK's
/// own LIST builds it.
fn page_url(
    directory: &PubkyResource,
    limit: u16,
    cursor: Option<&str>,
) -> Result<Url, pubky::Error> {
    let mut url = directory.to_transport_url()?;
    {
        let mut query = url.query_pairs_mut();
        query
            .append_key_only("shallow")
            .append_pair("limit", &limit.to_string());
        if let Some(cursor) = cursor {
            query.append_pair("cursor", cursor);
        }
    }
    Ok(url)
}

/// The cursor to read the page after `entries` with, or `None` when `entries`
/// was the last page (fewer than `limit` entries).
pub fn next_cursor(entries: &[PubkyResource], limit: u16) -> Option<String> {
    if entries.len() < usize::from(limit) {
        return None;
    }
    entries.last().map(PubkyResource::to_pubky_url)
}

/// The last path segment of `entry`, or `None` for a subdirectory.
pub fn entry_name(entry: &PubkyResource) -> Option<&str> {
    entry
        .path
        .as_str()
        .rsplit('/')
        .next()
        .filter(|name| !name.is_empty())
}

/// The entries of a directory page `response`, or `None` for a 404. Any other
/// non-success status is a client error.
async fn read_directory_page(
    response: reqwest::Response,
    directory: &PubkyResource,
    limit: u16,
    cursor: Option<&str>,
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
    parse_directory_page(&String::from_utf8_lossy(&body), directory, limit, cursor).map(Some)
}

/// Parses a page with one resource per line, as `pubky://` URLs (or the
/// `pubky<owner>/` form) or `https://_pubky.<owner>/<path>` transport URLs.
/// More than `limit` entries means the homeserver ignored the request, so the
/// page is refused, and so is a page with any entry that is malformed, owned
/// by someone else, or not a direct child of `directory` (a name, or a name
/// ending with `/` for a subdirectory), and a full page that ends at
/// `cursor` (paging would never advance).
fn parse_directory_page(
    text: &str,
    directory: &PubkyResource,
    limit: u16,
    cursor: Option<&str>,
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
                .is_some_and(is_direct_child);
        if !inside {
            return Err(EventProcessorError::InvalidEventLine(format!(
                "directory entry is not inside {}: {line}",
                directory.to_pubky_url()
            )));
        }
        entries.push(entry);
    }
    if cursor.is_some() && next_cursor(&entries, limit).as_deref() == cursor {
        return Err(EventProcessorError::InvalidEventLine(format!(
            "directory page of {} did not advance its cursor",
            directory.to_pubky_url()
        )));
    }
    Ok(entries)
}

/// Whether `rest`, an entry path after its directory's path, names a direct
/// child: one non-empty segment, with at most a trailing `/` for a
/// subdirectory. A nested path would read as the wrong entry name.
fn is_direct_child(rest: &str) -> bool {
    let name = rest.strip_suffix('/').unwrap_or(rest);
    !name.is_empty() && !name.contains('/')
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
    use pubky::Keypair;
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
            let entries = read_directory_page(response, &dir, PAGE, None)
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
        let urls: Vec<String> = parse_directory_page(&text, &directory(&owner), PAGE, None)
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
            assert_refused(parse_directory_page(&line, &dir, PAGE, None), &line);
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
            assert_refused(parse_directory_page(&line, &dir, PAGE, None), &line);
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
            assert_refused(parse_directory_page(&line, &dir, PAGE, None), &line);
        }
    }

    #[test]
    fn nested_entries_are_refused_and_direct_subdirectories_kept() {
        let owner = Keypair::random().public_key();
        let dir = directory(&owner);
        for path in [
            format!("{DIRECTORY_PATH}x/y"),
            format!("{DIRECTORY_PATH}x/y/"),
        ] {
            let line = pubky_line(&owner, &path);
            assert_refused(parse_directory_page(&line, &dir, PAGE, None), &line);
        }
        let line = pubky_line(&owner, &format!("{DIRECTORY_PATH}x/"));
        let entries = parse_directory_page(&line, &dir, PAGE, None).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entry_name(&entries[0]), None);
    }

    #[test]
    fn more_entries_than_the_limit_are_refused() {
        let owner = Keypair::random().public_key();
        let dir = directory(&owner);
        let text = full_page(|path| pubky_line(&owner, path));
        assert_eq!(
            parse_directory_page(&text, &dir, PAGE, None).unwrap().len(),
            PAGE as usize
        );
        assert_refused(
            parse_directory_page(&text, &dir, PAGE - 1, None),
            "limit - 1",
        );
    }

    #[tokio::test]
    async fn not_found_is_none_and_a_server_error_is_a_client_error() {
        let dir = directory(&Keypair::random().public_key());
        let response = respond("HTTP/1.1 404 Not Found\r\n", String::new()).await;
        assert_eq!(
            read_directory_page(response, &dir, PAGE, None)
                .await
                .unwrap(),
            None
        );

        let response = respond("HTTP/1.1 500 Internal Server Error\r\n", String::new()).await;
        let error = read_directory_page(response, &dir, PAGE, None)
            .await
            .expect_err("a 500 is not an empty directory");
        assert!(
            matches!(error, EventProcessorError::PubkyClientError(_)),
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
        let error = read_directory_page(response, &dir, PAGE, None)
            .await
            .expect_err("a streamed page over the budget must be refused");
        assert!(error.to_string().contains("byte limit"), "{error}");
    }

    #[test]
    fn a_full_page_that_ends_at_its_cursor_is_refused() {
        let owner = Keypair::random().public_key();
        let dir = directory(&owner);
        let text = full_page(|path| pubky_line(&owner, path));
        let last = pubky_line(&owner, &format!("{DIRECTORY_PATH}00499"));
        assert_refused(
            parse_directory_page(&text, &dir, PAGE, Some(&last)),
            "repeated cursor",
        );
        let earlier = pubky_line(&owner, &format!("{DIRECTORY_PATH}00000"));
        assert!(parse_directory_page(&text, &dir, PAGE, Some(&earlier)).is_ok());
    }

    #[test]
    fn only_a_full_page_has_a_next_cursor() {
        let owner = Keypair::random().public_key();
        let dir = directory(&owner);
        let text = full_page(|path| pubky_line(&owner, path));
        let entries = parse_directory_page(&text, &dir, PAGE, None).unwrap();
        assert_eq!(
            next_cursor(&entries, PAGE),
            Some(pubky_line(&owner, &format!("{DIRECTORY_PATH}00499")))
        );
        assert_eq!(next_cursor(&entries[..3], PAGE), None);
        assert_eq!(next_cursor(&[], PAGE), None);
    }

    #[test]
    fn entry_names_skip_subdirectories() {
        let owner = Keypair::random().public_key();
        let text = format!(
            "{}\n{}",
            pubky_line(&owner, &format!("{DIRECTORY_PATH}a")),
            pubky_line(&owner, &format!("{DIRECTORY_PATH}dir/"))
        );
        let entries = parse_directory_page(&text, &directory(&owner), PAGE, None).unwrap();
        let names: Vec<_> = entries.iter().map(entry_name).collect();
        assert_eq!(names, [Some("a"), None]);
    }

    #[test]
    fn the_page_url_carries_shallow_limit_and_an_encoded_cursor() {
        let owner = Keypair::random().public_key();
        let dir = directory(&owner);
        let base = format!("https://_pubky.{}/{DIRECTORY_PATH}", owner.z32());
        assert_eq!(
            page_url(&dir, PAGE, None).unwrap().as_str(),
            format!("{base}?shallow&limit=500")
        );
        let cursor = pubky_line(&owner, &format!("{DIRECTORY_PATH}a"));
        let url = page_url(&dir, PAGE, Some(&cursor)).unwrap();
        assert_eq!(
            url.query_pairs()
                .find(|(key, _)| key == "cursor")
                .map(|(_, v)| v.into_owned()),
            Some(cursor)
        );
        assert!(!url.query().unwrap_or_default().contains("://"), "{url}");
    }

    const PAGER_LIMIT: u16 = 3;
    const PAGER_MAX_PAGES: usize = 2;

    fn pager(owner: &pubky::PublicKey) -> DirectoryPager {
        DirectoryPager::new(directory(owner), PAGER_LIMIT, PAGER_MAX_PAGES)
    }

    fn entries(owner: &pubky::PublicKey, names: &[&str]) -> Option<Vec<PubkyResource>> {
        Some(
            names
                .iter()
                .map(|name| {
                    pubky_line(owner, &format!("{DIRECTORY_PATH}{name}"))
                        .parse()
                        .unwrap()
                })
                .collect(),
        )
    }

    fn names(names: &[&str], last: bool) -> PageStep {
        PageStep::Names {
            names: names.iter().map(|name| name.to_string()).collect(),
            last,
        }
    }

    #[test]
    fn a_short_page_is_the_last() {
        let owner = Keypair::random().public_key();
        let mut pager = pager(&owner);
        assert_eq!(pager.request(), (PAGER_LIMIT, None));
        assert_eq!(
            pager.step(entries(&owner, &["a", "b"])),
            names(&["a", "b"], true)
        );
        assert!(pager.done);

        let mut pager = self::pager(&owner);
        assert_eq!(pager.step(entries(&owner, &[])), names(&[], true));
        assert!(pager.done);
    }

    #[test]
    fn a_full_page_continues_after_its_last_entry() {
        let owner = Keypair::random().public_key();
        let mut pager = pager(&owner);
        assert_eq!(
            pager.step(entries(&owner, &["a", "b", "c"])),
            names(&["a", "b", "c"], false)
        );
        assert!(!pager.done);
        let cursor = pubky_line(&owner, &format!("{DIRECTORY_PATH}c"));
        assert_eq!(pager.request(), (PAGER_LIMIT, Some(cursor.as_str())));
    }

    #[test]
    fn names_repeated_across_pages_are_returned_once_and_subdirectories_skipped() {
        let owner = Keypair::random().public_key();
        let mut pager = pager(&owner);
        assert_eq!(
            pager.step(entries(&owner, &["a", "dir/", "b"])),
            names(&["a", "b"], false)
        );
        assert_eq!(pager.step(entries(&owner, &["b", "a"])), names(&[], true));
    }

    #[test]
    fn a_404_on_the_first_page_is_gone() {
        let owner = Keypair::random().public_key();
        let mut pager = pager(&owner);
        assert_eq!(pager.step(None), PageStep::Gone);
        assert!(pager.done);
    }

    /// A pager that has read `PAGER_MAX_PAGES` full pages.
    fn pager_at_cap(owner: &pubky::PublicKey) -> DirectoryPager {
        let mut pager = pager(owner);
        assert_eq!(
            pager.step(entries(owner, &["a", "b", "c"])),
            names(&["a", "b", "c"], false)
        );
        assert_eq!(
            pager.step(entries(owner, &["d", "e", "f"])),
            names(&["d", "e", "f"], false)
        );
        pager
    }

    #[test]
    fn the_page_after_the_cap_asks_for_one_entry_after_the_last_full_page() {
        let owner = Keypair::random().public_key();
        let pager = pager_at_cap(&owner);
        let cursor = pubky_line(&owner, &format!("{DIRECTORY_PATH}f"));
        assert_eq!(pager.request(), (1, Some(cursor.as_str())));
    }

    #[test]
    fn a_directory_that_ends_at_the_cap_ends() {
        let owner = Keypair::random().public_key();
        let mut pager = pager_at_cap(&owner);
        assert_eq!(pager.step(entries(&owner, &[])), PageStep::End);
        assert!(pager.done);

        let mut pager = pager_at_cap(&owner);
        assert_eq!(pager.step(None), PageStep::End);
        assert!(pager.done);
    }

    #[test]
    fn an_entry_past_the_cap_truncates_the_directory() {
        let owner = Keypair::random().public_key();
        let mut pager = pager_at_cap(&owner);
        assert_eq!(pager.step(entries(&owner, &["g"])), PageStep::Truncated);
        assert!(pager.done);
    }
}
