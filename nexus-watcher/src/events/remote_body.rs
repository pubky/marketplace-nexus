//! Bounded reads of homeserver responses.
//!
//! Every byte a homeserver sends is untrusted. A response is read chunk by
//! chunk against a byte budget and a deadline, so a hostile or broken
//! homeserver costs the watcher one failed fetch instead of unbounded memory
//! or a stalled processor.

use std::future::Future;
use std::time::Duration;

use nexus_common::models::event::EventProcessorError;
use tracing::debug;

use crate::events::handlers::listing::is_homeserver_not_found;

/// Budget for fetching one record, from the request to the last body byte.
pub const RECORD_FETCH_TIMEOUT: Duration = Duration::from_secs(30);

/// Largest record the watcher reads. Records are JSON documents: the longest
/// text field in the specs is a 50,000-character post, which fits in a small
/// fraction of this.
pub const MAX_RECORD_BYTES: usize = 2 * 1024 * 1024;

/// Longest event line the watcher accepts: `PUT pubky://<key>/<path>` with a
/// path of up to 4,096 bytes.
pub const MAX_EVENT_LINE_BYTES: usize = 4_160;

/// Reserve for the cursor line and rate-limit text around the event lines.
const EVENT_FEED_SLACK_BYTES: usize = 1024;

/// A declared length is never trusted for more than this up-front allocation.
const MAX_PREALLOCATION_BYTES: usize = 1024 * 1024;

/// Reads the whole body of `response`, refusing more than `max_bytes`.
///
/// A declared `Content-Length` over the budget is refused before any body byte
/// is read; an undeclared or understated length is caught while streaming.
pub async fn read_capped(
    response: reqwest::Response,
    max_bytes: usize,
    what: &str,
) -> Result<Vec<u8>, EventProcessorError> {
    read_within_budget(response, max_bytes)
        .await?
        .ok_or_else(|| too_large(what, max_bytes))
}

/// The error for a body of `what` over its `max_bytes` budget.
fn too_large(what: &str, max_bytes: usize) -> EventProcessorError {
    EventProcessorError::InvalidEventLine(format!("{what} exceeds the {max_bytes} byte limit"))
}

/// The whole body of `response`, or `None` when it is over `max_bytes`, as
/// [read_capped] reads it. An error means the body could not be read.
async fn read_within_budget(
    mut response: reqwest::Response,
    max_bytes: usize,
) -> Result<Option<Vec<u8>>, EventProcessorError> {
    let declared = response.content_length();
    if declared.is_some_and(|len| len > max_bytes as u64) {
        return Ok(None);
    }
    let mut body = Vec::with_capacity(
        declared
            .map_or(0, |len| len as usize)
            .min(max_bytes)
            .min(MAX_PREALLOCATION_BYTES),
    );
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|e| EventProcessorError::client_error(e.to_string()))?
    {
        if body.len() + chunk.len() > max_bytes {
            return Ok(None);
        }
        body.extend_from_slice(&chunk);
    }
    Ok(Some(body))
}

/// Runs `fetch` against `deadline`; a fetch that outlives it is a client error.
pub async fn within<T>(
    deadline: Duration,
    what: &str,
    fetch: impl Future<Output = Result<T, EventProcessorError>>,
) -> Result<T, EventProcessorError> {
    tokio::time::timeout(deadline, fetch).await.map_err(|_| {
        EventProcessorError::client_error(format!(
            "{what} timed out after {}s",
            deadline.as_secs_f32()
        ))
    })?
}

/// The record at `uri`, or `None` when the homeserver answers 404. The request
/// and the body read share [RECORD_FETCH_TIMEOUT], and the body is capped at
/// `max_bytes`. Every other failure is an error, so an unreachable homeserver
/// never reads as a deleted record.
pub async fn fetch_record_capped(
    uri: &str,
    max_bytes: usize,
) -> Result<Option<Vec<u8>>, EventProcessorError> {
    match fetch_record_outcome(uri, max_bytes).await? {
        RecordFetch::Found(body) => Ok(Some(body)),
        RecordFetch::NotFound => Ok(None),
        RecordFetch::TooLarge(e) => Err(e),
    }
}

/// What the homeserver answered for one record.
#[derive(Debug)]
pub enum RecordFetch {
    /// The record's body, inside its byte budget.
    Found(Vec<u8>),
    /// The homeserver answered 404.
    NotFound,
    /// The homeserver answered with a body over the byte budget; the error is
    /// the one [fetch_record_capped] returns for it.
    TooLarge(EventProcessorError),
}

/// Fetches the record at `uri` as [fetch_record_capped] does, but tells a body
/// over `max_bytes` apart from a failed request: the error case is only a
/// homeserver that could not be reached or did not answer in time.
pub async fn fetch_record_outcome(
    uri: &str,
    max_bytes: usize,
) -> Result<RecordFetch, EventProcessorError> {
    let pubky = nexus_common::db::PubkyConnector::get()?;
    within(RECORD_FETCH_TIMEOUT, &format!("fetching {uri}"), async {
        let response = match pubky.public_storage().get(uri).await {
            Ok(response) => response,
            Err(e) if is_homeserver_not_found(&e) => {
                debug!("Record {uri} is not on its homeserver");
                return Ok(RecordFetch::NotFound);
            }
            Err(e) => return Err(e.into()),
        };
        Ok(match read_within_budget(response, max_bytes).await? {
            Some(body) => RecordFetch::Found(body),
            None => RecordFetch::TooLarge(too_large(&format!("Record {uri}"), max_bytes)),
        })
    })
    .await
}

/// Byte budget for the events feed of one poll: `events_limit` events and the
/// cursor line, each at its longest, with their newlines.
pub fn event_feed_budget(events_limit: u32) -> usize {
    (events_limit as usize)
        .saturating_add(1)
        .saturating_mul(MAX_EVENT_LINE_BYTES + 1)
        .saturating_add(EVENT_FEED_SLACK_BYTES)
}

/// Splits an events feed into lines. The homeserver answers a `limit` request
/// with at most `limit` events and a final `cursor:` line, so more lines than
/// that mean the homeserver ignored the request. Truncating would drop the
/// cursor line and replay the same feed forever, so the feed is refused, and so
/// is a line longer than [MAX_EVENT_LINE_BYTES].
pub fn event_lines(text: &str, events_limit: u32) -> Result<Vec<String>, EventProcessorError> {
    let max_lines = (events_limit as usize).saturating_add(1);
    let mut lines = Vec::new();
    for line in text.trim().lines() {
        if lines.len() == max_lines {
            return Err(EventProcessorError::InvalidEventLine(format!(
                "events feed has more than {max_lines} lines for a limit of {events_limit}"
            )));
        }
        if line.len() > MAX_EVENT_LINE_BYTES {
            return Err(EventProcessorError::InvalidEventLine(format!(
                "events feed has a line of {} bytes, over the {MAX_EVENT_LINE_BYTES} byte limit",
                line.len()
            )));
        }
        lines.push(line.to_string());
    }
    Ok(lines)
}

#[cfg(test)]
pub(crate) mod test_server {
    //! A one-connection HTTP/1.1 server on loopback for body-reading tests.

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};

    pub async fn serve<F, Fut>(handler: F) -> reqwest::Response
    where
        F: FnOnce(TcpStream) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = ()> + Send + 'static,
    {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = [0u8; 1024];
            let _ = stream.read(&mut request).await;
            handler(stream).await;
        });
        reqwest::Client::new()
            .get(format!("http://{addr}/"))
            .send()
            .await
            .expect("test server must answer the request line and headers")
    }

    pub async fn write_chunk(stream: &mut TcpStream, data: &[u8]) -> std::io::Result<()> {
        stream
            .write_all(format!("{:x}\r\n", data.len()).as_bytes())
            .await?;
        stream.write_all(data).await?;
        stream.write_all(b"\r\n").await
    }
}

#[cfg(test)]
mod tests {
    use super::test_server::{serve, write_chunk};
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use tokio::io::AsyncWriteExt;

    #[tokio::test]
    async fn a_body_inside_the_budget_is_returned_whole() {
        let response = serve(|mut stream| async move {
            stream
                .write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\nConnection: close\r\n\r\nhello",
                )
                .await
                .unwrap();
        })
        .await;
        assert_eq!(read_capped(response, 1024, "test").await.unwrap(), b"hello");
    }

    #[tokio::test]
    async fn a_declared_length_over_the_budget_is_refused_before_any_body_byte() {
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
        let started = std::time::Instant::now();
        let error = read_capped(response, 1024, "test")
            .await
            .expect_err("a body declared at 100 MiB must not be read");
        assert!(matches!(error, EventProcessorError::InvalidEventLine(_)));
        assert!(started.elapsed() < Duration::from_secs(5));
    }

    #[tokio::test]
    async fn a_body_over_the_budget_reads_as_too_large_not_as_a_failure() {
        let response = serve(|mut stream| async move {
            stream
                .write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\nConnection: close\r\n\r\nhello",
                )
                .await
                .unwrap();
        })
        .await;
        assert_eq!(read_within_budget(response, 4).await.unwrap(), None);
    }

    #[tokio::test]
    async fn an_undeclared_body_is_cut_off_at_the_budget() {
        let sent = Arc::new(AtomicUsize::new(0));
        let sent_by_server = sent.clone();
        let response = serve(move |mut stream| async move {
            stream
                .write_all(
                    b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n",
                )
                .await
                .unwrap();
            let block = vec![b'a'; 64 * 1024];
            for _ in 0..1_600 {
                if write_chunk(&mut stream, &block).await.is_err() {
                    break;
                }
                sent_by_server.fetch_add(block.len(), Ordering::SeqCst);
            }
        })
        .await;
        let error = read_capped(response, 256 * 1024, "test")
            .await
            .expect_err("a streamed body over the budget must be refused");
        assert!(matches!(error, EventProcessorError::InvalidEventLine(_)));
        assert!(
            sent.load(Ordering::SeqCst) < 100 * 1024 * 1024,
            "the reader kept consuming a 100 MiB body"
        );
    }

    #[tokio::test]
    async fn a_body_dripping_past_the_deadline_is_a_client_error() {
        let response = serve(|mut stream| async move {
            stream
                .write_all(
                    b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n",
                )
                .await
                .unwrap();
            loop {
                if write_chunk(&mut stream, b"x").await.is_err() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await;
        let error = within(
            Duration::from_millis(300),
            "test fetch",
            read_capped(response, 1024 * 1024, "test"),
        )
        .await
        .expect_err("a body that never ends must hit the deadline");
        assert!(
            matches!(error, EventProcessorError::PubkyClientError(_)),
            "{error:?}"
        );
        assert!(error.to_string().contains("timed out"), "{error}");
    }

    #[test]
    fn a_full_page_of_longest_lines_fits_the_budget_and_parses() {
        let limit = 1000;
        let path = "p".repeat(MAX_EVENT_LINE_BYTES - "PUT pubky://".len() - 52 - 1);
        let line = format!("PUT pubky://{}/{path}", "k".repeat(52));
        assert_eq!(line.len(), MAX_EVENT_LINE_BYTES);
        let mut feed = vec![line; limit as usize];
        feed.push("cursor: 123456".to_string());
        let body = feed.join("\n");
        assert!(
            body.len() <= event_feed_budget(limit),
            "{} > {}",
            body.len(),
            event_feed_budget(limit)
        );
        assert_eq!(event_lines(&body, limit).unwrap().len(), limit as usize + 1);
    }

    #[test]
    fn the_feed_budget_cannot_overflow() {
        assert!(event_feed_budget(u32::MAX) > event_feed_budget(1000));
        assert!(event_lines("PUT pubky://a/pub/x", u32::MAX).is_ok());
    }

    #[test]
    fn a_line_over_the_limit_is_refused() {
        let long = format!("PUT pubky://a/{}", "p".repeat(MAX_EVENT_LINE_BYTES));
        assert!(matches!(
            event_lines(&long, 10),
            Err(EventProcessorError::InvalidEventLine(_))
        ));
    }

    #[test]
    fn a_feed_with_a_cursor_line_is_split_into_lines() {
        let text = "PUT pubky://a/pub/x\nDEL pubky://a/pub/y\ncursor: 12\n";
        let lines = event_lines(text, 2).unwrap();
        assert_eq!(
            lines,
            ["PUT pubky://a/pub/x", "DEL pubky://a/pub/y", "cursor: 12"]
        );
    }

    #[test]
    fn an_empty_feed_has_no_lines() {
        assert!(event_lines("  \n", 10).unwrap().is_empty());
    }

    #[test]
    fn a_feed_longer_than_the_limit_is_refused() {
        let text = (0..=5)
            .map(|i| format!("PUT pubky://a/pub/{i}"))
            .collect::<Vec<_>>()
            .join("\n");
        assert!(matches!(
            event_lines(&text, 3),
            Err(EventProcessorError::InvalidEventLine(_))
        ));
        assert!(event_lines(&text, 5).is_ok());
    }
}
