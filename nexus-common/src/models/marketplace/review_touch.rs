use crate::db::get_redis_conn;
use crate::db::kv::RedisResult;
use deadpool_redis::redis::{AsyncCommands, Script};

/// How long a review's last watcher mutation is remembered. A fence only has
/// to outlive the gap between reading a record from its homeserver and
/// indexing it, which is seconds (minutes under backoff), never a day.
pub const REVIEW_TOUCH_TTL_SECONDS: u64 = 24 * 60 * 60;

/// The watcher's most recent mutation of one review: a put or a deletion.
///
/// A reader that copies a record from the reviewer's homeserver into the
/// index outside the event feed (the throttled backfill) races the feed. It
/// takes a [`ReviewTouch::now_ms`] reading before it fetches, and consults
/// [`ReviewTouch::since`] around its write: a touch at or after that reading
/// means the feed changed the review after the fetch began, so the record it
/// holds may be stale or already deleted.
///
/// Both clocks are the Redis server's, so readers and writers on different
/// hosts compare without skew. The marker expires after
/// [`REVIEW_TOUCH_TTL_SECONDS`]; an absent marker means no mutation within
/// that window.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReviewTouch {
    /// Redis server time of the mutation, milliseconds.
    pub at_ms: i64,
    /// True when the mutation was the review's deletion.
    pub deleted: bool,
}

const TOUCH_SCRIPT: &str = r#"
    local time = redis.call('TIME')
    local now_ms = time[1] * 1000 + math.floor(time[2] / 1000)
    redis.call('SET', KEYS[1], string.format('%d:%s', now_ms, ARGV[1]), 'EX', ARGV[2])
    return now_ms
"#;

impl ReviewTouch {
    fn key(reviewer_id: &str, review_id: &str) -> String {
        format!("Reviews:Touched:{reviewer_id}:{review_id}")
    }

    /// The Redis server's clock in milliseconds.
    pub async fn now_ms() -> RedisResult<i64> {
        let mut redis_conn = get_redis_conn().await?;
        let (seconds, micros): (i64, i64) = deadpool_redis::redis::cmd("TIME")
            .query_async(&mut redis_conn)
            .await?;
        Ok(seconds * 1000 + micros / 1000)
    }

    /// Records that the watcher is about to put (`deleted == false`) or delete
    /// the review. Written before the mutation starts, so a reader that sees
    /// no touch after its fetch knows no mutation began since.
    pub async fn mark(reviewer_id: &str, review_id: &str, deleted: bool) -> RedisResult<()> {
        let mut redis_conn = get_redis_conn().await?;
        let _: i64 = Script::new(TOUCH_SCRIPT)
            .key(Self::key(reviewer_id, review_id))
            .arg(if deleted { "del" } else { "put" })
            .arg(REVIEW_TOUCH_TTL_SECONDS)
            .invoke_async(&mut redis_conn)
            .await?;
        Ok(())
    }

    /// The review's latest watcher mutation when it happened at or after
    /// `since_ms` (a [`ReviewTouch::now_ms`] reading), `None` otherwise.
    pub async fn since(
        reviewer_id: &str,
        review_id: &str,
        since_ms: i64,
    ) -> RedisResult<Option<ReviewTouch>> {
        let mut redis_conn = get_redis_conn().await?;
        let stored: Option<String> = redis_conn.get(Self::key(reviewer_id, review_id)).await?;
        Ok(stored
            .as_deref()
            .and_then(Self::parse)
            .filter(|touch| touch.at_ms >= since_ms))
    }

    fn parse(stored: &str) -> Option<ReviewTouch> {
        let (at_ms, kind) = stored.split_once(':')?;
        Some(ReviewTouch {
            at_ms: at_ms.parse().ok()?,
            deleted: kind == "del",
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_stored_touch_parses_to_its_kind_and_time() {
        assert_eq!(
            ReviewTouch::parse("1790000000123:del"),
            Some(ReviewTouch {
                at_ms: 1_790_000_000_123,
                deleted: true
            })
        );
        assert_eq!(
            ReviewTouch::parse("5:put"),
            Some(ReviewTouch {
                at_ms: 5,
                deleted: false
            })
        );
        assert_eq!(ReviewTouch::parse("garbage"), None);
        assert_eq!(ReviewTouch::parse("x:put"), None);
    }
}
