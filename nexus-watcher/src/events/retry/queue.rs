//! The retry queue the watcher replays: a `v0` user's failed flip, retried without limit while
//! they are at `v0`, and the `social/v1` writes the epoch gate defers behind it.
//!
//! Redis layout, under one prefix ([`RETRY_QUEUE_PREFIX`] in production):
//!
//! - `{prefix}:entry:{uri}`: the [`RetryEntry`] of the event at `uri`, as JSON. One entry per
//!   address: a newer event at the address replaces the older one.
//! - `{prefix}:due`: sorted set of the addresses to attempt, scored by `next_retry_at` (unix ms).
//! - `{prefix}:parked`: sorted set, every score 0, of the deferred addresses waiting for their
//!   user's flip, read per user through the lexicographic range of `pubky://{user_id}/`.
//!
//! An address is in at most one of the two sets. Removing a user's flip entry wakes their
//! parked entries in the same script: each moves to `due` with score 0, ahead of every
//! scheduled entry, so the pass that settled the flip replays them before it ends. A fetched
//! entry is removed or rescheduled only while the stored JSON is still the one fetched, so an
//! entry replaced meanwhile is left alone.

use chrono::Utc;
use deadpool_redis::redis::{self, Script};
use nexus_common::db::get_redis_conn;
use nexus_common::db::kv::{RedisError, RedisResult};
use nexus_common::models::event::{Event, EventType};
use serde::{Deserialize, Serialize};
use std::sync::{Arc, LazyLock};
use tracing::warn;

/// Prefix of the production retry queue's keys.
pub const RETRY_QUEUE_PREFIX: &str = "RetryQueue";

/// Why an event waits in the queue.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RetryKind {
    /// A `v0` user's `social/v1` profile PUT whose flip failed.
    Flip,
    /// A `v0` user's `social/v1` write held until their flip lands.
    Deferred,
}

/// One queued event.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RetryEntry {
    pub kind: RetryKind,
    pub event_type: EventType,
    pub uri: String,
    /// Failed attempts so far: a flip's first is the one that queued it, a deferred write
    /// counts the replays that failed once it was woken.
    pub attempts: u32,
    /// Unix ms of the next attempt; 0 while the entry is parked.
    pub next_retry_at: i64,
    /// Unix ns the event was queued at: two events queued at one address differ by it.
    pub queued_at: i64,
}

impl RetryEntry {
    fn new(kind: RetryKind, event: &Event, attempts: u32, next_retry_at: i64) -> Self {
        let now = Utc::now();
        Self {
            kind,
            event_type: event.event_type.clone(),
            uri: event.uri.clone(),
            attempts,
            next_retry_at,
            queued_at: now
                .timestamp_nanos_opt()
                .unwrap_or_else(|| now.timestamp_millis()),
        }
    }

    /// The entry of `event`, a flip that failed once, attempted again at `next_retry_at`.
    pub fn failed_flip(event: &Event, next_retry_at: i64) -> Self {
        Self::new(RetryKind::Flip, event, 1, next_retry_at)
    }

    /// The entry of `event`, deferred until its user's flip.
    pub fn deferred(event: &Event) -> Self {
        Self::new(RetryKind::Deferred, event, 0, 0)
    }

    /// The event line the entry replays.
    pub fn event_line(&self) -> String {
        format!("{} {}", self.event_type, self.uri)
    }
}

/// An entry as [`RetryQueue::fetch_due`] read it, with the stored JSON its guarded writes compare.
#[derive(Debug, Clone)]
pub struct FetchedEntry {
    pub entry: RetryEntry,
    stored: String,
}

/// What [`RetryQueue::repark_if`] did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Repark {
    Parked,
    /// Removed: its user already holds the most parked entries.
    Overflow,
    /// Left as stored: a newer entry replaced it, or it is gone.
    Superseded,
}

/// How many entries the queue holds in each set.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueueSizes {
    pub due: usize,
    pub parked: usize,
}

/// `KEYS`: entry, due, parked. `ARGV`: expected JSON (empty: remove whatever is stored),
/// address, first and last parked address of the user. Removes the flip entry and moves the
/// user's parked entries to `due` with score 0. Returns how many it woke, or -1 when the stored
/// entry is not the expected one.
static REMOVE_FLIP: LazyLock<Script> = LazyLock::new(|| {
    Script::new(
        r#"
        local stored = redis.call('GET', KEYS[1])
        if ARGV[1] ~= '' and stored ~= ARGV[1] then
            return -1
        end
        redis.call('DEL', KEYS[1])
        redis.call('ZREM', KEYS[2], ARGV[2])
        redis.call('ZREM', KEYS[3], ARGV[2])
        local parked = redis.call('ZRANGEBYLEX', KEYS[3], ARGV[3], ARGV[4])
        for _, member in ipairs(parked) do
            redis.call('ZADD', KEYS[2], 0, member)
            redis.call('ZREM', KEYS[3], member)
        end
        return #parked
        "#,
    )
});

/// `KEYS`: entry, due, parked. `ARGV`: expected JSON, address. Removes the entry while it is
/// the expected one; returns 1 when it did.
static REMOVE_IF: LazyLock<Script> = LazyLock::new(|| {
    Script::new(
        r#"
        if redis.call('GET', KEYS[1]) ~= ARGV[1] then
            return 0
        end
        redis.call('DEL', KEYS[1])
        redis.call('ZREM', KEYS[2], ARGV[2])
        redis.call('ZREM', KEYS[3], ARGV[2])
        return 1
        "#,
    )
});

/// `KEYS`: entry, due, parked. `ARGV`: expected JSON, new JSON, score, address. Writes the
/// new entry due at `score` while the stored one is the expected one; returns 1 when it did.
static RESCHEDULE_IF: LazyLock<Script> = LazyLock::new(|| {
    Script::new(
        r#"
        if redis.call('GET', KEYS[1]) ~= ARGV[1] then
            return 0
        end
        redis.call('SET', KEYS[1], ARGV[2])
        redis.call('ZADD', KEYS[2], ARGV[3], ARGV[4])
        redis.call('ZREM', KEYS[3], ARGV[4])
        return 1
        "#,
    )
});

/// `KEYS`: entry, due, parked. `ARGV`: expected JSON, new JSON, address, first and last parked
/// address of the user, the most the user may hold parked. While the stored entry is the
/// expected one, parks the new entry and returns 1, or, when the address is not parked and the
/// user already holds the most, removes the entry and returns 2. Returns 0 otherwise.
static REPARK_IF: LazyLock<Script> = LazyLock::new(|| {
    Script::new(
        r#"
        if redis.call('GET', KEYS[1]) ~= ARGV[1] then
            return 0
        end
        if not redis.call('ZSCORE', KEYS[3], ARGV[3])
            and redis.call('ZLEXCOUNT', KEYS[3], ARGV[4], ARGV[5]) >= tonumber(ARGV[6]) then
            redis.call('DEL', KEYS[1])
            redis.call('ZREM', KEYS[2], ARGV[3])
            return 2
        end
        redis.call('SET', KEYS[1], ARGV[2])
        redis.call('ZADD', KEYS[3], 0, ARGV[3])
        redis.call('ZREM', KEYS[2], ARGV[3])
        return 1
        "#,
    )
});

/// `KEYS`: entry, due, parked. `ARGV`: JSON, address, first and last parked address of the
/// user, the most the user may hold parked. Parks the entry unless the address is not parked
/// yet and the user already holds the most; returns 1 when it parked it.
static PARK: LazyLock<Script> = LazyLock::new(|| {
    Script::new(
        r#"
        if not redis.call('ZSCORE', KEYS[3], ARGV[2])
            and redis.call('ZLEXCOUNT', KEYS[3], ARGV[3], ARGV[4]) >= tonumber(ARGV[5]) then
            return 0
        end
        redis.call('SET', KEYS[1], ARGV[1])
        redis.call('ZADD', KEYS[3], 0, ARGV[2])
        redis.call('ZREM', KEYS[2], ARGV[2])
        return 1
        "#,
    )
});

/// `KEYS`: due, then the entry key of each `ARGV` address. Drops each address from `due` while
/// its entry is still missing, so an entry written since the fetch keeps its member.
static REMOVE_STALE: LazyLock<Script> = LazyLock::new(|| {
    Script::new(
        r#"
        for i, member in ipairs(ARGV) do
            if redis.call('EXISTS', KEYS[i + 1]) == 0 then
                redis.call('ZREM', KEYS[1], member)
            end
        end
        return 1
        "#,
    )
});

/// The watcher's retry queue, under one key prefix. Cheap to clone.
#[derive(Debug, Clone)]
pub struct RetryQueue {
    prefix: Arc<str>,
}

impl Default for RetryQueue {
    fn default() -> Self {
        Self::with_prefix(RETRY_QUEUE_PREFIX)
    }
}

fn serialize(entry: &RetryEntry) -> RedisResult<String> {
    serde_json::to_string(entry).map_err(|e| RedisError::SerializationFailed(Box::new(e)))
}

impl RetryQueue {
    /// A queue whose keys start with `prefix`. Tests give each run its own.
    pub fn with_prefix(prefix: impl Into<String>) -> Self {
        Self {
            prefix: Arc::from(prefix.into()),
        }
    }

    fn entry_key(&self, uri: &str) -> String {
        format!("{}:entry:{uri}", self.prefix)
    }

    fn due_key(&self) -> String {
        format!("{}:due", self.prefix)
    }

    fn parked_key(&self) -> String {
        format!("{}:parked", self.prefix)
    }

    /// The lexicographic bounds of every address of `user_id`: `/` is followed by `0`.
    fn user_range(user_id: &str) -> (String, String) {
        (
            format!("[pubky://{user_id}/"),
            format!("(pubky://{user_id}0"),
        )
    }

    /// The user whose storage `uri` addresses.
    fn user_of(uri: &str) -> Option<&str> {
        uri.strip_prefix("pubky://")?
            .split_once('/')
            .map(|(user_id, _)| user_id)
    }

    /// Queues `entry`, a flip, due at its `next_retry_at`, replacing what its address held.
    pub async fn queue_flip(&self, entry: &RetryEntry) -> RedisResult<()> {
        let payload = serialize(entry)?;
        let mut conn = get_redis_conn().await?;
        let mut pipe = redis::pipe();
        pipe.atomic()
            .cmd("SET")
            .arg(self.entry_key(&entry.uri))
            .arg(payload)
            .ignore()
            .cmd("ZADD")
            .arg(self.due_key())
            .arg(entry.next_retry_at)
            .arg(&entry.uri)
            .ignore()
            .cmd("ZREM")
            .arg(self.parked_key())
            .arg(&entry.uri)
            .ignore();
        let _: () = pipe.query_async(&mut conn).await?;
        Ok(())
    }

    /// Parks `entry`, a user's write deferred until their flip, replacing what its address held.
    /// Returns `false`, parking nothing, when the address is not parked yet and the user already
    /// holds `max_parked` parked entries.
    pub async fn park(&self, entry: &RetryEntry, max_parked: usize) -> RedisResult<bool> {
        let user_id = Self::user_of(&entry.uri)
            .ok_or_else(|| RedisError::InvalidInput(format!("{} names no user", entry.uri)))?;
        let payload = serialize(entry)?;
        let (first, last) = Self::user_range(user_id);
        let mut conn = get_redis_conn().await?;
        let parked: i64 = PARK
            .key(self.entry_key(&entry.uri))
            .key(self.due_key())
            .key(self.parked_key())
            .arg(payload)
            .arg(&entry.uri)
            .arg(first)
            .arg(last)
            .arg(max_parked)
            .invoke_async(&mut conn)
            .await?;
        Ok(parked == 1)
    }

    /// The entry queued at `uri`, if any.
    pub async fn get(&self, uri: &str) -> RedisResult<Option<RetryEntry>> {
        let mut conn = get_redis_conn().await?;
        let stored: Option<String> = redis::cmd("GET")
            .arg(self.entry_key(uri))
            .query_async(&mut conn)
            .await?;
        stored
            .map(|json| {
                serde_json::from_str(&json)
                    .map_err(|e| RedisError::DeserializationFailed(Box::new(e)))
            })
            .transpose()
    }

    /// Whether a flip waits at `flip_uri`, the user's `social/v1` profile address.
    pub async fn flip_queued(&self, flip_uri: &str) -> RedisResult<bool> {
        Ok(self.get(flip_uri).await?.is_some_and(|entry| {
            entry.kind == RetryKind::Flip && entry.event_type == EventType::Put
        }))
    }

    /// Settles the flip queued at `flip_uri`, a user's `social/v1` profile address: removes its
    /// entry, if any, and wakes the user's parked entries. Returns how many it woke.
    pub async fn resolve_flip(&self, flip_uri: &str) -> RedisResult<usize> {
        Ok(self.remove_flip(flip_uri, "").await?.unwrap_or(0))
    }

    /// Runs [`REMOVE_FLIP`]; `None` when the stored entry is not `expected`.
    async fn remove_flip(&self, flip_uri: &str, expected: &str) -> RedisResult<Option<usize>> {
        let user_id = Self::user_of(flip_uri)
            .ok_or_else(|| RedisError::InvalidInput(format!("{flip_uri} names no user")))?;
        let (first, last) = Self::user_range(user_id);
        let mut conn = get_redis_conn().await?;
        let woken: i64 = REMOVE_FLIP
            .key(self.entry_key(flip_uri))
            .key(self.due_key())
            .key(self.parked_key())
            .arg(expected)
            .arg(flip_uri)
            .arg(first)
            .arg(last)
            .invoke_async(&mut conn)
            .await?;
        Ok(usize::try_from(woken).ok())
    }

    /// Up to `limit` entries due at `now`, earliest first. A due address whose entry is gone is
    /// dropped from `due`; an entry that does not read back is removed, logged.
    pub async fn fetch_due(&self, now: i64, limit: usize) -> RedisResult<Vec<FetchedEntry>> {
        let mut conn = get_redis_conn().await?;
        let uris: Vec<String> = redis::cmd("ZRANGEBYSCORE")
            .arg(self.due_key())
            .arg("-inf")
            .arg(now)
            .arg("LIMIT")
            .arg(0)
            .arg(limit)
            .query_async(&mut conn)
            .await?;
        if uris.is_empty() {
            return Ok(Vec::new());
        }
        let keys: Vec<String> = uris.iter().map(|uri| self.entry_key(uri)).collect();
        let stored: Vec<Option<String>> =
            redis::cmd("MGET").arg(&keys).query_async(&mut conn).await?;
        drop(conn);

        let mut fetched = Vec::with_capacity(uris.len());
        let mut stale = Vec::new();
        for (uri, stored) in uris.into_iter().zip(stored) {
            let Some(stored) = stored else {
                stale.push(uri);
                continue;
            };
            match serde_json::from_str::<RetryEntry>(&stored) {
                Ok(entry) => fetched.push(FetchedEntry { entry, stored }),
                Err(e) => {
                    warn!(%uri, error = %e, "Retry entry does not read back; removing it");
                    self.remove_stored(&uri, &stored).await?;
                }
            }
        }
        self.remove_stale(&stale).await?;
        Ok(fetched)
    }

    async fn remove_stale(&self, uris: &[String]) -> RedisResult<()> {
        if uris.is_empty() {
            return Ok(());
        }
        let mut conn = get_redis_conn().await?;
        let mut invocation = REMOVE_STALE.prepare_invoke();
        invocation.key(self.due_key());
        for uri in uris {
            invocation.key(self.entry_key(uri)).arg(uri);
        }
        let _: i64 = invocation.invoke_async(&mut conn).await?;
        Ok(())
    }

    async fn remove_stored(&self, uri: &str, stored: &str) -> RedisResult<bool> {
        let mut conn = get_redis_conn().await?;
        let removed: i64 = REMOVE_IF
            .key(self.entry_key(uri))
            .key(self.due_key())
            .key(self.parked_key())
            .arg(stored)
            .arg(uri)
            .invoke_async(&mut conn)
            .await?;
        Ok(removed == 1)
    }

    /// Removes `fetched` while it is still the stored entry; a flip's removal wakes its user's
    /// parked entries. Returns whether it removed it.
    pub async fn remove_if(&self, fetched: &FetchedEntry) -> RedisResult<bool> {
        let uri = &fetched.entry.uri;
        match fetched.entry.kind {
            RetryKind::Flip if Self::user_of(uri).is_some() => {
                Ok(self.remove_flip(uri, &fetched.stored).await?.is_some())
            }
            _ => self.remove_stored(uri, &fetched.stored).await,
        }
    }

    /// Writes `fetched` back with `attempts`, due at `next_retry_at`, while it is still the
    /// stored entry. Returns whether it did.
    pub async fn reschedule_if(
        &self,
        fetched: &FetchedEntry,
        attempts: u32,
        next_retry_at: i64,
    ) -> RedisResult<bool> {
        let updated = RetryEntry {
            attempts,
            next_retry_at,
            ..fetched.entry.clone()
        };
        let payload = serialize(&updated)?;
        let mut conn = get_redis_conn().await?;
        let written: i64 = RESCHEDULE_IF
            .key(self.entry_key(&updated.uri))
            .key(self.due_key())
            .key(self.parked_key())
            .arg(&fetched.stored)
            .arg(payload)
            .arg(next_retry_at)
            .arg(&updated.uri)
            .invoke_async(&mut conn)
            .await?;
        Ok(written == 1)
    }

    /// Parks `fetched` again, a deferred entry whose user has a flip queued anew, while it is
    /// still the stored entry, under the same per-user cap of `max_parked` as [`Self::park`].
    pub async fn repark_if(
        &self,
        fetched: &FetchedEntry,
        max_parked: usize,
    ) -> RedisResult<Repark> {
        let user_id = Self::user_of(&fetched.entry.uri).ok_or_else(|| {
            RedisError::InvalidInput(format!("{} names no user", fetched.entry.uri))
        })?;
        let (first, last) = Self::user_range(user_id);
        let updated = RetryEntry {
            next_retry_at: 0,
            ..fetched.entry.clone()
        };
        let payload = serialize(&updated)?;
        let mut conn = get_redis_conn().await?;
        let outcome: i64 = REPARK_IF
            .key(self.entry_key(&updated.uri))
            .key(self.due_key())
            .key(self.parked_key())
            .arg(&fetched.stored)
            .arg(payload)
            .arg(&updated.uri)
            .arg(first)
            .arg(last)
            .arg(max_parked)
            .invoke_async(&mut conn)
            .await?;
        Ok(match outcome {
            1 => Repark::Parked,
            2 => Repark::Overflow,
            _ => Repark::Superseded,
        })
    }

    /// How many entries `user_id` holds parked.
    pub async fn parked_count(&self, user_id: &str) -> RedisResult<usize> {
        let (first, last) = Self::user_range(user_id);
        let mut conn = get_redis_conn().await?;
        Ok(redis::cmd("ZLEXCOUNT")
            .arg(self.parked_key())
            .arg(first)
            .arg(last)
            .query_async(&mut conn)
            .await?)
    }

    /// How many entries the queue holds due and parked.
    pub async fn sizes(&self) -> RedisResult<QueueSizes> {
        let mut conn = get_redis_conn().await?;
        let (due, parked): (usize, usize) = redis::pipe()
            .cmd("ZCARD")
            .arg(self.due_key())
            .cmd("ZCARD")
            .arg(self.parked_key())
            .query_async(&mut conn)
            .await?;
        Ok(QueueSizes { due, parked })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const USER: &str = "operrr8wsbpr3ue9d4qj41ge1kcc6r7fdiy6o3ugjrrhi4y77rdo";

    /// The bounds hold every address of the user and no address of a key that extends theirs.
    #[test]
    fn a_users_range_holds_exactly_their_addresses() {
        let (first, last) = RetryQueue::user_range(USER);
        let (first, last) = (&first[1..], &last[1..]);
        let inside = [
            format!("pubky://{USER}/pub/social/v1/tags/X.json"),
            format!("pubky://{USER}/pub/social/v1/follows/{USER}.json"),
            format!("pubky://{USER}/"),
        ];
        for uri in &inside {
            assert!(uri.as_str() >= first && uri.as_str() < last, "{uri}");
        }
        let outside = [
            format!("pubky://{USER}0/pub/social/v1/tags/X.json"),
            format!("pubky://{USER}a/pub/social/v1/tags/X.json"),
            format!("pubky://{}/pub/social/v1/tags/X.json", &USER[..51]),
            format!("pubky://{USER}"),
        ];
        for uri in &outside {
            assert!(!(uri.as_str() >= first && uri.as_str() < last), "{uri}");
        }
    }

    #[test]
    fn the_user_of_an_address_is_its_first_segment() {
        assert_eq!(
            RetryQueue::user_of(&format!("pubky://{USER}/pub/social/v1/profile.json")),
            Some(USER)
        );
        assert_eq!(RetryQueue::user_of(&format!("pubky://{USER}")), None);
        assert_eq!(RetryQueue::user_of("https://example.com/pub/x"), None);
    }

    #[test]
    fn an_entry_replays_its_event_line_and_reads_back() {
        let entry = RetryEntry {
            kind: RetryKind::Deferred,
            event_type: EventType::Del,
            uri: format!("pubky://{USER}/pub/social/v1/tags/X.json"),
            attempts: 2,
            next_retry_at: 0,
            queued_at: 7,
        };
        assert_eq!(
            entry.event_line(),
            format!("DEL pubky://{USER}/pub/social/v1/tags/X.json")
        );
        let json = serialize(&entry).unwrap();
        assert_eq!(serde_json::from_str::<RetryEntry>(&json).unwrap(), entry);
    }
}
