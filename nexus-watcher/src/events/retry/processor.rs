//! Replays the retry queue ([`super::queue`]): a `v0` user's failed flip, without limit while
//! they are at `v0`, and the `social/v1` writes deferred behind it, once it settles.
//!
//! The watcher runs one [`RetryProcessor::run_pass`] after each round of homeserver polls, on
//! the same task, so no event of a homeserver is handled while the pass runs. A pass attempts
//! the entries due at its start, each at most once:
//!
//! - an entry that fails again is scheduled after its backoff, later than the pass's start;
//! - a deferred write still waiting is parked, out of the due set;
//! - the writes a flip settled in the pass wakes are due at once, and replayed before the pass
//!   ends unless it reaches its attempt limit or shutdown first; those a flip settled on a new
//!   profile PUT wakes are replayed by the next pass.
//!
//! Once the flip lands the user's new writes are handled as they arrive, so a woken write
//! replayed later may be older than them. A PUT replay reads the record as it is then; a DEL
//! replays only while its record is still gone, read within the record fetch deadline.
//!
//! A pass also stops after [`RetryPolicy::max_entries_per_pass`] attempts, so no state the
//! queue can hold makes it spin. A deferred write is replayed at most
//! [`RetryPolicy::max_replays`] times once woken, Neo4j and Redis failures uncounted, and a
//! user holds at most [`RetryPolicy::max_parked_per_user`] parked writes, a write parked again
//! included.

use super::queue::{FetchedEntry, Repark, RetryEntry, RetryKind, RetryQueue};
use crate::events::epoch_metrics::{self, RetryOutcome};
use crate::events::handlers::listing::{record_on_homeserver, HomeserverRecord};
use crate::events::remote_body::{within, RECORD_FETCH_TIMEOUT};
use crate::events::{handle_gated, Handled, Moderation};
use chrono::{DateTime, Utc};
use nexus_common::models::event::{Event, EventProcessorError, EventType};
use std::future::Future;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch::Receiver;
use tracing::{debug, info, warn};

/// How the retry queue schedules and bounds its entries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetryPolicy {
    /// Backoff before a failed flip's first retry, doubled on each retry after it (ms).
    pub flip_initial_backoff_ms: i64,
    /// Longest backoff between two attempts of a flip (ms).
    pub flip_max_backoff_ms: i64,
    /// Backoff before a woken write's second replay, doubled on each replay after it (ms).
    pub replay_initial_backoff_ms: i64,
    /// Longest backoff between two replays of a woken write (ms).
    pub replay_max_backoff_ms: i64,
    /// Replays of a woken write, the first included, before it is dropped.
    pub max_replays: u32,
    /// Writes one user may hold parked behind their flip.
    pub max_parked_per_user: usize,
    /// Entries read from Redis at a time.
    pub batch_size: usize,
    /// Attempts one pass makes at most.
    pub max_entries_per_pass: usize,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            flip_initial_backoff_ms: 10_000,
            flip_max_backoff_ms: 3_600_000,
            replay_initial_backoff_ms: 10_000,
            replay_max_backoff_ms: 3_600_000,
            max_replays: 10,
            max_parked_per_user: 10_000,
            batch_size: 100,
            max_entries_per_pass: 10_000,
        }
    }
}

impl RetryPolicy {
    /// The backoff after `attempts` failed attempts of an entry of `kind`: the initial backoff,
    /// doubled for each attempt after the first, capped.
    pub fn backoff_ms(&self, kind: RetryKind, attempts: u32) -> i64 {
        let (initial, max) = match kind {
            RetryKind::Flip => (self.flip_initial_backoff_ms, self.flip_max_backoff_ms),
            RetryKind::Deferred => (self.replay_initial_backoff_ms, self.replay_max_backoff_ms),
        };
        2i64.checked_pow(attempts.saturating_sub(1))
            .and_then(|factor| initial.checked_mul(factor))
            .map_or(max, |backoff| backoff.min(max))
            .max(1)
    }
}

/// What a failed attempt does to its entry. Pure: see [`decide_failure`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Decision {
    /// The event fails the same way on every attempt, or has nothing left to remove: the entry
    /// leaves the queue.
    Dropped,
    /// The write waits on a flip queued anew: parked again.
    Repark,
    /// Attempted again after a backoff, with `attempts` recorded.
    Reschedule { attempts: u32 },
    /// The write used up its replays: the entry leaves the queue.
    Exhausted,
}

/// The decision for an entry of `kind`, with `attempts` failed attempts recorded, whose next
/// attempt failed with `error`.
///
/// A flip that fails again is rescheduled without limit; once the user is at `v1` the entry is
/// a profile edit, under the replay limit. Neo4j and Redis failures are rescheduled without
/// counting the attempt, so an outage of any length drops nothing.
pub fn decide_failure(
    kind: RetryKind,
    attempts: u32,
    error: &EventProcessorError,
    policy: &RetryPolicy,
) -> Decision {
    match error {
        EventProcessorError::DeferredUntilFlip { .. } if kind == RetryKind::Deferred => {
            Decision::Repark
        }
        EventProcessorError::SkipIndexing => Decision::Dropped,
        error if error.is_permanent() => Decision::Dropped,
        EventProcessorError::FlipFailed(_) => Decision::Reschedule {
            attempts: attempts.saturating_add(1),
        },
        error if error.is_store_failure() => Decision::Reschedule { attempts },
        _ if attempts.saturating_add(1) >= policy.max_replays => Decision::Exhausted,
        _ => Decision::Reschedule {
            attempts: attempts + 1,
        },
    }
}

/// What one pass did.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RetryPassStats {
    /// Entries attempted.
    pub attempted: usize,
    /// Entries handled, out of the queue.
    pub applied: usize,
    /// Woken writes the epoch gate ignored, their user left at `v0`, out of the queue.
    pub ignored: usize,
    /// Entries scheduled again.
    pub rescheduled: usize,
    /// Deferred writes parked again.
    pub reparked: usize,
    /// Entries out of the queue unhandled: refused for good, nothing left to do, or used up.
    pub dropped: usize,
    /// Failed entries a newer write replaced, or their own flip settled, while the attempt ran:
    /// left as stored.
    pub superseded: usize,
}

/// Replays the retry queue.
pub struct RetryProcessor {
    pub retry_queue: RetryQueue,
    pub policy: RetryPolicy,
    pub moderation: Arc<Moderation>,
    pub files_path: PathBuf,
    pub shutdown_rx: Receiver<bool>,
}

impl RetryProcessor {
    /// Attempts every entry due at `now` (unix ms), each at most once, until none is left, the
    /// pass reaches [`RetryPolicy::max_entries_per_pass`] or shutdown is signalled. Stops at the
    /// first Redis failure of the queue itself.
    pub async fn run_pass(&self, now: i64) -> Result<RetryPassStats, EventProcessorError> {
        let mut stats = RetryPassStats::default();
        loop {
            let room = self.policy.max_entries_per_pass - stats.attempted;
            if room == 0 {
                warn!(
                    attempted = stats.attempted,
                    "Retry pass reached its attempt limit; the rest waits for the next pass"
                );
                return Ok(stats);
            }
            let batch = self
                .retry_queue
                .fetch_due(now, self.policy.batch_size.min(room))
                .await?;
            if batch.is_empty() {
                return Ok(stats);
            }
            for fetched in batch {
                if *self.shutdown_rx.borrow() {
                    debug!("Shutdown detected; ending the retry pass");
                    return Ok(stats);
                }
                stats.attempted += 1;
                self.attempt(&fetched, now, &mut stats).await?;
            }
        }
    }

    async fn attempt(
        &self,
        fetched: &FetchedEntry,
        now: i64,
        stats: &mut RetryPassStats,
    ) -> Result<(), EventProcessorError> {
        let entry = &fetched.entry;
        let event = match Event::parse_event(&entry.event_line(), self.files_path.clone()) {
            Ok(Some(event)) => event,
            outcome => {
                warn!(
                    uri = %entry.uri,
                    kind = ?entry.kind,
                    parsed = ?outcome.map(|event| event.is_some()),
                    "Retry entry is no event line this watcher handles; removing it"
                );
                return self
                    .settle_removal(fetched, RetryOutcome::Dropped, stats)
                    .await;
            }
        };
        let user_id = &event.parsed_uri.user_id;
        let replayed = match self.superseded_del(entry, &event).await {
            Ok(true) => {
                info!(%user_id, uri = %entry.uri, "Deferred DEL superseded: the record was written again; removing it");
                return self
                    .settle_removal(fetched, RetryOutcome::Dropped, stats)
                    .await;
            }
            Ok(false) => handle_gated(&event, self.moderation.clone(), &self.retry_queue).await,
            Err(error) => Err(error),
        };
        let error = match replayed {
            Ok(Handled::Applied) => {
                debug!(%user_id, uri = %entry.uri, kind = ?entry.kind, attempts = entry.attempts, "Retry applied");
                return self
                    .settle_removal(fetched, RetryOutcome::Applied, stats)
                    .await;
            }
            Ok(Handled::Ignored) => {
                info!(%user_id, uri = %entry.uri, kind = ?entry.kind, "Woken write ignored: its user stayed at v0; removing it");
                return self
                    .settle_removal(fetched, RetryOutcome::Ignored, stats)
                    .await;
            }
            Err(error) => error,
        };
        match decide_failure(entry.kind, entry.attempts, &error, &self.policy) {
            Decision::Dropped => {
                warn!(%user_id, uri = %entry.uri, kind = ?entry.kind, %error, "Retry failed for good; removing it");
                self.settle_removal(fetched, RetryOutcome::Dropped, stats)
                    .await
            }
            Decision::Exhausted => {
                warn!(
                    %user_id,
                    uri = %entry.uri,
                    kind = ?entry.kind,
                    attempts = entry.attempts.saturating_add(1),
                    %error,
                    "Retry used up its attempts; removing it"
                );
                self.settle_removal(fetched, RetryOutcome::Exhausted, stats)
                    .await
            }
            Decision::Repark => {
                match self
                    .retry_queue
                    .repark_if(fetched, self.policy.max_parked_per_user)
                    .await?
                {
                    Repark::Parked => {
                        stats.reparked += 1;
                        epoch_metrics::record_retry(entry.kind, RetryOutcome::Parked, 1);
                    }
                    Repark::Overflow => {
                        warn!(
                            %user_id,
                            uri = %entry.uri,
                            max_parked = self.policy.max_parked_per_user,
                            "Deferred write dropped: parked again, the user already holds the most parked writes"
                        );
                        stats.dropped += 1;
                        epoch_metrics::record_retry(entry.kind, RetryOutcome::Overflow, 1);
                    }
                    Repark::Superseded => stats.superseded += 1,
                }
                Ok(())
            }
            Decision::Reschedule { attempts } => {
                let next_retry_at = now + self.policy.backoff_ms(entry.kind, attempts.max(1));
                if !self
                    .retry_queue
                    .reschedule_if(fetched, attempts, next_retry_at)
                    .await?
                {
                    debug!(
                        %user_id,
                        uri = %entry.uri,
                        %error,
                        "Retry entry was settled or replaced while its attempt ran"
                    );
                    stats.superseded += 1;
                    return Ok(());
                }
                warn!(
                    %user_id,
                    uri = %entry.uri,
                    kind = ?entry.kind,
                    attempts,
                    next_retry_at = %retry_time(next_retry_at),
                    %error,
                    "Retry failed; scheduled again"
                );
                stats.rescheduled += 1;
                epoch_metrics::record_retry(entry.kind, RetryOutcome::Rescheduled, 1);
                Ok(())
            }
        }
    }

    /// Whether `event`, the DEL of a deferred `entry`, is superseded: its record is on the
    /// homeserver again, written after it by the user once at `v1`. A PUT needs no such check, as
    /// its replay reads the record as it is now.
    async fn superseded_del(
        &self,
        entry: &RetryEntry,
        event: &Event,
    ) -> Result<bool, EventProcessorError> {
        if entry.kind != RetryKind::Deferred || event.event_type != EventType::Del {
            return Ok(false);
        }
        record_present_within(
            &entry.uri,
            RECORD_FETCH_TIMEOUT,
            record_on_homeserver(&entry.uri),
        )
        .await
    }

    /// Removes `fetched`, settled with `outcome`. An applied event counts as applied even when
    /// its entry is gone already: a flip that lands settles its own entry.
    async fn settle_removal(
        &self,
        fetched: &FetchedEntry,
        outcome: RetryOutcome,
        stats: &mut RetryPassStats,
    ) -> Result<(), EventProcessorError> {
        let removed = self.retry_queue.remove_if(fetched).await?;
        match outcome {
            RetryOutcome::Applied => stats.applied += 1,
            _ if !removed => {
                stats.superseded += 1;
                return Ok(());
            }
            RetryOutcome::Ignored => stats.ignored += 1,
            _ => stats.dropped += 1,
        }
        epoch_metrics::record_retry(fetched.entry.kind, outcome, 1);
        Ok(())
    }
}

/// Whether the record at `uri` is on its homeserver, as `check` reads it within `deadline`. The
/// pass runs on the watcher's only task, so a homeserver that hangs costs one deadline: the read
/// fails as a client error and the replay is counted and scheduled again.
async fn record_present_within(
    uri: &str,
    deadline: Duration,
    check: impl Future<Output = Result<HomeserverRecord, EventProcessorError>>,
) -> Result<bool, EventProcessorError> {
    let record = within(deadline, &format!("checking {uri}"), check).await?;
    Ok(record == HomeserverRecord::Present)
}

fn retry_time(unix_ms: i64) -> DateTime<Utc> {
    DateTime::<Utc>::from_timestamp_millis(unix_ms).unwrap_or_else(Utc::now)
}

/// Queues `event`, whose flip failed with `cause`, for its first retry after the policy's
/// backoff from `now`. A Redis failure is logged: the user stays at `v0` until their next
/// `social/v1` profile PUT.
pub async fn queue_failed_flip(
    retry_queue: &RetryQueue,
    policy: &RetryPolicy,
    event: &Event,
    cause: &EventProcessorError,
    now: i64,
) {
    let next_retry_at = now + policy.backoff_ms(RetryKind::Flip, 1);
    let entry = RetryEntry::failed_flip(event, next_retry_at);
    let user_id = &event.parsed_uri.user_id;
    match retry_queue.queue_flip(&entry).await {
        Ok(()) => {
            warn!(
                %user_id,
                uri = %event.uri,
                next_retry_at = %retry_time(next_retry_at),
                error = %cause,
                "Flip failed; queued for retry, the user's social/v1 writes deferred behind it"
            );
            epoch_metrics::record_retry(RetryKind::Flip, RetryOutcome::Queued, 1);
        }
        Err(e) => warn!(
            %user_id,
            uri = %event.uri,
            error = %cause,
            queue_error = %e,
            "Flip failed and could not be queued for retry"
        ),
    }
}

/// Parks `event`, deferred until its user's flip, queued at `flip_uri`. A write refused because
/// its user holds the most parked writes, or one Redis did not take, is logged and lost.
pub async fn park_deferred(
    retry_queue: &RetryQueue,
    policy: &RetryPolicy,
    event: &Event,
    flip_uri: &str,
) {
    let user_id = &event.parsed_uri.user_id;
    match retry_queue
        .park(&RetryEntry::deferred(event), policy.max_parked_per_user)
        .await
    {
        Ok(true) => epoch_metrics::record_retry(RetryKind::Deferred, RetryOutcome::Parked, 1),
        Ok(false) => {
            warn!(
                %user_id,
                uri = %event.uri,
                %flip_uri,
                max_parked = policy.max_parked_per_user,
                "Deferred write dropped: the user already holds the most parked writes"
            );
            epoch_metrics::record_retry(RetryKind::Deferred, RetryOutcome::Overflow, 1);
        }
        Err(e) => warn!(
            %user_id,
            uri = %event.uri,
            %flip_uri,
            queue_error = %e,
            "Deferred write could not be parked"
        ),
    }
}

/// Logs what a pass did, when it attempted anything.
pub fn log_pass(stats: &RetryPassStats) {
    if stats.attempted > 0 {
        info!(
            attempted = stats.attempted,
            applied = stats.applied,
            ignored = stats.ignored,
            rescheduled = stats.rescheduled,
            reparked = stats.reparked,
            dropped = stats.dropped,
            superseded = stats.superseded,
            "Retry pass done"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn policy() -> RetryPolicy {
        RetryPolicy {
            max_replays: 3,
            ..RetryPolicy::default()
        }
    }

    #[test]
    fn a_flip_that_fails_again_is_rescheduled_without_limit() {
        let error = EventProcessorError::FlipFailed(Box::new(
            EventProcessorError::GraphQueryFailed("missing label".into()),
        ));
        for attempts in [1, 2, 3, 50, u32::MAX - 1, u32::MAX] {
            assert_eq!(
                decide_failure(RetryKind::Flip, attempts, &error, &policy()),
                Decision::Reschedule {
                    attempts: attempts.saturating_add(1)
                },
                "after {attempts} attempts"
            );
        }
    }

    #[test]
    fn a_woken_write_is_replayed_a_bounded_number_of_times() {
        let error = EventProcessorError::missing_dependencies(vec!["pubky://dep".into()]);
        assert_eq!(
            decide_failure(RetryKind::Deferred, 0, &error, &policy()),
            Decision::Reschedule { attempts: 1 }
        );
        assert_eq!(
            decide_failure(RetryKind::Deferred, 1, &error, &policy()),
            Decision::Reschedule { attempts: 2 }
        );
        assert_eq!(
            decide_failure(RetryKind::Deferred, 2, &error, &policy()),
            Decision::Exhausted
        );
        assert_eq!(
            decide_failure(RetryKind::Flip, 2, &error, &policy()),
            Decision::Exhausted,
            "a flip entry whose user is at v1 is a profile edit, under the replay limit"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_stale_del_read_that_hangs_fails_at_the_record_fetch_deadline() {
        let uri = "pubky://user/pub/social/v1/tags/TAG";
        let started = tokio::time::Instant::now();
        let read = record_present_within(uri, RECORD_FETCH_TIMEOUT, std::future::pending());
        let error = tokio::time::timeout(RECORD_FETCH_TIMEOUT * 2, read)
            .await
            .expect("the read ends at its own deadline")
            .expect_err("a homeserver that never answers must not hold the pass");
        assert_eq!(started.elapsed(), RECORD_FETCH_TIMEOUT);
        assert!(
            matches!(error, EventProcessorError::PubkyClientError(_)),
            "{error:?}"
        );
        assert!(error.to_string().contains(uri), "{error}");
        assert_eq!(
            decide_failure(RetryKind::Deferred, 0, &error, &policy()),
            Decision::Reschedule { attempts: 1 },
            "the replay is counted, so a homeserver that keeps hanging uses it up"
        );

        let answered = |record| async move { Ok(record) };
        for (record, present) in [
            (HomeserverRecord::Present, true),
            (HomeserverRecord::Gone, false),
        ] {
            assert_eq!(
                record_present_within(uri, RECORD_FETCH_TIMEOUT, answered(record))
                    .await
                    .unwrap(),
                present
            );
        }
    }

    #[test]
    fn store_failures_are_rescheduled_uncounted() {
        for error in [
            EventProcessorError::GraphQueryFailed("down".into()),
            EventProcessorError::IndexOperationFailed("down".into()),
        ] {
            for kind in [RetryKind::Flip, RetryKind::Deferred] {
                assert_eq!(
                    decide_failure(kind, 7, &error, &policy()),
                    Decision::Reschedule { attempts: 7 },
                    "{kind:?}: {error}"
                );
            }
        }
    }

    #[test]
    fn a_write_still_waiting_is_parked_again_and_refusals_drop() {
        let waiting = EventProcessorError::DeferredUntilFlip {
            flip_uri: "pubky://user/pub/social/v1/profile.json".into(),
        };
        assert_eq!(
            decide_failure(RetryKind::Deferred, 0, &waiting, &policy()),
            Decision::Repark
        );
        for error in [
            EventProcessorError::SpecValidation("bytes".into()),
            EventProcessorError::UnsupportedTarget("target".into()),
            EventProcessorError::InvalidEventLine("line".into()),
            EventProcessorError::SkipIndexing,
        ] {
            for kind in [RetryKind::Flip, RetryKind::Deferred] {
                assert_eq!(
                    decide_failure(kind, 0, &error, &policy()),
                    Decision::Dropped,
                    "{kind:?}: {error}"
                );
            }
        }
    }

    #[test]
    fn backoff_doubles_from_the_initial_and_stays_capped_and_positive() {
        let policy = RetryPolicy::default();
        let flip: Vec<i64> = (1..=11)
            .map(|attempts| policy.backoff_ms(RetryKind::Flip, attempts))
            .collect();
        assert_eq!(
            flip,
            [
                10_000, 20_000, 40_000, 80_000, 160_000, 320_000, 640_000, 1_280_000, 2_560_000,
                3_600_000, 3_600_000
            ]
        );
        assert_eq!(policy.backoff_ms(RetryKind::Flip, u32::MAX), 3_600_000);
        assert_eq!(policy.backoff_ms(RetryKind::Deferred, 0), 10_000);
        let zero = RetryPolicy {
            replay_initial_backoff_ms: 0,
            ..policy
        };
        assert_eq!(zero.backoff_ms(RetryKind::Deferred, 5), 1);
    }
}
