use crate::db::{exec_single_row, fetch_row_from_graph, queries, GraphResult};
use neo4rs::Row;

/// Where a throttled-backfill job stands. Stored as `BackfillRun.status`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackfillStatus {
    /// The job's state exists but no run has started.
    Pending,
    /// A runner holds the lease and is working.
    Running,
    /// A budget or an interrupt stopped the run cleanly. Resumable.
    Paused,
    /// The circuit breaker or a database failure stopped the run. Resumable.
    Aborted,
    /// Every item and the aggregates were processed. Starting again is a
    /// no-op unless forced.
    Done,
}

impl BackfillStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            BackfillStatus::Pending => "pending",
            BackfillStatus::Running => "running",
            BackfillStatus::Paused => "paused",
            BackfillStatus::Aborted => "aborted",
            BackfillStatus::Done => "done",
        }
    }

    fn parse(status: &str) -> Option<Self> {
        match status {
            "pending" => Some(BackfillStatus::Pending),
            "running" => Some(BackfillStatus::Running),
            "paused" => Some(BackfillStatus::Paused),
            "aborted" => Some(BackfillStatus::Aborted),
            "done" => Some(BackfillStatus::Done),
            _ => None,
        }
    }
}

/// The counters a job accumulates, persisted with the cursor so a resumed run
/// reports totals and not just its own part.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BackfillCounters {
    /// Items finished (a review, a user, or an aggregate scope).
    pub items: i64,
    /// Homeserver requests sent (each GET and each LIST page).
    pub requests: i64,
    /// Reviews read from a homeserver and ingested.
    pub refetched: i64,
    /// Records that left the homeserver, or directories that are gone.
    pub gone: i64,
    /// Items whose homeserver could not be reached; skipped, not retried.
    pub unreachable: i64,
    /// Records that were read but could not be parsed or ingested.
    pub failed: i64,
    /// Aggregate scopes recomputed.
    pub aggregates: i64,
}

impl BackfillCounters {
    pub(crate) fn as_array(&self) -> [i64; 7] {
        [
            self.items,
            self.requests,
            self.refetched,
            self.gone,
            self.unreachable,
            self.failed,
            self.aggregates,
        ]
    }

    pub fn add(&mut self, other: &BackfillCounters) {
        self.items += other.items;
        self.requests += other.requests;
        self.refetched += other.refetched;
        self.gone += other.gone;
        self.unreachable += other.unreachable;
        self.failed += other.failed;
        self.aggregates += other.aggregates;
    }
}

/// The persisted state of one job: `(:BackfillRun {job, ...})`, separate from
/// the `Migration` nodes the migration runner reads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackfillRun {
    pub job: String,
    pub status: BackfillStatus,
    /// Keyset cursor within `phase`: every item up to and including it is
    /// finished. Empty before the first item.
    pub cursor: String,
    pub phase: String,
    /// Keys of items finished beyond the cursor (concurrent items complete out
    /// of order); a resumed run skips them.
    pub ahead: Vec<String>,
    pub counters: BackfillCounters,
    pub started_at: Option<i64>,
    pub updated_at: i64,
    pub lease_owner: Option<String>,
    pub lease_expires_at: Option<i64>,
    /// True while a runner's lease is unexpired, on the graph server's clock.
    pub lease_live: bool,
    /// Why the last run stopped, when it did not finish.
    pub stop_reason: Option<String>,
}

/// What claiming a job found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BackfillClaim {
    /// This runner now holds the lease, with the state to resume from.
    Acquired(Box<BackfillRun>),
    /// The job is `done`; nothing to run.
    AlreadyDone(Box<BackfillRun>),
    /// Another runner holds a live lease.
    Held(Box<BackfillRun>),
}

impl BackfillRun {
    /// The stored state of a job, `None` if it never started.
    pub async fn get(job: &str) -> GraphResult<Option<BackfillRun>> {
        let Some(row) = fetch_row_from_graph(queries::get::backfill_run(job)).await? else {
            return Ok(None);
        };
        Ok(Some(Self::from_row(&row)?))
    }

    /// Claims the job for `owner` with one atomic compare-and-set (see
    /// [`queries::put::claim_backfill_run`]). `force` re-runs a `done` job from
    /// the start and resets its cursor and counters.
    pub async fn claim(
        job: &str,
        first_phase: &str,
        owner: &str,
        lease_ms: i64,
        force: bool,
    ) -> GraphResult<BackfillClaim> {
        let claimed = fetch_row_from_graph(queries::put::claim_backfill_run(
            job,
            first_phase,
            owner,
            lease_ms,
            force,
        ))
        .await?
        .is_some();
        let state = Self::get(job).await?.ok_or_else(|| {
            crate::db::GraphError::Generic(format!("BackfillRun {job} vanished while claiming"))
        })?;
        Ok(if claimed {
            BackfillClaim::Acquired(Box::new(state))
        } else if state.status == BackfillStatus::Done {
            BackfillClaim::AlreadyDone(Box::new(state))
        } else {
            BackfillClaim::Held(Box::new(state))
        })
    }

    /// Extends the lease. `false` when another runner owns the job now.
    pub async fn renew(job: &str, owner: &str, lease_ms: i64) -> GraphResult<bool> {
        Ok(
            fetch_row_from_graph(queries::put::renew_backfill_lease(job, owner, lease_ms))
                .await?
                .is_some(),
        )
    }

    /// Persists the cursor, the finished items ahead of it and counter
    /// increments, and renews the lease. `false` when the lease was lost.
    pub async fn record_progress(
        job: &str,
        owner: &str,
        progress: &BackfillProgress,
        lease_ms: i64,
    ) -> GraphResult<bool> {
        Ok(fetch_row_from_graph(queries::put::record_backfill_progress(
            job,
            owner,
            &progress.cursor,
            &progress.phase,
            progress.ahead.clone(),
            progress.delta.as_array(),
            lease_ms,
        ))
        .await?
        .is_some())
    }

    /// Ends the run with `status` and releases the lease. `false` when the
    /// lease was lost (the job is another runner's now).
    pub async fn finish(
        job: &str,
        owner: &str,
        status: BackfillStatus,
        stop_reason: Option<&str>,
    ) -> GraphResult<bool> {
        Ok(fetch_row_from_graph(queries::put::finish_backfill_run(
            job,
            owner,
            status.as_str(),
            stop_reason,
        ))
        .await?
        .is_some())
    }

    /// Removes the job's state. Only for tests and operator repair.
    pub async fn delete(job: &str) -> GraphResult<()> {
        exec_single_row(queries::del::delete_backfill_run(job)).await
    }

    fn from_row(row: &Row) -> GraphResult<BackfillRun> {
        let status: String = row.get("status")?;
        let status = BackfillStatus::parse(&status).ok_or_else(|| {
            crate::db::GraphError::Generic(format!("unknown BackfillRun status {status}"))
        })?;
        let counter = |name: &str| -> GraphResult<i64> {
            Ok(row.get::<Option<i64>>(name)?.unwrap_or_default())
        };
        Ok(BackfillRun {
            job: row.get("job")?,
            status,
            cursor: row.get::<Option<String>>("cursor")?.unwrap_or_default(),
            phase: row.get("phase")?,
            ahead: row.get::<Option<Vec<String>>>("ahead")?.unwrap_or_default(),
            counters: BackfillCounters {
                items: counter("items")?,
                requests: counter("requests")?,
                refetched: counter("refetched")?,
                gone: counter("gone")?,
                unreachable: counter("unreachable")?,
                failed: counter("failed")?,
                aggregates: counter("aggregates")?,
            },
            started_at: row.get("started_at")?,
            updated_at: counter("updated_at")?,
            lease_owner: row.get("lease_owner")?,
            lease_expires_at: row.get("lease_expires_at")?,
            lease_live: row.get::<Option<bool>>("lease_live")?.unwrap_or(false),
            stop_reason: row.get("stop_reason")?,
        })
    }
}

/// One progress write: the new cursor, the keys finished beyond it, and the
/// counter increments since the previous write.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BackfillProgress {
    pub phase: String,
    pub cursor: String,
    pub ahead: Vec<String>,
    pub delta: BackfillCounters,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_status_round_trips_through_its_stored_name() {
        for status in [
            BackfillStatus::Pending,
            BackfillStatus::Running,
            BackfillStatus::Paused,
            BackfillStatus::Aborted,
            BackfillStatus::Done,
        ] {
            assert_eq!(BackfillStatus::parse(status.as_str()), Some(status));
        }
        assert_eq!(BackfillStatus::parse("finished"), None);
    }

    #[test]
    fn counters_add_field_by_field() {
        let mut total = BackfillCounters {
            items: 1,
            requests: 2,
            ..Default::default()
        };
        total.add(&BackfillCounters {
            items: 3,
            failed: 1,
            ..Default::default()
        });
        assert_eq!((total.items, total.requests, total.failed), (4, 2, 1));
    }
}
