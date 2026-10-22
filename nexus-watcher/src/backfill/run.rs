use crate::backfill::gate::{CountingGate, GateConfig, GateStop, RequestGate, ThrottledGate};
use crate::backfill::jobs::{BackfillJob, ItemResult, ItemTally, Phase};
use futures::stream::FuturesUnordered;
use futures::{FutureExt, StreamExt};
use nexus_common::models::backfill_run::{
    BackfillClaim, BackfillCounters, BackfillProgress, BackfillRun, BackfillStatus,
};
use nexus_common::types::DynError;
use std::collections::hash_map::RandomState;
use std::collections::{BTreeSet, HashSet, VecDeque};
use std::future::Future;
use std::hash::{BuildHasher, Hasher};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
use tracing::{info, warn};

/// Exit codes of `nexusd db backfill`.
pub const EXIT_OK: u8 = 0;
/// A refusal or an error before the run could start.
pub const EXIT_ERROR: u8 = 1;
/// The run aborted: the circuit breaker tripped, the lease was lost, or the
/// database failed. The cursor is kept.
pub const EXIT_ABORTED: u8 = 2;
/// The job is done, but some records failed.
pub const EXIT_FAILED_ITEMS: u8 = 3;

/// How many failed item ids a run logs and reports before it only counts.
const MAX_REPORTED_FAILURES: usize = 20;

/// Lets another task stop a run cleanly: the run ends `paused`, cursor kept.
#[derive(Clone)]
pub struct StopHandle(Arc<watch::Sender<bool>>);

impl Default for StopHandle {
    fn default() -> Self {
        StopHandle(Arc::new(watch::channel(false).0))
    }
}

impl StopHandle {
    pub fn stop(&self) {
        self.0.send_replace(true);
    }

    pub fn is_stopped(&self) -> bool {
        *self.0.borrow()
    }

    fn subscribe(&self) -> watch::Receiver<bool> {
        self.0.subscribe()
    }
}

#[derive(Clone)]
pub struct RunOptions {
    pub gate: GateConfig,
    /// Pause between aggregate recomputes, which are database work only.
    pub aggregate_pause: Duration,
    /// Re-run a `done` job from the start.
    pub force_rerun: bool,
    /// How long a lease lasts without renewal.
    pub lease: Duration,
    /// How often the holder renews its lease.
    pub lease_renewal: Duration,
    /// Work items read from the graph per query.
    pub batch: usize,
    pub stop: StopHandle,
}

impl Default for RunOptions {
    fn default() -> Self {
        RunOptions {
            gate: GateConfig::default(),
            aggregate_pause: Duration::from_millis(50),
            force_rerun: false,
            lease: Duration::from_secs(60),
            lease_renewal: Duration::from_secs(10),
            batch: 100,
            stop: StopHandle::default(),
        }
    }
}

/// One item or scope finished and its progress persisted.
pub struct ProgressEvent<'a> {
    pub phase: Phase,
    pub key: &'a str,
    /// Totals so far, including earlier runs.
    pub totals: &'a BackfillCounters,
}

/// Receives an event after each finished item or scope.
pub trait ProgressSink: Send + Sync {
    fn item_finished(&self, event: &ProgressEvent<'_>);
}

/// Logs a line every `every` items.
pub struct LogProgress {
    pub job: String,
    pub every: i64,
}

impl ProgressSink for LogProgress {
    fn item_finished(&self, event: &ProgressEvent<'_>) {
        let done = match event.phase {
            Phase::Fetch => event.totals.items,
            Phase::Aggregates => event.totals.aggregates,
        };
        if self.every > 0 && done % self.every == 0 {
            info!(
                "{}: {} {} done ({} requests, {} refetched, {} gone, {} unreachable, {} failed)",
                self.job,
                done,
                event.phase.as_str(),
                event.totals.requests,
                event.totals.refetched,
                event.totals.gone,
                event.totals.unreachable,
                event.totals.failed
            );
        }
    }
}

/// How a run ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    /// Every item and the aggregates were processed.
    Done,
    /// The job was already done: nothing ran and no request was sent.
    AlreadyDone,
    /// A budget or an interrupt stopped the run. Resumable; exit 0.
    Paused(String),
    /// The breaker, a lost lease or a database failure stopped the run. The
    /// cursor is kept; exit nonzero.
    Aborted(String),
    /// Another runner holds a live lease; nothing ran.
    Refused,
}

#[derive(Debug, Clone)]
pub struct BackfillReport {
    pub job: String,
    pub outcome: Outcome,
    /// The job's stored state after the run, with the totals of every run.
    pub state: BackfillRun,
    /// Items and scopes finished by this run alone.
    pub finished_this_run: i64,
}

impl BackfillReport {
    pub fn exit_code(&self) -> u8 {
        match &self.outcome {
            Outcome::Done if self.state.counters.failed > 0 => EXIT_FAILED_ITEMS,
            Outcome::Done | Outcome::AlreadyDone | Outcome::Paused(_) => EXIT_OK,
            Outcome::Aborted(_) => EXIT_ABORTED,
            Outcome::Refused => EXIT_ERROR,
        }
    }

    /// The summary `nexusd` prints.
    pub fn render(&self) -> String {
        let counters = &self.state.counters;
        let headline = match &self.outcome {
            Outcome::Done => format!("{}: done", self.job),
            Outcome::AlreadyDone => format!(
                "{}: already done, nothing was run (--force-rerun starts it again)",
                self.job
            ),
            Outcome::Paused(reason) => format!(
                "{}: paused, {reason}; run it again to resume at the {} phase",
                self.job, self.state.phase
            ),
            Outcome::Aborted(reason) => format!(
                "{}: aborted, {reason}; the cursor is kept, run it again to resume",
                self.job
            ),
            Outcome::Refused => format!(
                "{}: another runner holds the lease, nothing was run (lease expires at {} ms)",
                self.job,
                self.state.lease_expires_at.unwrap_or_default()
            ),
        };
        format!(
            "{headline}\n  totals over all runs: {} items, {} requests, {} refetched, {} gone, {} unreachable, {} failed, {} aggregates recomputed",
            counters.items,
            counters.requests,
            counters.refetched,
            counters.gone,
            counters.unreachable,
            counters.failed,
            counters.aggregates
        )
    }
}

/// What a dry run found: the work left and how long it takes at the rate.
#[derive(Debug, Clone, PartialEq)]
pub struct DryRun {
    pub job: String,
    pub status: Option<BackfillStatus>,
    pub phase: Phase,
    pub items_left: u64,
    pub scopes_left: u64,
    /// The fewest requests the remaining work takes.
    pub requests_at_least: u64,
    /// Time those requests take at the chosen rate.
    pub seconds_at_least: f64,
}

impl DryRun {
    pub fn render(&self, rate: f64) -> String {
        let status = self
            .status
            .map(BackfillStatus::as_str)
            .unwrap_or("never run");
        if self.status == Some(BackfillStatus::Done) {
            return format!(
                "{}: already done, nothing left (dry run, no request sent)",
                self.job
            );
        }
        format!(
            "{}: {status}; {} item(s) left in the fetch phase, {} aggregate scope(s) (database only)\n  at least {} homeserver request(s), at least {} at {rate} request(s) per second (dry run, no request sent)",
            self.job,
            self.items_left,
            self.scopes_left,
            self.requests_at_least,
            format_duration(self.seconds_at_least),
        )
    }
}

fn format_duration(seconds: f64) -> String {
    let total = seconds.ceil() as u64;
    let (hours, minutes, secs) = (total / 3600, (total % 3600) / 60, total % 60);
    match (hours, minutes) {
        (0, 0) => format!("{secs}s"),
        (0, _) => format!("{minutes}m {secs}s"),
        _ => format!("{hours}h {minutes}m {secs}s"),
    }
}

/// Counts what is left, with no request to any homeserver and no write.
pub async fn dry_run(job: &dyn BackfillJob, options: &RunOptions) -> Result<DryRun, DynError> {
    options.gate.validate()?;
    let state = BackfillRun::get(job.id()).await?;
    let phase = state
        .as_ref()
        .and_then(|state| Phase::parse(&state.phase))
        .unwrap_or(Phase::Fetch);
    let done = state
        .as_ref()
        .is_some_and(|state| state.status == BackfillStatus::Done);
    let (items_left, scopes_left) = if done {
        (0, 0)
    } else {
        let items = match phase {
            Phase::Fetch => {
                let cursor = state.as_ref().map(|s| s.cursor.as_str()).unwrap_or("");
                let ahead = state.as_ref().map_or(0, |s| s.ahead.len() as u64);
                job.count_after(cursor).await?.saturating_sub(ahead)
            }
            Phase::Aggregates => 0,
        };
        let scopes = if job.has_aggregates() {
            let cursor = match (&state, phase) {
                (Some(state), Phase::Aggregates) => state.cursor.clone(),
                _ => String::new(),
            };
            job.aggregate_scopes()
                .await?
                .iter()
                .filter(|scope| scope.key() > cursor)
                .count() as u64
        } else {
            0
        };
        (items, scopes)
    };
    let requests = items_left * job.min_requests_per_item();
    Ok(DryRun {
        job: job.id().to_string(),
        status: state.map(|state| state.status),
        phase,
        items_left,
        scopes_left,
        requests_at_least: requests,
        seconds_at_least: requests as f64 / options.gate.rate,
    })
}

enum Ending {
    Done,
    Stopped(GateStop),
}

struct Ctx<'a> {
    job: &'a dyn BackfillJob,
    options: &'a RunOptions,
    sink: &'a dyn ProgressSink,
    owner: String,
    gate: Arc<ThrottledGate>,
    lease_ms: i64,
    totals: BackfillCounters,
    finished_this_run: i64,
    reported_failures: Vec<String>,
}

impl Ctx<'_> {
    fn job_id(&self) -> &str {
        self.job.id()
    }

    fn stop_reason(&self) -> Option<GateStop> {
        if self.options.stop.is_stopped() {
            self.gate.stop(GateStop::Interrupted);
        }
        if self.gate.out_of_time() {
            self.gate.stop(GateStop::DurationBudget);
        }
        self.gate.stopped()
    }

    /// Persists one write's progress. `false` when the lease was lost.
    async fn commit(&mut self, progress: BackfillProgress) -> Result<bool, DynError> {
        let owned =
            BackfillRun::record_progress(self.job_id(), &self.owner, &progress, self.lease_ms)
                .await?;
        if owned {
            self.totals.add(&progress.delta);
        }
        Ok(owned)
    }

    fn note_failures(&mut self, tally: &ItemTally) {
        for id in &tally.failed_ids {
            if self.reported_failures.len() < MAX_REPORTED_FAILURES {
                self.reported_failures.push(id.clone());
                warn!("{}: item {id} failed", self.job_id());
            }
        }
    }
}

fn counters_of(tally: &ItemTally, requests: u64, items: i64) -> BackfillCounters {
    BackfillCounters {
        items,
        requests: requests as i64,
        refetched: tally.refetched,
        gone: tally.gone,
        unreachable: tally.unreachable,
        failed: tally.failed,
        aggregates: 0,
    }
}

fn runner_id() -> String {
    let random = RandomState::new().build_hasher().finish();
    format!("{}-{random:016x}", std::process::id())
}

/// Runs a job: claims its lease, resumes at its cursor, walks the work items
/// through the throttled gate persisting progress after every item, runs the
/// aggregates phase, and ends `done`, `paused` or `aborted`. A job that is
/// already `done` is a no-op unless `force_rerun` is set; a job another
/// runner holds is refused.
pub async fn run_job(
    job: &dyn BackfillJob,
    options: RunOptions,
    sink: &dyn ProgressSink,
) -> Result<BackfillReport, DynError> {
    options.gate.validate()?;
    let job_id = job.id().to_string();
    let lease_ms = options.lease.as_millis() as i64;

    if !options.force_rerun {
        if let Some(state) = BackfillRun::get(&job_id).await? {
            if state.status == BackfillStatus::Done {
                return Ok(BackfillReport {
                    job: job_id,
                    outcome: Outcome::AlreadyDone,
                    state,
                    finished_this_run: 0,
                });
            }
        }
    } else if let Some(state) = BackfillRun::get(&job_id).await? {
        warn!(
            "{job_id}: --force-rerun resets the job (was {}, cursor {:?}, {} items)",
            state.status.as_str(),
            state.cursor,
            state.counters.items
        );
    }

    let owner = runner_id();
    let state = match BackfillRun::claim(
        &job_id,
        Phase::Fetch.as_str(),
        &owner,
        lease_ms,
        options.force_rerun,
    )
    .await?
    {
        BackfillClaim::Acquired(state) => *state,
        BackfillClaim::AlreadyDone(state) => {
            return Ok(BackfillReport {
                job: job_id,
                outcome: Outcome::AlreadyDone,
                state: *state,
                finished_this_run: 0,
            })
        }
        BackfillClaim::Held(state) => {
            return Ok(BackfillReport {
                job: job_id,
                outcome: Outcome::Refused,
                state: *state,
                finished_this_run: 0,
            })
        }
    };

    let gate = Arc::new(ThrottledGate::new(options.gate.clone())?);
    let lease_lost = Arc::new(AtomicBool::new(false));
    let heartbeat = tokio::spawn(keep_lease(
        job_id.clone(),
        owner.clone(),
        lease_ms,
        options.lease_renewal,
        gate.clone(),
        lease_lost.clone(),
    ));
    let interrupt = {
        let (gate, mut stopped) = (gate.clone(), options.stop.subscribe());
        tokio::spawn(async move {
            if stopped.wait_for(|stopped| *stopped).await.is_ok() {
                gate.stop(GateStop::Interrupted);
            }
        })
    };

    let mut ctx = Ctx {
        job,
        options: &options,
        sink,
        owner: owner.clone(),
        gate: gate.clone(),
        lease_ms,
        totals: state.counters,
        finished_this_run: 0,
        reported_failures: Vec::new(),
    };
    let ending = drive(&mut ctx, state).await;
    heartbeat.abort();
    interrupt.abort();

    let (status, reason) = match &ending {
        Ok(Ending::Done) => (BackfillStatus::Done, None),
        Ok(Ending::Stopped(stop)) if stop.is_clean() => {
            (BackfillStatus::Paused, Some(stop.reason()))
        }
        Ok(Ending::Stopped(stop)) => (BackfillStatus::Aborted, Some(stop.reason())),
        Err(e) => (
            BackfillStatus::Aborted,
            Some(format!("database error: {e}")),
        ),
    };
    let lease_was_lost = matches!(&ending, Ok(Ending::Stopped(GateStop::LeaseLost)));
    if !lease_was_lost {
        match BackfillRun::finish(&job_id, &owner, status, reason.as_deref()).await {
            Ok(true) => {}
            Ok(false) => warn!("{job_id}: the lease was lost before the run could finish"),
            Err(e) => warn!("{job_id}: could not record the end of the run: {e}"),
        }
    }
    let finished_this_run = ctx.finished_this_run;
    let outcome = match (&ending, reason) {
        (Ok(Ending::Done), _) => Outcome::Done,
        (Ok(Ending::Stopped(stop)), Some(reason)) if stop.is_clean() => Outcome::Paused(reason),
        (_, Some(reason)) => Outcome::Aborted(reason),
        (_, None) => Outcome::Aborted("unknown".to_string()),
    };
    if !ctx.reported_failures.is_empty() {
        warn!(
            "{job_id}: failed items (first {}): {}",
            ctx.reported_failures.len(),
            ctx.reported_failures.join(", ")
        );
    }
    let state = BackfillRun::get(&job_id)
        .await?
        .ok_or_else(|| format!("BackfillRun {job_id} vanished"))?;
    Ok(BackfillReport {
        job: job_id,
        outcome,
        state,
        finished_this_run,
    })
}

async fn keep_lease(
    job_id: String,
    owner: String,
    lease_ms: i64,
    every: Duration,
    gate: Arc<ThrottledGate>,
    lease_lost: Arc<AtomicBool>,
) {
    loop {
        tokio::time::sleep(every).await;
        match BackfillRun::renew(&job_id, &owner, lease_ms).await {
            Ok(true) => {}
            Ok(false) => {
                warn!("{job_id}: another runner took the lease");
                lease_lost.store(true, Ordering::SeqCst);
                gate.stop(GateStop::LeaseLost);
                return;
            }
            // A renewal that fails to reach the graph is retried at the next
            // tick; the lease is lost only when it expires and someone takes it.
            Err(e) => warn!("{job_id}: could not renew the lease: {e}"),
        }
    }
}

async fn drive(ctx: &mut Ctx<'_>, state: BackfillRun) -> Result<Ending, DynError> {
    let phase = Phase::parse(&state.phase)
        .ok_or_else(|| format!("unknown phase {:?} stored on the job", state.phase))?;
    if phase == Phase::Fetch {
        if let Some(stop) = fetch_phase(ctx, &state).await? {
            return Ok(Ending::Stopped(stop));
        }
        if !ctx.job.has_aggregates() {
            return Ok(Ending::Done);
        }
        let to_aggregates = BackfillProgress {
            phase: Phase::Aggregates.as_str().to_string(),
            cursor: String::new(),
            ahead: Vec::new(),
            delta: BackfillCounters::default(),
        };
        if !ctx.commit(to_aggregates).await? {
            return Ok(Ending::Stopped(GateStop::LeaseLost));
        }
    }
    let cursor = if phase == Phase::Aggregates {
        state.cursor.clone()
    } else {
        String::new()
    };
    match aggregates_phase(ctx, cursor).await? {
        Some(stop) => Ok(Ending::Stopped(stop)),
        None => Ok(Ending::Done),
    }
}

type ItemFuture<'a> = Pin<Box<dyn Future<Output = (String, ItemResult, u64)> + Send + 'a>>;

fn run_item<'a>(
    job: &'a dyn BackfillJob,
    gate: &'a dyn RequestGate,
    key: String,
) -> ItemFuture<'a> {
    async move {
        let counting = CountingGate::new(gate);
        let result = job.process(&key, &counting).await;
        (key, result, counting.requests())
    }
    .boxed()
}

/// Walks the work items in key order with up to `concurrency` in flight.
///
/// The cursor is the highest key with every key before it finished; items
/// that finish out of order sit in `ahead` until the ones before them do, and
/// are persisted with the cursor so a resumed run skips them. Returns the
/// reason when the run stopped before the last item.
async fn fetch_phase(ctx: &mut Ctx<'_>, state: &BackfillRun) -> Result<Option<GateStop>, DynError> {
    let concurrency = ctx.options.gate.concurrency;
    let mut frontier = state.cursor.clone();
    let mut ahead: BTreeSet<String> = state.ahead.iter().cloned().collect();
    let mut scan_position = state.cursor.clone();
    let mut buffer: VecDeque<String> = VecDeque::new();
    let mut exhausted = false;
    let mut pending: VecDeque<String> = VecDeque::new();
    let mut finished: HashSet<String> = HashSet::new();
    let mut stopping: Option<GateStop> = None;
    let job = ctx.job;
    let gate = ctx.gate.clone();
    let mut in_flight: FuturesUnordered<ItemFuture<'_>> = FuturesUnordered::new();

    loop {
        while stopping.is_none() && in_flight.len() < concurrency {
            if let Some(stop) = ctx.stop_reason() {
                stopping = Some(stop);
                break;
            }
            if buffer.is_empty() && !exhausted {
                let batch = job.items_after(&scan_position, ctx.options.batch).await?;
                exhausted = batch.len() < ctx.options.batch;
                if let Some(last) = batch.last() {
                    scan_position = last.clone();
                }
                buffer.extend(batch);
            }
            let Some(key) = buffer.pop_front() else {
                break;
            };
            pending.push_back(key.clone());
            if ahead.contains(&key) {
                finished.insert(key);
                continue;
            }
            in_flight.push(run_item(job, gate.as_ref(), key));
        }
        advance_frontier(&mut frontier, &mut pending, &mut finished, &mut ahead);

        let Some((key, result, requests)) = in_flight.next().await else {
            break;
        };
        match result {
            ItemResult::Finished(tally) => {
                ctx.note_failures(&tally);
                finished.insert(key.clone());
                if frontier_waits_for(&pending, &key) {
                    ahead.insert(key.clone());
                }
                advance_frontier(&mut frontier, &mut pending, &mut finished, &mut ahead);
                let progress = BackfillProgress {
                    phase: Phase::Fetch.as_str().to_string(),
                    cursor: frontier.clone(),
                    ahead: ahead.iter().cloned().collect(),
                    delta: counters_of(&tally, requests, 1),
                };
                if !ctx.commit(progress).await? {
                    return Ok(Some(GateStop::LeaseLost));
                }
                ctx.finished_this_run += 1;
                ctx.sink.item_finished(&ProgressEvent {
                    phase: Phase::Fetch,
                    key: &key,
                    totals: &ctx.totals,
                });
            }
            ItemResult::Stopped { partial, stop } => {
                stopping.get_or_insert(stop);
                if requests > 0 {
                    let progress = BackfillProgress {
                        phase: Phase::Fetch.as_str().to_string(),
                        cursor: frontier.clone(),
                        ahead: ahead.iter().cloned().collect(),
                        delta: BackfillCounters {
                            requests: requests as i64,
                            refetched: partial.refetched,
                            ..Default::default()
                        },
                    };
                    if !ctx.commit(progress).await? {
                        return Ok(Some(GateStop::LeaseLost));
                    }
                }
            }
        }
    }
    Ok(stopping)
}

/// Whether finishing `key` leaves an earlier key still unfinished.
fn frontier_waits_for(pending: &VecDeque<String>, key: &str) -> bool {
    pending.front().is_some_and(|front| front != key)
}

fn advance_frontier(
    frontier: &mut String,
    pending: &mut VecDeque<String>,
    finished: &mut HashSet<String>,
    ahead: &mut BTreeSet<String>,
) {
    while let Some(front) = pending.front() {
        if !finished.remove(front) {
            break;
        }
        ahead.remove(front);
        frontier.clone_from(front);
        pending.pop_front();
    }
}

/// Recomputes each aggregate scope after the cursor once, in key order,
/// persisting the cursor after each, with a pause between scopes.
async fn aggregates_phase(ctx: &mut Ctx<'_>, cursor: String) -> Result<Option<GateStop>, DynError> {
    let scopes = ctx.job.aggregate_scopes().await?;
    for scope in scopes {
        let key = scope.key();
        if key <= cursor {
            continue;
        }
        if let Some(stop) = ctx.stop_reason() {
            return Ok(Some(stop));
        }
        ctx.job.recompute(&scope).await?;
        let progress = BackfillProgress {
            phase: Phase::Aggregates.as_str().to_string(),
            cursor: key.clone(),
            ahead: Vec::new(),
            delta: BackfillCounters {
                aggregates: 1,
                ..Default::default()
            },
        };
        if !ctx.commit(progress).await? {
            return Ok(Some(GateStop::LeaseLost));
        }
        ctx.finished_this_run += 1;
        ctx.sink.item_finished(&ProgressEvent {
            phase: Phase::Aggregates,
            key: &key,
            totals: &ctx.totals,
        });
        if !ctx.options.aggregate_pause.is_zero() {
            let mut stopped = ctx.options.stop.subscribe();
            tokio::select! {
                _ = tokio::time::sleep(ctx.options.aggregate_pause) => {}
                _ = stopped.wait_for(|stopped| *stopped) => {}
            }
        }
    }
    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn keys(items: &[&str]) -> VecDeque<String> {
        items.iter().map(|key| key.to_string()).collect()
    }

    #[test]
    fn the_frontier_advances_only_over_a_finished_prefix() {
        let mut frontier = String::new();
        let mut pending = keys(&["a", "b", "c", "d"]);
        let mut finished: HashSet<String> = ["c", "b"].iter().map(|k| k.to_string()).collect();
        let mut ahead: BTreeSet<String> = finished.iter().cloned().collect();
        advance_frontier(&mut frontier, &mut pending, &mut finished, &mut ahead);
        assert_eq!(frontier, "", "a is still in flight");
        assert_eq!(ahead.len(), 2);

        finished.insert("a".to_string());
        advance_frontier(&mut frontier, &mut pending, &mut finished, &mut ahead);
        assert_eq!(frontier, "c");
        assert!(ahead.is_empty());
        assert_eq!(pending, keys(&["d"]));
    }

    #[test]
    fn a_key_finishing_behind_the_front_is_ahead() {
        let pending = keys(&["a", "b"]);
        assert!(frontier_waits_for(&pending, "b"));
        assert!(!frontier_waits_for(&pending, "a"));
        assert!(!frontier_waits_for(&VecDeque::new(), "a"));
    }

    fn state(outcome: Outcome, failed: i64) -> BackfillReport {
        BackfillReport {
            job: "job".to_string(),
            outcome,
            state: BackfillRun {
                job: "job".to_string(),
                status: BackfillStatus::Done,
                cursor: String::new(),
                phase: "fetch".to_string(),
                ahead: Vec::new(),
                counters: BackfillCounters {
                    failed,
                    ..Default::default()
                },
                started_at: None,
                updated_at: 0,
                lease_owner: None,
                lease_expires_at: None,
                lease_live: false,
                stop_reason: None,
            },
            finished_this_run: 0,
        }
    }

    #[test]
    fn exit_codes_follow_the_outcome() {
        assert_eq!(state(Outcome::Done, 0).exit_code(), EXIT_OK);
        assert_eq!(state(Outcome::AlreadyDone, 0).exit_code(), EXIT_OK);
        assert_eq!(state(Outcome::AlreadyDone, 4).exit_code(), EXIT_OK);
        assert_eq!(state(Outcome::Paused("x".into()), 0).exit_code(), EXIT_OK);
        assert_eq!(state(Outcome::Done, 1).exit_code(), EXIT_FAILED_ITEMS);
        assert_eq!(
            state(Outcome::Aborted("x".into()), 0).exit_code(),
            EXIT_ABORTED
        );
        assert_eq!(state(Outcome::Refused, 0).exit_code(), EXIT_ERROR);
    }

    #[test]
    fn durations_read_as_hours_minutes_seconds() {
        assert_eq!(format_duration(0.2), "1s");
        assert_eq!(format_duration(59.0), "59s");
        assert_eq!(format_duration(61.0), "1m 1s");
        assert_eq!(format_duration(3_725.0), "1h 2m 5s");
    }
}
