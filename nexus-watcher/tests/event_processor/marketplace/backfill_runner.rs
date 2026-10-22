//! The backfill runner against the real graph with a scripted job: leases,
//! cursors, resume, budgets, the breaker and the once-only rule, without a
//! homeserver.

use anyhow::Result;
use async_trait::async_trait;
use nexus_common::db::exec_single_row;
use nexus_common::db::graph::Query;
use nexus_common::models::backfill_run::{
    BackfillClaim, BackfillCounters, BackfillProgress, BackfillRun, BackfillStatus,
};
use nexus_common::types::DynError;
use nexus_watcher::backfill::gate::{GateConfig, RequestGate};
use nexus_watcher::backfill::jobs::{AggregateScope, BackfillJob, ItemResult, ItemTally, Phase};
use nexus_watcher::backfill::run::{
    dry_run, run_job, Outcome, ProgressEvent, ProgressSink, RunOptions, StopHandle, EXIT_ABORTED,
    EXIT_ERROR, EXIT_FAILED_ITEMS, EXIT_OK,
};
use nexus_watcher::service::NexusWatcher;
use pubky::Keypair;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[derive(Clone, Copy, Debug)]
enum Behavior {
    /// One request that succeeds; the item is ingested.
    Ok,
    /// One request answered 404.
    Gone,
    /// One request that fails: the homeserver is unreachable.
    Unreachable,
    /// One request that succeeds, then the record cannot be ingested.
    Failed,
    /// Waits, then makes the request. Lets other items finish first.
    Late(Duration),
}

/// A job over in-memory items whose behaviour the test scripts per item.
struct ScriptedJob {
    key: String,
    items: Vec<String>,
    scopes: Vec<AggregateScope>,
    behavior: Mutex<HashMap<String, Behavior>>,
    requested: Mutex<Vec<String>>,
    recomputed: Mutex<Vec<String>>,
    work: Duration,
}

impl ScriptedJob {
    fn new(name: &str, items: usize, scopes: usize) -> Arc<Self> {
        let suffix = &Keypair::random().public_key().to_z32()[..8];
        Arc::new(ScriptedJob {
            key: format!("test-{name}-{suffix}"),
            items: (1..=items).map(|n| format!("k{n:02}")).collect(),
            scopes: (1..=scopes)
                .map(|n| AggregateScope::Listing {
                    owner_id: "owner".to_string(),
                    listing_id: format!("l{n:02}"),
                })
                .collect(),
            behavior: Mutex::new(HashMap::new()),
            requested: Mutex::new(Vec::new()),
            recomputed: Mutex::new(Vec::new()),
            work: Duration::from_millis(5),
        })
    }

    fn script(&self, key: &str, behavior: Behavior) {
        self.behavior
            .lock()
            .unwrap()
            .insert(key.to_string(), behavior);
    }

    fn requested(&self) -> Vec<String> {
        self.requested.lock().unwrap().clone()
    }

    fn take_requested(&self) -> Vec<String> {
        std::mem::take(&mut *self.requested.lock().unwrap())
    }

    fn recomputed(&self) -> Vec<String> {
        self.recomputed.lock().unwrap().clone()
    }
}

#[async_trait]
impl BackfillJob for ScriptedJob {
    fn id(&self) -> &str {
        &self.key
    }

    fn has_aggregates(&self) -> bool {
        !self.scopes.is_empty()
    }

    async fn items_after(&self, cursor: &str, limit: usize) -> Result<Vec<String>, DynError> {
        Ok(self
            .items
            .iter()
            .filter(|key| key.as_str() > cursor)
            .take(limit)
            .cloned()
            .collect())
    }

    async fn count_after(&self, cursor: &str) -> Result<u64, DynError> {
        Ok(self
            .items
            .iter()
            .filter(|key| key.as_str() > cursor)
            .count() as u64)
    }

    fn min_requests_per_item(&self) -> u64 {
        1
    }

    async fn process(&self, key: &str, gate: &dyn RequestGate) -> ItemResult {
        let behavior = self
            .behavior
            .lock()
            .unwrap()
            .get(key)
            .copied()
            .unwrap_or(Behavior::Ok);
        if let Behavior::Late(wait) = behavior {
            tokio::time::sleep(wait).await;
        }
        let permit = match gate.acquire().await {
            Ok(permit) => permit,
            Err(stop) => {
                return ItemResult::Stopped {
                    partial: ItemTally::default(),
                    stop,
                }
            }
        };
        self.requested.lock().unwrap().push(key.to_string());
        tokio::time::sleep(self.work).await;
        let mut tally = ItemTally::default();
        match behavior {
            Behavior::Ok | Behavior::Late(_) => {
                permit.succeeded();
                tally.refetched = 1;
            }
            Behavior::Gone => {
                permit.succeeded();
                tally.gone = 1;
            }
            Behavior::Unreachable => {
                permit.failed();
                tally.unreachable = 1;
            }
            Behavior::Failed => {
                permit.succeeded();
                tally.failed = 1;
                tally.failed_ids = vec![key.to_string()];
            }
        }
        ItemResult::Finished(tally)
    }

    async fn aggregate_scopes(&self) -> Result<Vec<AggregateScope>, DynError> {
        Ok(self.scopes.clone())
    }

    async fn recompute(&self, scope: &AggregateScope) -> Result<(), DynError> {
        self.recomputed.lock().unwrap().push(scope.key());
        Ok(())
    }
}

/// Records every finished item and optionally stops the run after a number
/// of them.
#[derive(Default)]
struct Recorder {
    events: Mutex<Vec<(Phase, String)>>,
    stop_after: Mutex<Option<(Phase, usize, StopHandle)>>,
}

impl Recorder {
    fn stopping_after(phase: Phase, count: usize, stop: StopHandle) -> Self {
        Recorder {
            events: Mutex::new(Vec::new()),
            stop_after: Mutex::new(Some((phase, count, stop))),
        }
    }

    fn keys(&self, phase: Phase) -> Vec<String> {
        self.events
            .lock()
            .unwrap()
            .iter()
            .filter(|(seen, _)| *seen == phase)
            .map(|(_, key)| key.clone())
            .collect()
    }
}

impl ProgressSink for Recorder {
    fn item_finished(&self, event: &ProgressEvent<'_>) {
        let mut events = self.events.lock().unwrap();
        events.push((event.phase, event.key.to_string()));
        let seen = events.iter().filter(|(p, _)| *p == event.phase).count();
        if let Some((phase, count, stop)) = self.stop_after.lock().unwrap().as_ref() {
            if *phase == event.phase && seen >= *count {
                stop.stop();
            }
        }
    }
}

fn fast_gate() -> GateConfig {
    GateConfig {
        rate: 20.0,
        backoff_initial: Duration::from_millis(5),
        backoff_max: Duration::from_millis(20),
        jitter: 0.0,
        ..GateConfig::default()
    }
}

fn options() -> RunOptions {
    RunOptions {
        gate: fast_gate(),
        aggregate_pause: Duration::ZERO,
        ..RunOptions::default()
    }
}

async fn stack() -> Result<()> {
    NexusWatcher::builder()
        .init_test_stack()
        .await
        .map_err(|e| anyhow::anyhow!("{e:?}"))
}

async fn state(job: &Arc<ScriptedJob>) -> BackfillRun {
    BackfillRun::get(&job.key)
        .await
        .unwrap()
        .expect("the job has state")
}

async fn cleanup(job: &Arc<ScriptedJob>) -> Result<()> {
    BackfillRun::delete(&job.key).await?;
    Ok(())
}

fn no_duplicates(keys: &[String]) {
    let mut sorted = keys.to_vec();
    sorted.sort();
    sorted.dedup();
    assert_eq!(
        sorted.len(),
        keys.len(),
        "a key was processed twice: {keys:?}"
    );
}

#[tokio_shared_rt::test(shared)]
async fn a_run_walks_every_item_once_persisting_each_and_ends_done() -> Result<()> {
    stack().await?;
    let job = ScriptedJob::new("walk", 6, 3);
    job.script("k02", Behavior::Gone);
    job.script("k03", Behavior::Unreachable);
    let sink = Recorder::default();

    let report = run_job(job.as_ref(), options(), &sink).await.unwrap();

    assert_eq!(report.outcome, Outcome::Done);
    assert_eq!(report.exit_code(), EXIT_OK);
    assert_eq!(job.requested().len(), 6);
    assert_eq!(sink.keys(Phase::Fetch), job.items);
    assert_eq!(sink.keys(Phase::Aggregates).len(), 3);
    let stored = state(&job).await;
    assert_eq!(stored.status, BackfillStatus::Done);
    assert_eq!(stored.lease_owner, None);
    assert_eq!(
        stored.counters,
        BackfillCounters {
            items: 6,
            requests: 6,
            refetched: 4,
            gone: 1,
            unreachable: 1,
            failed: 0,
            aggregates: 3,
        }
    );
    cleanup(&job).await
}

#[tokio_shared_rt::test(shared)]
async fn a_done_job_is_a_no_op_and_force_rerun_starts_it_over() -> Result<()> {
    stack().await?;
    let job = ScriptedJob::new("once", 4, 2);
    let first = run_job(job.as_ref(), options(), &Recorder::default())
        .await
        .unwrap();
    assert_eq!(first.outcome, Outcome::Done);
    let requests_after_first = job.requested().len();
    assert_eq!(requests_after_first, 4);
    let stored = state(&job).await;

    let sink = Recorder::default();
    let again = run_job(job.as_ref(), options(), &sink).await.unwrap();
    assert_eq!(again.outcome, Outcome::AlreadyDone);
    assert_eq!(again.exit_code(), EXIT_OK);
    assert_eq!(again.finished_this_run, 0);
    assert_eq!(
        job.requested().len(),
        requests_after_first,
        "a done job sends no request"
    );
    assert!(sink.keys(Phase::Fetch).is_empty());
    assert_eq!(state(&job).await, stored, "nothing was rewritten");
    assert!(again.render().contains("already done"));

    let forced = run_job(
        job.as_ref(),
        RunOptions {
            force_rerun: true,
            ..options()
        },
        &Recorder::default(),
    )
    .await
    .unwrap();
    assert_eq!(forced.outcome, Outcome::Done);
    assert_eq!(job.requested().len(), 8, "the forced run walked it again");
    let counters = state(&job).await.counters;
    assert_eq!(
        (counters.items, counters.requests, counters.aggregates),
        (4, 4, 2),
        "a forced run reports its own totals, not the first run's"
    );
    cleanup(&job).await
}

#[tokio_shared_rt::test(shared)]
async fn a_request_budget_pauses_and_the_resume_never_repeats_a_finished_item() -> Result<()> {
    stack().await?;
    let job = ScriptedJob::new("budget", 10, 2);
    let sink_one = Recorder::default();
    let first = run_job(
        job.as_ref(),
        RunOptions {
            gate: GateConfig {
                max_requests: Some(4),
                ..fast_gate()
            },
            ..options()
        },
        &sink_one,
    )
    .await
    .unwrap();

    assert!(
        matches!(first.outcome, Outcome::Paused(_)),
        "{:?}",
        first.outcome
    );
    assert_eq!(first.exit_code(), EXIT_OK);
    assert_eq!(first.state.status, BackfillStatus::Paused);
    assert_eq!(first.state.cursor, "k04");
    assert_eq!(first.state.counters.requests, 4);
    let first_requests = job.take_requested();
    assert_eq!(first_requests, ["k01", "k02", "k03", "k04"]);

    let sink_two = Recorder::default();
    let second = run_job(job.as_ref(), options(), &sink_two).await.unwrap();
    assert_eq!(second.outcome, Outcome::Done);
    let second_requests = job.take_requested();
    assert_eq!(second_requests.len(), 6);
    assert!(
        second_requests
            .iter()
            .all(|key| !first_requests.contains(key)),
        "no item finished before the stop is requested again"
    );
    let mut all = first_requests;
    all.extend(second_requests);
    no_duplicates(&all);
    assert_eq!(all.len(), 10);
    let totals = state(&job).await.counters;
    assert_eq!(
        (
            totals.items,
            totals.requests,
            totals.refetched,
            totals.aggregates
        ),
        (10, 10, 10, 2),
        "totals add up across the two runs"
    );
    cleanup(&job).await
}

#[tokio_shared_rt::test(shared)]
async fn concurrent_items_that_finish_out_of_order_are_kept_ahead_of_the_cursor() -> Result<()> {
    stack().await?;
    let job = ScriptedJob::new("ahead", 8, 0);
    job.script("k01", Behavior::Late(Duration::from_millis(400)));
    let stop = StopHandle::default();
    let sink = Recorder::stopping_after(Phase::Fetch, 2, stop.clone());
    let concurrent = RunOptions {
        gate: GateConfig {
            concurrency: 3,
            ..fast_gate()
        },
        stop,
        ..options()
    };

    let first = run_job(job.as_ref(), concurrent.clone(), &sink)
        .await
        .unwrap();

    assert!(
        matches!(first.outcome, Outcome::Paused(_)),
        "{:?}",
        first.outcome
    );
    assert_eq!(
        first.state.cursor, "",
        "k01 never finished, so the cursor cannot pass it"
    );
    assert!(
        !first.state.ahead.is_empty() && !first.state.ahead.contains(&"k01".to_string()),
        "the finished items behind k01 are kept ahead: {:?}",
        first.state.ahead
    );
    let ahead = first.state.ahead.clone();
    let first_requests = job.take_requested();
    assert!(!first_requests.contains(&"k01".to_string()));

    let second = run_job(
        job.as_ref(),
        RunOptions {
            stop: StopHandle::default(),
            ..concurrent
        },
        &Recorder::default(),
    )
    .await
    .unwrap();
    assert_eq!(second.outcome, Outcome::Done);
    let second_requests = job.take_requested();
    for key in &ahead {
        assert!(
            !second_requests.contains(key),
            "{key} finished in the first run and must not be requested again"
        );
    }
    assert!(second_requests.contains(&"k01".to_string()));
    let mut all = first_requests;
    all.extend(second_requests);
    no_duplicates(&all);
    assert_eq!(all.len(), 8);
    let stored = state(&job).await;
    assert_eq!(stored.counters.items, 8);
    assert!(stored.ahead.is_empty());
    assert_eq!(stored.cursor, "k08");
    cleanup(&job).await
}

#[tokio_shared_rt::test(shared)]
async fn the_breaker_aborts_with_the_cursor_kept_and_a_resume_continues() -> Result<()> {
    stack().await?;
    let job = ScriptedJob::new("breaker", 10, 0);
    for key in ["k05", "k06", "k07"] {
        job.script(key, Behavior::Unreachable);
    }
    let first = run_job(
        job.as_ref(),
        RunOptions {
            gate: GateConfig {
                max_consecutive_errors: 3,
                ..fast_gate()
            },
            ..options()
        },
        &Recorder::default(),
    )
    .await
    .unwrap();

    assert!(
        matches!(first.outcome, Outcome::Aborted(_)),
        "{:?}",
        first.outcome
    );
    assert_eq!(first.exit_code(), EXIT_ABORTED);
    assert_eq!(first.state.status, BackfillStatus::Aborted);
    assert!(first
        .state
        .stop_reason
        .as_deref()
        .is_some_and(|reason| reason.contains("circuit breaker")));
    assert_eq!(
        first.state.cursor, "k07",
        "the cursor is kept at the last item"
    );
    assert_eq!(first.state.counters.unreachable, 3);
    assert_eq!(first.state.lease_owner, None, "the lease is released");
    assert_eq!(
        job.take_requested().len(),
        7,
        "it stopped hammering at the trip"
    );

    job.script("k05", Behavior::Ok);
    let second = run_job(job.as_ref(), options(), &Recorder::default())
        .await
        .unwrap();
    assert_eq!(second.outcome, Outcome::Done);
    let resumed = job.take_requested();
    assert_eq!(resumed, ["k08", "k09", "k10"]);
    let totals = state(&job).await.counters;
    assert_eq!(
        (totals.items, totals.unreachable, totals.requests),
        (10, 3, 10)
    );
    cleanup(&job).await
}

#[tokio_shared_rt::test(shared)]
async fn gone_and_unreachable_items_are_not_retried_within_or_across_runs() -> Result<()> {
    stack().await?;
    let job = ScriptedJob::new("skipped", 6, 0);
    job.script("k02", Behavior::Gone);
    job.script("k03", Behavior::Unreachable);
    let stop = StopHandle::default();
    let sink = Recorder::stopping_after(Phase::Fetch, 4, stop.clone());
    let first = run_job(job.as_ref(), RunOptions { stop, ..options() }, &sink)
        .await
        .unwrap();
    assert!(matches!(first.outcome, Outcome::Paused(_)));

    let second = run_job(job.as_ref(), options(), &Recorder::default())
        .await
        .unwrap();
    assert_eq!(second.outcome, Outcome::Done);
    let requested = job.requested();
    assert_eq!(
        requested.len(),
        6,
        "each item, gone or unreachable too, once"
    );
    no_duplicates(&requested);
    let totals = state(&job).await.counters;
    assert_eq!(
        (totals.gone, totals.unreachable, totals.refetched),
        (1, 1, 4)
    );
    cleanup(&job).await
}

#[tokio_shared_rt::test(shared)]
async fn record_level_failures_end_the_job_done_with_a_nonzero_exit() -> Result<()> {
    stack().await?;
    let job = ScriptedJob::new("failed", 5, 0);
    job.script("k03", Behavior::Failed);
    let report = run_job(job.as_ref(), options(), &Recorder::default())
        .await
        .unwrap();
    assert_eq!(
        report.outcome,
        Outcome::Done,
        "a failed record does not abort"
    );
    assert_eq!(report.state.counters.failed, 1);
    assert_eq!(report.state.counters.items, 5);
    assert_eq!(report.exit_code(), EXIT_FAILED_ITEMS);
    assert!(report.render().contains("1 failed"));

    let again = run_job(job.as_ref(), options(), &Recorder::default())
        .await
        .unwrap();
    assert_eq!(again.outcome, Outcome::AlreadyDone);
    assert_eq!(again.exit_code(), EXIT_OK);
    cleanup(&job).await
}

#[tokio_shared_rt::test(shared)]
async fn a_time_budget_pauses_cleanly() -> Result<()> {
    stack().await?;
    let job = ScriptedJob::new("duration", 40, 0);
    let report = run_job(
        job.as_ref(),
        RunOptions {
            gate: GateConfig {
                rate: 5.0,
                max_duration: Some(Duration::from_millis(900)),
                ..fast_gate()
            },
            ..options()
        },
        &Recorder::default(),
    )
    .await
    .unwrap();
    assert!(
        matches!(report.outcome, Outcome::Paused(_)),
        "{:?}",
        report.outcome
    );
    assert_eq!(report.exit_code(), EXIT_OK);
    let finished = report.state.counters.items;
    assert!((1..40).contains(&finished), "finished {finished}");
    assert_eq!(report.state.cursor, format!("k{finished:02}"));
    cleanup(&job).await
}

#[tokio_shared_rt::test(shared)]
async fn the_aggregates_phase_resumes_without_recomputing_a_scope_twice() -> Result<()> {
    stack().await?;
    let job = ScriptedJob::new("aggregates", 3, 6);
    let stop = StopHandle::default();
    let sink = Recorder::stopping_after(Phase::Aggregates, 3, stop.clone());
    let first = run_job(job.as_ref(), RunOptions { stop, ..options() }, &sink)
        .await
        .unwrap();

    assert!(
        matches!(first.outcome, Outcome::Paused(_)),
        "{:?}",
        first.outcome
    );
    assert_eq!(first.state.status, BackfillStatus::Paused);
    assert_eq!(first.state.phase, "aggregates");
    assert_eq!(first.state.counters.aggregates, 3);
    assert_eq!(job.recomputed().len(), 3);
    assert_ne!(
        first.state.status,
        BackfillStatus::Done,
        "the job is done only after the aggregates"
    );

    let second = run_job(job.as_ref(), options(), &Recorder::default())
        .await
        .unwrap();
    assert_eq!(second.outcome, Outcome::Done);
    let recomputed = job.recomputed();
    assert_eq!(recomputed.len(), 6);
    no_duplicates(&recomputed);
    let mut sorted = recomputed.clone();
    sorted.sort();
    assert_eq!(recomputed, sorted, "scopes run in key order");
    assert_eq!(
        job.requested().len(),
        3,
        "the fetch phase did not run again"
    );
    assert_eq!(state(&job).await.counters.aggregates, 6);
    cleanup(&job).await
}

#[tokio_shared_rt::test(shared)]
async fn only_one_of_many_racing_claims_wins() -> Result<()> {
    stack().await?;
    for round in 0..5 {
        let key = format!(
            "test-claim-{round}-{}",
            &Keypair::random().public_key().to_z32()[..8]
        );
        let mut claims = Vec::new();
        for runner in 0..8 {
            let key = key.clone();
            claims.push(tokio::spawn(async move {
                BackfillRun::claim(&key, "fetch", &format!("runner-{runner}"), 60_000, false)
                    .await
                    .unwrap()
            }));
        }
        let mut acquired = 0;
        for claim in claims {
            if matches!(claim.await?, BackfillClaim::Acquired(_)) {
                acquired += 1;
            }
        }
        assert_eq!(
            acquired, 1,
            "round {round}: exactly one runner holds the lease"
        );
        BackfillRun::delete(&key).await?;
    }
    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn a_second_runner_is_refused_while_the_lease_is_live_and_renewed() -> Result<()> {
    stack().await?;
    let job = ScriptedJob::new("two", 40, 0);
    // A 300 ms lease renewed every 60 ms: the run outlives the lease several
    // times over and only the renewals keep it.
    let short_lease = RunOptions {
        lease: Duration::from_millis(300),
        lease_renewal: Duration::from_millis(60),
        ..options()
    };
    let first = {
        let (job, options) = (job.clone(), short_lease.clone());
        tokio::spawn(async move {
            run_job(job.as_ref(), options, &Recorder::default())
                .await
                .unwrap()
        })
    };
    tokio::time::sleep(Duration::from_millis(250)).await;
    let mut refused = 0;
    for _ in 0..4 {
        let second = run_job(job.as_ref(), short_lease.clone(), &Recorder::default())
            .await
            .unwrap();
        assert_eq!(second.outcome, Outcome::Refused);
        assert_eq!(second.exit_code(), EXIT_ERROR);
        refused += 1;
        tokio::time::sleep(Duration::from_millis(150)).await;
    }
    assert_eq!(refused, 4);
    let first = first.await?;
    assert_eq!(first.outcome, Outcome::Done);
    let requested = job.requested();
    assert_eq!(requested.len(), 40);
    no_duplicates(&requested);
    cleanup(&job).await
}

#[tokio_shared_rt::test(shared)]
async fn an_expired_lease_is_taken_over_and_the_old_holder_is_fenced_out() -> Result<()> {
    stack().await?;
    let job = ScriptedJob::new("takeover", 6, 0);
    let dead = "dead-runner";
    let claim = BackfillRun::claim(&job.key, "fetch", dead, 60_000, false).await?;
    assert!(matches!(claim, BackfillClaim::Acquired(_)));
    // The dead runner finished two items and recorded them.
    let owned = BackfillRun::record_progress(
        &job.key,
        dead,
        &BackfillProgress {
            phase: "fetch".to_string(),
            cursor: "k02".to_string(),
            ahead: Vec::new(),
            delta: BackfillCounters {
                items: 2,
                requests: 2,
                refetched: 2,
                ..Default::default()
            },
        },
        60_000,
    )
    .await?;
    assert!(owned);

    let live = run_job(job.as_ref(), options(), &Recorder::default())
        .await
        .unwrap();
    assert_eq!(live.outcome, Outcome::Refused, "a live lease is not taken");

    exec_single_row(
        Query::new(
            "test_expire_backfill_lease",
            "MATCH (b:BackfillRun {job: $job}) SET b.lease_expires_at = timestamp() - 1000",
        )
        .param("job", job.key.clone()),
    )
    .await?;
    let sink = Recorder::default();
    let taken = run_job(job.as_ref(), options(), &sink).await.unwrap();
    assert_eq!(taken.outcome, Outcome::Done);
    assert_eq!(
        job.requested(),
        ["k03", "k04", "k05", "k06"],
        "the new runner resumes at the dead runner's cursor"
    );
    assert_eq!(
        taken.state.counters.items, 6,
        "totals include the dead runner's"
    );

    let stale = BackfillRun::record_progress(
        &job.key,
        dead,
        &BackfillProgress {
            phase: "fetch".to_string(),
            cursor: "k01".to_string(),
            ahead: Vec::new(),
            delta: BackfillCounters::default(),
        },
        60_000,
    )
    .await?;
    assert!(!stale, "the old holder can no longer write");
    assert_eq!(state(&job).await.cursor, "k06");
    cleanup(&job).await
}

#[tokio_shared_rt::test(shared)]
async fn a_runner_that_loses_its_lease_stops_without_touching_the_job() -> Result<()> {
    stack().await?;
    let job = ScriptedJob::new("lost", 30, 0);
    let slow = RunOptions {
        lease: Duration::from_millis(400),
        lease_renewal: Duration::from_millis(50),
        ..options()
    };
    let running = {
        let (job, options) = (job.clone(), slow);
        tokio::spawn(async move {
            run_job(job.as_ref(), options, &Recorder::default())
                .await
                .unwrap()
        })
    };
    tokio::time::sleep(Duration::from_millis(300)).await;
    // Another runner takes the job over behind its back.
    exec_single_row(
        Query::new(
            "test_steal_backfill_lease",
            "MATCH (b:BackfillRun {job: $job}) SET b.lease_owner = 'usurper'",
        )
        .param("job", job.key.clone()),
    )
    .await?;

    let report = running.await?;
    assert!(
        matches!(report.outcome, Outcome::Aborted(_)),
        "{:?}",
        report.outcome
    );
    assert_eq!(report.exit_code(), EXIT_ABORTED);
    let stored = state(&job).await;
    assert_eq!(stored.lease_owner.as_deref(), Some("usurper"));
    assert_eq!(stored.status, BackfillStatus::Running);
    assert!(job.requested().len() < 30, "the old runner stopped working");
    cleanup(&job).await
}

#[tokio_shared_rt::test(shared)]
async fn a_dry_run_counts_the_work_left_and_sends_nothing() -> Result<()> {
    stack().await?;
    let job = ScriptedJob::new("dry", 10, 4);
    let fresh = dry_run(job.as_ref(), &options()).await.unwrap();
    assert_eq!((fresh.items_left, fresh.scopes_left), (10, 4));
    assert_eq!(fresh.requests_at_least, 10);
    assert_eq!(fresh.seconds_at_least, 10.0 / 20.0);
    assert_eq!(fresh.status, None);
    assert!(
        BackfillRun::get(&job.key).await?.is_none(),
        "a dry run writes nothing"
    );
    assert!(job.requested().is_empty());

    let slow_rate = RunOptions {
        gate: GateConfig {
            rate: 2.0,
            ..fast_gate()
        },
        ..options()
    };
    let rendered = dry_run(job.as_ref(), &slow_rate).await.unwrap().render(2.0);
    assert!(rendered.contains("10 item(s) left"), "{rendered}");
    assert!(rendered.contains("5s at 2 request"), "{rendered}");

    let stop = StopHandle::default();
    let sink = Recorder::stopping_after(Phase::Fetch, 4, stop.clone());
    run_job(job.as_ref(), RunOptions { stop, ..options() }, &sink)
        .await
        .unwrap();
    let resumed = dry_run(job.as_ref(), &options()).await.unwrap();
    assert_eq!(resumed.items_left, 6);
    assert_eq!(resumed.status, Some(BackfillStatus::Paused));

    run_job(job.as_ref(), options(), &Recorder::default())
        .await
        .unwrap();
    let done = dry_run(job.as_ref(), &options()).await.unwrap();
    assert_eq!((done.items_left, done.scopes_left), (0, 0));
    assert!(done.render(20.0).contains("already done"));
    cleanup(&job).await
}

#[tokio_shared_rt::test(shared)]
async fn force_rerun_cannot_steal_a_live_lease() -> Result<()> {
    stack().await?;
    let job = ScriptedJob::new("forcelive", 4, 0);
    BackfillRun::claim(&job.key, "fetch", "holder", 60_000, false).await?;
    let report = run_job(
        job.as_ref(),
        RunOptions {
            force_rerun: true,
            ..options()
        },
        &Recorder::default(),
    )
    .await
    .unwrap();
    assert_eq!(report.outcome, Outcome::Refused);
    assert!(job.requested().is_empty());
    assert_eq!(state(&job).await.lease_owner.as_deref(), Some("holder"));
    cleanup(&job).await
}

#[tokio_shared_rt::test(shared)]
async fn the_runner_rejects_flags_outside_the_hard_limits() -> Result<()> {
    stack().await?;
    let job = ScriptedJob::new("limits", 2, 0);
    for gate in [
        GateConfig {
            rate: 21.0,
            ..fast_gate()
        },
        GateConfig {
            concurrency: 5,
            ..fast_gate()
        },
    ] {
        let result = run_job(
            job.as_ref(),
            RunOptions { gate, ..options() },
            &Recorder::default(),
        )
        .await;
        assert!(result.is_err());
    }
    assert!(
        BackfillRun::get(&job.key).await?.is_none(),
        "a refused configuration claims nothing"
    );
    Ok(())
}
