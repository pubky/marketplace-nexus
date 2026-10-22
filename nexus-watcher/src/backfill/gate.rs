use async_trait::async_trait;
use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;
use tokio::sync::{watch, OwnedSemaphorePermit, Semaphore};
use tokio::time::Instant;

/// Hard ceilings on the operator's throughput flags.
pub const MAX_RATE: f64 = 20.0;
pub const MAX_CONCURRENCY: usize = 4;

/// Why a gate stopped handing out permits. A stopped gate stays stopped: the
/// run ends, its cursor is kept, and a later run resumes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GateStop {
    /// `--max-requests` requests were sent.
    RequestBudget,
    /// `--max-duration` elapsed.
    DurationBudget,
    /// `--max-consecutive-errors` requests in a row failed.
    Breaker { consecutive_errors: u32 },
    /// The operator interrupted the run.
    Interrupted,
    /// Another runner owns the job's lease now.
    LeaseLost,
}

impl GateStop {
    /// Budgets and an interrupt end a run `paused`; the breaker and a lost
    /// lease do not.
    pub fn is_clean(&self) -> bool {
        matches!(
            self,
            GateStop::RequestBudget | GateStop::DurationBudget | GateStop::Interrupted
        )
    }

    /// The reason stored on the job and printed in the summary.
    pub fn reason(&self) -> String {
        match self {
            GateStop::RequestBudget => "request budget reached (--max-requests)".to_string(),
            GateStop::DurationBudget => "time budget reached (--max-duration)".to_string(),
            GateStop::Breaker { consecutive_errors } => format!(
                "circuit breaker: {consecutive_errors} consecutive homeserver requests failed"
            ),
            GateStop::Interrupted => "interrupted".to_string(),
            GateStop::LeaseLost => "lease lost to another runner".to_string(),
        }
    }
}

impl std::fmt::Display for GateStop {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.reason())
    }
}

impl std::error::Error for GateStop {}

/// Front door of every homeserver request: a request waits for a permit, then
/// reports how it went through the permit. The unthrottled gate (the existing
/// migrations) hands permits out at once.
#[async_trait]
pub trait RequestGate: Send + Sync {
    async fn acquire(&self) -> Result<RequestPermit, GateStop>;
}

/// A request in flight. Report the outcome with [`RequestPermit::succeeded`]
/// or [`RequestPermit::failed`]; dropping it unreported (the request was
/// cancelled) frees the slot and changes nothing else.
pub struct RequestPermit {
    _slot: Option<OwnedSemaphorePermit>,
    reporter: Option<Arc<ThrottleState>>,
}

impl RequestPermit {
    /// A permit that meters nothing.
    pub fn unmetered() -> Self {
        RequestPermit {
            _slot: None,
            reporter: None,
        }
    }

    /// The homeserver answered, including a definitive 404. Resets the error
    /// streak and the backoff.
    pub fn succeeded(self) {
        if let Some(state) = &self.reporter {
            state.record_success();
        }
    }

    /// The request failed (transport error, unresolvable homeserver, rate
    /// limit, 5xx). Backs the next request off and counts toward the breaker.
    pub fn failed(self) {
        if let Some(state) = &self.reporter {
            state.record_failure();
        }
    }
}

/// No limit: the behaviour the registered migrations always had.
pub struct Unthrottled;

#[async_trait]
impl RequestGate for Unthrottled {
    async fn acquire(&self) -> Result<RequestPermit, GateStop> {
        Ok(RequestPermit::unmetered())
    }
}

/// Wraps a gate and counts the permits it issued, so one item's requests can
/// be attributed to it while the gate is shared.
pub struct CountingGate<'a> {
    inner: &'a dyn RequestGate,
    count: AtomicU64,
}

impl<'a> CountingGate<'a> {
    pub fn new(inner: &'a dyn RequestGate) -> Self {
        CountingGate {
            inner,
            count: AtomicU64::new(0),
        }
    }

    pub fn requests(&self) -> u64 {
        self.count.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl RequestGate for CountingGate<'_> {
    async fn acquire(&self) -> Result<RequestPermit, GateStop> {
        let permit = self.inner.acquire().await?;
        self.count.fetch_add(1, Ordering::SeqCst);
        Ok(permit)
    }
}

/// A source of jitter in `[0, 1)`.
pub trait Jitter: Send + Sync {
    fn unit(&self) -> f64;
}

/// Jitter from the standard library's randomly seeded hasher.
pub struct RandomJitter;

impl Jitter for RandomJitter {
    fn unit(&self) -> f64 {
        let bits = RandomState::new().build_hasher().finish() >> 11;
        bits as f64 / (1u64 << 53) as f64
    }
}

/// No jitter: the schedule is exact.
pub struct NoJitter;

impl Jitter for NoJitter {
    fn unit(&self) -> f64 {
        0.0
    }
}

#[derive(Debug, Clone)]
pub struct GateConfig {
    /// Requests per second across the whole run.
    pub rate: f64,
    /// Requests in flight at once.
    pub concurrency: usize,
    /// Stop after this many requests.
    pub max_requests: Option<u64>,
    /// Stop after this long.
    pub max_duration: Option<Duration>,
    /// Trip the breaker after this many failed requests in a row.
    pub max_consecutive_errors: u32,
    /// First backoff after a failure; doubles per consecutive failure.
    pub backoff_initial: Duration,
    /// Backoff ceiling.
    pub backoff_max: Duration,
    /// Fraction (0 to 1) of the spacing and the backoff added at random, so
    /// concurrent runners and retries do not move in step. Jitter only ever
    /// lengthens a wait, which keeps the rate cap strict.
    pub jitter: f64,
}

impl Default for GateConfig {
    fn default() -> Self {
        GateConfig {
            rate: 2.0,
            concurrency: 1,
            max_requests: None,
            max_duration: None,
            max_consecutive_errors: 20,
            backoff_initial: Duration::from_secs(1),
            backoff_max: Duration::from_secs(60),
            jitter: 0.2,
        }
    }
}

impl GateConfig {
    /// Rejects flags outside the hard limits.
    pub fn validate(&self) -> Result<(), String> {
        if !(self.rate > 0.0 && self.rate <= MAX_RATE) {
            return Err(format!(
                "--rate must be above 0 and at most {MAX_RATE} requests per second, got {}",
                self.rate
            ));
        }
        if self.concurrency == 0 || self.concurrency > MAX_CONCURRENCY {
            return Err(format!(
                "--concurrency must be between 1 and {MAX_CONCURRENCY}, got {}",
                self.concurrency
            ));
        }
        if self.max_consecutive_errors == 0 {
            return Err("--max-consecutive-errors must be at least 1".to_string());
        }
        if !(0.0..=1.0).contains(&self.jitter) {
            return Err("jitter must be between 0 and 1".to_string());
        }
        Ok(())
    }

    /// The wait after the `consecutive`-th failure in a row, before jitter.
    pub fn backoff_after(&self, consecutive: u32) -> Duration {
        let doublings = consecutive.saturating_sub(1).min(32);
        self.backoff_initial
            .saturating_mul(1u32.checked_shl(doublings).unwrap_or(u32::MAX))
            .min(self.backoff_max)
    }
}

struct Schedule {
    /// The earliest the next request may start under the rate.
    next_slot: Instant,
    /// The earliest any request may start after a failure.
    not_before: Instant,
    requests: u64,
    consecutive_errors: u32,
}

struct ThrottleState {
    config: GateConfig,
    jitter: Box<dyn Jitter>,
    schedule: Mutex<Schedule>,
    stop: watch::Sender<Option<GateStop>>,
}

impl ThrottleState {
    fn record_success(&self) {
        self.lock().consecutive_errors = 0;
    }

    fn record_failure(&self) {
        let mut schedule = self.lock();
        schedule.consecutive_errors += 1;
        let consecutive = schedule.consecutive_errors;
        let backoff = self
            .config
            .backoff_after(consecutive)
            .mul_f64(1.0 + self.config.jitter * self.jitter.unit())
            .min(self.config.backoff_max);
        schedule.not_before = schedule.not_before.max(Instant::now() + backoff);
        let tripped = consecutive >= self.config.max_consecutive_errors;
        drop(schedule);
        if tripped {
            self.stop(GateStop::Breaker {
                consecutive_errors: consecutive,
            });
        }
    }

    fn stop(&self, reason: GateStop) {
        self.stop.send_if_modified(|current| {
            if current.is_none() {
                *current = Some(reason);
                true
            } else {
                false
            }
        });
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Schedule> {
        self.schedule.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// A token bucket with no burst plus a concurrency cap, jittered backoff and a
/// consecutive-error breaker, in front of every homeserver request.
///
/// Requests start at least `1 / rate` seconds apart (plus jitter), so no
/// one-second window holds more than `rate` of them, and at most
/// `concurrency` are in flight. After a failed request every request waits a
/// backoff that doubles per consecutive failure up to a ceiling and resets on
/// the first success. `max_consecutive_errors` failures in a row stop the
/// gate for good, as do the request and time budgets and [`ThrottledGate::stop`].
pub struct ThrottledGate {
    state: Arc<ThrottleState>,
    slots: Arc<Semaphore>,
    deadline: Option<Instant>,
}

impl ThrottledGate {
    pub fn new(config: GateConfig) -> Result<Self, String> {
        Self::with_jitter(config, Box::new(RandomJitter))
    }

    pub fn with_jitter(config: GateConfig, jitter: Box<dyn Jitter>) -> Result<Self, String> {
        config.validate()?;
        let now = Instant::now();
        let deadline = config.max_duration.map(|duration| now + duration);
        let slots = Arc::new(Semaphore::new(config.concurrency));
        let (stop, _) = watch::channel(None);
        Ok(ThrottledGate {
            state: Arc::new(ThrottleState {
                config,
                jitter,
                schedule: Mutex::new(Schedule {
                    next_slot: now,
                    not_before: now,
                    requests: 0,
                    consecutive_errors: 0,
                }),
                stop,
            }),
            slots,
            deadline,
        })
    }

    /// Stops the gate: every waiting and later request gets `reason`. The
    /// first reason wins.
    pub fn stop(&self, reason: GateStop) {
        self.state.stop(reason);
    }

    /// Why the gate stopped, if it did.
    pub fn stopped(&self) -> Option<GateStop> {
        self.state.stop.borrow().clone()
    }

    /// Permits issued so far.
    pub fn requests(&self) -> u64 {
        self.state.lock().requests
    }

    /// Whether the time budget has run out.
    pub fn out_of_time(&self) -> bool {
        self.deadline
            .is_some_and(|deadline| Instant::now() >= deadline)
    }

    fn stop_reason(&self) -> Option<GateStop> {
        self.state.stop.borrow().clone()
    }
}

#[async_trait]
impl RequestGate for ThrottledGate {
    async fn acquire(&self) -> Result<RequestPermit, GateStop> {
        let mut stopped = self.state.stop.subscribe();
        if let Some(reason) = self.stop_reason() {
            return Err(reason);
        }
        let slot = tokio::select! {
            slot = self.slots.clone().acquire_owned() => {
                slot.expect("the gate's semaphore is never closed")
            }
            seen = stopped.wait_for(Option::is_some) => {
                drop(seen);
                return Err(self.stop_reason().unwrap_or(GateStop::Interrupted));
            }
        };

        let start = {
            let mut schedule = self.state.lock();
            if let Some(reason) = self.stop_reason() {
                return Err(reason);
            }
            if self
                .state
                .config
                .max_requests
                .is_some_and(|max| schedule.requests >= max)
            {
                self.stop(GateStop::RequestBudget);
                return Err(GateStop::RequestBudget);
            }
            let now = Instant::now();
            let start = now.max(schedule.next_slot).max(schedule.not_before);
            if self.deadline.is_some_and(|deadline| start >= deadline) {
                self.stop(GateStop::DurationBudget);
                return Err(GateStop::DurationBudget);
            }
            let spacing = Duration::from_secs_f64(1.0 / self.state.config.rate)
                .mul_f64(1.0 + self.state.config.jitter * self.state.jitter.unit());
            schedule.next_slot = start + spacing;
            schedule.requests += 1;
            start
        };

        tokio::select! {
            _ = tokio::time::sleep_until(start) => {}
            seen = stopped.wait_for(Option::is_some) => {
                drop(seen);
                self.state.lock().requests -= 1;
                return Err(self.stop_reason().unwrap_or(GateStop::Interrupted));
            }
        }
        Ok(RequestPermit {
            _slot: Some(slot),
            reporter: Some(self.state.clone()),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    fn gate(config: GateConfig) -> Arc<ThrottledGate> {
        Arc::new(ThrottledGate::with_jitter(config, Box::new(NoJitter)).unwrap())
    }

    fn config(rate: f64, concurrency: usize) -> GateConfig {
        GateConfig {
            rate,
            concurrency,
            ..GateConfig::default()
        }
    }

    /// Starts `total` requests across `workers` tasks, each holding its
    /// permit for `hold`, and returns when each started.
    async fn drive(
        gate: Arc<ThrottledGate>,
        workers: usize,
        total: usize,
        hold: Duration,
    ) -> (Vec<Instant>, usize) {
        let issued = Arc::new(AtomicUsize::new(0));
        let in_flight = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let starts = Arc::new(Mutex::new(Vec::new()));
        let mut tasks = Vec::new();
        for _ in 0..workers {
            let (gate, issued, in_flight, peak, starts) = (
                gate.clone(),
                issued.clone(),
                in_flight.clone(),
                peak.clone(),
                starts.clone(),
            );
            tasks.push(tokio::spawn(async move {
                while issued.fetch_add(1, Ordering::SeqCst) < total {
                    let Ok(permit) = gate.acquire().await else {
                        return;
                    };
                    starts.lock().unwrap().push(Instant::now());
                    let now = in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                    peak.fetch_max(now, Ordering::SeqCst);
                    tokio::time::sleep(hold).await;
                    in_flight.fetch_sub(1, Ordering::SeqCst);
                    permit.succeeded();
                }
            }));
        }
        for task in tasks {
            task.await.unwrap();
        }
        let starts = starts.lock().unwrap().clone();
        (starts, peak.load(Ordering::SeqCst))
    }

    fn most_in_any_second(starts: &[Instant]) -> usize {
        starts
            .iter()
            .map(|&from| {
                starts
                    .iter()
                    .filter(|&&at| at >= from && at < from + Duration::from_secs(1))
                    .count()
            })
            .max()
            .unwrap_or(0)
    }

    #[tokio::test(start_paused = true)]
    async fn never_more_than_the_rate_starts_in_any_second() {
        for rate in [1.0, 2.0, 5.0, 20.0] {
            let (starts, _) = drive(
                gate(config(rate, 4)),
                4,
                (rate * 6.0) as usize,
                Duration::ZERO,
            )
            .await;
            assert_eq!(starts.len(), (rate * 6.0) as usize);
            assert!(
                most_in_any_second(&starts) <= rate as usize,
                "rate {rate}: {} requests started inside one second",
                most_in_any_second(&starts)
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn the_rate_is_held_with_jitter_too() {
        let jittered = Arc::new(ThrottledGate::new(config(5.0, 4)).unwrap());
        let (starts, _) = drive(jittered, 4, 40, Duration::ZERO).await;
        assert!(most_in_any_second(&starts) <= 5);
    }

    #[tokio::test(start_paused = true)]
    async fn requests_are_spaced_by_the_reciprocal_of_the_rate() {
        let (starts, _) = drive(gate(config(2.0, 1)), 1, 5, Duration::ZERO).await;
        for pair in starts.windows(2) {
            assert_eq!(pair[1] - pair[0], Duration::from_millis(500));
        }
    }

    #[tokio::test(start_paused = true)]
    async fn concurrency_never_exceeds_the_cap() {
        for cap in 1..=MAX_CONCURRENCY {
            let (starts, peak) =
                drive(gate(config(MAX_RATE, cap)), 8, 32, Duration::from_secs(3)).await;
            assert_eq!(starts.len(), 32);
            assert_eq!(peak, cap, "peak in flight with a cap of {cap}");
        }
    }

    #[tokio::test(start_paused = true)]
    async fn backoff_doubles_to_the_ceiling_and_resets_on_success() {
        let mut cfg = config(MAX_RATE, 1);
        cfg.max_consecutive_errors = 100;
        let gate = gate(cfg);
        let mut waits = Vec::new();
        for _ in 0..9 {
            let before = Instant::now();
            let permit = gate.acquire().await.unwrap();
            waits.push(Instant::now() - before);
            permit.failed();
        }
        let seconds: Vec<u64> = waits.iter().map(|wait| wait.as_secs()).collect();
        // The first request is immediate; each failure then delays the next
        // by 1, 2, 4, ... seconds, capped at 60.
        assert_eq!(seconds, vec![0, 1, 2, 4, 8, 16, 32, 60, 60]);

        let before = Instant::now();
        let permit = gate.acquire().await.unwrap();
        assert_eq!((Instant::now() - before).as_secs(), 60);
        permit.succeeded();
        let permit = gate.acquire().await.unwrap();
        permit.failed();
        let before = Instant::now();
        let _ = gate.acquire().await.unwrap();
        assert_eq!(
            (Instant::now() - before).as_secs(),
            1,
            "a success starts the schedule over"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn backoff_jitter_only_lengthens_the_wait() {
        let gate = ThrottledGate::new(GateConfig {
            rate: MAX_RATE,
            ..GateConfig::default()
        })
        .unwrap();
        for _ in 0..10 {
            let permit = gate.acquire().await.unwrap();
            let before = Instant::now();
            permit.failed();
            let permit = gate.acquire().await.unwrap();
            let waited = Instant::now() - before;
            assert!(waited >= Duration::from_secs(1) && waited <= Duration::from_millis(1200));
            permit.succeeded();
        }
    }

    #[tokio::test(start_paused = true)]
    async fn the_breaker_trips_at_the_threshold_and_stays_tripped() {
        let mut cfg = config(MAX_RATE, 1);
        cfg.max_consecutive_errors = 3;
        let gate = gate(cfg);
        for _ in 0..2 {
            gate.acquire().await.unwrap().failed();
        }
        assert_eq!(gate.stopped(), None);
        gate.acquire().await.unwrap().failed();
        let tripped = GateStop::Breaker {
            consecutive_errors: 3,
        };
        assert_eq!(gate.stopped(), Some(tripped.clone()));
        assert!(matches!(gate.acquire().await, Err(stop) if stop == tripped));
        assert!(!tripped.is_clean());
    }

    #[tokio::test(start_paused = true)]
    async fn a_success_between_failures_keeps_the_breaker_closed() {
        let mut cfg = config(MAX_RATE, 1);
        cfg.max_consecutive_errors = 3;
        let gate = gate(cfg);
        for _ in 0..10 {
            gate.acquire().await.unwrap().failed();
            gate.acquire().await.unwrap().failed();
            gate.acquire().await.unwrap().succeeded();
        }
        assert_eq!(gate.stopped(), None);
    }

    #[tokio::test(start_paused = true)]
    async fn the_request_budget_stops_the_gate_cleanly() {
        let mut cfg = config(MAX_RATE, 1);
        cfg.max_requests = Some(3);
        let gate = gate(cfg);
        for _ in 0..3 {
            gate.acquire().await.unwrap().succeeded();
        }
        assert!(matches!(gate.acquire().await, Err(GateStop::RequestBudget)));
        assert_eq!(gate.requests(), 3);
        assert!(GateStop::RequestBudget.is_clean());
    }

    #[tokio::test(start_paused = true)]
    async fn the_time_budget_stops_the_gate_without_waiting_it_out() {
        let mut cfg = config(1.0, 1);
        cfg.max_duration = Some(Duration::from_millis(2500));
        let gate = gate(cfg);
        let begun = Instant::now();
        let mut sent = 0;
        while gate
            .acquire()
            .await
            .map(|permit| permit.succeeded())
            .is_ok()
        {
            sent += 1;
        }
        assert_eq!(sent, 3, "requests start at 0s, 1s and 2s");
        assert!(Instant::now() - begun < Duration::from_millis(2500));
        assert_eq!(gate.stopped(), Some(GateStop::DurationBudget));
    }

    #[tokio::test(start_paused = true)]
    async fn stopping_wakes_a_waiting_request() {
        let gate = gate(config(0.5, 1));
        gate.acquire().await.unwrap().succeeded();
        let waiter = {
            let gate = gate.clone();
            tokio::spawn(async move { gate.acquire().await.map(|_| ()) })
        };
        tokio::task::yield_now().await;
        gate.stop(GateStop::Interrupted);
        assert_eq!(waiter.await.unwrap(), Err(GateStop::Interrupted));
        assert_eq!(gate.requests(), 1, "a cancelled wait is not a request");
    }

    #[tokio::test(start_paused = true)]
    async fn the_counting_gate_counts_only_issued_permits() {
        let mut cfg = config(MAX_RATE, 1);
        cfg.max_requests = Some(2);
        let gate = gate(cfg);
        let counting = CountingGate::new(gate.as_ref());
        counting.acquire().await.unwrap().succeeded();
        counting.acquire().await.unwrap().succeeded();
        assert!(counting.acquire().await.is_err());
        assert_eq!(counting.requests(), 2);
    }

    #[test]
    fn the_flags_have_hard_limits() {
        assert!(config(MAX_RATE, MAX_CONCURRENCY).validate().is_ok());
        assert!(config(MAX_RATE + 0.5, 1).validate().is_err());
        assert!(config(0.0, 1).validate().is_err());
        assert!(config(2.0, MAX_CONCURRENCY + 1).validate().is_err());
        assert!(config(2.0, 0).validate().is_err());
        let mut cfg = config(2.0, 1);
        cfg.max_consecutive_errors = 0;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn the_backoff_schedule_is_one_second_doubling_to_sixty() {
        let cfg = GateConfig::default();
        let schedule: Vec<u64> = (1..=9).map(|n| cfg.backoff_after(n).as_secs()).collect();
        assert_eq!(schedule, vec![1, 2, 4, 8, 16, 32, 60, 60, 60]);
        assert_eq!(cfg.backoff_after(1_000).as_secs(), 60);
    }

    #[test]
    fn random_jitter_stays_inside_the_unit_interval() {
        for _ in 0..1000 {
            let unit = RandomJitter.unit();
            assert!((0.0..1.0).contains(&unit));
        }
    }
}
