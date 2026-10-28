//! Throttled, resumable, once-only backfills run as their own command
//! (`nexusd db backfill`), never as a registered migration.
//!
//! * [`gate`]: the request gate in front of every homeserver request (a token
//!   bucket, a concurrency cap, jittered backoff and a consecutive-error
//!   breaker) and the budgets that end a run cleanly.
//! * [`run`]: the runner. A `BackfillRun` node per job holds the status, the
//!   keyset cursor, the counters and an atomic lease; the runner persists
//!   progress after every item, resumes after a stop and refuses to run a
//!   finished job again.
//! * [`jobs`]: the two review jobs, built on the same fetch, ingest and
//!   recompute code the registered migrations use.

pub mod gate;
pub mod jobs;
pub mod run;
