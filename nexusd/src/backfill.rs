use crate::cli::{ReviewBackfillArgs, ReviewJob};
use nexus_common::types::DynError;
use nexus_watcher::backfill::gate::GateConfig;
use nexus_watcher::backfill::jobs::{BackfillJob, DiscoveryJob, TrustBucketsJob};
use nexus_watcher::backfill::run::{dry_run, run_job, LogProgress, RunOptions, StopHandle};
use std::time::Duration;
use tracing::warn;

/// The runner options the flags ask for.
pub fn run_options(args: &ReviewBackfillArgs, stop: StopHandle) -> RunOptions {
    RunOptions {
        gate: GateConfig {
            rate: args.rate,
            concurrency: args.concurrency as usize,
            max_requests: args.max_requests,
            max_duration: args.max_duration,
            max_consecutive_errors: args.max_consecutive_errors,
            ..GateConfig::default()
        },
        aggregate_pause: Duration::from_millis(args.aggregate_pause_ms),
        force_rerun: args.force_rerun,
        stop,
        ..RunOptions::default()
    }
}

fn job_for(job: ReviewJob) -> Box<dyn BackfillJob> {
    match job {
        ReviewJob::TrustBuckets => Box::new(TrustBucketsJob::new()),
        ReviewJob::Discovery => Box::new(DiscoveryJob::new()),
    }
}

/// `nexusd db backfill reviews`: prints the summary and returns the exit code
/// (0 done or paused, 1 refused, 2 aborted, 3 done with failed records).
pub async fn backfill_reviews_command(args: ReviewBackfillArgs) -> Result<u8, DynError> {
    let job = job_for(args.job);
    let stop = StopHandle::default();
    let options = run_options(&args, stop.clone());

    if args.dry_run {
        let report = dry_run(job.as_ref(), &options).await?;
        println!("{}", report.render(args.rate));
        return Ok(0);
    }

    let interrupt = stop.clone();
    tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            warn!("Interrupted: finishing the requests in flight, then pausing (interrupt again to exit now)");
            interrupt.stop();
            if tokio::signal::ctrl_c().await.is_ok() {
                std::process::exit(130);
            }
        }
    });

    let sink = LogProgress {
        job: job.id().to_string(),
        every: 25,
    };
    let report = run_job(job.as_ref(), options, &sink).await?;
    println!("{}", report.render());
    Ok(report.exit_code())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::{BackfillCommands, Cli, DbCommands, NexusCommands};
    use clap::Parser;

    fn args(flags: &[&str]) -> ReviewBackfillArgs {
        let cli = Cli::parse_from(
            [
                "nexusd",
                "db",
                "backfill",
                "reviews",
                "--job",
                "review-trust-buckets",
            ]
            .iter()
            .chain(flags),
        );
        match cli.command {
            Some(NexusCommands::Db(DbCommands::Backfill(BackfillCommands::Reviews(args)))) => args,
            other => panic!("not a review backfill: {other:?}"),
        }
    }

    #[test]
    fn the_flags_reach_the_gate_and_the_runner() {
        let options = run_options(
            &args(&[
                "--rate",
                "5",
                "--concurrency",
                "3",
                "--max-requests",
                "10",
                "--max-duration",
                "1m",
                "--max-consecutive-errors",
                "7",
                "--aggregate-pause-ms",
                "200",
                "--force-rerun",
            ]),
            StopHandle::default(),
        );
        assert_eq!(options.gate.rate, 5.0);
        assert_eq!(options.gate.concurrency, 3);
        assert_eq!(options.gate.max_requests, Some(10));
        assert_eq!(options.gate.max_duration, Some(Duration::from_secs(60)));
        assert_eq!(options.gate.max_consecutive_errors, 7);
        assert_eq!(options.aggregate_pause, Duration::from_millis(200));
        assert!(options.force_rerun);
        assert!(options.gate.validate().is_ok());
    }

    #[test]
    fn the_defaults_pass_the_gate_validation() {
        assert!(run_options(&args(&[]), StopHandle::default())
            .gate
            .validate()
            .is_ok());
    }

    #[test]
    fn each_job_has_its_own_id() {
        assert_eq!(
            job_for(ReviewJob::TrustBuckets).id(),
            "review-trust-buckets"
        );
        assert_eq!(job_for(ReviewJob::Discovery).id(), "review-discovery");
    }
}
