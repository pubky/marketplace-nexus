use clap::{Args, Parser, Subcommand, ValueEnum};
use nexus_common::file::{default_config_dir_path, validate_and_expand_path};
use nexus_webapi::mock::MockType;
use std::path::PathBuf;
use std::time::Duration;

#[derive(Parser, Debug)]
#[command(name = "pubky-nexus")]
#[command(about = "Pubky Nexus CLI", long_about = None)]
pub struct Cli {
    /// Directory containing `config.toml`
    #[arg(short, long, default_value_os_t = default_config_dir_path(), value_parser = validate_config_dir_path)]
    pub config_dir: PathBuf,

    #[command(subcommand)]
    pub command: Option<NexusCommands>,
}

impl Cli {
    pub fn receive_command(cli: Cli) -> NexusCommands {
        match cli.command {
            None => NexusCommands::Run {
                config_dir: cli.config_dir,
            },
            Some(command) => command,
        }
    }
}

/// Validate that the data_dir path is a directory.
/// It doesnt need to exist, but if it does, it needs to be a directory.
fn validate_config_dir_path(path: &str) -> Result<PathBuf, String> {
    validate_and_expand_path(PathBuf::from(path)).map_err(|e| e.to_string())
}

#[derive(Subcommand, Debug)]
pub enum NexusCommands {
    /// Run the API service
    Api(ApiArgs),

    /// Run the event watcher
    Watcher(WatcherArgs),

    /// Database operations
    #[command(subcommand)]
    Db(DbCommands),

    /// Run both the API and the Watcher (default when no arguments are given)
    #[command(hide = true)]
    Run {
        /// Path to the configuration file
        #[arg(short, long, default_value_os_t = default_config_dir_path(), value_parser = validate_config_dir_path)]
        config_dir: PathBuf,
    },
}

#[derive(Args, Debug)]
pub struct ApiArgs {
    /// Optional configuration file for the watcher
    #[arg(short, long, default_value_os_t = default_config_dir_path(), value_parser = validate_config_dir_path)]
    pub config_dir: PathBuf,
}

#[derive(Args, Debug)]
pub struct WatcherArgs {
    /// Optional configuration file for the watcher
    #[arg(short, long, default_value_os_t = default_config_dir_path(), value_parser = validate_config_dir_path)]
    pub config_dir: PathBuf,
}

#[derive(Subcommand, Debug)]
pub enum DbCommands {
    /// Clear the databases
    Clear,

    /// Mock the database (optional redis/graph). Usually for tests
    Mock(MockArgs),

    /// Manage database migrations
    #[command(subcommand)]
    Migration(MigrationCommands),

    /// Throttled, resumable, once-only backfills. Never run by `migration run`
    #[command(subcommand)]
    Backfill(BackfillCommands),

    /// Find marketplace listings whose record is gone from the seller's
    /// homeserver (missed DEL events) and optionally delete them
    PruneStaleListings(PruneStaleListingsArgs),
}

#[derive(Args, Debug)]
pub struct PruneStaleListingsArgs {
    /// Delete the stale listings. Without it the command only reports them
    #[arg(long)]
    pub apply: bool,

    /// Abort before deleting anything when more stale listings are found
    #[arg(long, default_value_t = 50)]
    pub max_prune: usize,
}

#[derive(Args, Debug)]
pub struct MockArgs {
    /// Specify which part of the database to mock: redis, graph, or both (default: both)
    #[arg(long)]
    pub mock_type: Option<MockType>,
}

#[derive(Subcommand, Debug)]
pub enum MigrationCommands {
    /// Create a new migration with a required migration name
    New(MigrationNewArgs),

    /// Run pending migrations
    Run,
}

#[derive(Args, Debug)]
pub struct MigrationNewArgs {
    /// The name of the new migration
    #[arg(required = true)]
    pub name: String,
}

#[derive(Subcommand, Debug)]
pub enum BackfillCommands {
    /// Backfill marketplace reviews from the reviewers' homeservers, under a
    /// request rate, a concurrency cap, budgets and a circuit breaker
    Reviews(ReviewBackfillArgs),
}

/// The review backfill jobs.
#[derive(ValueEnum, Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReviewJob {
    /// Re-read each verified review without `iat` so it lands in the trust
    /// buckets, then recompute every aggregate
    #[value(name = "review-trust-buckets")]
    TrustBuckets,
    /// List each indexed user's reviews directory and ingest the records the
    /// graph lacks
    #[value(name = "review-discovery")]
    Discovery,
}

#[derive(Args, Debug)]
pub struct ReviewBackfillArgs {
    /// The job to run
    #[arg(long, value_enum)]
    pub job: ReviewJob,

    /// Homeserver requests per second, across the whole run (maximum 20)
    #[arg(long, default_value_t = 2.0, value_parser = parse_rate)]
    pub rate: f64,

    /// Requests in flight at once (maximum 4)
    #[arg(long, default_value_t = 1, value_parser = clap::value_parser!(u32).range(1..=4))]
    pub concurrency: u32,

    /// Stop cleanly (status paused, exit 0) after this many requests
    #[arg(long)]
    pub max_requests: Option<u64>,

    /// Stop cleanly (status paused, exit 0) after this long, e.g. 90s, 45m, 2h
    #[arg(long, value_parser = parse_duration)]
    pub max_duration: Option<Duration>,

    /// Abort (status aborted, nonzero exit, cursor kept) after this many
    /// failed requests in a row
    #[arg(long, default_value_t = 20, value_parser = clap::value_parser!(u32).range(1..))]
    pub max_consecutive_errors: u32,

    /// Pause between aggregate recomputes, in milliseconds
    #[arg(long, default_value_t = 50)]
    pub aggregate_pause_ms: u64,

    /// Print the work left and the time it takes at the chosen rate, and send
    /// no request
    #[arg(long)]
    pub dry_run: bool,

    /// Run a job that is already done again from the start
    #[arg(long)]
    pub force_rerun: bool,
}

fn parse_rate(value: &str) -> Result<f64, String> {
    let rate: f64 = value
        .parse()
        .map_err(|_| format!("`{value}` is not a number"))?;
    if rate > 0.0 && rate <= 20.0 {
        Ok(rate)
    } else {
        Err("the rate must be above 0 and at most 20 requests per second".to_string())
    }
}

/// `90`, `90s`, `45m` or `2h`: a whole number with an optional unit.
fn parse_duration(value: &str) -> Result<Duration, String> {
    let (digits, unit) = match value.char_indices().find(|(_, c)| !c.is_ascii_digit()) {
        Some((at, _)) => value.split_at(at),
        None => (value, "s"),
    };
    let amount: u64 = digits
        .parse()
        .map_err(|_| format!("`{value}` is not a duration like 90s, 45m or 2h"))?;
    let seconds = match unit {
        "s" => amount,
        "m" => amount * 60,
        "h" => amount * 3600,
        _ => return Err(format!("`{value}` is not a duration like 90s, 45m or 2h")),
    };
    if seconds == 0 {
        return Err("the duration must be above zero".to_string());
    }
    Ok(Duration::from_secs(seconds))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(args: &[&str]) -> Result<Cli, clap::Error> {
        Cli::try_parse_from(["nexusd", "db", "backfill", "reviews"].iter().chain(args))
    }

    fn reviews(cli: Cli) -> ReviewBackfillArgs {
        match cli.command {
            Some(NexusCommands::Db(DbCommands::Backfill(BackfillCommands::Reviews(args)))) => args,
            other => panic!("not a review backfill: {other:?}"),
        }
    }

    #[test]
    fn the_defaults_are_two_requests_a_second_one_at_a_time() {
        let args = reviews(parse(&["--job", "review-trust-buckets"]).unwrap());
        assert_eq!(args.job, ReviewJob::TrustBuckets);
        assert_eq!((args.rate, args.concurrency), (2.0, 1));
        assert_eq!(args.max_consecutive_errors, 20);
        assert_eq!((args.max_requests, args.max_duration), (None, None));
        assert!(!args.dry_run && !args.force_rerun);
    }

    #[test]
    fn every_flag_parses() {
        let args = reviews(
            parse(&[
                "--job",
                "review-discovery",
                "--rate",
                "0.5",
                "--concurrency",
                "4",
                "--max-requests",
                "1000",
                "--max-duration",
                "2h",
                "--max-consecutive-errors",
                "5",
                "--aggregate-pause-ms",
                "0",
                "--dry-run",
                "--force-rerun",
            ])
            .unwrap(),
        );
        assert_eq!(args.job, ReviewJob::Discovery);
        assert_eq!((args.rate, args.concurrency), (0.5, 4));
        assert_eq!(args.max_requests, Some(1000));
        assert_eq!(args.max_duration, Some(Duration::from_secs(7200)));
        assert_eq!(args.max_consecutive_errors, 5);
        assert_eq!(args.aggregate_pause_ms, 0);
        assert!(args.dry_run && args.force_rerun);
    }

    #[test]
    fn the_hard_limits_are_refused() {
        let job = ["--job", "review-trust-buckets"];
        for bad in [
            ["--rate", "20.5"],
            ["--rate", "0"],
            ["--rate", "fast"],
            ["--concurrency", "5"],
            ["--concurrency", "0"],
            ["--max-consecutive-errors", "0"],
            ["--max-duration", "soon"],
            ["--max-duration", "0s"],
        ] {
            assert!(
                parse(&[job[0], job[1], bad[0], bad[1]]).is_err(),
                "{bad:?} must be refused"
            );
        }
        assert!(parse(&["--job", "review-everything"]).is_err());
        assert!(parse(&[]).is_err(), "the job is required");
    }

    #[test]
    fn durations_take_seconds_minutes_and_hours() {
        assert_eq!(parse_duration("90"), Ok(Duration::from_secs(90)));
        assert_eq!(parse_duration("90s"), Ok(Duration::from_secs(90)));
        assert_eq!(parse_duration("45m"), Ok(Duration::from_secs(2700)));
        assert_eq!(parse_duration("2h"), Ok(Duration::from_secs(7200)));
        assert!(parse_duration("2d").is_err());
        assert!(parse_duration("").is_err());
    }
}
