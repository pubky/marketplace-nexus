use crate::backfill::gate::{GateStop, RequestGate};
use crate::events::handlers::review::{
    rebucket_review, scan_user_reviews, Rebucket, ReviewBackfill, UserScanError,
};
use async_trait::async_trait;
use nexus_common::db::{fetch_all_rows_from_graph, queries};
use nexus_common::models::marketplace::{ReputationSummary, ReviewDetails};
use nexus_common::types::DynError;
use pubky_app_specs::PubkyAppReviewRole;
use tracing::warn;

/// Job id of the trust-bucket backfill.
pub const TRUST_BUCKETS_JOB: &str = "review-trust-buckets";
/// Job id of the review discovery backfill.
pub const DISCOVERY_JOB: &str = "review-discovery";

/// The phases of a job, in order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase {
    /// Walk the work items (reviews or users) in keyset order.
    Fetch,
    /// Recompute every aggregate scope once, database only.
    Aggregates,
}

impl Phase {
    pub fn as_str(self) -> &'static str {
        match self {
            Phase::Fetch => "fetch",
            Phase::Aggregates => "aggregates",
        }
    }

    pub fn parse(phase: &str) -> Option<Phase> {
        match phase {
            "fetch" => Some(Phase::Fetch),
            "aggregates" => Some(Phase::Aggregates),
            _ => None,
        }
    }
}

/// What one finished work item came to.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ItemTally {
    /// Reviews read from a homeserver and ingested.
    pub refetched: i64,
    /// Records that left the homeserver, or directories that are gone.
    pub gone: i64,
    /// Homeservers that could not be reached.
    pub unreachable: i64,
    /// Records read but not ingested.
    pub failed: i64,
    /// Ids of failed items, for the log.
    pub failed_ids: Vec<String>,
}

/// How processing one work item ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ItemResult {
    /// The item is finished; its cursor may move past it.
    Finished(ItemTally),
    /// The gate stopped the run before the item finished. It stays unfinished
    /// and a later run starts it again. `partial` is what was already done
    /// and will not recur on the repeat.
    Stopped { partial: ItemTally, stop: GateStop },
}

/// An aggregate scope to recompute.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AggregateScope {
    Subject {
        subject_id: String,
        role: PubkyAppReviewRole,
    },
    Listing {
        owner_id: String,
        listing_id: String,
    },
}

impl AggregateScope {
    /// The scope's place in the phase's keyset order.
    pub fn key(&self) -> String {
        match self {
            AggregateScope::Subject { subject_id, role } => {
                format!("subject:{subject_id}:{}", role.as_str())
            }
            AggregateScope::Listing {
                owner_id,
                listing_id,
            } => format!("listing:{owner_id}:{listing_id}"),
        }
    }
}

/// A backfill the runner can drive: an ordered list of work items it walks
/// with a persisted cursor, and optionally an aggregates phase.
#[async_trait]
pub trait BackfillJob: Send + Sync {
    /// The key of the job's `BackfillRun` node.
    fn id(&self) -> &str;

    /// Whether the job ends with the aggregates phase.
    fn has_aggregates(&self) -> bool;

    /// The next work item keys strictly after `cursor`, in key order.
    async fn items_after(&self, cursor: &str, limit: usize) -> Result<Vec<String>, DynError>;

    /// How many work items follow `cursor`.
    async fn count_after(&self, cursor: &str) -> Result<u64, DynError>;

    /// The least number of homeserver requests one item costs.
    fn min_requests_per_item(&self) -> u64;

    /// Processes one work item. Every homeserver request goes through `gate`.
    async fn process(&self, key: &str, gate: &dyn RequestGate) -> ItemResult;

    /// Every aggregate scope, in key order.
    async fn aggregate_scopes(&self) -> Result<Vec<AggregateScope>, DynError>;

    /// Recomputes one aggregate scope from the graph.
    async fn recompute(&self, scope: &AggregateScope) -> Result<(), DynError>;
}

/// Gives every verified review without `iat` its attestation time and price
/// weight by re-reading it from the reviewer's homeserver through the normal
/// ingest, then recomputes every aggregate once.
///
/// Work items are keyed `reviewer_id:review_id` and walked in that order. A
/// review that cannot be re-read (gone, unreachable) keeps no `iat`, stays out
/// of the trust buckets, and is not visited again by later runs of the job.
#[derive(Debug, Clone)]
pub struct TrustBucketsJob {
    run_key: String,
    only_reviewers: Option<Vec<String>>,
}

impl Default for TrustBucketsJob {
    fn default() -> Self {
        TrustBucketsJob {
            run_key: TRUST_BUCKETS_JOB.to_string(),
            only_reviewers: None,
        }
    }
}

impl TrustBucketsJob {
    pub fn new() -> Self {
        Self::default()
    }

    /// Keys the job's `BackfillRun` node under `key` instead of the job id, so
    /// two runs of the same job in one database keep separate state. The
    /// command never sets it.
    pub fn run_key(mut self, key: impl Into<String>) -> Self {
        self.run_key = key.into();
        self
    }

    /// Restricts the walk to these reviewers. The aggregates phase still
    /// covers every scope in the graph.
    pub fn only_reviewers(mut self, reviewers: Vec<String>) -> Self {
        self.only_reviewers = Some(reviewers);
        self
    }

    fn filter(&self) -> (bool, Vec<String>) {
        match &self.only_reviewers {
            Some(reviewers) => (true, reviewers.clone()),
            None => (false, Vec::new()),
        }
    }

    fn split(key: &str) -> Option<(&str, &str)> {
        key.split_once(':')
    }
}

#[async_trait]
impl BackfillJob for TrustBucketsJob {
    fn id(&self) -> &str {
        &self.run_key
    }

    fn has_aggregates(&self) -> bool {
        true
    }

    async fn items_after(&self, cursor: &str, limit: usize) -> Result<Vec<String>, DynError> {
        let (after_reviewer, after_review) = Self::split(cursor).unwrap_or(("", ""));
        let (restricted, only) = self.filter();
        let rows = fetch_all_rows_from_graph(queries::get::reviews_without_attestation_time_after(
            after_reviewer,
            after_review,
            limit as i64,
            restricted,
            only,
        ))
        .await?;
        let mut keys = Vec::with_capacity(rows.len());
        for row in rows {
            let reviewer_id: String = row.get("reviewer_id")?;
            let review_id: String = row.get("review_id")?;
            keys.push(format!("{reviewer_id}:{review_id}"));
        }
        Ok(keys)
    }

    async fn count_after(&self, cursor: &str) -> Result<u64, DynError> {
        let (after_reviewer, after_review) = Self::split(cursor).unwrap_or(("", ""));
        let (restricted, only) = self.filter();
        let rows =
            fetch_all_rows_from_graph(queries::get::count_reviews_without_attestation_time_after(
                after_reviewer,
                after_review,
                restricted,
                only,
            ))
            .await?;
        let total: i64 = rows
            .first()
            .map(|row| row.get("total"))
            .transpose()?
            .unwrap_or(0);
        Ok(total.max(0) as u64)
    }

    fn min_requests_per_item(&self) -> u64 {
        1
    }

    async fn process(&self, key: &str, gate: &dyn RequestGate) -> ItemResult {
        let Some((reviewer_id, review_id)) = Self::split(key) else {
            return failed_item(key, "malformed work item key");
        };
        match rebucket_review(gate, reviewer_id, review_id).await {
            Err(stop) => ItemResult::Stopped {
                partial: ItemTally::default(),
                stop,
            },
            Ok(Rebucket::Refetched) => ItemResult::Finished(ItemTally {
                refetched: 1,
                ..Default::default()
            }),
            Ok(Rebucket::Gone) => ItemResult::Finished(ItemTally {
                gone: 1,
                ..Default::default()
            }),
            Ok(Rebucket::Unreachable(e)) => {
                warn!(
                    "Review {key} is unreachable; it stays out of the trust buckets: {:?}",
                    e
                );
                ItemResult::Finished(ItemTally {
                    unreachable: 1,
                    ..Default::default()
                })
            }
            Ok(Rebucket::Invalid(e)) | Ok(Rebucket::Failed(e)) => {
                failed_item(key, &format!("{e:?}"))
            }
        }
    }

    async fn aggregate_scopes(&self) -> Result<Vec<AggregateScope>, DynError> {
        let (subjects, listings) = ReviewDetails::aggregate_scopes().await?;
        let mut scopes: Vec<AggregateScope> = subjects
            .into_iter()
            .map(|(subject_id, role)| AggregateScope::Subject { subject_id, role })
            .chain(
                listings
                    .into_iter()
                    .map(|(owner_id, listing_id)| AggregateScope::Listing {
                        owner_id,
                        listing_id,
                    }),
            )
            .collect();
        scopes.sort_by_key(AggregateScope::key);
        Ok(scopes)
    }

    async fn recompute(&self, scope: &AggregateScope) -> Result<(), DynError> {
        match scope {
            AggregateScope::Subject { subject_id, role } => {
                ReputationSummary::recompute_subject(subject_id, *role).await?;
            }
            AggregateScope::Listing {
                owner_id,
                listing_id,
            } => {
                ReputationSummary::recompute_listing(owner_id, listing_id).await?;
            }
        }
        Ok(())
    }
}

/// Finds reviews the events feed never delivered (published before the
/// watcher's replay cursor, or rejected by an older index): for each indexed
/// user it LISTs the reviews directory on their homeserver and ingests the
/// records the graph lacks, through the normal ingest.
///
/// Work items are user ids in id order. A user is one item: finished once the
/// whole directory was walked, started again from the top by a later run if
/// the run stopped inside it (reviews already ingested drop out).
#[derive(Debug, Clone)]
pub struct DiscoveryJob {
    run_key: String,
    only_users: Option<Vec<String>>,
}

impl Default for DiscoveryJob {
    fn default() -> Self {
        DiscoveryJob {
            run_key: DISCOVERY_JOB.to_string(),
            only_users: None,
        }
    }
}

impl DiscoveryJob {
    pub fn new() -> Self {
        Self::default()
    }

    /// Keys the job's `BackfillRun` node under `key` instead of the job id.
    /// The command never sets it.
    pub fn run_key(mut self, key: impl Into<String>) -> Self {
        self.run_key = key.into();
        self
    }

    /// Restricts the walk to these users.
    pub fn only_users(mut self, users: Vec<String>) -> Self {
        self.only_users = Some(users);
        self
    }

    fn filter(&self) -> (bool, Vec<String>) {
        match &self.only_users {
            Some(users) => (true, users.clone()),
            None => (false, Vec::new()),
        }
    }
}

#[async_trait]
impl BackfillJob for DiscoveryJob {
    fn id(&self) -> &str {
        &self.run_key
    }

    fn has_aggregates(&self) -> bool {
        false
    }

    async fn items_after(&self, cursor: &str, limit: usize) -> Result<Vec<String>, DynError> {
        let (restricted, only) = self.filter();
        let rows = fetch_all_rows_from_graph(queries::get::user_ids_after(
            cursor,
            limit as i64,
            restricted,
            only,
        ))
        .await?;
        let mut ids = Vec::with_capacity(rows.len());
        for row in rows {
            ids.push(row.get("id")?);
        }
        Ok(ids)
    }

    async fn count_after(&self, cursor: &str) -> Result<u64, DynError> {
        let (restricted, only) = self.filter();
        let rows =
            fetch_all_rows_from_graph(queries::get::count_user_ids_after(cursor, restricted, only))
                .await?;
        let total: i64 = rows
            .first()
            .map(|row| row.get("total"))
            .transpose()?
            .unwrap_or(0);
        Ok(total.max(0) as u64)
    }

    fn min_requests_per_item(&self) -> u64 {
        1
    }

    async fn process(&self, key: &str, gate: &dyn RequestGate) -> ItemResult {
        let mut summary = ReviewBackfill::default();
        match scan_user_reviews(gate, key, &mut summary).await {
            Ok(()) => ItemResult::Finished(ItemTally {
                refetched: summary.indexed as i64,
                gone: summary.gone as i64,
                unreachable: summary.unreachable as i64,
                failed: summary.failed as i64,
                failed_ids: Vec::new(),
            }),
            Err(UserScanError::Stopped(stop)) => ItemResult::Stopped {
                partial: ItemTally {
                    refetched: summary.indexed as i64,
                    ..Default::default()
                },
                stop,
            },
            Err(UserScanError::Failed(e)) => failed_item(key, &format!("{e:?}")),
        }
    }

    async fn aggregate_scopes(&self) -> Result<Vec<AggregateScope>, DynError> {
        Ok(Vec::new())
    }

    async fn recompute(&self, _scope: &AggregateScope) -> Result<(), DynError> {
        Ok(())
    }
}

fn failed_item(key: &str, why: &str) -> ItemResult {
    warn!("Backfill item {key} failed: {why}");
    ItemResult::Finished(ItemTally {
        failed: 1,
        failed_ids: vec![key.to_string()],
        ..Default::default()
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn phases_round_trip() {
        for phase in [Phase::Fetch, Phase::Aggregates] {
            assert_eq!(Phase::parse(phase.as_str()), Some(phase));
        }
        assert_eq!(Phase::parse("done"), None);
    }

    #[test]
    fn scope_keys_order_subjects_and_listings_stably() {
        let mut keys = [
            AggregateScope::Subject {
                subject_id: "b".into(),
                role: PubkyAppReviewRole::BuyerReviewingSeller,
            }
            .key(),
            AggregateScope::Listing {
                owner_id: "z".into(),
                listing_id: "1".into(),
            }
            .key(),
            AggregateScope::Subject {
                subject_id: "a".into(),
                role: PubkyAppReviewRole::BuyerReviewingSeller,
            }
            .key(),
        ];
        keys.sort();
        assert!(keys[0].starts_with("listing:"));
        assert!(keys[1] < keys[2]);
    }

    #[test]
    fn the_trust_bucket_key_splits_on_the_first_colon() {
        assert_eq!(
            TrustBucketsJob::split("reviewer:review"),
            Some(("reviewer", "review"))
        );
        assert_eq!(TrustBucketsJob::split("nocolon"), None);
    }
}
