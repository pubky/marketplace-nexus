use crate::backfill::gate::{GateStop, RequestGate, Unthrottled};
use crate::events::handlers::listing::is_homeserver_not_found;
use crate::events::retry::event::RetryEvent;
use crate::events::EventProcessorError;
use nexus_common::db::reindex::{get_all_user_ids, get_indexed_review_ids};
use nexus_common::db::{OperationOutcome, PubkyConnector};
use nexus_common::models::homeserver::Homeserver;
use nexus_common::models::marketplace::{
    ExcludedReviewer, ModeratedReview, ReputationSummary, ReviewDetails, ReviewModeration,
    ReviewModerationState, ReviewTouch,
};
use nexus_common::models::user::UserDetails;
use nexus_common::types::DynError;
use pubky_app_specs::{
    marketplace_review_uri_builder, PubkyAppMarketplaceReview, PubkyAppObject, PubkyAppReviewRole,
    PubkyId, Resource,
};
use tracing::{debug, info, warn};

/// Indexes a marketplace review record published on the reviewer's
/// homeserver.
///
/// The embedded purchase attestation is verified offline at ingest (parse →
/// Ed25519 signature against the self-certifying `iss` pubky → binding to
/// this exact review). Verification failure never rejects the record: the
/// review is indexed as unverified and labeled (ratified D5). WHO counts as
/// a trusted attestor is client policy — the index stores each verified
/// review's attestor pubky and lets consumers decide display.
///
/// The review is marked as touched before anything is written, so a reader
/// copying records into the index outside the event feed (the throttled
/// backfill) can tell the feed changed the review after its read began.
pub async fn sync_put(
    review: PubkyAppMarketplaceReview,
    user_id: PubkyId,
    review_id: String,
) -> Result<(), EventProcessorError> {
    ReviewTouch::mark(&user_id, &review_id, false).await?;
    index_review(review, user_id, review_id).await
}

/// The ingest [`sync_put`] runs, without marking the review as touched. For
/// callers that fence their own read against the event feed
/// ([`ingest_read_review`]); the feed itself goes through [`sync_put`].
pub async fn index_review(
    review: PubkyAppMarketplaceReview,
    user_id: PubkyId,
    review_id: String,
) -> Result<(), EventProcessorError> {
    debug!("Indexing new marketplace review: {}/{}", user_id, review_id);

    // The record owner must be the homeserver user the event came from;
    // a record claiming another reviewer's identity is rejected outright.
    if review.owner_pubky != user_id.as_ref() {
        return Err(EventProcessorError::generic(format!(
            "Review record owner {} does not match the event homeserver user {}",
            review.owner_pubky, user_id
        )));
    }

    let mut details = ReviewDetails::from_homeserver(&review, &user_id);
    // An edit keeps its supersession state until resolve_order_twins
    // re-evaluates it against the record's previous attested order.
    let prior = ReviewDetails::get_from_index(&user_id, &review_id).await?;
    details.superseded = prior.as_ref().is_some_and(|prior| prior.superseded);
    // Moderation lives in markers keyed by the moderator's tag, so a tag
    // replayed before the review, and every edit, keep the review moderated.
    // This read only keeps the text hidden from the first write on; the
    // locked derivation after the index write below decides what is stored.
    details.moderation = ReviewModeration::get(&user_id, &review_id).await?;

    // SAVE TO GRAPH: requires both the reviewer and the subject users
    let existed = match details.put_to_graph().await? {
        OperationOutcome::CreatedOrDeleted => false,
        OperationOutcome::Updated => true,
        OperationOutcome::MissingDependency => {
            return Err(missing_user_dependencies(&details).await);
        }
    };

    // SAVE TO INDEX: an edit refreshes the details JSON only; the review
    // keeps its original position in the review sorted sets
    details.put_to_index(existed).await?;
    // The locked derivation, which also covers a moderator tag handled while
    // this review was being written.
    if let Some(refreshed) = ReviewDetails::refresh_moderation(&user_id, &review_id).await? {
        details = refreshed;
    }

    // ONE COUNT PER ATTESTED ORDER: a scoped record supersedes its legacy twin
    details.resolve_order_twins(prior.as_ref()).await?;
    // ...and the order's seller fields count on one of its item reviews
    settle_seller_fields(&details, prior.as_ref()).await?;

    // AGGREGATES: recompute the affected scopes from the graph
    recompute_reputation(&details).await?;

    Ok(())
}

pub async fn del(user_id: PubkyId, review_id: String) -> Result<(), EventProcessorError> {
    ReviewTouch::mark(&user_id, &review_id, true).await?;
    unindex_review(user_id, review_id).await
}

/// Removes an indexed review, whatever its homeserver says now. Returns
/// `SkipIndexing` when it is not indexed.
async fn unindex_review(user_id: PubkyId, review_id: String) -> Result<(), EventProcessorError> {
    debug!("Deleting marketplace review: {}/{}", user_id, review_id);

    let Some(details) = ReviewDetails::get_from_index(&user_id, &review_id).await? else {
        return Err(EventProcessorError::SkipIndexing);
    };

    details.delete().await?;
    details.restore_order_twins().await?;
    settle_seller_fields(&details, None).await?;
    recompute_reputation(&details).await?;

    Ok(())
}

/// Moderates a review because the configured moderator tagged its URI: a
/// moderated label hides its text, a review-exclusion label leaves it out of
/// the aggregates. The marker is written first and holds for a review indexed
/// later; nothing is deleted.
#[allow(clippy::too_many_arguments)]
pub async fn moderate(
    reviewer_id: PubkyId,
    review_id: String,
    uri: &str,
    moderator_id: &PubkyId,
    tag_id: &str,
    label: &str,
    state: ReviewModerationState,
) -> Result<(), EventProcessorError> {
    info!(
        "Moderation tag '{label}' on review {reviewer_id}/{review_id}; marking it {}",
        state.as_str()
    );
    ModeratedReview::put(
        &reviewer_id,
        &review_id,
        uri,
        moderator_id,
        tag_id,
        label,
        state,
        chrono::Utc::now().timestamp_millis(),
    )
    .await?;
    refresh_moderation(&reviewer_id, &review_id).await?;
    Ok(())
}

/// Excludes every review a user wrote from the aggregates because the
/// configured moderator tagged the user with a review-exclusion label. The
/// user, their shop and their listings stay; reviews they publish later are
/// excluded too.
pub async fn exclude_reviewer(
    user_id: PubkyId,
    uri: &str,
    moderator_id: &PubkyId,
    tag_id: &str,
    label: &str,
) -> Result<(), EventProcessorError> {
    info!("Moderation tag '{label}' on user {user_id}; excluding their reviews");
    ExcludedReviewer::put(
        &user_id,
        uri,
        moderator_id,
        tag_id,
        label,
        chrono::Utc::now().timestamp_millis(),
    )
    .await?;
    refresh_reviewer_moderation(&user_id).await?;
    Ok(())
}

/// Handles the moderator deleting one of its moderation tags. Returns whether
/// the tag moderated a review or excluded a reviewer.
///
/// The tag's markers are first flagged as being released, which every
/// derivation of a review's moderation skips, then the affected reviews are
/// re-derived, then the markers are removed. A re-derivation running
/// alongside (the one-shot recompute next to a live watcher) therefore never
/// writes the released moderation back: one that read the markers before the
/// flag stores on the edge before the release's re-derivation does, and its
/// details write is rejected as older (see
/// [`ReviewDetails::store_moderation`]). A failure leaves the markers in place
/// so the retried tag deletion starts over.
pub async fn release_moderation(
    moderator_id: &PubkyId,
    tag_id: &str,
) -> Result<bool, EventProcessorError> {
    let reviews = ModeratedReview::reviews_of_tag(moderator_id, tag_id).await?;
    let reviewers = ExcludedReviewer::users_of_tag(moderator_id, tag_id).await?;
    if reviews.is_empty() && reviewers.is_empty() {
        return Ok(false);
    }
    ModeratedReview::mark_releasing(moderator_id, tag_id).await?;
    ExcludedReviewer::mark_releasing(moderator_id, tag_id).await?;
    for (reviewer_id, review_id) in &reviews {
        info!("Moderation tag removed; releasing review {reviewer_id}/{review_id}");
        refresh_moderation(reviewer_id, review_id).await?;
    }
    for reviewer_id in &reviewers {
        info!("Moderation tag removed; counting the reviews of {reviewer_id} again");
        refresh_reviewer_moderation(reviewer_id).await?;
    }
    ModeratedReview::delete(moderator_id, tag_id).await?;
    ExcludedReviewer::delete(moderator_id, tag_id).await?;
    Ok(true)
}

/// Re-derives the moderation of every review the user wrote, after a marker on
/// the user was written or flagged as being released. Returns how many
/// reviews changed.
pub async fn refresh_reviewer_moderation(reviewer_id: &str) -> Result<usize, EventProcessorError> {
    let mut changed = 0;
    for review_id in ReviewDetails::ids_of_reviewer(reviewer_id).await? {
        if refresh_moderation(reviewer_id, &review_id).await? {
            changed += 1;
        }
    }
    Ok(changed)
}

/// One-shot recompute: re-derives the moderation of every indexed review from
/// the markers, so the reviews of users moderated before review moderation
/// existed are hidden, and recomputes the aggregates that changed. Returns how
/// many reviews changed. Idempotent.
pub async fn recompute_all_review_moderation() -> Result<usize, EventProcessorError> {
    let mut changed = 0;
    for reviewer_id in ReviewDetails::reviewer_ids().await? {
        changed += refresh_reviewer_moderation(&reviewer_id).await?;
    }
    Ok(changed)
}

/// Outcome tally of the one-shot per-item recompute.
#[derive(Debug, Default)]
pub struct PerItemRecompute {
    /// Indexed reviews whose order twins were re-evaluated.
    pub reviews: usize,
    /// Attested orders whose counted seller-rating copy was settled.
    pub orders: usize,
    /// Subject and listing aggregates recomputed.
    pub aggregates: usize,
}

/// One-shot recompute for per-item reviews. Re-evaluates every indexed
/// review's order twins with the listing-aware match (a legacy review that an
/// order-scoped review of another item of its order superseded counts
/// again), flags the seller fields of reviews without an attested order,
/// settles each attested order's counted seller-rating copy, then recomputes
/// every subject and listing aggregate. Idempotent.
pub async fn recompute_per_item_counting() -> Result<PerItemRecompute, EventProcessorError> {
    let mut summary = PerItemRecompute::default();
    for reviewer_id in ReviewDetails::reviewer_ids().await? {
        for review_id in ReviewDetails::ids_of_reviewer(&reviewer_id).await? {
            if let Some(details) = ReviewDetails::get_from_index(&reviewer_id, &review_id).await? {
                details.resolve_order_twins(None).await?;
                summary.reviews += 1;
            }
        }
    }
    ReviewDetails::flag_unkeyed_seller_fields().await?;
    for (reviewer_id, attestor_id, order_ref, role) in ReviewDetails::order_keys().await? {
        ReviewDetails::resolve_seller_fields(&reviewer_id, &attestor_id, &order_ref, role).await?;
        summary.orders += 1;
    }
    let (subjects, listings) = ReviewDetails::aggregate_scopes().await?;
    for (subject_id, role) in &subjects {
        ReputationSummary::recompute_subject(subject_id, *role).await?;
    }
    for (listing_owner_id, listing_id) in &listings {
        ReputationSummary::recompute_listing(listing_owner_id, listing_id).await?;
    }
    summary.aggregates = subjects.len() + listings.len();
    Ok(summary)
}

/// Re-derives one review's moderation and, when it changed, its aggregates.
/// Returns whether it changed.
async fn refresh_moderation(
    reviewer_id: &str,
    review_id: &str,
) -> Result<bool, EventProcessorError> {
    match ReviewDetails::refresh_moderation(reviewer_id, review_id).await? {
        Some(details) => {
            settle_seller_fields(&details, None).await?;
            recompute_reputation(&details).await?;
            Ok(true)
        }
        None => Ok(false),
    }
}

/// Settles which item review of the review's attested order carries the
/// order's counted seller fields, and of the order the record had before an
/// edit when that differs. Run after anything that changes whether a copy
/// counts (a write, a deletion, supersession, moderation).
async fn settle_seller_fields(
    details: &ReviewDetails,
    prior: Option<&ReviewDetails>,
) -> Result<(), EventProcessorError> {
    let key = details.order_key();
    if let Some((reviewer_id, attestor_id, order_ref, role)) = key {
        ReviewDetails::resolve_seller_fields(reviewer_id, attestor_id, order_ref, role).await?;
    }
    if let Some(prior_key) = prior.and_then(ReviewDetails::order_key) {
        if Some(prior_key) != key {
            let (reviewer_id, attestor_id, order_ref, role) = prior_key;
            ReviewDetails::resolve_seller_fields(reviewer_id, attestor_id, order_ref, role).await?;
        }
    }
    Ok(())
}

/// Recomputes the subject-scoped aggregate and, for buyer reviews, the
/// listing-scoped aggregate of the review's listing.
pub async fn recompute_reputation(details: &ReviewDetails) -> Result<(), EventProcessorError> {
    ReputationSummary::recompute_subject(&details.subject_id, details.role).await?;
    if details.role == PubkyAppReviewRole::BuyerReviewingSeller {
        ReputationSummary::recompute_listing(&details.listing_owner_id, &details.listing_id)
            .await?;
    }
    Ok(())
}

/// Builds the missing-dependency error for a review whose graph write found
/// no user nodes: reports exactly the users that are absent and nudges the
/// homeserver ingest for the subject (who may live on an unmonitored
/// homeserver, mirroring the tag pipeline).
async fn missing_user_dependencies(details: &ReviewDetails) -> EventProcessorError {
    let mut dependency = Vec::new();
    for candidate in [&details.reviewer_id, &details.subject_id] {
        match UserDetails::get_by_id(candidate).await {
            Ok(Some(_)) => {}
            _ => {
                if candidate == &details.subject_id {
                    if let Err(e) = Homeserver::maybe_ingest_for_user(candidate).await {
                        tracing::error!("Failed to ingest homeserver: {e}");
                    }
                }
                if let Ok(user_id) = PubkyId::try_from(candidate.as_str()) {
                    dependency.push(RetryEvent::generate_index_key_from_uri(&user_id.to_uri()));
                }
            }
        }
    }
    if dependency.is_empty() {
        // The graph reported a missing node but both users resolve now:
        // a benign race; retry against the reviewer to re-run the event.
        if let Ok(user_id) = PubkyId::try_from(details.reviewer_id.as_str()) {
            dependency.push(RetryEvent::generate_index_key_from_uri(&user_id.to_uri()));
        }
    }
    EventProcessorError::missing_dependencies(dependency)
}

/// Outcome tally of one review-backfill pass.
#[derive(Debug, Default)]
pub struct ReviewBackfill {
    /// Reviews discovered on a homeserver and newly indexed.
    pub indexed: usize,
    /// Reviews already present in the index (skipped without a fetch).
    pub already_indexed: usize,
    /// Reviews listed on the homeserver that were gone by the time they were
    /// fetched, plus missing review directories; nothing to index and not a
    /// failure.
    pub gone: usize,
    /// Users whose reviews directory could not be reached. They are logged
    /// and skipped without blocking this one-shot migration.
    pub unreachable: usize,
    /// Reviews that could not be fetched, parsed, or ingested this pass.
    pub failed: usize,
}

/// Discovers and indexes marketplace reviews published BEFORE the deployed
/// watcher's replay cursor, which the events feed will never deliver.
///
/// Unlike the listing-terms backfill (whose candidates exist as incomplete
/// graph rows), a pre-cursor review has NO index row at all, so candidates
/// are discovered from the canonical source: for every indexed user, the
/// reviewer-owned `/pub/pubky.app/marketplace/v1/reviews/` directory is
/// LISTed on the homeserver (public, unauthenticated), already-indexed ids
/// are skipped, and each remaining record is fetched and run through the
/// normal ingest ([`sync_put`]) — offline attestation verification,
/// reputation recompute, and all.
///
/// Failures never abort the pass: they are tallied and the migration re-run
/// retries them (indexed reviews drop out via the already-indexed skip).
pub async fn backfill_unindexed_reviews() -> Result<ReviewBackfill, DynError> {
    let user_ids = get_all_user_ids().await?;
    let total = user_ids.len();
    info!(
        "Review backfill: scanning the reviews directory of {} indexed user(s)",
        total
    );

    // Bounded concurrency: the scan is network-bound (one homeserver LIST
    // per user, pkarr resolution included), and a sequential pass over
    // thousands of users outlives operator sessions. Sixteen in flight
    // keeps the staging homeserver comfortable and the wall time in
    // minutes.
    const CONCURRENCY: usize = 16;
    let mut summary = ReviewBackfill::default();
    let mut scanned = 0usize;
    let mut tasks = tokio::task::JoinSet::new();
    let mut queue = user_ids.into_iter();

    loop {
        while tasks.len() < CONCURRENCY {
            let Some(user_id) = queue.next() else { break };
            tasks.spawn(async move {
                let result = backfill_reviews_for_user(&user_id).await;
                (user_id, result)
            });
        }
        let Some(joined) = tasks.join_next().await else {
            break;
        };
        scanned += 1;
        if scanned.is_multiple_of(250) {
            info!("Review backfill: scanned {}/{} user(s)", scanned, total);
        }
        match joined {
            Ok((_, Ok(user_summary))) => {
                summary.indexed += user_summary.indexed;
                summary.already_indexed += user_summary.already_indexed;
                summary.gone += user_summary.gone;
                summary.unreachable += user_summary.unreachable;
                summary.failed += user_summary.failed;
            }
            Ok((user_id, Err(e))) => {
                warn!("Review backfill for user {} failed: {:?}", user_id, e);
                summary.failed += 1;
            }
            Err(e) => {
                warn!("Review backfill task panicked: {:?}", e);
                summary.failed += 1;
            }
        }
    }
    Ok(summary)
}

/// One user's backfill pass: LIST their reviews directory
/// (cursor-paginated), skip ids the graph already carries, ingest the rest.
pub async fn backfill_reviews_for_user(user_id: &str) -> Result<ReviewBackfill, DynError> {
    let mut summary = ReviewBackfill::default();
    match scan_user_reviews(&Unthrottled, user_id, &mut summary).await {
        Ok(()) => Ok(summary),
        Err(UserScanError::Failed(e)) => Err(e),
        Err(UserScanError::Stopped(stop)) => Err(Box::new(stop)),
    }
}

/// Why a user's directory scan did not finish.
#[derive(Debug)]
pub enum UserScanError {
    /// The request gate stopped the run mid-scan; the user is unfinished.
    Stopped(GateStop),
    /// The scan could not run (graph, client or request construction).
    Failed(DynError),
}

/// One user's discovery pass through a request gate. Every LIST page and
/// every record GET takes a permit from `gate`, and the tallies go to
/// `summary`, which holds what was done so far when the gate stops the scan.
pub async fn scan_user_reviews(
    gate: &dyn RequestGate,
    user_id: &str,
    summary: &mut ReviewBackfill,
) -> Result<(), UserScanError> {
    let indexed: std::collections::HashSet<String> = get_indexed_review_ids(user_id)
        .await
        .map_err(UserScanError::Failed)?
        .into_iter()
        .collect();

    let pubky = PubkyConnector::get().map_err(|e| UserScanError::Failed(e.into()))?;
    let storage = pubky.public_storage();
    let directory = format!("pubky://{user_id}/pub/pubky.app/marketplace/v1/reviews/");

    let mut cursor: Option<String> = None;
    loop {
        let mut request = storage
            .list(directory.as_str())
            .map_err(|e| UserScanError::Failed(e.into()))?
            .shallow(true)
            .limit(500);
        if let Some(cursor_value) = &cursor {
            request = request.cursor(cursor_value);
        }
        let permit = gate.acquire().await.map_err(UserScanError::Stopped)?;
        let entries = match request.send().await {
            Ok(entries) => {
                permit.succeeded();
                entries
            }
            // A user with no reviews directory answers 404. Other LIST
            // failures mean this user's homeserver is unreachable. Neither
            // case may permanently block the one-shot migration.
            Err(e) if is_homeserver_not_found(&e) => {
                permit.succeeded();
                debug!("Review directory for user {} is gone", user_id);
                summary.gone += 1;
                return Ok(());
            }
            Err(e) => {
                permit.failed();
                warn!(
                    "Review directory for user {} is unreachable; skipping: {:?}",
                    user_id, e
                );
                summary.unreachable += 1;
                return Ok(());
            }
        };
        if entries.is_empty() {
            return Ok(());
        }

        for entry in &entries {
            let url = entry.to_pubky_url();
            let Some(review_id) = url.rsplit('/').next().filter(|id| !id.is_empty()) else {
                continue;
            };
            if indexed.contains(review_id) {
                summary.already_indexed += 1;
                continue;
            }
            match ingest_review_through(gate, user_id, review_id).await {
                Ok(ReviewIngest::Ingested) => summary.indexed += 1,
                Ok(_) => {
                    debug!(
                        "Review {}/{} left the homeserver after it was listed; skipping",
                        user_id, review_id
                    );
                    summary.gone += 1;
                }
                Err(ReviewIngestError::Stopped(stop)) => return Err(UserScanError::Stopped(stop)),
                Err(e) => {
                    warn!(
                        "Review backfill could not ingest {}/{}: {:?}",
                        user_id, review_id, e
                    );
                    summary.failed += 1;
                }
            }
        }

        if entries.len() < 500 {
            return Ok(());
        }
        cursor = entries.last().map(|entry| entry.to_pubky_url());
    }
}

/// Why reading or ingesting one review record failed.
#[derive(Debug, thiserror::Error)]
pub enum ReviewIngestError {
    /// The request gate stopped before the request was sent.
    #[error("{0}")]
    Stopped(GateStop),
    /// The homeserver could not be reached or did not answer.
    #[error("homeserver request failed: {0}")]
    Unreachable(EventProcessorError),
    /// The homeserver answered with something that is not a review record.
    #[error("not a valid review record: {0}")]
    Invalid(EventProcessorError),
    /// The record was read but the index could not take it.
    #[error("could not be indexed: {0}")]
    Index(EventProcessorError),
}

impl From<ReviewIngestError> for EventProcessorError {
    fn from(error: ReviewIngestError) -> Self {
        match error {
            ReviewIngestError::Stopped(stop) => EventProcessorError::generic(stop.to_string()),
            ReviewIngestError::Unreachable(e)
            | ReviewIngestError::Invalid(e)
            | ReviewIngestError::Index(e) => e,
        }
    }
}

/// How ingesting one review through the fence ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReviewIngest {
    /// The record is indexed.
    Ingested,
    /// The record is not on the homeserver, or the feed deleted it after it
    /// was read; nothing is indexed (and nothing stale is left behind).
    Gone,
    /// The event feed changed the review after it was read, so the record in
    /// hand may be stale: read it again.
    Changed,
}

/// How often one review is read again because the feed kept changing it.
const MAX_READS_PER_REVIEW: usize = 3;

/// Fetches one review record from the reviewer's homeserver (the canonical
/// source) and runs the normal ingest. Returns `false` without indexing when
/// the record no longer exists on the homeserver.
pub async fn ingest_review_from_homeserver(
    user_id: &str,
    review_id: &str,
) -> Result<bool, EventProcessorError> {
    match ingest_review_through(&Unthrottled, user_id, review_id).await? {
        ReviewIngest::Ingested => Ok(true),
        ReviewIngest::Gone | ReviewIngest::Changed => Ok(false),
    }
}

/// Reads one review from the reviewer's homeserver through `gate` and indexes
/// it, guarded against the event feed: a review the feed deleted after it was
/// read is never brought back, and one the feed edited is read again.
pub async fn ingest_review_through(
    gate: &dyn RequestGate,
    user_id: &str,
    review_id: &str,
) -> Result<ReviewIngest, ReviewIngestError> {
    for _ in 0..MAX_READS_PER_REVIEW {
        let read = read_review(gate, user_id, review_id).await?;
        match ingest_read_review(read).await? {
            ReviewIngest::Changed => continue,
            ended => return Ok(ended),
        }
    }
    Err(ReviewIngestError::Index(EventProcessorError::generic(
        format!("review {user_id}/{review_id} changed on each of {MAX_READS_PER_REVIEW} reads"),
    )))
}

/// A review record read from its homeserver, with the moment the read began
/// on the Redis clock. The record may be written to the index only if the
/// event feed has not touched the review since that moment.
pub struct ReviewRead {
    reviewer_id: String,
    review_id: String,
    started_ms: i64,
    /// The record, `None` when the homeserver answered 404.
    pub record: Option<PubkyAppMarketplaceReview>,
}

impl ReviewRead {
    /// The Redis clock reading, in milliseconds, taken before the request.
    pub fn started_ms(&self) -> i64 {
        self.started_ms
    }
}

/// Step one of a fenced ingest: takes the clock reading, then reads the
/// record.
pub async fn read_review(
    gate: &dyn RequestGate,
    user_id: &str,
    review_id: &str,
) -> Result<ReviewRead, ReviewIngestError> {
    let started_ms = ReviewTouch::now_ms()
        .await
        .map_err(|e| ReviewIngestError::Index(e.into()))?;
    let record = fetch_review_record(gate, user_id, review_id).await?;
    Ok(ReviewRead {
        reviewer_id: user_id.to_string(),
        review_id: review_id.to_string(),
        started_ms,
        record,
    })
}

/// Step two of a fenced ingest: indexes the record unless the feed touched
/// the review since it was read.
///
/// The check runs before the write (a deletion or edit seen by then makes the
/// record stale) and again after it (the feed may have started between the
/// check and the end of the write: a deletion that began then found the
/// review half written, so the write is undone; an edit asks for a re-read).
pub async fn ingest_read_review(read: ReviewRead) -> Result<ReviewIngest, ReviewIngestError> {
    let Some(review) = read.record.clone() else {
        return Ok(ReviewIngest::Gone);
    };
    let reviewer = PubkyId::try_from(read.reviewer_id.as_str())
        .map_err(|e| ReviewIngestError::Invalid(EventProcessorError::generic(e)))?;
    let touched = ReviewTouch::since(&read.reviewer_id, &read.review_id, read.started_ms)
        .await
        .map_err(|e| ReviewIngestError::Index(e.into()))?;
    if let Some(touch) = touched {
        return Ok(if touch.deleted {
            ReviewIngest::Gone
        } else {
            ReviewIngest::Changed
        });
    }
    index_review(review, reviewer, read.review_id.clone())
        .await
        .map_err(ReviewIngestError::Index)?;
    settle_after_write(&read)
        .await
        .map_err(ReviewIngestError::Index)
}

/// The check after a fenced write (see [`ingest_read_review`]).
pub async fn settle_after_write(read: &ReviewRead) -> Result<ReviewIngest, EventProcessorError> {
    let touched = ReviewTouch::since(&read.reviewer_id, &read.review_id, read.started_ms).await?;
    match touched {
        None => Ok(ReviewIngest::Ingested),
        Some(touch) if touch.deleted => {
            let reviewer = PubkyId::try_from(read.reviewer_id.as_str())
                .map_err(EventProcessorError::generic)?;
            match unindex_review(reviewer, read.review_id.clone()).await {
                Ok(()) | Err(EventProcessorError::SkipIndexing) => Ok(ReviewIngest::Gone),
                Err(e) => Err(e),
            }
        }
        Some(_) => Ok(ReviewIngest::Changed),
    }
}

/// Reads one review record from the reviewer's homeserver through `gate`;
/// `None` when the record no longer exists there.
pub async fn fetch_review_record(
    gate: &dyn RequestGate,
    user_id: &str,
    review_id: &str,
) -> Result<Option<PubkyAppMarketplaceReview>, ReviewIngestError> {
    let uri = marketplace_review_uri_builder(user_id.to_string(), review_id.to_string());

    let permit = gate.acquire().await.map_err(ReviewIngestError::Stopped)?;
    let pubky = match PubkyConnector::get() {
        Ok(pubky) => pubky,
        Err(e) => {
            permit.failed();
            return Err(ReviewIngestError::Unreachable(e.into()));
        }
    };
    let response = match pubky.public_storage().get(&uri).await {
        Ok(response) => response,
        Err(e) if is_homeserver_not_found(&e) => {
            permit.succeeded();
            return Ok(None);
        }
        Err(e) => {
            permit.failed();
            return Err(ReviewIngestError::Unreachable(e.into()));
        }
    };
    let blob = match response.bytes().await {
        Ok(blob) => {
            permit.succeeded();
            blob
        }
        Err(e) => {
            permit.failed();
            return Err(ReviewIngestError::Unreachable(
                EventProcessorError::client_error(e.to_string()),
            ));
        }
    };

    let resource = Resource::MarketplaceReview(review_id.to_string());
    let pubky_object = PubkyAppObject::from_resource(&resource, &blob)
        .map_err(|e| ReviewIngestError::Invalid(EventProcessorError::generic(e)))?;
    match pubky_object {
        PubkyAppObject::MarketplaceReview(review) => Ok(Some(review)),
        _ => Err(ReviewIngestError::Invalid(EventProcessorError::generic(
            format!("Expected a marketplace review record at {uri}"),
        ))),
    }
}

/// Outcome tally of the one-shot trust-bucket backfill.
#[derive(Debug, Default)]
pub struct TrustBucketBackfill {
    /// Verified reviews re-read from their homeserver, now carrying their
    /// attestation's `iat` and price weight.
    pub refetched: usize,
    /// Reviews whose record left the homeserver; they stay out of the trust
    /// buckets until their deletion is indexed.
    pub gone: usize,
    /// Reviews whose homeserver could not be reached; they stay out of the
    /// trust buckets until their next edit.
    pub unreachable: usize,
    /// Reviews that were read but could not be ingested.
    pub failed: usize,
    /// Subject and listing aggregates recomputed.
    pub aggregates: usize,
}

/// What re-reading one review without `iat` came to.
#[derive(Debug)]
pub enum Rebucket {
    /// Re-read and ingested; the review now carries `iat` and price weight.
    Refetched,
    /// The record left the homeserver; the review stays unbucketed.
    Gone,
    /// The homeserver could not be reached; the review stays unbucketed.
    Unreachable(EventProcessorError),
    /// The homeserver answered with something that is not a review record.
    Invalid(EventProcessorError),
    /// The record was read but could not be ingested.
    Failed(EventProcessorError),
}

/// Re-reads one verified review without `iat` from its reviewer's homeserver
/// through `gate` and runs the normal ingest, guarded against the event feed.
/// Errs only when the gate stops before the request is sent.
pub async fn rebucket_review(
    gate: &dyn RequestGate,
    reviewer_id: &str,
    review_id: &str,
) -> Result<Rebucket, GateStop> {
    match ingest_review_through(gate, reviewer_id, review_id).await {
        Ok(ReviewIngest::Ingested) => Ok(Rebucket::Refetched),
        Ok(_) => Ok(Rebucket::Gone),
        Err(ReviewIngestError::Stopped(stop)) => Err(stop),
        Err(ReviewIngestError::Unreachable(e)) => Ok(Rebucket::Unreachable(e)),
        Err(ReviewIngestError::Invalid(e)) => Ok(Rebucket::Invalid(e)),
        Err(ReviewIngestError::Index(e)) => Ok(Rebucket::Failed(e)),
    }
}

/// One-shot backfill for the trust buckets. Verified reviews indexed before
/// the index kept their attestation's `iat` and price weight are re-read from
/// the reviewer's homeserver and run through the normal ingest (the index
/// never stored the attestation, so it cannot derive them offline). Every
/// subject and listing aggregate is then recomputed, so each carries its
/// buckets and unverified count. Idempotent: a re-run re-reads only reviews
/// still without `iat`.
pub async fn backfill_trust_buckets() -> Result<TrustBucketBackfill, EventProcessorError> {
    let mut summary = TrustBucketBackfill::default();
    for (reviewer_id, review_id) in ReviewDetails::without_attestation_time().await? {
        match rebucket_review(&Unthrottled, &reviewer_id, &review_id)
            .await
            .map_err(|stop| EventProcessorError::generic(stop.to_string()))?
        {
            Rebucket::Refetched => summary.refetched += 1,
            Rebucket::Gone => {
                debug!(
                    "Review {}/{} left the homeserver; it stays unbucketed",
                    reviewer_id, review_id
                );
                summary.gone += 1;
            }
            Rebucket::Unreachable(e) | Rebucket::Invalid(e) => {
                warn!(
                    "Review {}/{} is unreachable; it stays unbucketed until its next edit: {:?}",
                    reviewer_id, review_id, e
                );
                summary.unreachable += 1;
            }
            Rebucket::Failed(e) => {
                warn!(
                    "Trust-bucket backfill could not ingest {}/{}: {:?}",
                    reviewer_id, review_id, e
                );
                summary.failed += 1;
            }
        }
    }
    let (subjects, listings) = ReviewDetails::aggregate_scopes().await?;
    for (subject_id, role) in &subjects {
        ReputationSummary::recompute_subject(subject_id, *role).await?;
    }
    for (listing_owner_id, listing_id) in &listings {
        ReputationSummary::recompute_listing(listing_owner_id, listing_id).await?;
    }
    summary.aggregates = subjects.len() + listings.len();
    Ok(summary)
}
