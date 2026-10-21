use crate::events::handlers::listing::is_homeserver_not_found;
use crate::events::retry::event::RetryEvent;
use crate::events::EventProcessorError;
use nexus_common::db::reindex::{get_all_user_ids, get_indexed_review_ids};
use nexus_common::db::{OperationOutcome, PubkyConnector};
use nexus_common::models::homeserver::Homeserver;
use nexus_common::models::marketplace::{
    ExcludedReviewer, ModeratedReview, ReputationSummary, ReviewDetails, ReviewModeration,
    ReviewModerationState,
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
pub async fn sync_put(
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
    details.moderation = ReviewModeration::get(&user_id, &review_id, None).await?;

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
    // A moderator tag handled while this review was being written.
    if let Some(refreshed) = ReviewDetails::refresh_moderation(&user_id, &review_id, None).await? {
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
    refresh_moderation(&reviewer_id, &review_id, None).await?;
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
    refresh_reviewer_moderation(&user_id, None).await?;
    Ok(())
}

/// Handles the moderator deleting one of its moderation tags. Returns whether
/// the tag moderated a review or excluded a reviewer.
///
/// The affected reviews are re-derived without the tag's marker before the
/// marker is removed, so a failure leaves the marker in place and the retried
/// tag deletion starts over.
pub async fn release_moderation(
    moderator_id: &PubkyId,
    tag_id: &str,
) -> Result<bool, EventProcessorError> {
    let reviews = ModeratedReview::reviews_of_tag(moderator_id, tag_id).await?;
    let reviewers = ExcludedReviewer::users_of_tag(moderator_id, tag_id).await?;
    if reviews.is_empty() && reviewers.is_empty() {
        return Ok(false);
    }
    let releasing = Some((moderator_id.as_ref(), tag_id));
    for (reviewer_id, review_id) in &reviews {
        info!("Moderation tag removed; releasing review {reviewer_id}/{review_id}");
        refresh_moderation(reviewer_id, review_id, releasing).await?;
    }
    for reviewer_id in &reviewers {
        info!("Moderation tag removed; counting the reviews of {reviewer_id} again");
        refresh_reviewer_moderation(reviewer_id, releasing).await?;
    }
    ModeratedReview::delete(moderator_id, tag_id).await?;
    ExcludedReviewer::delete(moderator_id, tag_id).await?;
    Ok(true)
}

/// Re-derives the moderation of every review the user wrote, after a marker on
/// the user was written or (with `releasing`) is being removed. Returns how
/// many reviews changed.
pub async fn refresh_reviewer_moderation(
    reviewer_id: &str,
    releasing: Option<(&str, &str)>,
) -> Result<usize, EventProcessorError> {
    let mut changed = 0;
    for review_id in ReviewDetails::ids_of_reviewer(reviewer_id).await? {
        if refresh_moderation(reviewer_id, &review_id, releasing).await? {
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
        changed += refresh_reviewer_moderation(&reviewer_id, None).await?;
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
    for (attestor_id, order_ref, role) in ReviewDetails::order_keys().await? {
        ReviewDetails::resolve_seller_fields(&attestor_id, &order_ref, role).await?;
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
    releasing: Option<(&str, &str)>,
) -> Result<bool, EventProcessorError> {
    match ReviewDetails::refresh_moderation(reviewer_id, review_id, releasing).await? {
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
    if let Some((attestor_id, order_ref, role)) = key {
        ReviewDetails::resolve_seller_fields(attestor_id, order_ref, role).await?;
    }
    if let Some(prior_key) = prior.and_then(ReviewDetails::order_key) {
        if Some(prior_key) != key {
            let (attestor_id, order_ref, role) = prior_key;
            ReviewDetails::resolve_seller_fields(attestor_id, order_ref, role).await?;
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
    backfill_user_reviews(user_id, &mut summary).await?;
    Ok(summary)
}

async fn backfill_user_reviews(
    user_id: &str,
    summary: &mut ReviewBackfill,
) -> Result<(), DynError> {
    let indexed: std::collections::HashSet<String> =
        get_indexed_review_ids(user_id).await?.into_iter().collect();

    let pubky = PubkyConnector::get()?;
    let storage = pubky.public_storage();
    let directory = format!("pubky://{user_id}/pub/pubky.app/marketplace/v1/reviews/");

    let mut cursor: Option<String> = None;
    loop {
        let mut request = storage.list(directory.as_str())?.shallow(true).limit(500);
        if let Some(cursor_value) = &cursor {
            request = request.cursor(cursor_value);
        }
        let entries = match request.send().await {
            Ok(entries) => entries,
            // A user with no reviews directory answers 404. Other LIST
            // failures mean this user's homeserver is unreachable. Neither
            // case may permanently block the one-shot migration.
            Err(e) if is_homeserver_not_found(&e) => {
                debug!("Review directory for user {} is gone", user_id);
                summary.gone += 1;
                return Ok(());
            }
            Err(e) => {
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
            match ingest_review_from_homeserver(user_id, review_id).await {
                Ok(true) => summary.indexed += 1,
                Ok(false) => {
                    debug!(
                        "Review {}/{} left the homeserver after it was listed; skipping",
                        user_id, review_id
                    );
                    summary.gone += 1;
                }
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

/// Fetches one review record from the reviewer's homeserver (the canonical
/// source) and runs the normal ingest. Returns `false` without indexing when
/// the record no longer exists on the homeserver.
pub async fn ingest_review_from_homeserver(
    user_id: &str,
    review_id: &str,
) -> Result<bool, EventProcessorError> {
    let reviewer = PubkyId::try_from(user_id).map_err(EventProcessorError::generic)?;
    let uri = marketplace_review_uri_builder(user_id.to_string(), review_id.to_string());

    let pubky = PubkyConnector::get()?;
    let response = match pubky.public_storage().get(&uri).await {
        Ok(response) => response,
        Err(e) if is_homeserver_not_found(&e) => return Ok(false),
        Err(e) => return Err(e.into()),
    };
    let blob = response
        .bytes()
        .await
        .map_err(|e| EventProcessorError::client_error(e.to_string()))?;

    let resource = Resource::MarketplaceReview(review_id.to_string());
    let pubky_object =
        PubkyAppObject::from_resource(&resource, &blob).map_err(EventProcessorError::generic)?;
    match pubky_object {
        PubkyAppObject::MarketplaceReview(review) => {
            sync_put(review, reviewer, review_id.to_string()).await?;
            Ok(true)
        }
        _ => Err(EventProcessorError::generic(format!(
            "Expected a marketplace review record at {uri}"
        ))),
    }
}
