use crate::routes::v0::endpoints::{
    LISTING_REVIEWS_ROUTE, SHOP_REPUTATION_ROUTE, SHOP_REVIEWS_ROUTE,
};
use crate::{Error, Result};
use axum::extract::{Path, Query};
use axum::Json;
use nexus_common::models::marketplace::{
    AttestorWindow, FilteredReviewStream, ReputationBucket, ReputationSnippet, ReputationSummary,
    ReviewDetails, ReviewMatch, ReviewModeration, ReviewModerationState, ReviewReach,
    ReviewResponseDetails, ReviewSort, ReviewStream, ReviewStreamFilter, ReviewStreamParams,
    ReviewStreamScope, ReviewView, ScopedReputationSummary,
};
use nexus_common::types::Pagination;
use pubky_app_specs::PubkyAppReviewRole;
use serde::{Deserialize, Serialize};
use tracing::debug;
use utoipa::{OpenApi, ToSchema};

#[derive(Deserialize, Debug, ToSchema)]
pub struct ReviewStreamQuery {
    #[serde(flatten)]
    pub pagination: Pagination,
    /// Review direction. Defaults to `buyer_reviewing_seller` (the public
    /// seller-reputation surface). `seller_reviewing_buyer` serves
    /// negotiation contexts only (ratified D8) — clients must not build
    /// public buyer profiles from it.
    pub role: Option<PubkyAppReviewRole>,
    /// The viewer's pubky; `reach` and `tagged_by_viewer` need it.
    pub viewer_id: Option<String>,
    /// Keeps reviewers in the viewer's follow reach.
    pub reach: Option<ReviewReach>,
    /// Follow hops for `reach=wot`, 1-3 (default 2).
    pub depth: Option<String>,
    /// Keeps reviewers the viewer tagged with any of 1-5 comma-separated labels.
    pub tagged_by_viewer: Option<String>,
    /// Keeps reviews whose attestation these windows cover:
    /// `<pubky>[:<from>:<until>],…`.
    pub attestors: Option<String>,
    /// Review order; defaults to `newest`.
    pub sort: Option<ReviewSort>,
}

/// A review stream response: the plain page when no filter or sort
/// parameter is given (the original shape), the filtered page with its
/// `matched` figures otherwise.
#[derive(Serialize, Debug, ToSchema)]
#[serde(untagged)]
pub enum ReviewStreamResponse {
    Page(ReviewStream),
    Filtered(FilteredReviewStream),
}

#[derive(Deserialize, Debug, ToSchema)]
pub struct ReputationQuery {
    /// Review direction; defaults to `buyer_reviewing_seller`.
    pub role: Option<PubkyAppReviewRole>,
    /// Trusted attestors with optional trust windows:
    /// `<pubky>[:<from>:<until>],…` (see the route description).
    pub attestors: Option<String>,
}

impl ReviewStreamQuery {
    pub fn initialize_defaults(&mut self) {
        self.pagination.skip.get_or_insert(0);
        self.pagination.limit = Some(self.pagination.limit.unwrap_or(10).min(30));
    }

    /// The validated filter, `None` when no filter or sort parameter is given.
    fn filter(&self) -> Result<Option<ReviewStreamFilter>> {
        ReviewStreamFilter::parse(ReviewStreamParams {
            viewer_id: self.viewer_id.as_deref(),
            reach: self.reach,
            depth: self.depth.as_deref(),
            tagged_by_viewer: self.tagged_by_viewer.as_deref(),
            attestors: self.attestors.as_deref(),
            sort: self.sort,
        })
        .map_err(|message| Error::invalid_input(&message))
    }
}

/// Serves the filtered stream of `scope` when `filter` is given.
async fn filtered_page(
    scope: ReviewStreamScope,
    filter: ReviewStreamFilter,
    pagination: Pagination,
) -> Result<Json<ReviewStreamResponse>> {
    let stream = ReviewStream::get_filtered(&scope, &filter, pagination).await?;
    Ok(Json(ReviewStreamResponse::Filtered(stream)))
}

#[utoipa::path(
    get,
    path = SHOP_REVIEWS_ROUTE,
    description = r#"Paged reviews about a subject, newest-indexed first, with subject responses joined.

Every entry carries a `verified` flag: true iff the review's embedded purchase attestation parsed as a
compact JWS, its Ed25519 signature verified against the self-certifying `iss` pubky, and its claims bind
to the review (reviewer/subject/listing/role). Verification proves WHICH key signed — `attestor_id` names
it — never that the signer is legitimate; clients apply their own attestor trust list at display time.
Unverified reviews are included and must be labeled as such (ratified D5). A verified review also carries
its attestation's `iat` (issued at, seconds), which trust windows apply to, and its `price_weight` (1-5,
from the attestation's `price_decade` claim; 1 without it); both are `null` for an unverified review.

Every entry also carries `moderation`: `null`, or `{state, reason}` when Pubky Market's configured
moderator acted on the review. `text_hidden` withholds the text (returned empty) and keeps the ratings in
the aggregates; `excluded` withholds the text and leaves the review out of every aggregate. `reason` is
the moderator's label (e.g. `hatespeech`, `review_fraud`).

**Filters and sorts.** With any of `viewer_id`, `reach`, `depth`, `tagged_by_viewer`, `attestors` or `sort`,
the response is `{"reviews": [<entry>…], "matched": {"buyers", "orders", "avg"}}` instead of the bare list.
- `reach=following|friends|wot` keeps reviewers the viewer follows, follows mutually, or reaches within
  `depth` (1-3, default 2; `reach=wot` only) follow hops; the viewer's own reviews are left out.
- `tagged_by_viewer=<1-5 comma-separated labels>` keeps reviewers the viewer tagged with any label (only
  the viewer's tags count). Both need `viewer_id`, and combine (both must hold).
- `attestors=<pubky>[:<from>:<until>],…` keeps reviews whose attestation one window covers (as on the
  reputation routes; unverified reviews drop out).
- `sort=newest|value|rating_desc|rating_asc`: newest indexed first (default); highest `price_weight` first,
  reviews without one last; highest or lowest `ratings.overall` first. Ties go newest first.
- `matched` counts every match, not only the page, by counting rules v2: distinct buyers, distinct attested
  orders, and `avg`, the mean over buyers of each buyer's mean `overall` (`null` when nothing counts).
  Unverified and moderation-excluded reviews are listed but never counted.
The index holds follows and tags between the users it indexes (its own homeserver's users on a
homeserver-scoped deployment), so reach and tags see only those.
400 on a malformed or inconsistent parameter."#,
    tag = "Marketplace",
    params(
        ("seller_id" = String, Path, description = "Subject Pubky ID (the reviewed user)"),
        ("role" = Option<PubkyAppReviewRole>, Query, description = "Review direction; defaults to buyer_reviewing_seller"),
        ("skip" = Option<usize>, Query, description = "Skip N reviews"),
        ("limit" = Option<usize>, Query, description = "Retrieve N reviews"),
        ("viewer_id" = Option<String>, Query, description = "Viewer Pubky ID; needed by reach and tagged_by_viewer"),
        ("reach" = Option<ReviewReach>, Query, description = "following, friends or wot"),
        ("depth" = Option<u8>, Query, description = "Follow hops for reach=wot, 1-3 (default 2)"),
        ("tagged_by_viewer" = Option<String>, Query, description = "1-5 comma-separated labels of the viewer's tags on reviewers"),
        ("attestors" = Option<String>, Query, description = "Trusted attestors: <pubky>[:<from>:<until>],…"),
        ("sort" = Option<ReviewSort>, Query, description = "newest (default), value, rating_desc or rating_asc"),
    ),
    responses(
        (status = 200, description = "Review page: the bare list, or {reviews, matched} with a filter or sort parameter (empty when none matches)", body = ReviewStreamResponse),
        (status = 400, description = "Malformed or inconsistent filter parameter"),
        (status = 500, description = "Internal server error")
    )
)]
pub async fn shop_reviews_handler(
    Path(seller_id): Path<String>,
    Query(mut query): Query<ReviewStreamQuery>,
) -> Result<Json<ReviewStreamResponse>> {
    debug!("GET {SHOP_REVIEWS_ROUTE} seller_id:{seller_id}");

    query.initialize_defaults();
    let role = query
        .role
        .unwrap_or(PubkyAppReviewRole::BuyerReviewingSeller);

    if let Some(filter) = query.filter()? {
        let scope = ReviewStreamScope::Subject {
            subject_id: seller_id,
            role,
        };
        return filtered_page(scope, filter, query.pagination).await;
    }
    let stream = ReviewStream::get_by_subject(&seller_id, role, query.pagination).await?;
    Ok(Json(ReviewStreamResponse::Page(stream.unwrap_or_default())))
}

#[utoipa::path(
    get,
    path = SHOP_REPUTATION_ROUTE,
    description = r#"Full reputation aggregate of a subject: counts, verified counts, star average and
histogram, sub-rating averages, response/edited-late counts, and the per-attestor breakdown of verified
reviews. The aggregate is recomputed from public records and reproducible by any third party.

`buckets` holds the counting rules v2 figures per attestor pubky (orders, buyers, per-buyer and weighted
rating sums, histogram, `first_iat`/`last_iat`; in the buyer-reviewing-seller role also the seller rating
sums `seller_buyers` and `sum_seller_buyer_means`), and `unverified_count` the reviews that never count.
With `attestors=<pubky>[:<from>:<until>],…` (at most 20 entries; `from` and `until` are attestation `iat`
seconds, window `[from, until)`, either end may be empty; a pubky may repeat for several windows) the
response adds `scoped`: the same figures over the counted reviews inside any window, one buyer counted once
across attestors (zero counts when nothing matches).

404 means no review is indexed for the subject in that role — the explicit "New seller" state. Clients
must render absence, never a fabricated 0.0."#,
    tag = "Marketplace",
    params(
        ("seller_id" = String, Path, description = "Subject Pubky ID (the reviewed user)"),
        ("role" = Option<PubkyAppReviewRole>, Query, description = "Review direction; defaults to buyer_reviewing_seller"),
        ("attestors" = Option<String>, Query, description = "Trusted attestors: <pubky>[:<from>:<until>],…"),
    ),
    responses(
        (status = 200, description = "Reputation summary", body = ScopedReputationSummary),
        (status = 400, description = "Malformed attestors parameter"),
        (status = 404, description = "No indexed reviews for this subject and role"),
        (status = 500, description = "Internal server error")
    )
)]
pub async fn shop_reputation_handler(
    Path(seller_id): Path<String>,
    Query(query): Query<ReputationQuery>,
) -> Result<Json<ScopedReputationSummary>> {
    debug!("GET {SHOP_REPUTATION_ROUTE} seller_id:{seller_id}");

    let role = query
        .role
        .unwrap_or(PubkyAppReviewRole::BuyerReviewingSeller);
    let windows = query
        .attestors
        .as_deref()
        .map(AttestorWindow::parse_list)
        .transpose()
        .map_err(|message| Error::invalid_input(&message))?;

    let Some(summary) = ReputationSummary::get_by_subject(&seller_id, role).await? else {
        return Err(Error::ReputationNotFound {
            subject_id: seller_id,
        });
    };
    let scoped = match &windows {
        Some(windows) => {
            Some(ReputationSummary::scoped_by_subject(&seller_id, role, windows).await?)
        }
        None => None,
    };
    Ok(Json(ScopedReputationSummary { summary, scoped }))
}

#[utoipa::path(
    get,
    path = LISTING_REVIEWS_ROUTE,
    description = r#"Paged buyer reviews of one listing, newest-indexed first, with seller responses joined.
Same verification and moderation semantics as the subject review stream.

**Filters and sorts.** With any of `viewer_id`, `reach`, `depth`, `tagged_by_viewer`, `attestors` or `sort`,
the response is `{"reviews": [<entry>…], "matched": {"buyers", "orders", "avg"}}` instead of the bare list.
- `reach=following|friends|wot` keeps reviewers the viewer follows, follows mutually, or reaches within
  `depth` (1-3, default 2; `reach=wot` only) follow hops; the viewer's own reviews are left out.
- `tagged_by_viewer=<1-5 comma-separated labels>` keeps reviewers the viewer tagged with any label (only
  the viewer's tags count). Both need `viewer_id`, and combine (both must hold).
- `attestors=<pubky>[:<from>:<until>],…` keeps reviews whose attestation one window covers (as on the
  reputation routes; unverified reviews drop out).
- `sort=newest|value|rating_desc|rating_asc`: newest indexed first (default); highest `price_weight` first,
  reviews without one last; highest or lowest `ratings.overall` first. Ties go newest first.
- `matched` counts every match, not only the page, by counting rules v2: distinct buyers, distinct attested
  orders, and `avg`, the mean over buyers of each buyer's mean `overall` (`null` when nothing counts).
  Unverified and moderation-excluded reviews are listed but never counted.
The index holds follows and tags between the users it indexes (its own homeserver's users on a
homeserver-scoped deployment), so reach and tags see only those.
400 on a malformed or inconsistent parameter."#,
    tag = "Marketplace",
    params(
        ("seller_id" = String, Path, description = "Seller Pubky ID"),
        ("listing_id" = String, Path, description = "Listing Crockford32 ID"),
        ("skip" = Option<usize>, Query, description = "Skip N reviews"),
        ("limit" = Option<usize>, Query, description = "Retrieve N reviews"),
        ("viewer_id" = Option<String>, Query, description = "Viewer Pubky ID; needed by reach and tagged_by_viewer"),
        ("reach" = Option<ReviewReach>, Query, description = "following, friends or wot"),
        ("depth" = Option<u8>, Query, description = "Follow hops for reach=wot, 1-3 (default 2)"),
        ("tagged_by_viewer" = Option<String>, Query, description = "1-5 comma-separated labels of the viewer's tags on reviewers"),
        ("attestors" = Option<String>, Query, description = "Trusted attestors: <pubky>[:<from>:<until>],…"),
        ("sort" = Option<ReviewSort>, Query, description = "newest (default), value, rating_desc or rating_asc"),
    ),
    responses(
        (status = 200, description = "Review page: the bare list, or {reviews, matched} with a filter or sort parameter (empty when none matches)", body = ReviewStreamResponse),
        (status = 400, description = "Malformed or inconsistent filter parameter"),
        (status = 500, description = "Internal server error")
    )
)]
pub async fn listing_reviews_handler(
    Path((seller_id, listing_id)): Path<(String, String)>,
    Query(mut query): Query<ReviewStreamQuery>,
) -> Result<Json<ReviewStreamResponse>> {
    debug!("GET {LISTING_REVIEWS_ROUTE} seller_id:{seller_id}, listing_id:{listing_id}");

    query.initialize_defaults();

    if let Some(filter) = query.filter()? {
        let scope = ReviewStreamScope::Listing {
            listing_owner_id: seller_id,
            listing_id,
        };
        return filtered_page(scope, filter, query.pagination).await;
    }
    let stream = ReviewStream::get_by_listing(&seller_id, &listing_id, query.pagination).await?;
    Ok(Json(ReviewStreamResponse::Page(stream.unwrap_or_default())))
}

#[derive(OpenApi)]
#[openapi(
    paths(shop_reviews_handler, shop_reputation_handler, listing_reviews_handler),
    components(schemas(
        ReviewStream,
        ReviewStreamResponse,
        FilteredReviewStream,
        ReviewMatch,
        ReviewReach,
        ReviewSort,
        ReviewView,
        ReviewDetails,
        ReviewModeration,
        ReviewModerationState,
        ReviewResponseDetails,
        ReputationSummary,
        ReputationSnippet,
        ReputationBucket,
        ScopedReputationSummary,
        PubkyAppReviewRole
    ))
)]
pub struct MarketplaceReviewsApiDoc;
