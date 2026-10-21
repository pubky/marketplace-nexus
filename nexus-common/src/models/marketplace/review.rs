use super::{
    AttestorWindow, CountedReview, ReputationBucket, ReviewModeration, ReviewResponseDetails,
};
use crate::db::kv::{RedisResult, SortOrder};
use crate::db::{
    exec_single_row, execute_graph_operation, fetch_all_rows_from_graph, queries, GraphResult,
    OperationOutcome, RedisOps,
};
use crate::models::error::ModelResult;
use crate::types::Pagination;
use chrono::Utc;
use pubky_app_specs::{
    marketplace_review_uri_builder, PubkyAppMarketplaceReview, PubkyAppPurchaseAttestation,
    PubkyAppReviewRole, PubkyId,
};
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use utoipa::ToSchema;

pub const REVIEW_SUBJECT_KEY_PARTS: [&str; 2] = ["Reviews", "Subject"];
pub const REVIEW_LISTING_KEY_PARTS: [&str; 2] = ["Reviews", "Listing"];

/// The marketplace transaction service's review edit window. Records whose
/// `updated_at` moved beyond this window after `created_at` are flagged
/// `edited_late`: the window is app policy, the record is user property, and
/// the index surfaces the divergence instead of pretending the window is
/// protocol-enforced.
const REVIEW_EDIT_WINDOW_SECONDS: i64 = 24 * 60 * 60;

/// The indexed details of a marketplace review record.
///
/// Reviews live on the **reviewer's** homeserver; the embedded purchase
/// attestation (compact JWS) is verified at ingest. `verified` is a
/// cryptographic fact — the attestation parsed, its Ed25519 signature
/// verified against the self-certifying `iss` pubky, and its claims bind to
/// this exact review — never a trust statement. WHO is a trusted attestor is
/// client policy: the index records `attestor_id` on every verified review
/// and lets consumers apply their own attestor trust list at display time.
#[derive(Serialize, Deserialize, ToSchema, Clone, Debug, PartialEq)]
pub struct ReviewDetails {
    pub review_id: String,
    pub uri: String,
    /// The reviewer (record owner; the record lives on their homeserver).
    pub reviewer_id: String,
    /// The reviewed counterparty; aggregates key on this user.
    pub subject_id: String,
    pub listing_owner_id: String,
    pub listing_id: String,
    pub role: PubkyAppReviewRole,
    pub rating_overall: i64,
    pub rating_item_accuracy: Option<i64>,
    pub rating_shipping: Option<i64>,
    pub rating_communication: Option<i64>,
    pub text: String,
    /// True iff the embedded attestation parsed, signature-verified against
    /// its `iss` pubky, and bound to this review. Signer identity is in
    /// `attestor_id`; trusting that signer is the consumer's decision.
    pub verified: bool,
    /// The attestor pubky (`iss` claim) of a verified attestation.
    pub attestor_id: Option<String>,
    /// The attestor-salted order reference of a verified attestation.
    /// Distinct refs on one living review witness distinct attested orders.
    pub order_ref: Option<String>,
    /// The `iat` claim (issued at, seconds) of a verified attestation; `null`
    /// for an unverified review. Attestor trust windows apply to it.
    #[serde(default)]
    pub iat: Option<i64>,
    /// The 1-5 price weight of a verified review, from its attestation's
    /// `price_decade` claim (specs `price_weight`; 1 without the claim);
    /// `null` for an unverified review.
    #[serde(default)]
    pub price_weight: Option<u8>,
    /// True when `updated_at` moved more than the marketplace's 24h edit
    /// window after `created_at` (see [`REVIEW_EDIT_WINDOW_SECONDS`]).
    pub edited_late: bool,
    /// True when the record carries `orderRef` (its ID is per attested order).
    #[serde(default)]
    pub order_scoped: bool,
    /// True when an order-scoped record carries this legacy record's
    /// attestation: it counts once, as the scoped record, so this one is left
    /// out of aggregates and review streams.
    #[serde(default)]
    pub superseded: bool,
    /// The configured moderator's action on the review, if any. Review
    /// streams withhold the text of a moderated review (`text` is empty);
    /// an `excluded` review is also left out of every aggregate.
    #[serde(default)]
    pub moderation: Option<ReviewModeration>,
    pub created_at: String,
    pub updated_at: String,
    pub revision: i64,
    pub indexed_at: i64,
}

impl RedisOps for ReviewDetails {}

impl ReviewDetails {
    /// Builds the indexed details from a validated homeserver record,
    /// running the offline attestation verification recipe (parse →
    /// signature → binding). A failed verification never rejects the record:
    /// unverified reviews are indexed and labeled (ratified D5).
    pub fn from_homeserver(review: &PubkyAppMarketplaceReview, reviewer_id: &PubkyId) -> Self {
        let (verified, attestor_id, order_ref, iat, price_weight) =
            match PubkyAppPurchaseAttestation::verify_for_review(review) {
                Ok(attestation) => (
                    true,
                    Some(attestation.claims.iss.clone()),
                    Some(attestation.claims.order_ref.clone()),
                    Some(attestation.claims.iat),
                    Some(attestation.price_weight()),
                ),
                Err(_) => (false, None, None, None, None),
            };

        ReviewDetails {
            review_id: review.review_id.clone(),
            uri: marketplace_review_uri_builder(reviewer_id.to_string(), review.review_id.clone()),
            reviewer_id: reviewer_id.to_string(),
            subject_id: review.subject_pubky.clone(),
            listing_owner_id: review.listing_owner_pubky.clone(),
            listing_id: review.listing_id.clone(),
            role: review.role,
            rating_overall: review.ratings.overall,
            rating_item_accuracy: review.ratings.item_accuracy,
            rating_shipping: review.ratings.shipping,
            rating_communication: review.ratings.communication,
            text: review.text.clone(),
            verified,
            attestor_id,
            order_ref,
            iat,
            price_weight,
            edited_late: is_edited_late(&review.created_at, &review.updated_at),
            order_scoped: review.is_order_scoped(),
            superseded: false,
            moderation: None,
            created_at: review.created_at.clone(),
            updated_at: review.updated_at.clone(),
            revision: review.revision,
            indexed_at: Utc::now().timestamp_millis(),
        }
    }

    pub async fn get_from_index(
        reviewer_id: &str,
        review_id: &str,
    ) -> RedisResult<Option<ReviewDetails>> {
        Self::try_from_index_json(&[reviewer_id, review_id], None).await
    }

    /// Writes the `REVIEWED` edge between the reviewer and the subject.
    /// Returns `MissingDependency` when either user is not indexed yet.
    pub async fn put_to_graph(&self) -> GraphResult<OperationOutcome> {
        let query = queries::put::create_review(self)?;
        execute_graph_operation(query).await
    }

    /// Stores the details JSON and, unless this is an edit of an already
    /// indexed review, adds the review to the per-subject (and, for buyer
    /// reviews, per-listing) sorted sets. Edits keep the original position.
    pub async fn put_to_index(&self, is_edit: bool) -> RedisResult<()> {
        self.put_index_json(&[&self.reviewer_id, &self.review_id], None, None)
            .await?;
        if is_edit {
            return Ok(());
        }
        self.add_to_streams().await
    }

    /// The sorted sets this review is listed in: the subject's and, for
    /// buyer reviews, the listing's. A listing's star rating comes from
    /// buyers; seller-side reviews of buyers reference the listing but do not
    /// rate it.
    fn stream_keys(&self) -> Vec<Vec<&str>> {
        let mut keys = vec![[
            &REVIEW_SUBJECT_KEY_PARTS[..],
            &[self.subject_id.as_str(), self.role.as_str()],
        ]
        .concat()];
        if self.role == PubkyAppReviewRole::BuyerReviewingSeller {
            keys.push(
                [
                    &REVIEW_LISTING_KEY_PARTS[..],
                    &[self.listing_owner_id.as_str(), self.listing_id.as_str()],
                ]
                .concat(),
            );
        }
        keys
    }

    async fn add_to_streams(&self) -> RedisResult<()> {
        let member = format!("{}:{}", self.reviewer_id, self.review_id);
        let score = self.indexed_at as f64;
        for key in self.stream_keys() {
            Self::put_index_sorted_set(&key, &[(score, member.as_str())], None, None).await?;
        }
        Ok(())
    }

    async fn remove_from_streams(&self) -> RedisResult<()> {
        let member = format!("{}:{}", self.reviewer_id, self.review_id);
        for key in self.stream_keys() {
            Self::remove_from_index_sorted_set(None, &key, &[member.as_str()]).await?;
        }
        Ok(())
    }

    /// The other verified reviews by this reviewer, in this role, that carry
    /// the same attested order `(attestor, order_ref)` for the same listing:
    /// a legacy record and its order-scoped twin. Returns
    /// `(review_id, order_scoped)` pairs.
    async fn order_twins(&self) -> GraphResult<Vec<(String, bool)>> {
        let (Some(attestor_id), Some(order_ref)) = (&self.attestor_id, &self.order_ref) else {
            return Ok(Vec::new());
        };
        let rows = fetch_all_rows_from_graph(queries::get::review_order_twins(
            &self.reviewer_id,
            &self.review_id,
            self.role.as_str(),
            attestor_id,
            order_ref,
            &self.listing_owner_id,
            &self.listing_id,
        ))
        .await?;
        Ok(rows
            .into_iter()
            .filter_map(|row| {
                let review_id: String = row.get("review_id").ok()?;
                let order_scoped: bool = row.get("order_scoped").unwrap_or(false);
                Some((review_id, order_scoped))
            })
            .collect())
    }

    async fn set_superseded(
        reviewer_id: &str,
        review_id: &str,
        superseded: bool,
    ) -> ModelResult<()> {
        exec_single_row(queries::put::set_review_superseded(
            reviewer_id,
            review_id,
            superseded,
        ))
        .await?;
        if let Some(mut details) = Self::get_from_index(reviewer_id, review_id).await? {
            details.superseded = superseded;
            details
                .put_index_json(&[reviewer_id, review_id], None, None)
                .await?;
            if superseded {
                details.remove_from_streams().await?;
            } else {
                details.add_to_streams().await?;
            }
        }
        Ok(())
    }

    /// Counts a verified review once per attested order and item (specs
    /// `.14` counting rule): an order-scoped record supersedes every legacy
    /// record carrying the same `(attestor, order_ref)` in the same role for
    /// the same listing, and a legacy record that arrives after its scoped
    /// twin is superseded at once. Superseded records stay indexed but leave
    /// the aggregates and the review streams. Unverified records are never
    /// part of this.
    ///
    /// `prior` is the record's indexed state before this write. An edit that
    /// drops or changes a scoped record's attested order (say, its
    /// attestation no longer verifies) frees the legacy twins it superseded,
    /// and a superseded record that no longer qualifies counts again.
    pub async fn resolve_order_twins(&self, prior: Option<&ReviewDetails>) -> ModelResult<()> {
        if let Some(prior) = prior {
            let same_scoped_order = self.verified
                && self.order_scoped
                && prior.attestor_id == self.attestor_id
                && prior.order_ref == self.order_ref;
            if !same_scoped_order {
                prior.restore_order_twins().await?;
            }
        }
        let twins = if self.verified {
            self.order_twins().await?
        } else {
            Vec::new()
        };
        if self.verified && self.order_scoped {
            for (twin_id, twin_scoped) in &twins {
                if !twin_scoped {
                    Self::set_superseded(&self.reviewer_id, twin_id, true).await?;
                }
            }
        }
        let superseded = self.verified
            && !self.order_scoped
            && twins.iter().any(|(_, twin_scoped)| *twin_scoped);
        if superseded != self.superseded {
            Self::set_superseded(&self.reviewer_id, &self.review_id, superseded).await?;
        }
        Ok(())
    }

    /// After an order-scoped review is deleted, its legacy twins count again.
    pub async fn restore_order_twins(&self) -> ModelResult<()> {
        if !self.verified || !self.order_scoped {
            return Ok(());
        }
        for (twin_id, twin_scoped) in self.order_twins().await? {
            if !twin_scoped {
                Self::set_superseded(&self.reviewer_id, &twin_id, false).await?;
            }
        }
        Ok(())
    }

    /// The reviewer's attested order `(reviewer_id, attestor_id, order_ref,
    /// role)` a verified review belongs to: its seller fields count once per
    /// order and buyer.
    pub fn order_key(&self) -> Option<(&str, &str, &str, PubkyAppReviewRole)> {
        if !self.verified {
            return None;
        }
        match (&self.attestor_id, &self.order_ref) {
            (Some(attestor_id), Some(order_ref)) => {
                Some((&self.reviewer_id, attestor_id, order_ref, self.role))
            }
            _ => None,
        }
    }

    /// Settles which copy of a reviewer's attested order's seller fields
    /// counts (see [`queries::put::resolve_seller_fields`]).
    pub async fn resolve_seller_fields(
        reviewer_id: &str,
        attestor_id: &str,
        order_ref: &str,
        role: PubkyAppReviewRole,
    ) -> GraphResult<()> {
        exec_single_row(queries::put::resolve_seller_fields(
            reviewer_id,
            attestor_id,
            order_ref,
            role.as_str(),
        ))
        .await
    }

    /// Every reviewer's attested order with a verified review in the graph.
    pub async fn order_keys() -> GraphResult<Vec<(String, String, String, PubkyAppReviewRole)>> {
        let rows = fetch_all_rows_from_graph(queries::get::review_order_keys()).await?;
        let mut keys = Vec::with_capacity(rows.len());
        for row in rows {
            let role: String = row.get("role")?;
            let Some(role) = role_from_graph(&role) else {
                continue;
            };
            if let (Some(attestor_id), Some(order_ref)) =
                (row.get("attestor_id")?, row.get("order_ref")?)
            {
                keys.push((row.get("reviewer_id")?, attestor_id, order_ref, role));
            }
        }
        Ok(keys)
    }

    /// Every review aggregate scope in the graph: the `(subject_id, role)`
    /// subject scopes and the `(listing_owner_id, listing_id)` listing scopes.
    pub async fn aggregate_scopes(
    ) -> GraphResult<(Vec<(String, PubkyAppReviewRole)>, Vec<(String, String)>)> {
        let rows = fetch_all_rows_from_graph(queries::get::review_aggregate_scopes()).await?;
        let mut subjects = std::collections::BTreeSet::new();
        let mut listings = std::collections::BTreeSet::new();
        for row in rows {
            let subject_id: Option<String> = row.get("subject_id")?;
            let role: String = row.get("role")?;
            if role_from_graph(&role).is_none() {
                continue;
            }
            if let Some(subject_id) = subject_id {
                subjects.insert((subject_id, role));
            }
            let listing_owner_id: Option<String> = row.get("listing_owner_id")?;
            let listing_id: Option<String> = row.get("listing_id")?;
            if let (Some(listing_owner_id), Some(listing_id)) = (listing_owner_id, listing_id) {
                listings.insert((listing_owner_id, listing_id));
            }
        }
        let subjects = subjects
            .into_iter()
            .filter_map(|(subject_id, role)| role_from_graph(&role).map(|role| (subject_id, role)))
            .collect();
        Ok((subjects, listings.into_iter().collect()))
    }

    /// Flags every review edge without an attested order as counting its own
    /// seller fields.
    pub async fn flag_unkeyed_seller_fields() -> GraphResult<()> {
        exec_single_row(queries::put::flag_unkeyed_seller_fields()).await
    }

    /// The ids of every review the reviewer has in the graph.
    pub async fn ids_of_reviewer(reviewer_id: &str) -> GraphResult<Vec<String>> {
        let rows =
            fetch_all_rows_from_graph(queries::get::review_ids_of_reviewer(reviewer_id)).await?;
        let mut review_ids = Vec::with_capacity(rows.len());
        for row in rows {
            if let Some(review_id) = row.get("review_id")? {
                review_ids.push(review_id);
            }
        }
        Ok(review_ids)
    }

    /// Verified reviews indexed before the index kept their attestation's
    /// `iat` and price weight: `(reviewer_id, review_id)` pairs.
    pub async fn without_attestation_time() -> GraphResult<Vec<(String, String)>> {
        let rows =
            fetch_all_rows_from_graph(queries::get::reviews_without_attestation_time()).await?;
        let mut reviews = Vec::with_capacity(rows.len());
        for row in rows {
            reviews.push((row.get("reviewer_id")?, row.get("review_id")?));
        }
        Ok(reviews)
    }

    /// The ids of every user who has a review in the graph.
    pub async fn reviewer_ids() -> GraphResult<Vec<String>> {
        let rows = fetch_all_rows_from_graph(queries::get::reviewer_ids()).await?;
        let mut reviewer_ids = Vec::with_capacity(rows.len());
        for row in rows {
            if let Some(reviewer_id) = row.get("reviewer_id")? {
                reviewer_ids.push(reviewer_id);
            }
        }
        Ok(reviewer_ids)
    }

    /// Re-derives an indexed review's moderation from the markers (see
    /// [`ReviewModeration::get`]) and stores it on the edge and the details
    /// JSON. Returns the updated details when the moderation changed, `None`
    /// when it did not or the review is not indexed (its ingest reads the
    /// markers). `releasing` is a moderator tag being removed.
    pub async fn refresh_moderation(
        reviewer_id: &str,
        review_id: &str,
        releasing: Option<(&str, &str)>,
    ) -> ModelResult<Option<Self>> {
        let Some(mut details) = Self::get_from_index(reviewer_id, review_id).await? else {
            return Ok(None);
        };
        let moderation = ReviewModeration::get(reviewer_id, review_id, releasing).await?;
        if details.moderation == moderation {
            return Ok(None);
        }
        exec_single_row(queries::put::set_review_moderation(
            reviewer_id,
            review_id,
            moderation.as_ref(),
        ))
        .await?;
        details.moderation = moderation;
        details
            .put_index_json(&[reviewer_id, review_id], None, None)
            .await?;
        Ok(Some(details))
    }

    /// The review as review streams serve it: a moderated review's text is
    /// withheld.
    fn public(mut self) -> Self {
        if self.moderation.is_some() {
            self.text = String::new();
        }
        self
    }

    /// Removes the review from the graph and every Redis index.
    pub async fn delete(&self) -> ModelResult<()> {
        exec_single_row(queries::del::delete_review(
            &self.reviewer_id,
            &self.review_id,
        ))
        .await?;
        Self::remove_from_index_multiple_json(&[&[&self.reviewer_id, &self.review_id]]).await?;
        self.remove_from_streams().await?;
        Ok(())
    }
}

/// One entry of a paged review list: the review plus, when the subject has
/// published one, their response record (subject-only, one per review —
/// ratified D7; authorization is structural, `owner == subjectPubky`).
#[derive(Serialize, Deserialize, ToSchema, Debug)]
pub struct ReviewView {
    pub review: ReviewDetails,
    pub response: Option<ReviewResponseDetails>,
}

/// A page of reviews with joined responses.
#[derive(Serialize, Deserialize, ToSchema, Debug, Default)]
pub struct ReviewStream(pub Vec<ReviewView>);

impl RedisOps for ReviewStream {}

impl ReviewStream {
    /// Retrieves a page of reviews about a subject in the given role,
    /// newest-indexed first, with responses joined.
    pub async fn get_by_subject(
        subject_id: &str,
        role: PubkyAppReviewRole,
        pagination: Pagination,
    ) -> ModelResult<Option<Self>> {
        let key_parts = [&REVIEW_SUBJECT_KEY_PARTS[..], &[subject_id, role.as_str()]].concat();
        Self::from_sorted_set(&key_parts, pagination).await
    }

    /// Retrieves a page of buyer reviews about a listing, newest-indexed
    /// first, with responses joined.
    pub async fn get_by_listing(
        listing_owner_id: &str,
        listing_id: &str,
        pagination: Pagination,
    ) -> ModelResult<Option<Self>> {
        let key_parts = [
            &REVIEW_LISTING_KEY_PARTS[..],
            &[listing_owner_id, listing_id],
        ]
        .concat();
        Self::from_sorted_set(&key_parts, pagination).await
    }

    async fn from_sorted_set(
        key_parts: &[&str],
        pagination: Pagination,
    ) -> ModelResult<Option<Self>> {
        let Pagination {
            skip,
            limit,
            start,
            end,
        } = pagination;

        let members = ReviewDetails::try_from_index_sorted_set(
            key_parts,
            start,
            end,
            skip,
            limit,
            SortOrder::Descending,
            None,
        )
        .await?
        .unwrap_or_default();

        if members.is_empty() {
            return Ok(None);
        }

        let review_keys: Vec<(String, String)> = members
            .into_iter()
            .filter_map(|(member, _)| {
                member
                    .split_once(':')
                    .map(|(reviewer, review_id)| (reviewer.to_string(), review_id.to_string()))
            })
            .collect();

        let views = Self::hydrate(&review_keys).await?;
        Ok((!views.is_empty()).then_some(Self(views)))
    }

    /// Reads the indexed details of `(reviewer_id, review_id)` reviews, in
    /// order, with their responses joined. Reviews missing from the index
    /// are skipped.
    async fn hydrate(review_keys: &[(String, String)]) -> ModelResult<Vec<ReviewView>> {
        if review_keys.is_empty() {
            return Ok(Vec::new());
        }
        let review_key_slices: Vec<Vec<&str>> = review_keys
            .iter()
            .map(|(reviewer, review_id)| vec![reviewer.as_str(), review_id.as_str()])
            .collect();
        let review_key_refs: Vec<&[&str]> =
            review_key_slices.iter().map(|parts| &parts[..]).collect();
        let reviews: Vec<ReviewDetails> =
            ReviewDetails::try_from_index_multiple_json(&review_key_refs)
                .await?
                .into_iter()
                .flatten()
                .collect();

        if reviews.is_empty() {
            return Ok(Vec::new());
        }

        // Responses live under [responder == subject, review_id].
        let response_key_slices: Vec<Vec<&str>> = reviews
            .iter()
            .map(|review| vec![review.subject_id.as_str(), review.review_id.as_str()])
            .collect();
        let response_key_refs: Vec<&[&str]> =
            response_key_slices.iter().map(|parts| &parts[..]).collect();
        let responses =
            ReviewResponseDetails::try_from_index_multiple_json(&response_key_refs).await?;

        Ok(reviews
            .into_iter()
            .zip(responses)
            .map(|(review, response)| ReviewView {
                review: review.public(),
                response,
            })
            .collect())
    }

    /// Retrieves a page of a scope's reviews under `filter`, sorted by
    /// `filter.sort`, with the counted figures of every match (not only the
    /// page) in `matched`. Reads the scope from the graph: a scope holds one
    /// seller's (or one listing's) reviews, so matching, sorting and counting
    /// run over the whole scope in memory.
    pub async fn get_filtered(
        scope: &ReviewStreamScope,
        filter: &ReviewStreamFilter,
        pagination: Pagination,
    ) -> ModelResult<FilteredReviewStream> {
        let query = queries::get::filtered_review_rows(
            scope,
            &filter.reviewers,
            pagination.start,
            pagination.end,
        );
        let mut rows = Vec::new();
        for row in fetch_all_rows_from_graph(query).await? {
            rows.push(FilteredReviewRow {
                reviewer_id: row.get("reviewer_id")?,
                review_id: row.get("review_id")?,
                indexed_at: row.get("indexed_at")?,
                rating_overall: row.get("rating_overall")?,
                price_weight: row.get("price_weight")?,
                attestor_id: row.get("attestor_id")?,
                order_ref: row.get("order_ref")?,
                iat: row.get("iat")?,
                counted: row.get("counted")?,
                seller_fields_counted: row.get("seller_fields_counted")?,
                rating_shipping: row.get("rating_shipping")?,
                rating_communication: row.get("rating_communication")?,
            });
        }
        let seller_score = matches!(
            scope,
            ReviewStreamScope::Subject {
                role: PubkyAppReviewRole::BuyerReviewingSeller,
                ..
            }
        );
        let (page, matched) = filter.apply(rows, pagination.skip, pagination.limit, seller_score);
        let reviews = Self::hydrate(&page).await?;
        Ok(FilteredReviewStream { reviews, matched })
    }
}

/// The scope a filtered review stream reads: the reviews about a subject in
/// one role, or the buyer reviews of one listing.
#[derive(Clone, Debug, PartialEq)]
pub enum ReviewStreamScope {
    Subject {
        subject_id: String,
        role: PubkyAppReviewRole,
    },
    Listing {
        listing_owner_id: String,
        listing_id: String,
    },
}

/// The viewer's social reach a filtered review stream keeps reviewers from.
#[derive(Deserialize, Serialize, ToSchema, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ReviewReach {
    /// Reviewers the viewer follows.
    Following,
    /// Reviewers the viewer follows who follow the viewer back.
    Friends,
    /// Reviewers within `depth` follow hops of the viewer.
    Wot,
}

/// The review stream orders.
#[derive(Deserialize, Serialize, ToSchema, Clone, Copy, Debug, Default, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ReviewSort {
    /// Newest indexed first (the unfiltered stream's order).
    #[default]
    Newest,
    /// Highest price weight first; reviews without one last.
    Value,
    /// Highest `ratings.overall` first.
    RatingDesc,
    /// Lowest `ratings.overall` first.
    RatingAsc,
}

/// Most labels `tagged_by_viewer` may carry.
pub const MAX_TAGGED_BY_VIEWER_LABELS: usize = 5;
/// The follow hops `reach=wot` takes without `depth`.
pub const DEFAULT_WOT_DEPTH: u8 = 2;
/// The deepest `reach=wot` traversal.
pub const MAX_WOT_DEPTH: u8 = 3;

/// Which reviewers a filtered review stream keeps, relative to the viewer.
/// The graph only holds follows and tags between indexed users.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ReviewerFilter {
    pub viewer_id: Option<String>,
    pub reach: Option<ReviewReach>,
    /// Follow hops for [`ReviewReach::Wot`].
    pub depth: u8,
    /// Labels of the viewer's own tags on reviewers (any label matches);
    /// empty keeps every reviewer.
    pub tagged_by_viewer: Vec<String>,
}

impl ReviewerFilter {
    /// True when the viewer's follows or tags narrow the reviewers.
    pub fn uses_viewer(&self) -> bool {
        self.reach.is_some() || !self.tagged_by_viewer.is_empty()
    }
}

/// A filtered, sorted review stream request.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ReviewStreamFilter {
    pub reviewers: ReviewerFilter,
    /// Keeps only reviews whose attestation one of these windows covers.
    pub attestors: Option<Vec<AttestorWindow>>,
    pub sort: ReviewSort,
}

/// The raw review stream parameters, before validation.
#[derive(Clone, Debug, Default)]
pub struct ReviewStreamParams<'a> {
    pub viewer_id: Option<&'a str>,
    pub reach: Option<ReviewReach>,
    pub depth: Option<&'a str>,
    pub tagged_by_viewer: Option<&'a str>,
    pub attestors: Option<&'a str>,
    pub sort: Option<ReviewSort>,
}

impl ReviewStreamFilter {
    /// Validates the parameters. `Ok(None)` when none is given: the plain
    /// stream. `reach` and `tagged_by_viewer` need `viewer_id`; `depth`
    /// (1-3, default 2) needs `reach=wot`; `tagged_by_viewer` takes 1-5
    /// comma-separated labels, compared as tag labels are stored (trimmed,
    /// lowercase).
    pub fn parse(params: ReviewStreamParams) -> Result<Option<Self>, String> {
        let ReviewStreamParams {
            viewer_id,
            reach,
            depth,
            tagged_by_viewer,
            attestors,
            sort,
        } = params;
        if viewer_id.is_none()
            && reach.is_none()
            && depth.is_none()
            && tagged_by_viewer.is_none()
            && attestors.is_none()
            && sort.is_none()
        {
            return Ok(None);
        }

        let viewer_id = viewer_id
            .map(|viewer_id| {
                PubkyId::try_from(viewer_id)
                    .map(|_| viewer_id.to_string())
                    .map_err(|_| "viewer_id must be a pubky".to_string())
            })
            .transpose()?;
        let depth = match (depth, reach) {
            (None, _) => DEFAULT_WOT_DEPTH,
            (Some(depth), Some(ReviewReach::Wot)) => depth
                .parse::<u8>()
                .ok()
                .filter(|depth| (1..=MAX_WOT_DEPTH).contains(depth))
                .ok_or_else(|| format!("depth must be between 1 and {MAX_WOT_DEPTH}"))?,
            (Some(_), _) => return Err("depth applies to reach=wot only".to_string()),
        };
        let tagged_by_viewer = match tagged_by_viewer {
            None => Vec::new(),
            Some(labels) => {
                let labels: Vec<String> = labels
                    .split(',')
                    .map(|label| label.trim().to_lowercase())
                    .filter(|label| !label.is_empty())
                    .collect();
                if labels.is_empty() || labels.len() > MAX_TAGGED_BY_VIEWER_LABELS {
                    return Err(format!(
                        "tagged_by_viewer takes 1 to {MAX_TAGGED_BY_VIEWER_LABELS} labels"
                    ));
                }
                labels
            }
        };
        let reviewers = ReviewerFilter {
            viewer_id,
            reach,
            depth,
            tagged_by_viewer,
        };
        if reviewers.uses_viewer() && reviewers.viewer_id.is_none() {
            return Err("reach and tagged_by_viewer need viewer_id".to_string());
        }
        let attestors = attestors.map(AttestorWindow::parse_list).transpose()?;
        Ok(Some(ReviewStreamFilter {
            reviewers,
            attestors,
            sort: sort.unwrap_or_default(),
        }))
    }

    /// Keeps the rows the attestor windows cover, sorts them, and returns the
    /// `(reviewer_id, review_id)` keys of the page with the figures of the
    /// counted matches. `seller_score` adds the RV2 seller figures.
    fn apply(
        &self,
        rows: Vec<FilteredReviewRow>,
        skip: Option<usize>,
        limit: Option<usize>,
        seller_score: bool,
    ) -> (Vec<(String, String)>, ReviewMatch) {
        let mut rows: Vec<FilteredReviewRow> = match &self.attestors {
            None => rows,
            Some(windows) => rows
                .into_iter()
                .filter(|row| match (&row.attestor_id, row.iat) {
                    (Some(attestor_id), Some(iat)) => windows
                        .iter()
                        .any(|window| window.covers_attestation(attestor_id, iat)),
                    _ => false,
                })
                .collect(),
        };

        let counted: Vec<CountedReview> = rows.iter().filter_map(|row| row.counted()).collect();
        let matched = ReviewMatch::from_bucket(&ReputationBucket::compute(&counted, seller_score));

        rows.sort_by(|a, b| self.sort.compare(a, b));
        let page = rows
            .into_iter()
            .skip(skip.unwrap_or(0))
            .take(limit.unwrap_or(usize::MAX))
            .map(|row| (row.reviewer_id, row.review_id))
            .collect();
        (page, matched)
    }
}

impl ReviewSort {
    fn compare(self, a: &FilteredReviewRow, b: &FilteredReviewRow) -> Ordering {
        let newest = (b.indexed_at, &b.reviewer_id, &b.review_id).cmp(&(
            a.indexed_at,
            &a.reviewer_id,
            &a.review_id,
        ));
        match self {
            ReviewSort::Newest => newest,
            ReviewSort::Value => b.price_weight.cmp(&a.price_weight).then(newest),
            ReviewSort::RatingDesc => b.rating_overall.cmp(&a.rating_overall).then(newest),
            ReviewSort::RatingAsc => a.rating_overall.cmp(&b.rating_overall).then(newest),
        }
    }
}

/// One review of a filtered stream's scope, as the graph returns it.
#[derive(Clone, Debug, PartialEq)]
struct FilteredReviewRow {
    reviewer_id: String,
    review_id: String,
    indexed_at: i64,
    rating_overall: i64,
    price_weight: Option<i64>,
    attestor_id: Option<String>,
    order_ref: Option<String>,
    iat: Option<i64>,
    /// Verified with an `iat`, and not excluded by moderation.
    counted: bool,
    /// Whether this review carries its order's counted seller rating.
    seller_fields_counted: bool,
    rating_shipping: Option<i64>,
    rating_communication: Option<i64>,
}

impl FilteredReviewRow {
    fn counted(&self) -> Option<CountedReview> {
        let (true, Some(attestor_id), Some(order_ref), Some(iat)) =
            (self.counted, &self.attestor_id, &self.order_ref, self.iat)
        else {
            return None;
        };
        Some(CountedReview {
            reviewer_id: self.reviewer_id.clone(),
            attestor_id: attestor_id.clone(),
            order_ref: order_ref.clone(),
            rating_overall: self.rating_overall,
            price_weight: self
                .price_weight
                .and_then(|weight| u8::try_from(weight).ok())
                .unwrap_or(1),
            iat,
            seller_fields_counted: self.seller_fields_counted,
            rating_shipping: self.rating_shipping,
            rating_communication: self.rating_communication,
        })
    }
}

/// The counted figures of a filtered stream's matches (counting rules v2,
/// as [`ReputationBucket`]): distinct buyers, distinct attested orders
/// `(attestor, order_ref)`, and the mean over buyers of each buyer's mean
/// `ratings.overall` (`null` when nothing counts). Unverified and excluded
/// reviews are listed but never counted.
///
/// On the seller scope (the `buyer_reviewing_seller` subject stream) it also
/// carries the RV2 seller rating, as [`ReputationBucket`]'s seller figures:
/// `seller_buyers` buyers with an order carrying shipping or communication,
/// and `seller_avg`, the mean over them of their mean order score
/// ((shipping + communication) / 2, or the one present), `null` when no
/// buyer has one. Both keys are absent on other scopes.
#[derive(Serialize, Deserialize, ToSchema, Clone, Debug, PartialEq)]
pub struct ReviewMatch {
    pub buyers: u64,
    pub orders: u64,
    pub avg: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub seller_buyers: Option<u64>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "present_option"
    )]
    #[schema(value_type = Option<f64>)]
    pub seller_avg: Option<Option<f64>>,
}

/// Reads a present key, `null` included, as `Some`: an absent key stays
/// `None` through `#[serde(default)]`.
fn present_option<'de, D>(deserializer: D) -> Result<Option<Option<f64>>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Option::<f64>::deserialize(deserializer).map(Some)
}

impl ReviewMatch {
    fn from_bucket(bucket: &ReputationBucket) -> Self {
        let seller_avg = match (bucket.seller_buyers, bucket.sum_seller_buyer_means) {
            (Some(buyers), Some(sum)) => Some((buyers > 0).then(|| sum / buyers as f64)),
            _ => None,
        };
        ReviewMatch {
            buyers: bucket.buyers,
            orders: bucket.orders,
            avg: (bucket.buyers > 0).then(|| bucket.sum_buyer_means / bucket.buyers as f64),
            seller_buyers: bucket.seller_buyers,
            seller_avg,
        }
    }
}

/// A filtered review stream page: the page and the figures of every match.
#[derive(Serialize, Deserialize, ToSchema, Debug)]
pub struct FilteredReviewStream {
    pub reviews: Vec<ReviewView>,
    pub matched: ReviewMatch,
}

/// The review role stored on a `REVIEWED` edge.
fn role_from_graph(role: &str) -> Option<PubkyAppReviewRole> {
    [
        PubkyAppReviewRole::BuyerReviewingSeller,
        PubkyAppReviewRole::SellerReviewingBuyer,
    ]
    .into_iter()
    .find(|candidate| candidate.as_str() == role)
}

/// True when `updated_at` is more than the marketplace edit window after
/// `created_at`. Unparseable timestamps (already spec-validated upstream)
/// degrade to `false` rather than mislabeling the record.
fn is_edited_late(created_at: &str, updated_at: &str) -> bool {
    let (Ok(created), Ok(updated)) = (
        chrono::DateTime::parse_from_rfc3339(created_at),
        chrono::DateTime::parse_from_rfc3339(updated_at),
    ) else {
        return false;
    };
    (updated - created).num_seconds() > REVIEW_EDIT_WINDOW_SECONDS
}

#[cfg(test)]
mod filter_tests {
    use super::*;
    use std::sync::LazyLock;

    static ATTESTOR: LazyLock<String> =
        LazyLock::new(|| pubky::Keypair::random().public_key().to_z32());

    fn params<'a>() -> ReviewStreamParams<'a> {
        ReviewStreamParams::default()
    }

    #[allow(clippy::too_many_arguments)]
    fn row(
        reviewer: &str,
        review: &str,
        indexed_at: i64,
        rating: i64,
        weight: Option<i64>,
        order: Option<&str>,
        iat: Option<i64>,
        counted: bool,
    ) -> FilteredReviewRow {
        FilteredReviewRow {
            reviewer_id: reviewer.to_string(),
            review_id: review.to_string(),
            indexed_at,
            rating_overall: rating,
            price_weight: weight,
            attestor_id: order.map(|_| ATTESTOR.clone()),
            order_ref: order.map(str::to_string),
            iat,
            counted,
            seller_fields_counted: true,
            rating_shipping: None,
            rating_communication: None,
        }
    }

    fn rows() -> Vec<FilteredReviewRow> {
        vec![
            row("b1", "r1", 100, 5, Some(1), Some("o1"), Some(1_000), true),
            row("b1", "r2", 300, 3, Some(4), Some("o2"), Some(3_000), true),
            row("b2", "r3", 200, 1, Some(2), Some("o3"), Some(2_000), true),
            row("b3", "r4", 400, 4, None, None, None, false),
            // Verified but excluded by moderation: listed, never counted.
            row("b4", "r5", 50, 2, Some(5), Some("o5"), Some(500), false),
        ]
    }

    fn ids(page: &[(String, String)]) -> Vec<&str> {
        page.iter().map(|(_, review)| review.as_str()).collect()
    }

    #[test]
    fn no_parameter_keeps_the_plain_stream() {
        assert_eq!(ReviewStreamFilter::parse(params()), Ok(None));
    }

    #[test]
    fn parameters_validate() {
        let viewer = Some(ATTESTOR.as_str());
        let filter = ReviewStreamFilter::parse(ReviewStreamParams {
            viewer_id: viewer,
            reach: Some(ReviewReach::Wot),
            tagged_by_viewer: Some(" Seller , trusted,,"),
            ..params()
        })
        .unwrap()
        .unwrap();
        assert_eq!(filter.reviewers.depth, DEFAULT_WOT_DEPTH);
        assert_eq!(filter.reviewers.tagged_by_viewer, ["seller", "trusted"]);
        assert_eq!(filter.sort, ReviewSort::Newest);

        let sort_only = ReviewStreamFilter::parse(ReviewStreamParams {
            sort: Some(ReviewSort::Value),
            ..params()
        })
        .unwrap()
        .unwrap();
        assert!(!sort_only.reviewers.uses_viewer());

        for invalid in [
            ReviewStreamParams {
                reach: Some(ReviewReach::Following),
                ..params()
            },
            ReviewStreamParams {
                tagged_by_viewer: Some("seller"),
                ..params()
            },
            ReviewStreamParams {
                viewer_id: Some("not-a-pubky"),
                ..params()
            },
            ReviewStreamParams {
                viewer_id: viewer,
                reach: Some(ReviewReach::Wot),
                depth: Some("4"),
                ..params()
            },
            ReviewStreamParams {
                viewer_id: viewer,
                reach: Some(ReviewReach::Following),
                depth: Some("1"),
                ..params()
            },
            ReviewStreamParams {
                viewer_id: viewer,
                tagged_by_viewer: Some(" , "),
                ..params()
            },
            ReviewStreamParams {
                viewer_id: viewer,
                tagged_by_viewer: Some("a,b,c,d,e,f"),
                ..params()
            },
            ReviewStreamParams {
                attestors: Some(""),
                ..params()
            },
        ] {
            assert!(
                ReviewStreamFilter::parse(invalid.clone()).is_err(),
                "{invalid:?}"
            );
        }
    }

    #[test]
    fn every_sort_orders_the_matches_with_newest_ties() {
        let order = |sort| {
            let filter = ReviewStreamFilter {
                sort,
                ..Default::default()
            };
            let (page, _) = filter.apply(rows(), None, None, false);
            ids(&page).join(",")
        };
        assert_eq!(order(ReviewSort::Newest), "r4,r2,r3,r1,r5");
        assert_eq!(order(ReviewSort::Value), "r5,r2,r3,r1,r4");
        assert_eq!(order(ReviewSort::RatingDesc), "r1,r4,r2,r5,r3");
        assert_eq!(order(ReviewSort::RatingAsc), "r3,r5,r2,r4,r1");
    }

    #[test]
    fn matched_counts_every_counted_match_and_pages_do_not_change_it() {
        let filter = ReviewStreamFilter::default();
        let (page, matched) = filter.apply(rows(), Some(1), Some(2), false);
        assert_eq!(ids(&page), ["r2", "r3"]);
        // b1: (5 + 3) / 2, b2: 1; the unverified and excluded rows don't count.
        assert_eq!(
            matched,
            ReviewMatch {
                buyers: 2,
                orders: 3,
                avg: Some((4.0 + 1.0) / 2.0),
                seller_buyers: None,
                seller_avg: None
            }
        );
    }

    #[test]
    fn attestor_windows_keep_only_covered_reviews() {
        let filter = ReviewStreamFilter {
            attestors: Some(
                AttestorWindow::parse_list(&format!("{}:400:2500", *ATTESTOR)).unwrap(),
            ),
            ..Default::default()
        };
        let (page, matched) = filter.apply(rows(), None, None, false);
        // r5 is covered and listed, but excluded from the counts.
        assert_eq!(ids(&page), ["r3", "r1", "r5"]);
        assert_eq!(
            matched,
            ReviewMatch {
                buyers: 2,
                orders: 2,
                avg: Some(3.0),
                seller_buyers: None,
                seller_avg: None
            }
        );

        let nothing = ReviewStreamFilter {
            attestors: Some(AttestorWindow::parse_list(&format!("{}:9000:", *ATTESTOR)).unwrap()),
            ..Default::default()
        };
        let (page, matched) = nothing.apply(rows(), None, None, false);
        assert!(page.is_empty());
        assert_eq!(
            matched,
            ReviewMatch {
                buyers: 0,
                orders: 0,
                avg: None,
                seller_buyers: None,
                seller_avg: None
            }
        );
    }

    #[test]
    fn the_seller_scope_adds_the_rv2_seller_score() {
        let mut rows = rows();
        // b1 rated shipping 4 on one order and both (5, 2) on the other;
        // b2 rated communication 2 only; b4's copy is excluded.
        rows[0].rating_shipping = Some(4);
        rows[1].rating_shipping = Some(5);
        rows[1].rating_communication = Some(2);
        rows[2].rating_communication = Some(2);
        rows[4].rating_shipping = Some(1);
        let (_, matched) = ReviewStreamFilter::default().apply(rows.clone(), None, None, true);
        assert_eq!(matched.seller_buyers, Some(2));
        assert_eq!(
            matched.seller_avg,
            Some(Some(((4.0 + 3.5) / 2.0 + 2.0) / 2.0))
        );
        let json = serde_json::to_value(&matched).unwrap();
        assert_eq!(json["seller_buyers"], 2);

        // A copy not flagged as its order's seller rating does not count.
        rows[2].seller_fields_counted = false;
        let (_, matched) = ReviewStreamFilter::default().apply(rows.clone(), None, None, true);
        assert_eq!(matched.seller_buyers, Some(1));

        // Legacy reviews without seller fields: the keys stay, avg is null.
        let (_, legacy) = ReviewStreamFilter::default().apply(self::rows(), None, None, true);
        assert_eq!(
            (legacy.seller_buyers, legacy.seller_avg),
            (Some(0), Some(None))
        );
        let json = serde_json::to_value(&legacy).unwrap();
        assert_eq!(json.get("seller_avg"), Some(&serde_json::Value::Null));
        assert_eq!(serde_json::from_value::<ReviewMatch>(json).unwrap(), legacy);

        // Other scopes leave both keys out.
        let (_, listing) = ReviewStreamFilter::default().apply(rows, None, None, false);
        let json = serde_json::to_value(&listing).unwrap();
        assert!(json.get("seller_buyers").is_none() && json.get("seller_avg").is_none());
        assert_eq!(
            serde_json::from_value::<ReviewMatch>(json).unwrap(),
            listing
        );
    }
}
