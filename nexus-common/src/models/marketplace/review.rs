use super::{ReviewModeration, ReviewResponseDetails};
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
        let (verified, attestor_id, order_ref) =
            match PubkyAppPurchaseAttestation::verify_for_review(review) {
                Ok(attestation) => (
                    true,
                    Some(attestation.claims.iss.clone()),
                    Some(attestation.claims.order_ref.clone()),
                ),
                Err(_) => (false, None, None),
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
            return Ok(None);
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

        let views = reviews
            .into_iter()
            .zip(responses)
            .map(|(review, response)| ReviewView {
                review: review.public(),
                response,
            })
            .collect();

        Ok(Some(Self(views)))
    }
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
