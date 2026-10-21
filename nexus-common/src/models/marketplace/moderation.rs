use crate::db::{
    exec_single_row, fetch_all_rows_from_graph, fetch_row_from_graph, queries, GraphResult,
};
use crate::models::error::ModelResult;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// Moderation marker hiding a marketplace listing.
///
/// A marker is written when the configured moderator tags a listing URI with a
/// moderated label, and is keyed by the moderator's tag record, so the
/// moderator removing that tag removes exactly that marker. It lives in the
/// graph as a standalone node that references the listing only by
/// `(owner_id, listing_id)`: it does not depend on the listing node or on the
/// seller `User` node, so it holds whether the tag event is replayed before or
/// after the listing event, and it survives deletion and re-creation of the
/// listing. A listing with at least one marker is never indexed.
pub struct ModeratedListing;

impl ModeratedListing {
    /// Records (or refreshes) the marker written by one moderator tag.
    pub async fn put(
        owner_id: &str,
        listing_id: &str,
        uri: &str,
        moderator_id: &str,
        tag_id: &str,
        label: &str,
        moderated_at: i64,
    ) -> GraphResult<()> {
        exec_single_row(queries::put::put_moderated_listing(
            owner_id,
            listing_id,
            uri,
            moderator_id,
            tag_id,
            label,
            moderated_at,
        ))
        .await
    }

    /// Whether any moderator tag currently hides the listing.
    pub async fn is_moderated(owner_id: &str, listing_id: &str) -> GraphResult<bool> {
        let row = fetch_row_from_graph(queries::get::listing_moderation_marker(
            owner_id, listing_id,
        ))
        .await?;
        Ok(row.is_some())
    }

    /// Whether a moderator tag other than `(moderator_id, tag_id)` hides the listing.
    pub async fn is_moderated_besides(
        owner_id: &str,
        listing_id: &str,
        moderator_id: &str,
        tag_id: &str,
    ) -> GraphResult<bool> {
        let row = fetch_row_from_graph(queries::get::listing_moderation_marker_besides(
            owner_id,
            listing_id,
            moderator_id,
            tag_id,
        ))
        .await?;
        Ok(row.is_some())
    }

    /// The `(owner_id, listing_id)` pairs hidden by one moderator tag record.
    pub async fn listings_of_tag(
        moderator_id: &str,
        tag_id: &str,
    ) -> GraphResult<Vec<(String, String)>> {
        let rows = fetch_all_rows_from_graph(queries::get::moderated_listings_of_tag(
            moderator_id,
            tag_id,
        ))
        .await?;
        let mut listings = Vec::with_capacity(rows.len());
        for row in rows {
            if let (Some(owner_id), Some(listing_id)) =
                (row.get("owner_id")?, row.get("listing_id")?)
            {
                listings.push((owner_id, listing_id));
            }
        }
        Ok(listings)
    }

    /// Removes the marker written by one moderator tag record.
    pub async fn delete(moderator_id: &str, tag_id: &str) -> GraphResult<()> {
        exec_single_row(queries::del::delete_moderated_listing(moderator_id, tag_id)).await
    }
}

/// Moderation marker hiding a user's marketplace shop and listings.
///
/// Written when the configured moderator tags a user's profile URI with a
/// moderated label. It is keyed by the moderator's tag record, so the
/// moderator removing that tag removes exactly that marker, and it is a
/// standalone graph node that references the user only by id: it holds
/// whether the tag event is replayed before or after the user's profile,
/// shop and listing events. A shop or listing whose owner has at least one
/// marker is never indexed.
pub struct ModeratedUser;

impl ModeratedUser {
    /// Records (or refreshes) the marker written by one moderator tag.
    pub async fn put(
        user_id: &str,
        uri: &str,
        moderator_id: &str,
        tag_id: &str,
        label: &str,
        moderated_at: i64,
    ) -> GraphResult<()> {
        exec_single_row(queries::put::put_moderated_user(
            user_id,
            uri,
            moderator_id,
            tag_id,
            label,
            moderated_at,
        ))
        .await
    }

    /// Whether any moderator tag currently hides the user's shop and listings.
    pub async fn is_moderated(user_id: &str) -> GraphResult<bool> {
        let row = fetch_row_from_graph(queries::get::user_moderation_marker(user_id)).await?;
        Ok(row.is_some())
    }

    /// Whether a moderator tag other than `(moderator_id, tag_id)` hides the user.
    pub async fn is_moderated_besides(
        user_id: &str,
        moderator_id: &str,
        tag_id: &str,
    ) -> GraphResult<bool> {
        let row = fetch_row_from_graph(queries::get::user_moderation_marker_besides(
            user_id,
            moderator_id,
            tag_id,
        ))
        .await?;
        Ok(row.is_some())
    }

    /// The ids of the users hidden by one moderator tag record.
    pub async fn users_of_tag(moderator_id: &str, tag_id: &str) -> GraphResult<Vec<String>> {
        let rows =
            fetch_all_rows_from_graph(queries::get::moderated_users_of_tag(moderator_id, tag_id))
                .await?;
        let mut users = Vec::with_capacity(rows.len());
        for row in rows {
            if let Some(user_id) = row.get("user_id")? {
                users.push(user_id);
            }
        }
        Ok(users)
    }

    /// Flags the marker of one moderator tag record as being released: review
    /// moderation stops applying it, the shop and listings stay hidden until
    /// [`Self::delete`].
    pub async fn mark_releasing(moderator_id: &str, tag_id: &str) -> GraphResult<()> {
        exec_single_row(queries::put::mark_moderated_user_releasing(
            moderator_id,
            tag_id,
        ))
        .await
    }

    /// Removes the marker written by one moderator tag record.
    pub async fn delete(moderator_id: &str, tag_id: &str) -> GraphResult<()> {
        exec_single_row(queries::del::delete_moderated_user(moderator_id, tag_id)).await
    }
}

/// Moderation marker on one marketplace review.
///
/// Written when the configured moderator tags a review URI with a moderated
/// label (the review's text is hidden) or a review-exclusion label (the review
/// leaves the aggregates). Like the listing marker it is keyed by the
/// moderator's tag record and is a standalone node that references the review
/// only by `(reviewer_id, review_id)`: it holds whether the tag event is
/// replayed before or after the review, and across edits of the review.
/// Nothing is deleted; the review's `moderation` is derived from the markers.
pub struct ModeratedReview;

impl ModeratedReview {
    /// Records (or refreshes) the marker written by one moderator tag.
    #[allow(clippy::too_many_arguments)]
    pub async fn put(
        reviewer_id: &str,
        review_id: &str,
        uri: &str,
        moderator_id: &str,
        tag_id: &str,
        label: &str,
        state: ReviewModerationState,
        moderated_at: i64,
    ) -> GraphResult<()> {
        exec_single_row(queries::put::put_moderated_review(
            reviewer_id,
            review_id,
            uri,
            moderator_id,
            tag_id,
            label,
            state.as_str(),
            moderated_at,
        ))
        .await
    }

    /// The `(reviewer_id, review_id)` pairs marked by one moderator tag record.
    pub async fn reviews_of_tag(
        moderator_id: &str,
        tag_id: &str,
    ) -> GraphResult<Vec<(String, String)>> {
        let rows =
            fetch_all_rows_from_graph(queries::get::moderated_reviews_of_tag(moderator_id, tag_id))
                .await?;
        let mut reviews = Vec::with_capacity(rows.len());
        for row in rows {
            if let (Some(reviewer_id), Some(review_id)) =
                (row.get("reviewer_id")?, row.get("review_id")?)
            {
                reviews.push((reviewer_id, review_id));
            }
        }
        Ok(reviews)
    }

    /// Flags the marker of one moderator tag record as being released: review
    /// moderation stops applying it.
    pub async fn mark_releasing(moderator_id: &str, tag_id: &str) -> GraphResult<()> {
        exec_single_row(queries::put::mark_moderated_review_releasing(
            moderator_id,
            tag_id,
        ))
        .await
    }

    /// Removes the marker written by one moderator tag record.
    pub async fn delete(moderator_id: &str, tag_id: &str) -> GraphResult<()> {
        exec_single_row(queries::del::delete_moderated_review(moderator_id, tag_id)).await
    }
}

/// Moderation marker excluding every review a user wrote from the aggregates
/// (a fraud ring), written when the configured moderator tags the user's
/// profile URI with a review-exclusion label. It does not hide the user, their
/// shop or their listings. Keyed by the moderator's tag record and standalone,
/// like the other markers, so it also holds for reviews indexed after it.
pub struct ExcludedReviewer;

impl ExcludedReviewer {
    /// Records (or refreshes) the marker written by one moderator tag.
    pub async fn put(
        user_id: &str,
        uri: &str,
        moderator_id: &str,
        tag_id: &str,
        label: &str,
        moderated_at: i64,
    ) -> GraphResult<()> {
        exec_single_row(queries::put::put_excluded_reviewer(
            user_id,
            uri,
            moderator_id,
            tag_id,
            label,
            moderated_at,
        ))
        .await
    }

    /// The ids of the users whose reviews one moderator tag record excludes.
    pub async fn users_of_tag(moderator_id: &str, tag_id: &str) -> GraphResult<Vec<String>> {
        let rows = fetch_all_rows_from_graph(queries::get::excluded_reviewers_of_tag(
            moderator_id,
            tag_id,
        ))
        .await?;
        let mut users = Vec::with_capacity(rows.len());
        for row in rows {
            if let Some(user_id) = row.get("user_id")? {
                users.push(user_id);
            }
        }
        Ok(users)
    }

    /// Flags the marker of one moderator tag record as being released: review
    /// moderation stops applying it.
    pub async fn mark_releasing(moderator_id: &str, tag_id: &str) -> GraphResult<()> {
        exec_single_row(queries::put::mark_excluded_reviewer_releasing(
            moderator_id,
            tag_id,
        ))
        .await
    }

    /// Removes the marker written by one moderator tag record.
    pub async fn delete(moderator_id: &str, tag_id: &str) -> GraphResult<()> {
        exec_single_row(queries::del::delete_excluded_reviewer(moderator_id, tag_id)).await
    }
}

/// What moderation does to a review.
#[derive(Serialize, Deserialize, ToSchema, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ReviewModerationState {
    /// The review's text is withheld; its ratings still count.
    TextHidden,
    /// The review is left out of every aggregate and its text is withheld.
    Excluded,
}

impl ReviewModerationState {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::TextHidden => "text_hidden",
            Self::Excluded => "excluded",
        }
    }

    fn from_graph(value: &str) -> Option<Self> {
        match value {
            "text_hidden" => Some(Self::TextHidden),
            "excluded" => Some(Self::Excluded),
            _ => None,
        }
    }
}

/// The moderation applied to a review: its state and the moderator's label as
/// the reason.
#[derive(Serialize, Deserialize, ToSchema, Clone, Debug, PartialEq, Eq)]
pub struct ReviewModeration {
    pub state: ReviewModerationState,
    /// The moderator tag's label, e.g. `hatespeech` or `review_fraud`.
    pub reason: String,
}

impl ReviewModeration {
    /// Derives a review's moderation from the markers that apply to it: a
    /// marker on the review, a review-exclusion marker on its reviewer, or a
    /// moderation marker hiding its reviewer (which hides the text, ratings
    /// kept). Exclusion wins over hidden text; among markers of one state the
    /// lowest tag id gives the reason, so a replay picks the same one.
    /// Markers flagged as being released are left out, in every process, from
    /// the moment the release starts.
    pub async fn get(reviewer_id: &str, review_id: &str) -> GraphResult<Option<Self>> {
        let row =
            fetch_row_from_graph(queries::get::review_moderation(reviewer_id, review_id)).await?;
        let Some(row) = row else {
            return Ok(None);
        };
        let state: String = row.get("state")?;
        let reason: String = row.get("reason")?;
        Ok(ReviewModerationState::from_graph(&state).map(|state| Self { state, reason }))
    }
}

/// Whether the moderation of pubky.app's moderator keeps a listing out of the
/// marketplace index, and which moderator tag does it.
#[derive(Serialize, Deserialize, ToSchema, Clone, Copy, Debug, PartialEq, Eq)]
pub struct ListingModerationState {
    /// The listing, or its seller, is moderated. A moderated listing must
    /// not be sold, bid on or offered on.
    pub moderated: bool,
    /// A moderator tag on the listing itself hides it.
    pub listing_moderated: bool,
    /// A moderator tag on the seller hides the seller's shop and every listing.
    pub seller_moderated: bool,
}

impl ListingModerationState {
    pub fn new(listing_moderated: bool, seller_moderated: bool) -> Self {
        Self {
            moderated: listing_moderated || seller_moderated,
            listing_moderated,
            seller_moderated,
        }
    }

    /// Reads the markers for a listing. It answers from the moderation
    /// markers only, so it is the same whether or not the listing is indexed:
    /// a listing the index never saw, or never will, can still be moderated.
    pub async fn get(owner_id: &str, listing_id: &str) -> ModelResult<Self> {
        let (listing_moderated, seller_moderated) = tokio::try_join!(
            ModeratedListing::is_moderated(owner_id, listing_id),
            ModeratedUser::is_moderated(owner_id),
        )?;
        Ok(Self::new(listing_moderated, seller_moderated))
    }
}

#[cfg(test)]
mod tests {
    use super::ListingModerationState;

    #[test]
    fn a_listing_is_moderated_when_either_marker_hides_it() {
        assert!(!ListingModerationState::new(false, false).moderated);
        assert!(ListingModerationState::new(true, false).moderated);
        assert!(ListingModerationState::new(false, true).moderated);
        assert!(ListingModerationState::new(true, true).moderated);
    }
}
