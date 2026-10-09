use crate::db::{
    exec_single_row, fetch_all_rows_from_graph, fetch_row_from_graph, queries, GraphResult,
};

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
