use crate::routes::v0::endpoints::{LISTING_MODERATION_ROUTE, LISTING_ROUTE};
use crate::{Error, Result};
use axum::extract::{Path, Query};
use axum::Json;
use nexus_common::models::marketplace::{
    AttestorWindow, ListingDetailView, ListingDetails, ListingModerationState, ListingSaleFormat,
    ListingStreamEntry, ReputationBucket, ReputationSummary,
};
use pubky_app_specs::{
    PubkyAppFulfillmentMethod, PubkyAppListingCondition, PubkyAppListingState, PubkyAppReviewRole,
};
use serde::Deserialize;
use tracing::debug;
use utoipa::{OpenApi, ToSchema};

#[derive(Deserialize, Debug, Default, ToSchema)]
pub struct ListingDetailsQuery {
    /// Trusted attestors with optional trust windows:
    /// `<pubky>[:<from>:<until>],…` (see the route description).
    pub attestors: Option<String>,
}

#[utoipa::path(
    get,
    path = LISTING_ROUTE,
    description = "Listing details together with the compact seller and listing reputation objects. \
Reputation fields are absent (not zero) when no review is indexed for the scope. \
An `active` auction whose `auction_ends_at` has been reached is served with `state` `ended`. \
With `attestors=<pubky>[:<from>:<until>],…` (at most 20 entries; `from` and `until` are attestation \
`iat` seconds, window `[from, until)`, either end may be empty; a pubky may repeat for several windows), \
the response adds `scoped_reputation` (the seller) and `scoped_listing_reputation` (this listing): the \
exact counting figures of the counted reviews inside any window, one buyer counted once across \
attestors. Both are present (zero counts when nothing matches) whenever the parameter is given.",

    tag = "Marketplace",
    params(
        ("seller_id" = String, Path, description = "Seller Pubky ID"),
        ("listing_id" = String, Path, description = "Listing Crockford32 ID"),
        ("attestors" = Option<String>, Query, description = "Trusted attestors: <pubky>[:<from>:<until>],…"),
    ),
    responses(
        (status = 200, description = "Listing details", body = ListingDetailView),
        (status = 400, description = "Malformed attestors parameter"),
        (status = 404, description = "Listing not found"),
        (status = 500, description = "Internal server error")
    )
)]
pub async fn listing_details_handler(
    Path((seller_id, listing_id)): Path<(String, String)>,
    Query(query): Query<ListingDetailsQuery>,
) -> Result<Json<ListingDetailView>> {
    debug!("GET {LISTING_ROUTE} seller_id:{seller_id}, listing_id:{listing_id}");

    let windows = query
        .attestors
        .as_deref()
        .map(AttestorWindow::parse_list)
        .transpose()
        .map_err(|message| Error::invalid_input(&message))?;

    let Some(details) = ListingDetails::get_by_id(&seller_id, &listing_id).await? else {
        return Err(Error::ListingNotFound {
            seller_id,
            listing_id,
        });
    };

    let (seller_reputation, listing_reputation) = tokio::join!(
        ReputationSummary::get_by_subject(&seller_id, PubkyAppReviewRole::BuyerReviewingSeller),
        ReputationSummary::get_by_listing(&seller_id, &listing_id),
    );

    let (scoped_reputation, scoped_listing_reputation) = match &windows {
        Some(windows) => {
            let (seller, listing) = tokio::join!(
                ReputationSummary::scoped_by_subject(
                    &seller_id,
                    PubkyAppReviewRole::BuyerReviewingSeller,
                    windows,
                ),
                ReputationSummary::scoped_by_listing(&seller_id, &listing_id, windows),
            );
            (Some(seller?), Some(listing?))
        }
        None => (None, None),
    };

    Ok(Json(ListingDetailView {
        entry: ListingStreamEntry {
            details,
            reputation: seller_reputation?.map(|summary| summary.snippet()),
            listing_reputation: listing_reputation?.map(|summary| summary.snippet()),
        },
        scoped_reputation,
        scoped_listing_reputation,
    }))
}

#[utoipa::path(
    get,
    path = LISTING_MODERATION_ROUTE,
    description = "Whether the configured moderator's tags keep a listing out of the marketplace \
index, because a tag hides the listing itself or hides its seller (which hides the seller's shop and \
every listing). A listing that is not moderated, including one the index has never seen, answers \
`moderated: false`. A transaction service reads this before it sells, takes a bid on, or takes an \
offer on a listing.",
    tag = "Marketplace",
    params(
        ("seller_id" = String, Path, description = "Seller Pubky ID"),
        ("listing_id" = String, Path, description = "Listing Crockford32 ID")
    ),
    responses(
        (status = 200, description = "Moderation state of the listing", body = ListingModerationState),
        (status = 500, description = "Internal server error")
    )
)]
pub async fn listing_moderation_handler(
    Path((seller_id, listing_id)): Path<(String, String)>,
) -> Result<Json<ListingModerationState>> {
    debug!("GET {LISTING_MODERATION_ROUTE} seller_id:{seller_id}, listing_id:{listing_id}");

    Ok(Json(
        ListingModerationState::get(&seller_id, &listing_id).await?,
    ))
}

#[derive(OpenApi)]
#[openapi(
    paths(listing_details_handler, listing_moderation_handler),
    components(schemas(
        ListingDetails,
        ListingDetailView,
        ListingModerationState,
        ListingStreamEntry,
        ReputationBucket,
        ListingSaleFormat,
        PubkyAppListingState,
        PubkyAppListingCondition,
        PubkyAppFulfillmentMethod
    ))
)]
pub struct ListingDetailsApiDoc;

#[cfg(test)]
mod tests {
    use super::ListingDetailsApiDoc;
    use utoipa::OpenApi;

    #[test]
    fn listing_openapi_omits_reserve_fields() {
        let openapi = serde_json::to_value(ListingDetailsApiDoc::openapi()).expect("OpenAPI JSON");
        let schema = openapi.to_string();
        for forbidden in [
            "auction_reserve_price_minor",
            "reservePrice",
            "reserve_price",
            "reserveMet",
            "reserve_met",
        ] {
            assert!(
                !schema.contains(forbidden),
                "OpenAPI must not expose {forbidden}"
            );
        }
    }
}
