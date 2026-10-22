use crate::routes::v0::endpoints::{ATTESTOR_ANNOTATIONS_ROUTE, ATTESTOR_PROFILE_ROUTE};
use crate::{Error, Result};
use axum::extract::{Path, Query};
use axum::Json;
use nexus_common::models::marketplace::{
    AttestorAnnotationDetails, AttestorKeyWindow, AttestorProfileDetails, MAX_ANNOTATION_BATCH,
};
use pubky_app_specs::{PubkyAppAttestorOutcome, PubkyAppAttestorOutcomeKind, PubkyId};
use serde::Deserialize;
use tracing::debug;
use utoipa::{OpenApi, ToSchema};

#[derive(Deserialize, Debug, ToSchema)]
pub struct AttestorAnnotationsQuery {
    /// Comma-separated `order_ref`s, at most 100.
    pub order_refs: Option<String>,
}

fn parse_attestor_id(attestor_id: &str) -> Result<()> {
    PubkyId::try_from(attestor_id)
        .map(|_| ())
        .map_err(|e| Error::invalid_input(&format!("attestor_id: {e}")))
}

fn parse_order_refs(order_refs: Option<&str>) -> Result<Vec<String>> {
    let Some(order_refs) = order_refs.filter(|value| !value.is_empty()) else {
        return Err(Error::invalid_input("order_refs is required"));
    };
    let order_refs: Vec<String> = order_refs.split(',').map(str::to_string).collect();
    if order_refs.iter().any(String::is_empty) {
        return Err(Error::invalid_input(
            "order_refs must not hold an empty entry",
        ));
    }
    if order_refs.len() > MAX_ANNOTATION_BATCH {
        return Err(Error::invalid_input(&format!(
            "order_refs holds at most {MAX_ANNOTATION_BATCH} entries"
        )));
    }
    Ok(order_refs)
}

#[utoipa::path(
    get,
    path = ATTESTOR_PROFILE_ROUTE,
    description = r#"The attestor's profile, published at `/pub/pubky.app/marketplace/v1/attestor/profile` on the
attestor's own homeserver. Only a profile whose proof verifies against the attestor pubky is indexed.

`record` is the profile exactly as published, `proof` included, so a client can verify it offline against
`uri`. `key_windows` lists the profile's `keys` in record order with `valid_from`, `valid_until` and
`compromised_from` in seconds (rounded up), the unit of a review's `iat` and of `attestors=` windows.
A client may only narrow its own trust in a key from these windows, never add a key from them.

The index reads only the homeservers it is configured to poll: a profile published on any other homeserver
is not indexed."#,
    tag = "Marketplace",
    params(
        ("attestor_id" = String, Path, description = "Attestor Pubky ID"),
    ),
    responses(
        (status = 200, description = "Attestor profile", body = AttestorProfileDetails),
        (status = 400, description = "Malformed attestor_id"),
        (status = 404, description = "No profile indexed for this attestor"),
        (status = 500, description = "Internal server error")
    )
)]
pub async fn attestor_profile_handler(
    Path(attestor_id): Path<String>,
) -> Result<Json<AttestorProfileDetails>> {
    debug!("GET {ATTESTOR_PROFILE_ROUTE} attestor_id:{attestor_id}");

    parse_attestor_id(&attestor_id)?;
    match AttestorProfileDetails::get_from_index(&attestor_id).await? {
        Some(profile) => Ok(Json(profile)),
        None => Err(Error::AttestorProfileNotFound { attestor_id }),
    }
}

#[utoipa::path(
    get,
    path = ATTESTOR_ANNOTATIONS_ROUTE,
    description = r#"The attestor's annotations for a batch of orders, published at
`/pub/pubky.app/marketplace/v1/attestor/annotations/{order_ref}` on the attestor's own homeserver.

The response is an array aligned with `order_refs`: entry `i` answers the `i`-th requested `order_ref`, and is
`null` when no annotation for it is indexed. A repeated `order_ref` is answered at each position. Only an
annotation whose proof verifies against the attestor pubky is indexed, and a lower `revision` never replaces
a higher one. `outcomes` lists `{kind: "refunded" | "returned", on: "YYYY-MM-DD"}`; `record` is the
annotation exactly as published, `proof` included, so a client can verify it offline against `uri`.

400 when `attestor_id` is not a pubky, or `order_refs` is missing, empty, holds an empty entry or more
than 100 entries."#,
    tag = "Marketplace",
    params(
        ("attestor_id" = String, Path, description = "Attestor Pubky ID"),
        ("order_refs" = String, Query, description = "Comma-separated order_refs, at most 100"),
    ),
    responses(
        (status = 200, description = "Annotations aligned with order_refs, null where none is indexed", body = Vec<Option<AttestorAnnotationDetails>>),
        (status = 400, description = "Malformed attestor_id or order_refs"),
        (status = 500, description = "Internal server error")
    )
)]
pub async fn attestor_annotations_handler(
    Path(attestor_id): Path<String>,
    Query(query): Query<AttestorAnnotationsQuery>,
) -> Result<Json<Vec<Option<AttestorAnnotationDetails>>>> {
    debug!("GET {ATTESTOR_ANNOTATIONS_ROUTE} attestor_id:{attestor_id}");

    parse_attestor_id(&attestor_id)?;
    let order_refs = parse_order_refs(query.order_refs.as_deref())?;
    Ok(Json(
        AttestorAnnotationDetails::get_many(&attestor_id, &order_refs).await?,
    ))
}

#[derive(OpenApi)]
#[openapi(
    paths(attestor_profile_handler, attestor_annotations_handler),
    components(schemas(
        AttestorProfileDetails,
        AttestorKeyWindow,
        AttestorAnnotationDetails,
        PubkyAppAttestorOutcome,
        PubkyAppAttestorOutcomeKind
    ))
)]
pub struct AttestorApiDoc;

#[cfg(test)]
mod tests {
    use super::{parse_order_refs, MAX_ANNOTATION_BATCH};

    #[test]
    fn order_refs_keep_request_order_and_repeats() {
        let parsed = parse_order_refs(Some("b,a,b")).unwrap();
        assert_eq!(parsed, vec!["b", "a", "b"]);
    }

    #[test]
    fn malformed_order_refs_are_refused() {
        assert!(parse_order_refs(None).is_err());
        assert!(parse_order_refs(Some("")).is_err());
        assert!(parse_order_refs(Some("a,,b")).is_err());
        assert!(parse_order_refs(Some("a,")).is_err());
        let at_limit = vec!["a"; MAX_ANNOTATION_BATCH].join(",");
        assert_eq!(
            parse_order_refs(Some(&at_limit)).unwrap().len(),
            MAX_ANNOTATION_BATCH
        );
        let over = vec!["a"; MAX_ANNOTATION_BATCH + 1].join(",");
        assert!(parse_order_refs(Some(&over)).is_err());
    }
}
