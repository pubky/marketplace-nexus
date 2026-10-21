use crate::events::EventProcessorError;
use nexus_common::models::marketplace::{AttestorAnnotationDetails, AttestorProfileDetails};
use pubky_app_specs::{
    PubkyAppAttestorAnnotation, PubkyAppAttestorProfile, PubkyAppObject, PubkyId,
};
use serde_json::Value;
use tracing::debug;

/// Indexes an attestor profile or annotation fetched from `uri`.
///
/// The record is accepted only when its proof verifies against the pubky
/// that owns `uri` (and, for an annotation, when its `orderRef` is the path
/// ID), so a record signed by another key or copied to another path is
/// rejected and leaves the index unchanged. The verified blob is kept
/// verbatim, proof included, so clients can verify the indexed copy
/// offline.
pub async fn sync_put(
    uri: &str,
    blob: &[u8],
    attestor_id: PubkyId,
) -> Result<(), EventProcessorError> {
    debug!("Indexing attestor record: {uri}");

    let verified = PubkyAppObject::from_uri(uri, blob).map_err(EventProcessorError::generic)?;
    let record: Value = serde_json::from_slice(blob).map_err(EventProcessorError::generic)?;
    match verified {
        PubkyAppObject::AttestorProfile(profile) => put_profile(profile, record, attestor_id).await,
        PubkyAppObject::AttestorAnnotation(annotation) => {
            put_annotation(annotation, record, attestor_id).await
        }
        _ => Err(EventProcessorError::generic(format!(
            "Not an attestor record URI: {uri}"
        ))),
    }
}

async fn put_profile(
    profile: PubkyAppAttestorProfile,
    record: Value,
    attestor_id: PubkyId,
) -> Result<(), EventProcessorError> {
    let details = AttestorProfileDetails::from_verified(&profile, record, &attestor_id)
        .map_err(EventProcessorError::generic)?;
    details.put_to_index().await?;
    Ok(())
}

/// A newer or equal `revision` replaces the indexed annotation; a lower one
/// is ignored.
async fn put_annotation(
    annotation: PubkyAppAttestorAnnotation,
    record: Value,
    attestor_id: PubkyId,
) -> Result<(), EventProcessorError> {
    if let Some(indexed) =
        AttestorAnnotationDetails::get_from_index(&attestor_id, &annotation.order_ref).await?
    {
        if indexed.revision > annotation.revision {
            debug!(
                "Ignoring annotation {}/{} revision {}: revision {} is indexed",
                attestor_id, annotation.order_ref, annotation.revision, indexed.revision
            );
            return Ok(());
        }
    }

    AttestorAnnotationDetails::from_verified(&annotation, record, &attestor_id)
        .put_to_index()
        .await?;
    Ok(())
}

pub async fn del_profile(attestor_id: PubkyId) -> Result<(), EventProcessorError> {
    debug!("Deleting attestor profile: {attestor_id}");
    AttestorProfileDetails::delete(&attestor_id).await?;
    Ok(())
}

pub async fn del_annotation(
    attestor_id: PubkyId,
    order_ref: String,
) -> Result<(), EventProcessorError> {
    debug!("Deleting attestor annotation: {attestor_id}/{order_ref}");
    AttestorAnnotationDetails::delete(&attestor_id, &order_ref).await?;
    Ok(())
}
