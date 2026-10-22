use crate::db::kv::RedisResult;
use crate::db::RedisOps;
use crate::models::error::ModelResult;
use chrono::{DateTime, Utc};
use pubky_app_specs::{
    attestor_annotation_uri_builder, attestor_profile_uri_builder, PubkyAppAttestorAnnotation,
    PubkyAppAttestorKey, PubkyAppAttestorOutcome, PubkyAppAttestorProfile, PubkyId,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use utoipa::ToSchema;

/// The most `order_ref`s one annotations batch may ask for.
pub const MAX_ANNOTATION_BATCH: usize = 100;

/// One key of an attestor profile, its RFC 3339 bounds converted to whole
/// seconds: the unit of a review's `iat` and of the `attestors=` windows.
/// Each bound is rounded up, so for an integer `iat` the comparisons are
/// exact: the key covers `valid_from <= iat < valid_until`, and attestations
/// with `iat >= compromised_from` must not be trusted.
#[derive(Serialize, Deserialize, ToSchema, Clone, Debug, PartialEq)]
pub struct AttestorKeyWindow {
    pub pubky: String,
    pub valid_from: i64,
    /// `null` while the key is in use.
    pub valid_until: Option<i64>,
    /// `null` unless the attestor reported the key compromised.
    pub compromised_from: Option<i64>,
}

impl AttestorKeyWindow {
    fn from_key(key: &PubkyAppAttestorKey) -> Result<Self, String> {
        Ok(AttestorKeyWindow {
            pubky: key.pubky.clone(),
            valid_from: ceil_seconds(&key.valid_from)?,
            valid_until: key.valid_until.as_deref().map(ceil_seconds).transpose()?,
            compromised_from: key
                .compromised_from
                .as_deref()
                .map(ceil_seconds)
                .transpose()?,
        })
    }
}

fn ceil_seconds(rfc3339: &str) -> Result<i64, String> {
    let millis = DateTime::parse_from_rfc3339(rfc3339)
        .map_err(|e| format!("Invalid RFC 3339 datetime {rfc3339:?}: {e}"))?
        .timestamp_millis();
    Ok(millis.div_euclid(1000) + i64::from(millis.rem_euclid(1000) != 0))
}

/// The indexed attestor profile, published at
/// `/pub/pubky.app/marketplace/v1/attestor/profile` on the attestor's own
/// homeserver. Only a profile whose proof verifies against its owner is
/// indexed.
///
/// A client may only narrow its own trust in a key from these windows and
/// never adds a key from them: whoever holds a stolen key can rewrite the
/// profile.
#[derive(Serialize, Deserialize, ToSchema, Clone, Debug, PartialEq)]
pub struct AttestorProfileDetails {
    /// The attestor pubky (record owner).
    pub attestor_id: String,
    pub uri: String,
    /// The profile's `keys`, in record order, with their bounds in seconds.
    pub key_windows: Vec<AttestorKeyWindow>,
    /// The record exactly as published, `proof` and unknown members
    /// included, so a client can verify it offline against `uri`.
    #[schema(value_type = Object)]
    pub record: Value,
    pub indexed_at: i64,
}

impl RedisOps for AttestorProfileDetails {}

impl AttestorProfileDetails {
    /// Builds the details of a verified profile; `record` is the verified
    /// blob parsed as JSON.
    pub fn from_verified(
        profile: &PubkyAppAttestorProfile,
        record: Value,
        attestor_id: &PubkyId,
    ) -> Result<Self, String> {
        Ok(AttestorProfileDetails {
            attestor_id: attestor_id.to_string(),
            uri: attestor_profile_uri_builder(attestor_id.to_string()),
            key_windows: profile
                .keys
                .iter()
                .map(AttestorKeyWindow::from_key)
                .collect::<Result<_, _>>()?,
            record,
            indexed_at: Utc::now().timestamp_millis(),
        })
    }

    pub async fn get_from_index(attestor_id: &str) -> RedisResult<Option<Self>> {
        Self::try_from_index_json(&[attestor_id], None).await
    }

    pub async fn put_to_index(&self) -> RedisResult<()> {
        self.put_index_json(&[&self.attestor_id], None, None).await
    }

    pub async fn delete(attestor_id: &str) -> ModelResult<()> {
        Self::remove_from_index_multiple_json(&[&[attestor_id]]).await?;
        Ok(())
    }
}

/// The indexed outcomes of one attested order, published at
/// `/pub/pubky.app/marketplace/v1/attestor/annotations/{order_ref}` on the
/// attestor's own homeserver. Only an annotation whose proof verifies
/// against its owner is indexed, and a lower `revision` never replaces a
/// higher one.
#[derive(Serialize, Deserialize, ToSchema, Clone, Debug, PartialEq)]
pub struct AttestorAnnotationDetails {
    /// The attestor pubky (record owner).
    pub attestor_id: String,
    pub order_ref: String,
    pub uri: String,
    /// `{kind: "refunded" | "returned", on: "YYYY-MM-DD"}`, in record order.
    pub outcomes: Vec<PubkyAppAttestorOutcome>,
    pub revision: i64,
    /// The record exactly as published, `proof` and unknown members
    /// included, so a client can verify it offline against `uri`.
    #[schema(value_type = Object)]
    pub record: Value,
    pub indexed_at: i64,
}

impl RedisOps for AttestorAnnotationDetails {}

impl AttestorAnnotationDetails {
    /// Builds the details of a verified annotation; `record` is the verified
    /// blob parsed as JSON.
    pub fn from_verified(
        annotation: &PubkyAppAttestorAnnotation,
        record: Value,
        attestor_id: &PubkyId,
    ) -> Self {
        AttestorAnnotationDetails {
            attestor_id: attestor_id.to_string(),
            order_ref: annotation.order_ref.clone(),
            uri: attestor_annotation_uri_builder(
                attestor_id.to_string(),
                annotation.order_ref.clone(),
            ),
            outcomes: annotation.outcomes.clone(),
            revision: annotation.revision,
            record,
            indexed_at: Utc::now().timestamp_millis(),
        }
    }

    pub async fn get_from_index(attestor_id: &str, order_ref: &str) -> RedisResult<Option<Self>> {
        Self::try_from_index_json(&[attestor_id, order_ref], None).await
    }

    /// The attestor's annotations for `order_refs`, one entry per requested
    /// `order_ref` in request order, `None` where none is indexed.
    pub async fn get_many(
        attestor_id: &str,
        order_refs: &[String],
    ) -> RedisResult<Vec<Option<Self>>> {
        let key_parts: Vec<[&str; 2]> = order_refs
            .iter()
            .map(|order_ref| [attestor_id, order_ref.as_str()])
            .collect();
        let key_parts: Vec<&[&str]> = key_parts.iter().map(|parts| parts.as_slice()).collect();
        Self::try_from_index_multiple_json(&key_parts).await
    }

    pub async fn put_to_index(&self) -> RedisResult<()> {
        self.put_index_json(&[&self.attestor_id, &self.order_ref], None, None)
            .await
    }

    pub async fn delete(attestor_id: &str, order_ref: &str) -> ModelResult<()> {
        Self::remove_from_index_multiple_json(&[&[attestor_id, order_ref]]).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::ceil_seconds;

    #[test]
    fn key_bounds_round_up_to_whole_seconds() {
        assert_eq!(ceil_seconds("2026-08-01T00:00:00Z"), Ok(1_785_542_400));
        assert_eq!(ceil_seconds("2026-08-01T00:00:00.000Z"), Ok(1_785_542_400));
        assert_eq!(ceil_seconds("2026-08-01T00:00:00.001Z"), Ok(1_785_542_401));
        assert_eq!(ceil_seconds("2026-08-01T00:00:00.999Z"), Ok(1_785_542_401));
        assert_eq!(ceil_seconds("2026-08-01T01:00:00+01:00"), Ok(1_785_542_400));
        assert!(ceil_seconds("2026-08-01").is_err());
    }
}
