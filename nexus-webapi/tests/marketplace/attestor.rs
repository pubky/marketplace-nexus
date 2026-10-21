use crate::utils::{get_request, invalid_get_request};
use anyhow::Result;
use axum::http::StatusCode;
use nexus_common::models::marketplace::{AttestorAnnotationDetails, AttestorProfileDetails};
use pubky::Keypair;
use pubky_app_specs::{
    attestor_annotation_uri_builder, attestor_profile_uri_builder, PubkyAppAttestorAnnotation,
    PubkyAppAttestorIssuancePolicy, PubkyAppAttestorKey, PubkyAppAttestorOperator,
    PubkyAppAttestorOutcome, PubkyAppAttestorOutcomeKind, PubkyAppAttestorProfile, PubkyId,
    AMOUNT_BAND_POLICY_BOTH_SIDES_CONSENT, PRICE_DECADE_POLICY_ALWAYS,
};
use serde_json::{json, Value};

const PREVIOUS_KEY: &str = "9wm1eqn4wddis385xdgsym7b5ge9549zpcjhmdszymicopxj6acy";

/// Verifies a signed record the way the watcher does and stores it.
async fn seed_profile(attestor_kp: &Keypair) -> Result<Value> {
    let attestor_id = attestor_kp.public_key().to_z32();
    let profile = PubkyAppAttestorProfile {
        operator: PubkyAppAttestorOperator {
            name: "Pubky Market".to_string(),
            url: "https://pubky-market.example".to_string(),
            policy_url: "https://pubky-market.example/attestor-policy".to_string(),
        },
        keys: vec![
            PubkyAppAttestorKey {
                pubky: attestor_id.clone(),
                valid_from: "2026-08-01T00:00:00Z".to_string(),
                valid_until: None,
                compromised_from: None,
            },
            PubkyAppAttestorKey {
                pubky: PREVIOUS_KEY.to_string(),
                valid_from: "2026-01-01T00:00:00Z".to_string(),
                valid_until: Some("2026-08-01T00:00:00Z".to_string()),
                compromised_from: Some("2026-07-15T00:00:00Z".to_string()),
            },
        ],
        successor: None,
        issuance_policy: PubkyAppAttestorIssuancePolicy {
            edit_window_hours: 24,
            blind_window_days: 14,
            amount_band: AMOUNT_BAND_POLICY_BOTH_SIDES_CONSENT.to_string(),
            price_decade: PRICE_DECADE_POLICY_ALWAYS.to_string(),
            min_order_value: None,
        },
        proof: String::new(),
    }
    .sign(&attestor_kp.secret())
    .map_err(anyhow::Error::msg)?;
    let blob = profile.to_canonical_json().map_err(anyhow::Error::msg)?;
    let uri = attestor_profile_uri_builder(attestor_id.clone());
    let verified = PubkyAppAttestorProfile::from_verified_json(&uri, blob.as_bytes())
        .map_err(anyhow::Error::msg)?;
    let record: Value = serde_json::from_str(&blob)?;
    let id = PubkyId::try_from(attestor_id.as_str()).map_err(anyhow::Error::msg)?;
    AttestorProfileDetails::from_verified(&verified, record.clone(), &id)
        .map_err(anyhow::Error::msg)?
        .put_to_index()
        .await?;
    Ok(record)
}

async fn seed_annotation(
    attestor_kp: &Keypair,
    order_ref: &str,
    revision: i64,
    outcomes: Vec<PubkyAppAttestorOutcome>,
) -> Result<Value> {
    let attestor_id = attestor_kp.public_key().to_z32();
    let annotation = PubkyAppAttestorAnnotation {
        order_ref: order_ref.to_string(),
        outcomes,
        revision,
        proof: String::new(),
    }
    .sign(&attestor_kp.secret())
    .map_err(anyhow::Error::msg)?;
    let blob = annotation.to_canonical_json().map_err(anyhow::Error::msg)?;
    let uri = attestor_annotation_uri_builder(attestor_id.clone(), order_ref.to_string());
    let verified = PubkyAppAttestorAnnotation::from_verified_json(&uri, blob.as_bytes())
        .map_err(anyhow::Error::msg)?;
    let record: Value = serde_json::from_str(&blob)?;
    let id = PubkyId::try_from(attestor_id.as_str()).map_err(anyhow::Error::msg)?;
    AttestorAnnotationDetails::from_verified(&verified, record.clone(), &id)
        .put_to_index()
        .await?;
    Ok(record)
}

fn outcome(kind: PubkyAppAttestorOutcomeKind, on: &str) -> PubkyAppAttestorOutcome {
    PubkyAppAttestorOutcome {
        kind,
        on: on.to_string(),
    }
}

#[tokio_shared_rt::test(shared)]
async fn test_attestor_profile_is_served_with_its_key_windows() -> Result<()> {
    crate::utils::host_url().await;

    let attestor_kp = Keypair::random();
    let attestor_id = attestor_kp.public_key().to_z32();
    let record = seed_profile(&attestor_kp).await?;

    let body = get_request(&format!("/v0/marketplace/attestor/{attestor_id}/profile")).await?;
    assert_eq!(body["attestor_id"], attestor_id.as_str());
    assert_eq!(
        body["uri"],
        format!("pubky://{attestor_id}/pub/pubky.app/marketplace/v1/attestor/profile")
    );
    assert_eq!(
        body["key_windows"],
        json!([
            {
                "pubky": attestor_id,
                "valid_from": 1_785_542_400,
                "valid_until": null,
                "compromised_from": null
            },
            {
                "pubky": PREVIOUS_KEY,
                "valid_from": 1_767_225_600,
                "valid_until": 1_785_542_400,
                "compromised_from": 1_784_073_600
            }
        ])
    );
    assert_eq!(body["record"], record);
    assert!(body["indexed_at"].is_i64());
    // The served record verifies offline against the served URI
    PubkyAppAttestorProfile::from_verified_json(
        body["uri"].as_str().unwrap(),
        &serde_json::to_vec(&body["record"])?,
    )
    .map_err(anyhow::Error::msg)?;

    let unknown = Keypair::random().public_key().to_z32();
    invalid_get_request(
        &format!("/v0/marketplace/attestor/{unknown}/profile"),
        StatusCode::NOT_FOUND,
    )
    .await?;
    invalid_get_request(
        "/v0/marketplace/attestor/not-a-pubky/profile",
        StatusCode::BAD_REQUEST,
    )
    .await?;

    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn test_attestor_annotations_batch_is_aligned_with_the_request() -> Result<()> {
    crate::utils::host_url().await;

    let attestor_kp = Keypair::random();
    let attestor_id = attestor_kp.public_key().to_z32();
    let other_kp = Keypair::random();
    let refunded = outcome(PubkyAppAttestorOutcomeKind::Refunded, "2026-10-14");
    let returned = outcome(PubkyAppAttestorOutcomeKind::Returned, "2026-10-20");

    let ref_a = "a".repeat(64);
    let ref_b = "b".repeat(64);
    let ref_missing = "f".repeat(64);
    let record_a = seed_annotation(&attestor_kp, &ref_a, 1, vec![refunded.clone()]).await?;
    let record_b = seed_annotation(&attestor_kp, &ref_b, 3, vec![refunded, returned]).await?;
    // Another attestor's annotation for the missing order never answers for this attestor
    seed_annotation(&other_kp, &ref_missing, 1, vec![]).await?;

    let route = format!("/v0/marketplace/attestor/{attestor_id}/annotations");
    let body = get_request(&format!(
        "{route}?order_refs={ref_b},{ref_missing},{ref_a},{ref_b}"
    ))
    .await?;
    let entries = body.as_array().expect("Annotations should be an array");
    assert_eq!(entries.len(), 4);
    assert_eq!(entries[1], Value::Null);
    assert_eq!(entries[0], entries[3]);

    assert_eq!(entries[0]["attestor_id"], attestor_id.as_str());
    assert_eq!(entries[0]["order_ref"], ref_b.as_str());
    assert_eq!(
        entries[0]["uri"],
        format!("pubky://{attestor_id}/pub/pubky.app/marketplace/v1/attestor/annotations/{ref_b}")
    );
    assert_eq!(entries[0]["revision"], 3);
    assert_eq!(
        entries[0]["outcomes"],
        json!([
            {"kind": "refunded", "on": "2026-10-14"},
            {"kind": "returned", "on": "2026-10-20"}
        ])
    );
    assert_eq!(entries[0]["record"], record_b);
    assert!(entries[0]["indexed_at"].is_i64());

    assert_eq!(entries[2]["order_ref"], ref_a.as_str());
    assert_eq!(entries[2]["revision"], 1);
    assert_eq!(entries[2]["record"], record_a);
    PubkyAppAttestorAnnotation::from_verified_json(
        entries[2]["uri"].as_str().unwrap(),
        &serde_json::to_vec(&entries[2]["record"])?,
    )
    .map_err(anyhow::Error::msg)?;

    // Nothing indexed: every entry is null
    let unknown = Keypair::random().public_key().to_z32();
    let body = get_request(&format!(
        "/v0/marketplace/attestor/{unknown}/annotations?order_refs={ref_a},{ref_b}"
    ))
    .await?;
    assert_eq!(body, json!([null, null]));

    // Malformed requests
    for query in [
        String::new(),
        "?order_refs=".to_string(),
        format!("?order_refs={ref_a},,{ref_b}"),
        format!("?order_refs={}", vec![ref_a.as_str(); 101].join(",")),
    ] {
        invalid_get_request(&format!("{route}{query}"), StatusCode::BAD_REQUEST).await?;
    }
    invalid_get_request(
        &format!("/v0/marketplace/attestor/not-a-pubky/annotations?order_refs={ref_a}"),
        StatusCode::BAD_REQUEST,
    )
    .await?;
    let body = get_request(&format!(
        "{route}?order_refs={}",
        vec![ref_a.as_str(); 100].join(",")
    ))
    .await?;
    assert_eq!(body.as_array().unwrap().len(), 100);

    Ok(())
}
