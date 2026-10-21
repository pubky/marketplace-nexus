use crate::event_processor::utils::watcher::WatcherTest;
use anyhow::Result;
use nexus_common::models::marketplace::{
    AttestorAnnotationDetails, AttestorKeyWindow, AttestorProfileDetails,
};
use pubky::{Keypair, ResourcePath};
use pubky_app_specs::{
    traits::{HasIdPath, HasPath},
    PubkyAppAttestorAnnotation, PubkyAppAttestorIssuancePolicy, PubkyAppAttestorKey,
    PubkyAppAttestorOperator, PubkyAppAttestorOutcome, PubkyAppAttestorOutcomeKind,
    PubkyAppAttestorProfile, AMOUNT_BAND_POLICY_BOTH_SIDES_CONSENT, PRICE_DECADE_POLICY_ALWAYS,
};
use serde_json::Value;

/// SP2's canonical attestor record vectors, copied verbatim from specs at
/// `4ea7ee3` (`src/test_fixtures/attestor-records-v1.json`).
const ATTESTOR_RECORDS: &str = include_str!("fixtures/attestor-records-v1.json");

/// The vectors' test-only attestor key: 32 bytes of 0x07.
const VECTOR_SECRET_KEY: [u8; 32] = [7u8; 32];

const AUG_1_2026: i64 = 1_785_542_400;
const JUL_15_2026: i64 = 1_784_073_600;
const JAN_1_2026: i64 = 1_767_225_600;

fn profile_path() -> ResourcePath {
    PubkyAppAttestorProfile::create_path().parse().unwrap()
}

fn annotation_path(order_ref: &str) -> ResourcePath {
    PubkyAppAttestorAnnotation::create_path(order_ref)
        .parse()
        .unwrap()
}

fn test_profile(key_pubky: &str) -> PubkyAppAttestorProfile {
    PubkyAppAttestorProfile {
        operator: PubkyAppAttestorOperator {
            name: "Pubky Market".to_string(),
            url: "https://pubky-market.example".to_string(),
            policy_url: "https://pubky-market.example/attestor-policy".to_string(),
        },
        keys: vec![PubkyAppAttestorKey {
            pubky: key_pubky.to_string(),
            valid_from: "2026-08-01T00:00:00Z".to_string(),
            valid_until: None,
            compromised_from: None,
        }],
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
}

fn outcome(kind: PubkyAppAttestorOutcomeKind, on: &str) -> PubkyAppAttestorOutcome {
    PubkyAppAttestorOutcome {
        kind,
        on: on.to_string(),
    }
}

fn signed_annotation(
    signer: &Keypair,
    order_ref: &str,
    revision: i64,
    outcomes: Vec<PubkyAppAttestorOutcome>,
) -> PubkyAppAttestorAnnotation {
    PubkyAppAttestorAnnotation {
        order_ref: order_ref.to_string(),
        outcomes,
        revision,
        proof: String::new(),
    }
    .sign(&signer.secret())
    .unwrap()
}

async fn indexed_annotation(
    attestor_id: &str,
    order_ref: &str,
) -> Option<AttestorAnnotationDetails> {
    AttestorAnnotationDetails::get_from_index(attestor_id, order_ref)
        .await
        .unwrap()
}

async fn indexed_profile(attestor_id: &str) -> Option<AttestorProfileDetails> {
    AttestorProfileDetails::get_from_index(attestor_id)
        .await
        .unwrap()
}

async fn register_attestor(test: &mut WatcherTest, kp: &Keypair) -> Result<String> {
    test.register_user(kp).await?;
    Ok(kp.public_key().to_z32())
}

#[tokio_shared_rt::test(shared)]
async fn test_attestor_profile_is_indexed_with_its_key_windows() -> Result<()> {
    let mut test = WatcherTest::setup().await?;
    let vectors: Value = serde_json::from_str(ATTESTOR_RECORDS)?;
    assert_eq!(vectors["attestorSecretKeyHex"], "07".repeat(32));

    let attestor_kp = Keypair::from_secret(&VECTOR_SECRET_KEY);
    let attestor_id = register_attestor(&mut test, &attestor_kp).await?;
    assert_eq!(vectors["attestorPubky"], attestor_id.as_str());

    // SP2's profile vector, published verbatim on the attestor's homeserver
    let profile = &vectors["profile"];
    test.put(&attestor_kp, &profile_path(), &profile["record"])
        .await?;

    let indexed = indexed_profile(&attestor_id)
        .await
        .expect("The verified profile was not indexed");
    assert_eq!(indexed.attestor_id, attestor_id);
    assert_eq!(indexed.uri, profile["uri"].as_str().unwrap());
    assert_eq!(indexed.record, profile["record"]);
    assert_eq!(
        indexed.key_windows,
        vec![
            AttestorKeyWindow {
                pubky: attestor_id.clone(),
                valid_from: AUG_1_2026,
                valid_until: None,
                compromised_from: None,
            },
            AttestorKeyWindow {
                pubky: "9wm1eqn4wddis385xdgsym7b5ge9549zpcjhmdszymicopxj6acy".to_string(),
                valid_from: JAN_1_2026,
                valid_until: Some(AUG_1_2026),
                compromised_from: Some(JUL_15_2026),
            },
        ]
    );
    // The stored copy still verifies offline against its URI
    PubkyAppAttestorProfile::from_verified_json(
        &indexed.uri,
        &serde_json::to_vec(&indexed.record)?,
    )
    .expect("The indexed profile must verify offline");

    // A changed profile replaces the stored one
    let mut changed: PubkyAppAttestorProfile = serde_json::from_value(profile["record"].clone())?;
    changed.keys[0].compromised_from = Some("2026-10-01T00:00:00.500Z".to_string());
    let changed = changed.sign(&VECTOR_SECRET_KEY).unwrap();
    test.put(&attestor_kp, &profile_path(), &changed).await?;
    let indexed = indexed_profile(&attestor_id)
        .await
        .expect("The changed profile was not indexed");
    assert_eq!(indexed.key_windows[0].compromised_from, Some(1_790_812_801));
    assert_eq!(indexed.record["proof"], changed.proof.as_str());

    // SP2's annotation vector
    let annotation = &vectors["annotation"];
    let order_ref = annotation["record"]["orderRef"].as_str().unwrap();
    test.put(
        &attestor_kp,
        &annotation_path(order_ref),
        &annotation["record"],
    )
    .await?;
    let indexed = indexed_annotation(&attestor_id, order_ref)
        .await
        .expect("The verified annotation was not indexed");
    assert_eq!(indexed.uri, annotation["uri"].as_str().unwrap());
    assert_eq!(indexed.revision, 2);
    assert_eq!(
        indexed.outcomes,
        vec![
            outcome(PubkyAppAttestorOutcomeKind::Returned, "2026-10-12"),
            outcome(PubkyAppAttestorOutcomeKind::Refunded, "2026-10-14"),
        ]
    );
    assert_eq!(indexed.record, annotation["record"]);
    PubkyAppAttestorAnnotation::from_verified_json(
        &indexed.uri,
        &serde_json::to_vec(&indexed.record)?,
    )
    .expect("The indexed annotation must verify offline");

    // Deleting either record removes it from the index
    test.del(&attestor_kp, &profile_path()).await?;
    assert!(indexed_profile(&attestor_id).await.is_none());
    test.del(&attestor_kp, &annotation_path(order_ref)).await?;
    assert!(indexed_annotation(&attestor_id, order_ref).await.is_none());

    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn test_attestor_records_with_a_bad_proof_or_wrong_owner_are_rejected() -> Result<()> {
    let mut test = WatcherTest::setup().await?;
    let attestor_kp = Keypair::random();
    let attestor_id = register_attestor(&mut test, &attestor_kp).await?;
    let other_kp = Keypair::random();
    let other_id = register_attestor(&mut test, &other_kp).await?;
    let order_ref = "b".repeat(64);
    let refunded = || vec![outcome(PubkyAppAttestorOutcomeKind::Refunded, "2026-10-14")];

    // A valid revision 1 is indexed
    let valid = signed_annotation(&attestor_kp, &order_ref, 1, refunded());
    test.put(&attestor_kp, &annotation_path(&order_ref), &valid)
        .await?;
    assert_eq!(
        indexed_annotation(&attestor_id, &order_ref)
            .await
            .expect("The valid annotation was not indexed")
            .revision,
        1
    );

    // A body changed after signing fails its proof and leaves the index unchanged
    let mut tampered = valid.clone();
    tampered.revision = 2;
    tampered
        .outcomes
        .push(outcome(PubkyAppAttestorOutcomeKind::Returned, "2026-10-15"));
    test.put(&attestor_kp, &annotation_path(&order_ref), &tampered)
        .await?;

    // So does a record signed by another key on the attestor's own path
    let forged = signed_annotation(&other_kp, &order_ref, 3, refunded());
    test.put(&attestor_kp, &annotation_path(&order_ref), &forged)
        .await?;

    // And one without a proof
    let mut unsigned = signed_annotation(&attestor_kp, &order_ref, 4, refunded());
    unsigned.proof = String::new();
    test.put(&attestor_kp, &annotation_path(&order_ref), &unsigned)
        .await?;

    let indexed = indexed_annotation(&attestor_id, &order_ref)
        .await
        .expect("A rejected record must not remove the indexed one");
    assert_eq!(indexed.revision, 1);
    assert_eq!(indexed.outcomes, refunded());
    assert_eq!(indexed.record["proof"], valid.proof.as_str());

    // The attestor's honest annotation copied to another owner's homeserver
    let copied_ref = "c".repeat(64);
    let honest = signed_annotation(&attestor_kp, &copied_ref, 1, refunded());
    test.put(&other_kp, &annotation_path(&copied_ref), &honest)
        .await?;
    assert!(indexed_annotation(&other_id, &copied_ref).await.is_none());
    assert!(indexed_annotation(&attestor_id, &copied_ref)
        .await
        .is_none());

    // An honest annotation published under another order's path
    let other_ref = "d".repeat(64);
    test.put(&attestor_kp, &annotation_path(&other_ref), &honest)
        .await?;
    assert!(indexed_annotation(&attestor_id, &other_ref).await.is_none());

    // The attestor's honest profile copied to another owner's homeserver
    let profile = test_profile(&attestor_id)
        .sign(&attestor_kp.secret())
        .unwrap();
    test.put(&other_kp, &profile_path(), &profile).await?;
    assert!(indexed_profile(&other_id).await.is_none());

    // A profile changed after signing
    let mut tampered_profile = profile.clone();
    tampered_profile.keys[0].compromised_from = None;
    tampered_profile.keys[0].valid_from = "2020-01-01T00:00:00Z".to_string();
    test.put(&attestor_kp, &profile_path(), &tampered_profile)
        .await?;
    assert!(indexed_profile(&attestor_id).await.is_none());

    // The same profile on its owner's homeserver is indexed
    test.put(&attestor_kp, &profile_path(), &profile).await?;
    let indexed = indexed_profile(&attestor_id)
        .await
        .expect("The honest profile was not indexed");
    assert_eq!(indexed.key_windows[0].valid_from, AUG_1_2026);

    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn test_attestor_annotation_revision_bump_replaces() -> Result<()> {
    let mut test = WatcherTest::setup().await?;
    let attestor_kp = Keypair::random();
    let attestor_id = register_attestor(&mut test, &attestor_kp).await?;
    let order_ref = "e".repeat(64);
    let path = annotation_path(&order_ref);
    let refunded = outcome(PubkyAppAttestorOutcomeKind::Refunded, "2026-10-14");
    let returned = outcome(PubkyAppAttestorOutcomeKind::Returned, "2026-10-20");

    let first = signed_annotation(&attestor_kp, &order_ref, 1, vec![refunded.clone()]);
    test.put(&attestor_kp, &path, &first).await?;
    let indexed = indexed_annotation(&attestor_id, &order_ref)
        .await
        .expect("Revision 1 was not indexed");
    assert_eq!(indexed.revision, 1);
    assert_eq!(indexed.outcomes, vec![refunded.clone()]);

    // Revision 2 replaces revision 1
    let second = signed_annotation(
        &attestor_kp,
        &order_ref,
        2,
        vec![refunded.clone(), returned.clone()],
    );
    test.put(&attestor_kp, &path, &second).await?;
    let indexed = indexed_annotation(&attestor_id, &order_ref)
        .await
        .expect("Revision 2 was not indexed");
    assert_eq!(indexed.revision, 2);
    assert_eq!(indexed.outcomes, vec![refunded.clone(), returned.clone()]);
    assert_eq!(indexed.record["proof"], second.proof.as_str());

    // A validly signed lower revision never replaces a higher one
    test.put(&attestor_kp, &path, &first).await?;
    let indexed = indexed_annotation(&attestor_id, &order_ref)
        .await
        .expect("Revision 2 must stay indexed");
    assert_eq!(indexed.revision, 2);
    assert_eq!(indexed.outcomes, vec![refunded, returned]);

    test.del(&attestor_kp, &path).await?;
    assert!(indexed_annotation(&attestor_id, &order_ref).await.is_none());

    Ok(())
}
