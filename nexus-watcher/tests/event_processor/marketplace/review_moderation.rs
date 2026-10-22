use super::moderation::{moderator_keypair, seller_profile, tag_event_uri, MODERATED_LABEL};
use super::utils::{
    attestor_pubky, sign_attestation, test_attestation_claims, test_attestor_key, test_listing,
    test_review, test_shop,
};
use crate::event_processor::utils::default_moderation_tests;
use crate::event_processor::utils::watcher::{
    retrieve_and_handle_event_line, HomeserverHashIdPath, WatcherTest,
};
use anyhow::Result;
use chrono::Utc;
use nexus_common::db::graph::Query;
use nexus_common::db::kv::put_field_if_exists;
use nexus_common::db::{exec_single_row, RedisOps};
use nexus_common::models::event::EventProcessorError;
use nexus_common::models::marketplace::{
    ExcludedReviewer, ModeratedReview, ModeratedUser, ReputationSummary, ReviewDetails,
    ReviewModeration, ReviewModerationState, ReviewStream, ReviewView,
};
use nexus_common::models::user::UserDetails;
use nexus_common::types::Pagination;
use nexus_watcher::events::handlers;
use pubky::{Keypair, ResourcePath};
use pubky_app_specs::{
    marketplace_review_uri_builder, traits::HasPath, traits::HashId, user_uri_builder,
    PubkyAppListingCondition, PubkyAppMarketplaceReview, PubkyAppReviewRole, PubkyAppShop,
    PubkyAppTag, PubkyId,
};
use std::sync::Arc;

const EXCLUDE_LABEL: &str = "review_fraud";

fn tag_on_review(reviewer_id: &str, review_id: &str, label: &str) -> PubkyAppTag {
    PubkyAppTag {
        uri: marketplace_review_uri_builder(reviewer_id.to_string(), review_id.to_string()),
        label: label.to_string(),
        created_at: Utc::now().timestamp_millis(),
    }
}

fn tag_on_user(user_id: &str, label: &str) -> PubkyAppTag {
    PubkyAppTag {
        uri: user_uri_builder(user_id.to_string()),
        label: label.to_string(),
        created_at: Utc::now().timestamp_millis(),
    }
}

fn moderation(state: ReviewModerationState, reason: &str) -> Option<ReviewModeration> {
    Some(ReviewModeration {
        state,
        reason: reason.to_string(),
    })
}

struct Market {
    seller_kp: Keypair,
    seller_id: String,
    listing_id: String,
}

async fn market(test: &mut WatcherTest, name: &str) -> Result<Market> {
    let seller_kp = Keypair::random();
    let seller_id = test
        .create_user(&seller_kp, &seller_profile(&format!("{name}:Seller")))
        .await?;
    let shop_path: ResourcePath = PubkyAppShop::create_path().parse()?;
    test.put(&seller_kp, &shop_path, &test_shop(&seller_id))
        .await?;
    let listing = test_listing(
        &seller_id,
        "Moderated review subject",
        "electronics",
        PubkyAppListingCondition::Excellent,
        20_000,
    );
    let (listing_id, _path) = test.create_listing(&seller_kp, &listing).await?;
    Ok(Market {
        seller_kp,
        seller_id,
        listing_id,
    })
}

/// A verified buyer review of the market's listing for one attested order.
fn buyer_review(
    market: &Market,
    buyer_id: &str,
    order_seed: char,
    overall: i64,
    text: &str,
) -> PubkyAppMarketplaceReview {
    let attestor = test_attestor_key();
    let claims = test_attestation_claims(
        &attestor_pubky(&attestor),
        buyer_id,
        &market.seller_id,
        &market.seller_id,
        &market.listing_id,
        order_seed,
    );
    let mut review = test_review(
        buyer_id,
        &market.seller_id,
        &market.seller_id,
        &market.listing_id,
        overall,
        text,
        &sign_attestation(&claims, &attestor),
    );
    review.order_ref = Some(claims.order_ref);
    review
}

async fn subject_entries(seller_id: &str) -> Vec<ReviewView> {
    ReviewStream::get_by_subject(
        seller_id,
        PubkyAppReviewRole::BuyerReviewingSeller,
        Pagination::default(),
    )
    .await
    .unwrap()
    .map(|stream| stream.0)
    .unwrap_or_default()
}

async fn listing_entries(seller_id: &str, listing_id: &str) -> Vec<ReviewView> {
    ReviewStream::get_by_listing(seller_id, listing_id, Pagination::default())
        .await
        .unwrap()
        .map(|stream| stream.0)
        .unwrap_or_default()
}

/// The review as both review streams serve it.
async fn served(market: &Market, review_id: &str) -> ReviewDetails {
    let subject = subject_entries(&market.seller_id)
        .await
        .into_iter()
        .find(|entry| entry.review.review_id == review_id)
        .expect("the subject stream lists the review")
        .review;
    let listing = listing_entries(&market.seller_id, &market.listing_id)
        .await
        .into_iter()
        .find(|entry| entry.review.review_id == review_id)
        .expect("the listing stream lists the review")
        .review;
    assert_eq!(subject, listing);
    subject
}

/// Asserts the review's moderation on the details cache and both streams, and
/// that the streams withhold the text of a moderated review.
async fn assert_moderation(
    market: &Market,
    reviewer_id: &str,
    review_id: &str,
    expected: Option<ReviewModeration>,
    text: &str,
) {
    let indexed = ReviewDetails::get_from_index(reviewer_id, review_id)
        .await
        .unwrap()
        .expect("a moderated review stays indexed");
    assert_eq!(indexed.moderation, expected);
    assert_eq!(indexed.text, text, "the index keeps the text");
    let served = served(market, review_id).await;
    assert_eq!(served.moderation, expected);
    if expected.is_some() {
        assert_eq!(served.text, "", "a moderated review's text is withheld");
    } else {
        assert_eq!(served.text, text);
    }
}

/// Asserts the seller's and the listing's aggregates: `count` reviews with
/// `avg` stars, or no aggregate at all for zero.
async fn assert_counted(market: &Market, count: u64, avg: f64) {
    let subject = ReputationSummary::get_by_subject(
        &market.seller_id,
        PubkyAppReviewRole::BuyerReviewingSeller,
    )
    .await
    .unwrap();
    let listing = ReputationSummary::get_by_listing(&market.seller_id, &market.listing_id)
        .await
        .unwrap();
    if count == 0 {
        assert!(subject.is_none());
        assert!(listing.is_none());
        return;
    }
    for summary in [subject, listing] {
        let summary = summary.expect("the aggregate exists");
        assert_eq!(summary.count, count);
        assert_eq!(summary.verified_count, count);
        assert_eq!(summary.avg, avg);
    }
}

/// Tolerates what the processor would put in the retry queue: an event whose
/// owner, or whose tagged review, is not indexed yet.
async fn replay(line: &str) -> Result<()> {
    match retrieve_and_handle_event_line(line, Arc::new(default_moderation_tests())).await {
        Ok(()) | Err(EventProcessorError::SkipIndexing) => Ok(()),
        Err(e) if format!("{e:?}").contains("MissingDependency") => Ok(()),
        Err(e) => Err(anyhow::anyhow!("{e:?}")),
    }
}

fn pubky_id(id: &str) -> PubkyId {
    PubkyId::try_from(id).expect("valid pubky id")
}

#[tokio_shared_rt::test(shared)]
async fn a_moderated_label_hides_the_review_text_and_removing_the_tag_releases_it() -> Result<()> {
    let mut test = WatcherTest::setup().await?;
    let market = market(&mut test, "Watcher:ReviewHide").await?;
    let buyer_kp = Keypair::random();
    let buyer_id = test
        .create_user(&buyer_kp, &seller_profile("Watcher:ReviewHide:Buyer"))
        .await?;
    let moderator_kp = moderator_keypair().await;
    test.create_user(
        &moderator_kp,
        &seller_profile("Watcher:ReviewHide:Moderator"),
    )
    .await?;

    let text = "Hateful words the moderator hides.";
    let review = buyer_review(&market, &buyer_id, '1', 4, text);
    let (review_id, review_path) = test.create_review(&buyer_kp, &review).await?;
    assert_moderation(&market, &buyer_id, &review_id, None, text).await;
    assert_counted(&market, 1, 4.0).await;

    let tag = tag_on_review(&buyer_id, &review_id, MODERATED_LABEL);
    test.put(&moderator_kp, &tag.hs_path(), &tag).await?;
    assert_moderation(
        &market,
        &buyer_id,
        &review_id,
        moderation(ReviewModerationState::TextHidden, MODERATED_LABEL),
        text,
    )
    .await;
    // Hidden text keeps its ratings in the aggregates.
    assert_counted(&market, 1, 4.0).await;

    test.del(&moderator_kp, &tag.hs_path()).await?;
    assert_moderation(&market, &buyer_id, &review_id, None, text).await;
    assert_counted(&market, 1, 4.0).await;
    assert!(
        ModeratedReview::reviews_of_tag(&default_moderation_tests().id, &tag.create_id())
            .await?
            .is_empty()
    );

    test.del(&buyer_kp, &review_path).await?;
    test.cleanup_user(&buyer_kp).await?;
    test.cleanup_user(&market.seller_kp).await?;
    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn a_moderator_tag_replayed_before_or_after_the_review_moderates_it() -> Result<()> {
    let mut test = WatcherTest::setup().await?;
    let market = market(&mut test, "Watcher:ReviewReplay").await?;
    let buyer_kp = Keypair::random();
    let buyer_id = test
        .create_user(&buyer_kp, &seller_profile("Watcher:ReviewReplay:Buyer"))
        .await?;
    let moderator_kp = moderator_keypair().await;
    let moderator_id = test
        .create_user(
            &moderator_kp,
            &seller_profile("Watcher:ReviewReplay:Moderator"),
        )
        .await?;

    let text = "Fraudulent review replayed.";
    let review = buyer_review(&market, &buyer_id, '2', 1, text);
    let (review_id, review_path) = test.create_review(&buyer_kp, &review).await?;
    let tag = tag_on_review(&buyer_id, &review_id, EXCLUDE_LABEL);
    test.put(&moderator_kp, &tag.hs_path(), &tag).await?;
    let excluded = moderation(ReviewModerationState::Excluded, EXCLUDE_LABEL);
    assert_moderation(&market, &buyer_id, &review_id, excluded.clone(), text).await;

    let tag_put = format!("PUT {}", tag_event_uri(&moderator_id, &tag));
    let review_put = format!(
        "PUT {}",
        marketplace_review_uri_builder(buyer_id.clone(), review_id.clone())
    );
    // A reindex starts from clean databases: wipe the review and the marker.
    for order in [[&tag_put, &review_put], [&review_put, &tag_put]] {
        ModeratedReview::delete(&moderator_id, &tag.create_id()).await?;
        handlers::review::del(pubky_id(&buyer_id), review_id.clone()).await?;
        assert!(ReviewDetails::get_from_index(&buyer_id, &review_id)
            .await?
            .is_none());

        for line in order {
            replay(line).await?;
        }
        assert_moderation(&market, &buyer_id, &review_id, excluded.clone(), text).await;
        assert_counted(&market, 0, 0.0).await;
    }

    test.del(&moderator_kp, &tag.hs_path()).await?;
    assert_moderation(&market, &buyer_id, &review_id, None, text).await;
    assert_counted(&market, 1, 1.0).await;

    test.del(&buyer_kp, &review_path).await?;
    test.cleanup_user(&buyer_kp).await?;
    test.cleanup_user(&market.seller_kp).await?;
    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn an_edit_keeps_a_hidden_review_hidden() -> Result<()> {
    let mut test = WatcherTest::setup().await?;
    let market = market(&mut test, "Watcher:ReviewEdit").await?;
    let buyer_kp = Keypair::random();
    let buyer_id = test
        .create_user(&buyer_kp, &seller_profile("Watcher:ReviewEdit:Buyer"))
        .await?;
    let moderator_kp = moderator_keypair().await;
    test.create_user(
        &moderator_kp,
        &seller_profile("Watcher:ReviewEdit:Moderator"),
    )
    .await?;

    let review = buyer_review(&market, &buyer_id, 'e', 2, "Abusive first draft.");
    let (review_id, review_path) = test.create_review(&buyer_kp, &review).await?;
    let tag = tag_on_review(&buyer_id, &review_id, MODERATED_LABEL);
    test.put(&moderator_kp, &tag.hs_path(), &tag).await?;

    let mut edited = review.clone();
    edited.text = "Still abusive, edited.".to_string();
    edited.ratings.overall = 3;
    edited.revision = 2;
    edited.updated_at = "2026-08-20T13:00:00Z".to_string();
    let (edited_id, _path) = test.create_review(&buyer_kp, &edited).await?;
    assert_eq!(edited_id, review_id, "an edit keeps the record's path");

    let hidden = moderation(ReviewModerationState::TextHidden, MODERATED_LABEL);
    assert_moderation(
        &market,
        &buyer_id,
        &review_id,
        hidden,
        "Still abusive, edited.",
    )
    .await;
    assert_counted(&market, 1, 3.0).await;

    test.del(&moderator_kp, &tag.hs_path()).await?;
    assert_moderation(
        &market,
        &buyer_id,
        &review_id,
        None,
        "Still abusive, edited.",
    )
    .await;

    test.del(&buyer_kp, &review_path).await?;
    test.cleanup_user(&buyer_kp).await?;
    test.cleanup_user(&market.seller_kp).await?;
    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn an_excluded_review_leaves_the_aggregates_and_stays_listed() -> Result<()> {
    let mut test = WatcherTest::setup().await?;
    let market = market(&mut test, "Watcher:ReviewExclude").await?;
    let honest_kp = Keypair::random();
    let honest_id = test
        .create_user(&honest_kp, &seller_profile("Watcher:ReviewExclude:Honest"))
        .await?;
    let fraud_kp = Keypair::random();
    let fraud_id = test
        .create_user(&fraud_kp, &seller_profile("Watcher:ReviewExclude:Fraud"))
        .await?;
    let moderator_kp = moderator_keypair().await;
    test.create_user(
        &moderator_kp,
        &seller_profile("Watcher:ReviewExclude:Moderator"),
    )
    .await?;

    let honest = buyer_review(&market, &honest_id, '3', 5, "Great seller.");
    let (honest_review_id, honest_path) = test.create_review(&honest_kp, &honest).await?;
    let fraud = buyer_review(&market, &fraud_id, 'f', 1, "Paid to trash this seller.");
    let (fraud_review_id, fraud_path) = test.create_review(&fraud_kp, &fraud).await?;
    assert_counted(&market, 2, 3.0).await;

    let tag = tag_on_review(&fraud_id, &fraud_review_id, EXCLUDE_LABEL);
    test.put(&moderator_kp, &tag.hs_path(), &tag).await?;
    assert_counted(&market, 1, 5.0).await;
    assert_moderation(
        &market,
        &fraud_id,
        &fraud_review_id,
        moderation(ReviewModerationState::Excluded, EXCLUDE_LABEL),
        "Paid to trash this seller.",
    )
    .await;
    assert_moderation(
        &market,
        &honest_id,
        &honest_review_id,
        None,
        "Great seller.",
    )
    .await;
    assert_eq!(subject_entries(&market.seller_id).await.len(), 2);
    assert_eq!(
        listing_entries(&market.seller_id, &market.listing_id)
            .await
            .len(),
        2
    );

    test.del(&moderator_kp, &tag.hs_path()).await?;
    assert_counted(&market, 2, 3.0).await;

    test.del(&fraud_kp, &fraud_path).await?;
    test.del(&honest_kp, &honest_path).await?;
    test.cleanup_user(&fraud_kp).await?;
    test.cleanup_user(&honest_kp).await?;
    test.cleanup_user(&market.seller_kp).await?;
    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn moderating_a_user_hides_their_review_text_and_keeps_their_ratings() -> Result<()> {
    let mut test = WatcherTest::setup().await?;
    let market = market(&mut test, "Watcher:ReviewUserMod").await?;
    let buyer_kp = Keypair::random();
    let buyer_id = test
        .create_user(&buyer_kp, &seller_profile("Watcher:ReviewUserMod:Buyer"))
        .await?;
    let moderator_kp = moderator_keypair().await;
    test.create_user(
        &moderator_kp,
        &seller_profile("Watcher:ReviewUserMod:Moderator"),
    )
    .await?;

    let first = buyer_review(&market, &buyer_id, '4', 2, "Written before moderation.");
    let (first_id, first_path) = test.create_review(&buyer_kp, &first).await?;

    let tag = tag_on_user(&buyer_id, MODERATED_LABEL);
    test.put(&moderator_kp, &tag.hs_path(), &tag).await?;
    assert!(ModeratedUser::is_moderated(&buyer_id).await?);
    let hidden = moderation(ReviewModerationState::TextHidden, MODERATED_LABEL);
    assert_moderation(
        &market,
        &buyer_id,
        &first_id,
        hidden.clone(),
        "Written before moderation.",
    )
    .await;
    assert_counted(&market, 1, 2.0).await;

    // A review published while the user is moderated is hidden as it lands.
    let second = buyer_review(&market, &buyer_id, '5', 4, "Written while moderated.");
    let (second_id, second_path) = test.create_review(&buyer_kp, &second).await?;
    assert_moderation(
        &market,
        &buyer_id,
        &second_id,
        hidden,
        "Written while moderated.",
    )
    .await;
    assert_counted(&market, 2, 3.0).await;

    test.del(&moderator_kp, &tag.hs_path()).await?;
    assert!(!ModeratedUser::is_moderated(&buyer_id).await?);
    assert_moderation(
        &market,
        &buyer_id,
        &first_id,
        None,
        "Written before moderation.",
    )
    .await;
    assert_moderation(
        &market,
        &buyer_id,
        &second_id,
        None,
        "Written while moderated.",
    )
    .await;
    assert_counted(&market, 2, 3.0).await;

    test.del(&buyer_kp, &first_path).await?;
    test.del(&buyer_kp, &second_path).await?;
    test.cleanup_user(&buyer_kp).await?;
    test.cleanup_user(&market.seller_kp).await?;
    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn review_fraud_on_a_user_excludes_every_review_they_wrote_without_hiding_them() -> Result<()>
{
    let mut test = WatcherTest::setup().await?;
    let market = market(&mut test, "Watcher:ReviewRing").await?;
    let honest_kp = Keypair::random();
    let honest_id = test
        .create_user(&honest_kp, &seller_profile("Watcher:ReviewRing:Honest"))
        .await?;
    let ring_kp = Keypair::random();
    let ring_id = test
        .create_user(&ring_kp, &seller_profile("Watcher:ReviewRing:Ring"))
        .await?;
    let moderator_kp = moderator_keypair().await;
    test.create_user(
        &moderator_kp,
        &seller_profile("Watcher:ReviewRing:Moderator"),
    )
    .await?;

    let honest = buyer_review(&market, &honest_id, '6', 2, "Slow shipping.");
    let (honest_review_id, honest_path) = test.create_review(&honest_kp, &honest).await?;
    let ring_first = buyer_review(&market, &ring_id, '7', 5, "Best seller ever.");
    let (ring_first_id, ring_first_path) = test.create_review(&ring_kp, &ring_first).await?;
    assert_counted(&market, 2, 3.5).await;

    let tag = tag_on_user(&ring_id, EXCLUDE_LABEL);
    test.put(&moderator_kp, &tag.hs_path(), &tag).await?;
    let excluded = moderation(ReviewModerationState::Excluded, EXCLUDE_LABEL);
    assert_moderation(
        &market,
        &ring_id,
        &ring_first_id,
        excluded.clone(),
        "Best seller ever.",
    )
    .await;
    assert_counted(&market, 1, 2.0).await;
    // Excluding a user's reviews does not hide the user.
    assert!(!ModeratedUser::is_moderated(&ring_id).await?);
    assert_eq!(
        UserDetails::get_by_id(&ring_id)
            .await?
            .expect("the user stays indexed")
            .name,
        "Watcher:ReviewRing:Ring"
    );

    // A review the user writes later is excluded as it lands.
    let ring_second = buyer_review(&market, &ring_id, '8', 5, "Buy here again.");
    let (ring_second_id, ring_second_path) = test.create_review(&ring_kp, &ring_second).await?;
    assert_moderation(
        &market,
        &ring_id,
        &ring_second_id,
        excluded,
        "Buy here again.",
    )
    .await;
    assert_counted(&market, 1, 2.0).await;
    assert_moderation(
        &market,
        &honest_id,
        &honest_review_id,
        None,
        "Slow shipping.",
    )
    .await;

    test.del(&moderator_kp, &tag.hs_path()).await?;
    assert!(
        ExcludedReviewer::users_of_tag(&default_moderation_tests().id, &tag.create_id())
            .await?
            .is_empty()
    );
    assert_moderation(&market, &ring_id, &ring_first_id, None, "Best seller ever.").await;
    assert_counted(&market, 3, 4.0).await;

    test.del(&ring_kp, &ring_first_path).await?;
    test.del(&ring_kp, &ring_second_path).await?;
    test.del(&honest_kp, &honest_path).await?;
    test.cleanup_user(&ring_kp).await?;
    test.cleanup_user(&honest_kp).await?;
    test.cleanup_user(&market.seller_kp).await?;
    Ok(())
}

/// A release re-derives the reviews before it removes the marker. Another
/// process re-deriving them in between (the one-shot recompute next to a live
/// watcher) must not write the released moderation back: that write would
/// outlive the marker.
#[tokio_shared_rt::test(shared)]
async fn a_rederivation_during_a_release_does_not_bring_the_moderation_back() -> Result<()> {
    let mut test = WatcherTest::setup().await?;
    let market = market(&mut test, "Watcher:ReviewReleaseRace").await?;
    let buyer_kp = Keypair::random();
    let buyer_id = test
        .create_user(
            &buyer_kp,
            &seller_profile("Watcher:ReviewReleaseRace:Buyer"),
        )
        .await?;
    let moderator_kp = moderator_keypair().await;
    test.create_user(
        &moderator_kp,
        &seller_profile("Watcher:ReviewReleaseRace:Moderator"),
    )
    .await?;
    let moderator_id = default_moderation_tests().id;

    let text = "Reviewed while the moderator changes its mind.";
    let review = buyer_review(&market, &buyer_id, 'b', 3, text);
    let (review_id, review_path) = test.create_review(&buyer_kp, &review).await?;

    // Moderating the user deletes their profile, so that case comes last.
    let cases = [
        (
            tag_on_user(&buyer_id, EXCLUDE_LABEL),
            ReviewModerationState::Excluded,
        ),
        (
            tag_on_review(&buyer_id, &review_id, EXCLUDE_LABEL),
            ReviewModerationState::Excluded,
        ),
        (
            tag_on_review(&buyer_id, &review_id, MODERATED_LABEL),
            ReviewModerationState::TextHidden,
        ),
        (
            tag_on_user(&buyer_id, MODERATED_LABEL),
            ReviewModerationState::TextHidden,
        ),
    ];
    for (tag, state) in cases {
        test.put(&moderator_kp, &tag.hs_path(), &tag).await?;
        assert_moderation(
            &market,
            &buyer_id,
            &review_id,
            moderation(state, &tag.label),
            text,
        )
        .await;

        // The release's steps up to the marker removal, with the other
        // process's re-derivation forced right before that removal.
        let tag_id = tag.create_id();
        ModeratedReview::mark_releasing(&moderator_id, &tag_id).await?;
        ExcludedReviewer::mark_releasing(&moderator_id, &tag_id).await?;
        ModeratedUser::mark_releasing(&moderator_id, &tag_id).await?;
        handlers::review::refresh_reviewer_moderation(&buyer_id).await?;
        assert_moderation(&market, &buyer_id, &review_id, None, text).await;
        handlers::review::refresh_reviewer_moderation(&buyer_id).await?;
        assert_moderation(&market, &buyer_id, &review_id, None, text).await;
        assert_counted(&market, 1, 3.0).await;

        // The release itself finishes with nothing to bring back.
        test.del(&moderator_kp, &tag.hs_path()).await?;
        assert_moderation(&market, &buyer_id, &review_id, None, text).await;
        assert_counted(&market, 1, 3.0).await;
        assert!(ModeratedReview::reviews_of_tag(&moderator_id, &tag_id)
            .await?
            .is_empty());
        assert!(ExcludedReviewer::users_of_tag(&moderator_id, &tag_id)
            .await?
            .is_empty());
        assert!(!ModeratedUser::is_moderated(&buyer_id).await?);
    }

    test.del(&buyer_kp, &review_path).await?;
    test.cleanup_user(&buyer_kp).await?;
    test.cleanup_user(&market.seller_kp).await?;
    Ok(())
}

/// A re-derivation reads the indexed review before it writes the moderation.
/// The review deleted in between must stay deleted.
#[tokio_shared_rt::test(shared)]
async fn a_moderation_write_after_the_review_is_deleted_does_not_index_it_again() -> Result<()> {
    let mut test = WatcherTest::setup().await?;
    let market = market(&mut test, "Watcher:ReviewDeleteRace").await?;
    let buyer_kp = Keypair::random();
    let buyer_id = test
        .create_user(&buyer_kp, &seller_profile("Watcher:ReviewDeleteRace:Buyer"))
        .await?;
    let review = buyer_review(&market, &buyer_id, 'd', 4, "Deleted while re-derived.");
    let (review_id, review_path) = test.create_review(&buyer_kp, &review).await?;
    assert!(ReviewDetails::get_from_index(&buyer_id, &review_id)
        .await?
        .is_some());

    test.del(&buyer_kp, &review_path).await?;
    // The write a re-derivation that read the review before the deletion makes.
    let written = put_field_if_exists(
        &ReviewDetails::prefix().await,
        &format!("{buyer_id}:{review_id}"),
        "moderation",
        &moderation(ReviewModerationState::Excluded, EXCLUDE_LABEL),
    )
    .await?;
    assert!(!written);
    assert!(ReviewDetails::get_from_index(&buyer_id, &review_id)
        .await?
        .is_none());
    assert_counted(&market, 0, 0.0).await;

    test.cleanup_user(&buyer_kp).await?;
    test.cleanup_user(&market.seller_kp).await?;
    Ok(())
}

/// A marker deleted between the query's match and its property reads still
/// comes back as a row, with every property but its keys read as null. These
/// are those rows: one marker of each kind that has kept only the keys the
/// query matches it by, and one that has kept its tag id but lost its label.
const HOLLOW_MARKERS: [&str; 4] = [
    "CREATE (:ModeratedReview {reviewer_id: $id, review_id: $review_id})",
    "CREATE (:ExcludedReviewer {user_id: $id})",
    "CREATE (:ModeratedUser {user_id: $id})",
    "CREATE (:ModeratedUser {user_id: $id, tag_id: 'half-deleted'})",
];

async fn put_hollow_marker(create: &str, id: &str, review_id: &str) -> Result<()> {
    exec_single_row(
        Query::new("put_hollow_review_marker", create)
            .param("id", id)
            .param("review_id", review_id),
    )
    .await?;
    Ok(())
}

async fn delete_hollow_markers(id: &str) -> Result<()> {
    for delete in [
        "MATCH (m:ModeratedReview {reviewer_id: $id}) DELETE m",
        "MATCH (m:ExcludedReviewer {user_id: $id}) DELETE m",
        "MATCH (m:ModeratedUser {user_id: $id}) DELETE m",
    ] {
        exec_single_row(Query::new("delete_hollow_review_marker", delete).param("id", id)).await?;
    }
    Ok(())
}

/// Reading a review's moderation beside a marker being deleted must never
/// fail, and a marker that is gone must not decide the outcome: it applies no
/// moderation, and it does not outrank the markers still in place.
#[tokio_shared_rt::test(shared)]
async fn a_marker_deleted_while_it_is_read_applies_no_moderation_and_fails_nothing() -> Result<()> {
    let _test = WatcherTest::setup().await?;
    let reviewer_id = Keypair::random().public_key().z32();
    let review_id = "hollowreview";

    for create in HOLLOW_MARKERS {
        put_hollow_marker(create, &reviewer_id, review_id).await?;
        assert_eq!(
            ReviewModeration::get(&reviewer_id, review_id).await?,
            None,
            "{create}"
        );
        delete_hollow_markers(&reviewer_id).await?;
    }

    // A marker that is gone, an exclusion that sorts before everything else
    // if it is read, and a marker that is still there.
    for create in HOLLOW_MARKERS {
        put_hollow_marker(create, &reviewer_id, review_id).await?;
    }
    let moderator_id = default_moderation_tests().id;
    ModeratedReview::put(
        &reviewer_id,
        review_id,
        &marketplace_review_uri_builder(reviewer_id.clone(), review_id.to_string()),
        &moderator_id,
        "hollow-live-tag",
        MODERATED_LABEL,
        ReviewModerationState::TextHidden,
        Utc::now().timestamp_millis(),
    )
    .await?;
    assert_eq!(
        ReviewModeration::get(&reviewer_id, review_id).await?,
        moderation(ReviewModerationState::TextHidden, MODERATED_LABEL)
    );

    ModeratedReview::delete(&moderator_id, "hollow-live-tag").await?;
    assert_eq!(ReviewModeration::get(&reviewer_id, review_id).await?, None);
    delete_hollow_markers(&reviewer_id).await?;
    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn review_tags_that_are_not_the_moderators_moderate_nothing() -> Result<()> {
    let mut test = WatcherTest::setup().await?;
    let market = market(&mut test, "Watcher:ReviewModNone").await?;
    let buyer_kp = Keypair::random();
    let buyer_id = test
        .create_user(&buyer_kp, &seller_profile("Watcher:ReviewModNone:Buyer"))
        .await?;
    let other_kp = Keypair::random();
    test.create_user(&other_kp, &seller_profile("Watcher:ReviewModNone:Other"))
        .await?;
    let moderator_kp = moderator_keypair().await;
    test.create_user(
        &moderator_kp,
        &seller_profile("Watcher:ReviewModNone:Moderator"),
    )
    .await?;

    let text = "A fair review.";
    let review = buyer_review(&market, &buyer_id, '9', 3, text);
    let (review_id, review_path) = test.create_review(&buyer_kp, &review).await?;

    // Another user, the seller and the reviewer using both moderation labels
    // on the review and on the reviewer.
    for tagger in [&other_kp, &market.seller_kp, &buyer_kp] {
        for label in [MODERATED_LABEL, EXCLUDE_LABEL] {
            let on_review = tag_on_review(&buyer_id, &review_id, label);
            test.put(tagger, &on_review.hs_path(), &on_review).await?;
            let on_user = tag_on_user(&buyer_id, label);
            test.put(tagger, &on_user.hs_path(), &on_user).await?;
        }
    }
    // The moderator using a label outside both lists.
    let unrelated = tag_on_review(&buyer_id, &review_id, "helpful");
    test.put(&moderator_kp, &unrelated.hs_path(), &unrelated)
        .await?;

    assert_moderation(&market, &buyer_id, &review_id, None, text).await;
    assert_counted(&market, 1, 3.0).await;
    assert!(!ModeratedUser::is_moderated(&buyer_id).await?);

    test.del(&buyer_kp, &review_path).await?;
    test.cleanup_user(&buyer_kp).await?;
    test.cleanup_user(&market.seller_kp).await?;
    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn the_one_shot_recompute_hides_reviews_of_users_moderated_before_it() -> Result<()> {
    let mut test = WatcherTest::setup().await?;
    let market = market(&mut test, "Watcher:ReviewRecompute").await?;
    let buyer_kp = Keypair::random();
    let buyer_id = test
        .create_user(&buyer_kp, &seller_profile("Watcher:ReviewRecompute:Buyer"))
        .await?;
    let text = "Indexed before review moderation shipped.";
    let review = buyer_review(&market, &buyer_id, 'a', 5, text);
    let (review_id, review_path) = test.create_review(&buyer_kp, &review).await?;

    // A user moderated by a release without review moderation: the marker
    // exists but the review was never marked.
    let moderator_id = default_moderation_tests().id;
    let tag = tag_on_user(&buyer_id, MODERATED_LABEL);
    ModeratedUser::put(
        &buyer_id,
        &tag.uri,
        &moderator_id,
        &tag.create_id(),
        MODERATED_LABEL,
        Utc::now().timestamp_millis(),
    )
    .await?;
    assert_moderation(&market, &buyer_id, &review_id, None, text).await;

    assert!(handlers::review::recompute_all_review_moderation().await? >= 1);
    assert_moderation(
        &market,
        &buyer_id,
        &review_id,
        moderation(ReviewModerationState::TextHidden, MODERATED_LABEL),
        text,
    )
    .await;
    assert_counted(&market, 1, 5.0).await;

    ModeratedUser::delete(&moderator_id, &tag.create_id()).await?;
    handlers::review::recompute_all_review_moderation().await?;
    assert_moderation(&market, &buyer_id, &review_id, None, text).await;

    test.del(&buyer_kp, &review_path).await?;
    test.cleanup_user(&buyer_kp).await?;
    test.cleanup_user(&market.seller_kp).await?;
    Ok(())
}
