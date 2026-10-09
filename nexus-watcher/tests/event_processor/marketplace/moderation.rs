use super::utils::test_listing;
use crate::event_processor::utils::default_moderation_tests;
use crate::event_processor::utils::watcher::{
    retrieve_and_handle_event_line, HomeserverHashIdPath, WatcherTest,
};
use anyhow::Result;
use chrono::Utc;
use nexus_common::db::kv::SortOrder;
use nexus_common::db::{fetch_all_rows_from_graph, queries};
use nexus_common::models::event::EventType;
use nexus_common::models::marketplace::{
    ListingDetails, ListingStream, ListingStreamFilters, ListingStreamSorting, ModeratedListing,
};
use nexus_common::models::tag::listing::TagListing;
use nexus_common::models::tag::traits::TagCollection;
use nexus_common::types::Pagination;
use nexus_watcher::events::handlers::listing::release_moderation;
use nexus_watcher::events::retry::event::RetryEvent;
use pubky::{recovery_file, Keypair};
use pubky_app_specs::{
    listing_uri_builder,
    traits::{HasIdPath, HashId, TimestampId},
    PubkyAppListing, PubkyAppListingCondition, PubkyAppTag, PubkyAppUser, PubkyId,
};
use std::sync::Arc;
use tokio::fs;

/// The label `default_moderation_tests` moderates on behalf of the test moderator.
const MODERATED_LABEL: &str = "label_to_moderate";

fn seller_profile(name: &str) -> PubkyAppUser {
    PubkyAppUser {
        bio: None,
        image: None,
        links: None,
        name: name.to_string(),
        status: None,
    }
}

async fn moderator_keypair() -> Keypair {
    let recovery = fs::read("./tests/event_processor/utils/moderator_key.pkarr")
        .await
        .unwrap();
    recovery_file::decrypt_recovery_file(&recovery, "password").unwrap()
}

fn tag_on_listing(owner_id: &str, listing_id: &str, label: &str) -> PubkyAppTag {
    PubkyAppTag {
        uri: listing_uri_builder(owner_id.to_string(), listing_id.to_string()),
        label: label.to_string(),
        created_at: Utc::now().timestamp_millis(),
    }
}

fn tag_event_uri(tagger_id: &str, tag: &PubkyAppTag) -> String {
    format!(
        "pubky://{tagger_id}{}",
        PubkyAppTag::create_path(&tag.create_id())
    )
}

fn seller_filters(seller_id: &str) -> ListingStreamFilters {
    ListingStreamFilters {
        seller_id: Some(seller_id.to_string()),
        ..Default::default()
    }
}

async fn seller_stream_ids(seller_id: &str) -> Vec<String> {
    ListingStream::get_listings(
        seller_filters(seller_id),
        Pagination::default(),
        SortOrder::Descending,
        ListingStreamSorting::Timeline,
    )
    .await
    .unwrap()
    .map(|stream| stream.0.into_iter().map(|entry| entry.id.clone()).collect())
    .unwrap_or_default()
}

/// Asserts the listing is everywhere in the index: graph, details cache,
/// the per-seller stream and the detail read.
async fn assert_listed(owner_id: &str, listing_id: &str) {
    assert!(ListingDetails::get_from_graph(owner_id, listing_id)
        .await
        .unwrap()
        .is_some());
    assert!(ListingDetails::get_from_index(owner_id, listing_id)
        .await
        .unwrap()
        .is_some());
    assert!(seller_stream_ids(owner_id)
        .await
        .contains(&listing_id.to_string()));
    assert!(ListingDetails::get_by_id(owner_id, listing_id)
        .await
        .unwrap()
        .is_some());
}

/// Asserts no part of the index serves the listing.
async fn assert_hidden(owner_id: &str, listing_id: &str) {
    assert!(ListingDetails::get_from_graph(owner_id, listing_id)
        .await
        .unwrap()
        .is_none());
    assert!(ListingDetails::get_from_index(owner_id, listing_id)
        .await
        .unwrap()
        .is_none());
    assert!(!seller_stream_ids(owner_id)
        .await
        .contains(&listing_id.to_string()));
    assert!(ListingDetails::get_by_id(owner_id, listing_id)
        .await
        .unwrap()
        .is_none());
}

#[tokio_shared_rt::test(shared)]
async fn moderated_listing_is_hidden_and_restored_when_the_moderator_removes_the_tag() -> Result<()>
{
    let mut test = WatcherTest::setup().await?;
    let seller_kp = Keypair::random();
    let seller_id = test
        .create_user(&seller_kp, &seller_profile("Watcher:ModListing:Seller"))
        .await?;
    let moderator_kp = moderator_keypair().await;
    let moderator_id = test
        .create_user(
            &moderator_kp,
            &seller_profile("Watcher:ModListing:Moderator"),
        )
        .await?;

    let listing = test_listing(
        &seller_id,
        "Moderated boots",
        "fashion",
        PubkyAppListingCondition::New,
        12_000,
    );
    let (listing_id, listing_path) = test.create_listing(&seller_kp, &listing).await?;
    assert_listed(&seller_id, &listing_id).await;

    // A community tag on the listing goes away with it
    let community_tag = tag_on_listing(&seller_id, &listing_id, "handmade");
    test.put(&seller_kp, &community_tag.hs_path(), &community_tag)
        .await?;
    assert!(
        TagListing::get_by_id(&seller_id, Some(&listing_id), None, None, None, None, None)
            .await?
            .is_some_and(|tags| !tags.is_empty())
    );

    // The moderator tags the listing: it disappears from every read
    let moderation_tag = tag_on_listing(&seller_id, &listing_id, MODERATED_LABEL);
    let moderation_path = moderation_tag.hs_path();
    test.put(&moderator_kp, &moderation_path, &moderation_tag)
        .await?;

    assert_hidden(&seller_id, &listing_id).await;
    assert!(ModeratedListing::is_moderated(&seller_id, &listing_id).await?);
    assert!(
        TagListing::get_by_id(&seller_id, Some(&listing_id), None, None, None, None, None)
            .await?
            .is_none_or(|tags| tags.is_empty()),
        "community tags of a moderated listing are removed with it"
    );

    // The moderator's own tag is a marker, not a community tag
    let moderator_tag_rows = fetch_all_rows_from_graph(queries::get::moderated_listings_of_tag(
        &moderator_id,
        &moderation_tag.create_id(),
    ))
    .await?;
    assert_eq!(moderator_tag_rows.len(), 1);

    // A later edit by the seller does not bring it back
    let mut edited = listing.clone();
    edited.listing_id = listing_id.clone();
    edited.title = "Moderated boots v2".to_string();
    edited.revision = 2;
    edited.updated_at = "2025-01-02T00:00:00Z".to_string();
    test.put(&seller_kp, &listing_path, &edited).await?;
    assert_hidden(&seller_id, &listing_id).await;

    // The moderator removes the tag: the listing is back with the seller's latest record
    test.del(&moderator_kp, &moderation_path).await?;
    assert!(!ModeratedListing::is_moderated(&seller_id, &listing_id).await?);
    assert_listed(&seller_id, &listing_id).await;
    let restored = ListingDetails::get_by_id(&seller_id, &listing_id)
        .await?
        .expect("restored listing");
    assert_eq!(restored.title, "Moderated boots v2");
    assert_eq!(restored.revision, 2);

    test.del(&seller_kp, &listing_path).await?;
    assert_hidden(&seller_id, &listing_id).await;
    test.cleanup_user(&seller_kp).await?;
    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn moderation_tag_before_the_listing_keeps_it_hidden_without_a_seller_profile() -> Result<()>
{
    let mut test = WatcherTest::setup().await?;
    // The seller has an account but no profile.json, so no User node: an
    // unmoderated listing would wait in the retry queue for it.
    let seller_kp = Keypair::random();
    test.register_user(&seller_kp).await?;
    let seller_id = seller_kp.public_key().to_z32();
    let moderator_kp = moderator_keypair().await;
    test.create_user(
        &moderator_kp,
        &seller_profile("Watcher:ModBefore:Moderator"),
    )
    .await?;

    let mut listing = test_listing(
        &seller_id,
        "Tagged before published",
        "fashion",
        PubkyAppListingCondition::New,
        5_000,
    );
    listing.listing_id = listing.create_id();
    let listing_id = listing.listing_id.clone();
    let listing_path: pubky::ResourcePath = PubkyAppListing::create_path(&listing_id).parse()?;

    // The tag event comes first
    let moderation_tag = tag_on_listing(&seller_id, &listing_id, MODERATED_LABEL);
    test.put(&moderator_kp, &moderation_tag.hs_path(), &moderation_tag)
        .await?;
    assert!(ModeratedListing::is_moderated(&seller_id, &listing_id).await?);

    // Then the listing event: it is not indexed and does not wait for the seller
    test.put(&seller_kp, &listing_path, &listing).await?;
    assert_hidden(&seller_id, &listing_id).await;
    let retry_key = format!(
        "{}:{}",
        EventType::Put,
        RetryEvent::generate_index_key(&listing_uri_builder(seller_id.clone(), listing_id.clone()))
            .expect("listing retry key")
    );
    assert!(
        RetryEvent::check_uri(&retry_key).await?.is_none(),
        "a moderated listing must not wait in the retry queue for its seller"
    );

    // The seller's profile arriving later changes nothing
    test.create_profile(&seller_kp, &seller_profile("Watcher:ModBefore:Seller"))
        .await?;
    assert_hidden(&seller_id, &listing_id).await;

    test.del(&moderator_kp, &moderation_tag.hs_path()).await?;
    test.del(&seller_kp, &listing_path).await?;
    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn replaying_the_tag_and_the_listing_in_either_order_keeps_the_listing_hidden() -> Result<()>
{
    let mut test = WatcherTest::setup().await?;
    let seller_kp = Keypair::random();
    let seller_id = test
        .create_user(&seller_kp, &seller_profile("Watcher:ModReplay:Seller"))
        .await?;
    let moderator_kp = moderator_keypair().await;
    let moderator_id = test
        .create_user(
            &moderator_kp,
            &seller_profile("Watcher:ModReplay:Moderator"),
        )
        .await?;

    let listing = test_listing(
        &seller_id,
        "Replayed boots",
        "fashion",
        PubkyAppListingCondition::New,
        7_000,
    );
    let (listing_id, listing_path) = test.create_listing(&seller_kp, &listing).await?;
    let moderation_tag = tag_on_listing(&seller_id, &listing_id, MODERATED_LABEL);
    test.put(&moderator_kp, &moderation_tag.hs_path(), &moderation_tag)
        .await?;
    assert_hidden(&seller_id, &listing_id).await;

    let tag_put = format!("PUT {}", tag_event_uri(&moderator_id, &moderation_tag));
    let listing_put = format!(
        "PUT {}",
        listing_uri_builder(seller_id.clone(), listing_id.clone())
    );
    let moderation = Arc::new(default_moderation_tests());
    let tag_id = moderation_tag.create_id();

    // A reindex starts from clean databases: wipe the listing's state and the marker
    for order in [[&tag_put, &listing_put], [&listing_put, &tag_put]] {
        ModeratedListing::delete(&moderator_id, &tag_id).await?;
        nexus_watcher::events::handlers::listing::del(
            PubkyId::try_from(seller_id.as_str()).map_err(anyhow::Error::msg)?,
            listing_id.clone(),
        )
        .await?;
        assert!(!ModeratedListing::is_moderated(&seller_id, &listing_id).await?);

        for line in order {
            retrieve_and_handle_event_line(line, moderation.clone()).await?;
        }
        assert_hidden(&seller_id, &listing_id).await;
        assert!(ModeratedListing::is_moderated(&seller_id, &listing_id).await?);
    }

    test.del(&moderator_kp, &moderation_tag.hs_path()).await?;
    assert_listed(&seller_id, &listing_id).await;
    test.del(&seller_kp, &listing_path).await?;
    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn tags_that_are_not_the_moderators_do_not_hide_a_listing() -> Result<()> {
    let mut test = WatcherTest::setup().await?;
    let seller_kp = Keypair::random();
    let seller_id = test
        .create_user(&seller_kp, &seller_profile("Watcher:ModNone:Seller"))
        .await?;
    let other_kp = Keypair::random();
    let other_id = test
        .create_user(&other_kp, &seller_profile("Watcher:ModNone:Other"))
        .await?;
    let moderator_kp = moderator_keypair().await;
    test.create_user(&moderator_kp, &seller_profile("Watcher:ModNone:Moderator"))
        .await?;

    let listing = test_listing(
        &seller_id,
        "Unmoderated boots",
        "fashion",
        PubkyAppListingCondition::New,
        9_000,
    );
    let (listing_id, listing_path) = test.create_listing(&seller_kp, &listing).await?;

    // Another user using the moderated label, and the seller tagging their own listing with it
    for tagger in [&other_kp, &seller_kp] {
        let tag = tag_on_listing(&seller_id, &listing_id, MODERATED_LABEL);
        test.put(tagger, &tag.hs_path(), &tag).await?;
    }
    assert!(!ModeratedListing::is_moderated(&seller_id, &listing_id).await?);
    assert_listed(&seller_id, &listing_id).await;
    let tags = TagListing::get_by_id(&seller_id, Some(&listing_id), None, None, None, None, None)
        .await?
        .expect("community tags are indexed");
    assert_eq!(tags[0].label, MODERATED_LABEL);
    assert_eq!(tags[0].taggers_count, 2);
    assert!(tags[0].taggers.contains(&other_id));

    // The moderator using a label outside the moderated set is a normal tag
    let unrelated = tag_on_listing(&seller_id, &listing_id, "nice-boots");
    test.put(&moderator_kp, &unrelated.hs_path(), &unrelated)
        .await?;
    assert!(!ModeratedListing::is_moderated(&seller_id, &listing_id).await?);
    assert_listed(&seller_id, &listing_id).await;

    test.del(&seller_kp, &listing_path).await?;
    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn listing_stays_hidden_until_every_moderation_tag_is_removed() -> Result<()> {
    let mut test = WatcherTest::setup().await?;
    let seller_kp = Keypair::random();
    let seller_id = test
        .create_user(&seller_kp, &seller_profile("Watcher:ModTwo:Seller"))
        .await?;
    let moderator_kp = moderator_keypair().await;
    let moderator_id = test
        .create_user(&moderator_kp, &seller_profile("Watcher:ModTwo:Moderator"))
        .await?;

    let listing = test_listing(
        &seller_id,
        "Doubly moderated boots",
        "fashion",
        PubkyAppListingCondition::New,
        3_000,
    );
    let (listing_id, listing_path) = test.create_listing(&seller_kp, &listing).await?;
    let moderation_tag = tag_on_listing(&seller_id, &listing_id, MODERATED_LABEL);
    test.put(&moderator_kp, &moderation_tag.hs_path(), &moderation_tag)
        .await?;
    assert_hidden(&seller_id, &listing_id).await;

    // A second moderation record for the same listing (another tag id)
    ModeratedListing::put(
        &seller_id,
        &listing_id,
        &listing_uri_builder(seller_id.clone(), listing_id.clone()),
        &moderator_id,
        "second-tag",
        "moderated",
        Utc::now().timestamp_millis(),
    )
    .await?;

    test.del(&moderator_kp, &moderation_tag.hs_path()).await?;
    assert!(ModeratedListing::is_moderated(&seller_id, &listing_id).await?);
    assert_hidden(&seller_id, &listing_id).await;

    assert!(
        release_moderation(
            &PubkyId::try_from(moderator_id.as_str()).map_err(anyhow::Error::msg)?,
            "second-tag"
        )
        .await?
    );
    assert!(!ModeratedListing::is_moderated(&seller_id, &listing_id).await?);
    assert_listed(&seller_id, &listing_id).await;

    test.del(&seller_kp, &listing_path).await?;
    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn removing_the_tag_does_not_resurrect_a_listing_the_seller_deleted() -> Result<()> {
    let mut test = WatcherTest::setup().await?;
    let seller_kp = Keypair::random();
    let seller_id = test
        .create_user(&seller_kp, &seller_profile("Watcher:ModGone:Seller"))
        .await?;
    let moderator_kp = moderator_keypair().await;
    test.create_user(&moderator_kp, &seller_profile("Watcher:ModGone:Moderator"))
        .await?;

    let listing = test_listing(
        &seller_id,
        "Deleted while moderated",
        "fashion",
        PubkyAppListingCondition::New,
        2_000,
    );
    let (listing_id, listing_path) = test.create_listing(&seller_kp, &listing).await?;
    let moderation_tag = tag_on_listing(&seller_id, &listing_id, MODERATED_LABEL);
    test.put(&moderator_kp, &moderation_tag.hs_path(), &moderation_tag)
        .await?;
    test.del(&seller_kp, &listing_path).await?;

    test.del(&moderator_kp, &moderation_tag.hs_path()).await?;
    assert!(!ModeratedListing::is_moderated(&seller_id, &listing_id).await?);
    assert_hidden(&seller_id, &listing_id).await;
    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn marker_written_while_a_listing_is_being_indexed_removes_it_and_graph_reads_exclude_it(
) -> Result<()> {
    let mut test = WatcherTest::setup().await?;
    let seller_kp = Keypair::random();
    let seller_id = test
        .create_user(&seller_kp, &seller_profile("Watcher:ModRace:Seller"))
        .await?;
    let listing = test_listing(
        &seller_id,
        "Race boots",
        "fashion",
        PubkyAppListingCondition::New,
        4_000,
    );
    let (listing_id, listing_path) = test.create_listing(&seller_kp, &listing).await?;
    assert_listed(&seller_id, &listing_id).await;

    // A moderator tag handled elsewhere has written its marker, but its removal
    // of the listing has not run yet.
    ModeratedListing::put(
        &seller_id,
        &listing_id,
        &listing_uri_builder(seller_id.clone(), listing_id.clone()),
        "moderator",
        "racing-tag",
        "moderated",
        Utc::now().timestamp_millis(),
    )
    .await?;

    // Graph-backed reads already exclude it
    assert!(ListingDetails::get_from_graph(&seller_id, &listing_id)
        .await?
        .is_none());
    let graph_filtered = ListingStream::get_listings(
        ListingStreamFilters {
            seller_id: Some(seller_id.clone()),
            condition: Some(PubkyAppListingCondition::New),
            ..Default::default()
        },
        Pagination::default(),
        SortOrder::Descending,
        ListingStreamSorting::Timeline,
    )
    .await?;
    assert!(graph_filtered.is_none_or(|stream| stream.0.is_empty()));

    // The seller's next write finishes the removal instead of re-adding the listing
    let mut edited = listing.clone();
    edited.listing_id = listing_id.clone();
    edited.revision = 2;
    edited.updated_at = "2025-01-02T00:00:00Z".to_string();
    test.put(&seller_kp, &listing_path, &edited).await?;
    assert_hidden(&seller_id, &listing_id).await;

    ModeratedListing::delete("moderator", "racing-tag").await?;
    test.del(&seller_kp, &listing_path).await?;
    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn moderation_leaves_the_sellers_other_listings_alone() -> Result<()> {
    let mut test = WatcherTest::setup().await?;
    let seller_kp = Keypair::random();
    let seller_id = test
        .create_user(&seller_kp, &seller_profile("Watcher:ModScope:Seller"))
        .await?;
    let moderator_kp = moderator_keypair().await;
    test.create_user(&moderator_kp, &seller_profile("Watcher:ModScope:Moderator"))
        .await?;

    let listing = test_listing(
        &seller_id,
        "Kept boots",
        "fashion",
        PubkyAppListingCondition::New,
        1_000,
    );
    let (kept_id, kept_path) = test.create_listing(&seller_kp, &listing).await?;
    let (hidden_id, hidden_path) = test.create_listing(&seller_kp, &listing).await?;

    let moderation_tag = tag_on_listing(&seller_id, &hidden_id, MODERATED_LABEL);
    test.put(&moderator_kp, &moderation_tag.hs_path(), &moderation_tag)
        .await?;

    assert_hidden(&seller_id, &hidden_id).await;
    assert_listed(&seller_id, &kept_id).await;
    assert_eq!(seller_stream_ids(&seller_id).await, vec![kept_id.clone()]);

    test.del(&moderator_kp, &moderation_tag.hs_path()).await?;
    test.del(&seller_kp, &hidden_path).await?;
    test.del(&seller_kp, &kept_path).await?;
    Ok(())
}
