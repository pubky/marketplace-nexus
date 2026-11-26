use super::super::moderation::{assert_hidden, assert_listed, seller_profile};
use super::super::utils::{test_listing, test_shop};
use super::{
    follow, follow_path, follows_edge, graph_epoch, profile_path, social_profile, tag, tag_edge_ids,
};
use crate::event_processor::users::utils::find_user_counts;
use crate::event_processor::utils::watcher::{HomeserverHashIdPath, HomeserverPath, WatcherTest};
use anyhow::Result;
use chrono::Utc;
use nexus_common::models::marketplace::{ListingDetails, ShopDetails};
use nexus_common::models::tag::listing::TagListing;
use nexus_common::models::tag::traits::TagCollection;
use nexus_common::models::user::UserDetails;
use pubky::{Keypair, ResourcePath};
use pubky_app_specs::traits::{HasIdPath, HasPath, HashId};
use pubky_app_specs::{
    listing_uri_builder, user_uri_builder, PubkyAppFollow, PubkyAppListingCondition, PubkyAppShop,
    PubkyAppTag, PubkyAppUser,
};
use pubky_social_specs::traits::HashId as _;

fn pubky_app_tag(uri: String, label: &str) -> PubkyAppTag {
    PubkyAppTag {
        uri,
        label: label.to_string(),
        created_at: Utc::now().timestamp_millis(),
    }
}

async fn listing_taggers(owner_id: &str, listing_id: &str, label: &str) -> Result<usize> {
    let tags =
        TagListing::get_from_index(owner_id, Some(listing_id), None, None, None, None, false)
            .await?
            .unwrap_or_default();
    Ok(tags
        .iter()
        .find(|tag| tag.label == label)
        .map_or(0, |tag| tag.taggers_count))
}

/// An existing `pubky.app` seller migrates. Before their `social/v1` profile arrives, the
/// migrator's `social/v1` copies of their follows and tags are ignored, so nothing counts
/// twice. The profile flips them to `v1`: it applies, their tag on another user's profile
/// takes the id of its `social/v1` copy, and from then on their `pubky.app` social writes are
/// ignored while their `social/v1` follows, untags and unfollows apply. Their marketplace
/// records under `pubky.app` index before and after the flip.
#[tokio_shared_rt::test(shared)]
async fn a_pubky_app_seller_migrates_to_social_v1() -> Result<()> {
    let mut test = WatcherTest::setup().await?;
    let user_label = format!("trusted{}", Utc::now().timestamp_micros() % 1_000_000);
    let listing_label = format!("handmade{}", Utc::now().timestamp_micros() % 1_000_000);

    let seller_kp = Keypair::random();
    let seller_id = test
        .create_user(&seller_kp, &seller_profile("Watcher:SocialV1:Migrating"))
        .await?;
    let friend_kp = Keypair::random();
    let friend_id = test
        .create_user(&friend_kp, &seller_profile("Watcher:SocialV1:Friend"))
        .await?;
    let other_kp = Keypair::random();
    let other_id = test
        .create_user(&other_kp, &seller_profile("Watcher:SocialV1:Other"))
        .await?;
    assert_eq!(
        graph_epoch(&seller_id).await?,
        None,
        "a pubky.app user is v0"
    );

    // Indexed from pubky.app while the seller is v0
    let v0_follow = PubkyAppFollow {
        created_at: Utc::now().timestamp_millis(),
    };
    let v0_follow_path: ResourcePath = PubkyAppFollow::create_path(&friend_id).parse()?;
    test.put(&seller_kp, &v0_follow_path, &v0_follow).await?;
    let v0_user_tag = pubky_app_tag(user_uri_builder(friend_id.clone()), &user_label);
    test.put(&seller_kp, &v0_user_tag.hs_path(), &v0_user_tag)
        .await?;
    let shop_path: ResourcePath = PubkyAppShop::create_path().parse()?;
    test.put(&seller_kp, &shop_path, &test_shop(&seller_id))
        .await?;
    let listing = test_listing(
        &seller_id,
        "Migrating boots",
        "fashion",
        PubkyAppListingCondition::New,
        11_000,
    );
    let (listing_id, listing_path) = test.create_listing(&seller_kp, &listing).await?;
    let v0_listing_tag = pubky_app_tag(
        listing_uri_builder(seller_id.clone(), listing_id.clone()),
        &listing_label,
    );
    test.put(&seller_kp, &v0_listing_tag.hs_path(), &v0_listing_tag)
        .await?;

    assert!(follows_edge(&seller_id, &friend_id).await?);
    assert_eq!(find_user_counts(&seller_id).await.following, 1);
    assert_eq!(find_user_counts(&seller_id).await.tagged, 2);
    assert_eq!(
        tag_edge_ids(&seller_id, &user_label).await?,
        [v0_user_tag.create_id()]
    );
    assert_eq!(
        listing_taggers(&seller_id, &listing_id, &listing_label).await?,
        1
    );

    // The migrator copies the follow and the tags to social/v1 first: ignored while v0
    let v1_follow_path = follow(&mut test, &seller_kp, &friend_id).await?;
    let (v1_user_tag, v1_user_tag_path) = tag(
        &mut test,
        &seller_kp,
        &pubky_social_specs::user_uri_builder(friend_id.clone()),
        &user_label,
    )
    .await?;
    let (v1_listing_tag, v1_listing_tag_path) = tag(
        &mut test,
        &seller_kp,
        &listing_uri_builder(seller_id.clone(), listing_id.clone()),
        &listing_label,
    )
    .await?;
    assert_eq!(
        v1_listing_tag.create_id(),
        v0_listing_tag.create_id(),
        "a listing target keeps its address, so its tag keeps its id"
    );
    assert_ne!(v1_user_tag.create_id(), v0_user_tag.create_id());
    assert_eq!(graph_epoch(&seller_id).await?, None);
    assert_eq!(find_user_counts(&seller_id).await.following, 1);
    assert_eq!(find_user_counts(&seller_id).await.tagged, 2);
    assert_eq!(
        tag_edge_ids(&seller_id, &user_label).await?,
        [v0_user_tag.create_id()]
    );
    assert_eq!(
        listing_taggers(&seller_id, &listing_id, &listing_label).await?,
        1
    );
    // A social/v1 write with no pubky.app counterpart is not read before the flip either
    follow(&mut test, &seller_kp, &other_id).await?;
    assert!(!follows_edge(&seller_id, &other_id).await?);

    // The profile, written last, flips the seller
    test.put(
        &seller_kp,
        &profile_path(),
        social_profile("Watcher:SocialV1:Migrated"),
    )
    .await?;
    assert_eq!(graph_epoch(&seller_id).await?.as_deref(), Some("v1"));
    assert_eq!(
        UserDetails::get_by_id(&seller_id).await?.unwrap().name,
        "Watcher:SocialV1:Migrated"
    );
    assert_eq!(
        tag_edge_ids(&seller_id, &user_label).await?,
        [v1_user_tag.create_id()],
        "the tag on the friend's profile carries the id of its social/v1 copy"
    );
    assert_eq!(find_user_counts(&seller_id).await.tagged, 2);

    // Stale pubky.app social writes are ignored, PUT and DEL
    test.put(
        &seller_kp,
        &PubkyAppUser::hs_path(),
        seller_profile("Watcher:SocialV1:Stale"),
    )
    .await?;
    let stale_follow_path: ResourcePath = PubkyAppFollow::create_path(&other_id).parse()?;
    test.put(&seller_kp, &stale_follow_path, &v0_follow).await?;
    let stale_tag = pubky_app_tag(user_uri_builder(other_id.clone()), &user_label);
    test.put(&seller_kp, &stale_tag.hs_path(), &stale_tag)
        .await?;
    test.del(&seller_kp, &v0_follow_path).await?;
    test.del(&seller_kp, &v0_user_tag.hs_path()).await?;
    test.del(&seller_kp, &v0_listing_tag.hs_path()).await?;
    assert_eq!(
        UserDetails::get_by_id(&seller_id).await?.unwrap().name,
        "Watcher:SocialV1:Migrated"
    );
    assert!(!follows_edge(&seller_id, &other_id).await?);
    assert!(follows_edge(&seller_id, &friend_id).await?);
    assert_eq!(
        tag_edge_ids(&seller_id, &user_label).await?,
        [v1_user_tag.create_id()]
    );
    assert_eq!(
        listing_taggers(&seller_id, &listing_id, &listing_label).await?,
        1
    );

    // Marketplace records index after the flip: a new listing, an edit, the shop
    let second = test_listing(
        &seller_id,
        "After the flip",
        "fashion",
        PubkyAppListingCondition::Good,
        7_000,
    );
    let (second_id, second_path) = test.create_listing(&seller_kp, &second).await?;
    assert_listed(&seller_id, &second_id).await;
    let mut edited = listing.clone();
    edited.listing_id = listing_id.clone();
    edited.title = "Migrating boots v2".to_string();
    edited.revision = 2;
    edited.updated_at = "2025-01-02T00:00:00Z".to_string();
    test.put(&seller_kp, &listing_path, &edited).await?;
    assert_eq!(
        ListingDetails::get_by_id(&seller_id, &listing_id)
            .await?
            .unwrap()
            .title,
        "Migrating boots v2"
    );
    test.put(&seller_kp, &shop_path, &test_shop(&seller_id))
        .await?;
    assert!(ShopDetails::get_from_index(&seller_id).await?.is_some());

    // social/v1 writes of the v1 seller apply
    follow(&mut test, &seller_kp, &other_id).await?;
    assert!(follows_edge(&seller_id, &other_id).await?);
    test.del(&seller_kp, &v1_follow_path).await?;
    assert!(!follows_edge(&seller_id, &friend_id).await?);
    test.del(&seller_kp, &v1_user_tag_path).await?;
    assert!(tag_edge_ids(&seller_id, &user_label).await?.is_empty());
    test.del(&seller_kp, &v1_listing_tag_path).await?;
    assert_eq!(
        listing_taggers(&seller_id, &listing_id, &listing_label).await?,
        0
    );
    let counts = find_user_counts(&seller_id).await;
    assert_eq!(counts.following, 1);
    assert_eq!(counts.tagged, 0);

    // A pubky.app marketplace DEL still applies to the v1 seller
    test.del(&seller_kp, &listing_path).await?;
    assert_hidden(&seller_id, &listing_id).await;
    test.del(&seller_kp, &second_path).await?;
    test.del(&seller_kp, &shop_path).await?;
    test.del(&seller_kp, &follow_path(&other_id)).await?;
    test.del(&seller_kp, &profile_path()).await?;
    test.cleanup_user(&friend_kp).await?;
    test.cleanup_user(&other_kp).await?;
    Ok(())
}
