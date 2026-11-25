use super::super::moderation::{assert_hidden, assert_listed, seller_profile, MODERATED_LABEL};
use super::super::utils::{test_listing, test_shop};
use super::{create_social_user, graph_epoch, own_moderator, profile_path, tag};
use crate::event_processor::utils::watcher::{HomeserverHashIdPath, WatcherTest};
use anyhow::Result;
use chrono::Utc;
use nexus_common::models::marketplace::{
    ExcludedReviewer, ModeratedListing, ModeratedUser, ShopDetails,
};
use nexus_common::models::tag::listing::TagListing;
use nexus_common::models::tag::traits::TagCollection;
use nexus_common::models::user::UserDetails;
use pubky::{Keypair, ResourcePath};
use pubky_app_specs::traits::{HasPath, HashId};
use pubky_app_specs::{
    listing_uri_builder, user_uri_builder, PubkyAppListingCondition, PubkyAppShop, PubkyAppTag,
};
use pubky_social_specs::traits::HashId as _;

/// A seller with a shop and one listing, published through `pubky.app`.
async fn seller_with_listing(
    test: &mut WatcherTest,
    name: &str,
) -> Result<(Keypair, String, String, ResourcePath, ResourcePath)> {
    let seller_kp = Keypair::random();
    let seller_id = test.create_user(&seller_kp, &seller_profile(name)).await?;
    let shop_path: ResourcePath = PubkyAppShop::create_path().parse()?;
    test.put(&seller_kp, &shop_path, &test_shop(&seller_id))
        .await?;
    let listing = test_listing(
        &seller_id,
        name,
        "fashion",
        PubkyAppListingCondition::New,
        8_000,
    );
    let (listing_id, listing_path) = test.create_listing(&seller_kp, &listing).await?;
    assert_listed(&seller_id, &listing_id).await;
    Ok((seller_kp, seller_id, listing_id, listing_path, shop_path))
}

/// The configured moderator, migrated to Social v1, writes its takedown tags under
/// `/pub/social/v1/tags/`: a listing it tags is taken down and comes back when it deletes the
/// tag, a user it tags at their `social/v1` profile is hidden with their shop and listings and
/// restored the same way, and a review-exclusion label on a user excludes their reviews. The
/// same label from any other user is an ordinary community tag.
#[tokio_shared_rt::test(shared)]
async fn a_social_v1_moderator_takes_down_and_restores() -> Result<()> {
    let mut test = WatcherTest::setup().await?;
    let (moderator_kp, _) = own_moderator(&mut test);
    let moderator_id =
        create_social_user(&mut test, &moderator_kp, "Watcher:SocialV1:Moderator").await?;
    assert_eq!(graph_epoch(&moderator_id).await?.as_deref(), Some("v1"));
    let (seller_kp, seller_id, listing_id, listing_path, shop_path) =
        seller_with_listing(&mut test, "Watcher:SocialV1:ModeratedSeller").await?;
    let listing_uri = listing_uri_builder(seller_id.clone(), listing_id.clone());

    // Anyone else's tag with the moderated label is a community tag
    let member_kp = Keypair::random();
    create_social_user(&mut test, &member_kp, "Watcher:SocialV1:Member").await?;
    let (_, member_tag_path) = tag(&mut test, &member_kp, &listing_uri, MODERATED_LABEL).await?;
    assert_listed(&seller_id, &listing_id).await;
    assert!(TagListing::get_from_index(
        &seller_id,
        Some(&listing_id),
        None,
        None,
        None,
        None,
        false
    )
    .await?
    .is_some_and(|tags| tags.iter().any(|t| t.label == MODERATED_LABEL)));
    test.del(&member_kp, &member_tag_path).await?;

    // Listing takedown and release
    let (listing_takedown, listing_takedown_path) =
        tag(&mut test, &moderator_kp, &listing_uri, MODERATED_LABEL).await?;
    assert_hidden(&seller_id, &listing_id).await;
    assert!(ModeratedListing::is_moderated(&seller_id, &listing_id).await?);
    assert_eq!(
        ModeratedListing::listings_of_tag(&moderator_id, &listing_takedown.create_id())
            .await?
            .len(),
        1
    );
    test.del(&moderator_kp, &listing_takedown_path).await?;
    assert!(!ModeratedListing::is_moderated(&seller_id, &listing_id).await?);
    assert_listed(&seller_id, &listing_id).await;

    // User takedown at the social/v1 profile address, and release
    let (_, user_takedown_path) = tag(
        &mut test,
        &moderator_kp,
        &pubky_social_specs::user_uri_builder(seller_id.clone()),
        MODERATED_LABEL,
    )
    .await?;
    assert!(ModeratedUser::is_moderated(&seller_id).await?);
    assert_hidden(&seller_id, &listing_id).await;
    assert!(ShopDetails::get_from_graph(&seller_id).await?.is_none());
    test.del(&moderator_kp, &user_takedown_path).await?;
    assert!(!ModeratedUser::is_moderated(&seller_id).await?);
    assert_listed(&seller_id, &listing_id).await;
    assert!(ShopDetails::get_from_graph(&seller_id).await?.is_some());
    assert!(UserDetails::get_by_id(&seller_id).await?.is_some());

    // Review exclusion of a user, and its release
    let (exclusion, exclusion_path) = tag(
        &mut test,
        &moderator_kp,
        &format!("pubky://{seller_id}"),
        "review_fraud",
    )
    .await?;
    assert_eq!(
        ExcludedReviewer::users_of_tag(&moderator_id, &exclusion.create_id()).await?,
        std::slice::from_ref(&seller_id)
    );
    assert_listed(&seller_id, &listing_id).await;
    test.del(&moderator_kp, &exclusion_path).await?;
    assert!(
        ExcludedReviewer::users_of_tag(&moderator_id, &exclusion.create_id())
            .await?
            .is_empty()
    );

    test.del(&seller_kp, &listing_path).await?;
    test.del(&seller_kp, &shop_path).await?;
    test.cleanup_user(&seller_kp).await?;
    test.del(&member_kp, &profile_path()).await?;
    test.del(&moderator_kp, &profile_path()).await?;
    Ok(())
}

/// A seller who only has a `social/v1` profile, hidden by a user takedown, is restored from
/// that profile when the takedown is lifted: their user comes back at `v1` with their shop and
/// listing, never from a `pubky.app` profile.
#[tokio_shared_rt::test(shared)]
async fn a_hidden_social_v1_seller_is_restored_from_their_social_v1_profile() -> Result<()> {
    let mut test = WatcherTest::setup().await?;
    let (moderator_kp, _) = own_moderator(&mut test);
    create_social_user(
        &mut test,
        &moderator_kp,
        "Watcher:SocialV1:RestoringModerator",
    )
    .await?;
    let seller_kp = Keypair::random();
    let seller_id =
        create_social_user(&mut test, &seller_kp, "Watcher:SocialV1:RestoredSeller").await?;
    let shop_path: ResourcePath = PubkyAppShop::create_path().parse()?;
    test.put(&seller_kp, &shop_path, &test_shop(&seller_id))
        .await?;
    let listing = test_listing(
        &seller_id,
        "Restored from social/v1",
        "fashion",
        PubkyAppListingCondition::New,
        6_000,
    );
    let (listing_id, listing_path) = test.create_listing(&seller_kp, &listing).await?;
    assert_listed(&seller_id, &listing_id).await;

    let (_, takedown_path) = tag(
        &mut test,
        &moderator_kp,
        &format!("pubky://{seller_id}"),
        MODERATED_LABEL,
    )
    .await?;
    assert!(ModeratedUser::is_moderated(&seller_id).await?);
    assert_hidden(&seller_id, &listing_id).await;
    assert!(UserDetails::get_by_id(&seller_id).await?.is_none());

    test.del(&moderator_kp, &takedown_path).await?;
    assert!(!ModeratedUser::is_moderated(&seller_id).await?);
    let details = UserDetails::get_by_id(&seller_id)
        .await?
        .expect("the seller is restored");
    assert_eq!(details.name, "Watcher:SocialV1:RestoredSeller");
    assert_eq!(graph_epoch(&seller_id).await?.as_deref(), Some("v1"));
    assert!(ShopDetails::get_from_graph(&seller_id).await?.is_some());
    assert_listed(&seller_id, &listing_id).await;

    test.del(&seller_kp, &listing_path).await?;
    test.del(&seller_kp, &shop_path).await?;
    test.del(&seller_kp, &profile_path()).await?;
    test.del(&moderator_kp, &profile_path()).await?;
    Ok(())
}

/// A takedown the moderator placed through `pubky.app` holds across the moderator's own
/// migration: its `pubky.app` DEL after the flip is ignored, and the DEL of the `social/v1`
/// copy the migrator wrote lifts it, a user takedown included, whose tag id changes with the
/// profile address.
#[tokio_shared_rt::test(shared)]
async fn a_takedown_holds_across_the_moderators_migration() -> Result<()> {
    let mut test = WatcherTest::setup().await?;
    let (moderator_kp, _) = own_moderator(&mut test);
    let moderator_id = test
        .create_user(
            &moderator_kp,
            &seller_profile("Watcher:SocialV1:MigratingModerator"),
        )
        .await?;
    let (listing_seller_kp, listing_seller_id, listing_id, listing_path, listing_shop_path) =
        seller_with_listing(&mut test, "Watcher:SocialV1:TakenDownListing").await?;
    let (user_seller_kp, user_seller_id, user_listing_id, user_listing_path, user_shop_path) =
        seller_with_listing(&mut test, "Watcher:SocialV1:TakenDownUser").await?;
    let listing_uri = listing_uri_builder(listing_seller_id.clone(), listing_id.clone());

    // Takedowns through pubky.app while the moderator is v0
    let v0_listing_takedown = PubkyAppTag {
        uri: listing_uri.clone(),
        label: MODERATED_LABEL.to_string(),
        created_at: Utc::now().timestamp_millis(),
    };
    test.put(
        &moderator_kp,
        &v0_listing_takedown.hs_path(),
        &v0_listing_takedown,
    )
    .await?;
    let v0_user_takedown = PubkyAppTag {
        uri: user_uri_builder(user_seller_id.clone()),
        label: MODERATED_LABEL.to_string(),
        created_at: Utc::now().timestamp_millis(),
    };
    test.put(
        &moderator_kp,
        &v0_user_takedown.hs_path(),
        &v0_user_takedown,
    )
    .await?;
    assert_hidden(&listing_seller_id, &listing_id).await;
    assert!(ModeratedUser::is_moderated(&user_seller_id).await?);
    assert_hidden(&user_seller_id, &user_listing_id).await;

    // The migrator copies both tags, then the profile flips the moderator
    let (_, v1_listing_takedown_path) =
        tag(&mut test, &moderator_kp, &listing_uri, MODERATED_LABEL).await?;
    let (v1_user_takedown, v1_user_takedown_path) = tag(
        &mut test,
        &moderator_kp,
        &pubky_social_specs::user_uri_builder(user_seller_id.clone()),
        MODERATED_LABEL,
    )
    .await?;
    test.put(
        &moderator_kp,
        &profile_path(),
        super::social_profile("Watcher:SocialV1:MigratedModerator"),
    )
    .await?;
    assert_eq!(graph_epoch(&moderator_id).await?.as_deref(), Some("v1"));
    assert_eq!(
        ModeratedUser::users_of_tag(&moderator_id, &v1_user_takedown.create_id()).await?,
        std::slice::from_ref(&user_seller_id),
        "the user takedown carries the id of its social/v1 copy"
    );
    assert!(
        ModeratedUser::users_of_tag(&moderator_id, &v0_user_takedown.create_id())
            .await?
            .is_empty()
    );

    // The pubky.app DELs no longer lift anything
    test.del(&moderator_kp, &v0_listing_takedown.hs_path())
        .await?;
    test.del(&moderator_kp, &v0_user_takedown.hs_path()).await?;
    assert_hidden(&listing_seller_id, &listing_id).await;
    assert!(ModeratedUser::is_moderated(&user_seller_id).await?);
    assert_hidden(&user_seller_id, &user_listing_id).await;

    // The social/v1 DELs do
    test.del(&moderator_kp, &v1_listing_takedown_path).await?;
    assert!(!ModeratedListing::is_moderated(&listing_seller_id, &listing_id).await?);
    assert_listed(&listing_seller_id, &listing_id).await;
    test.del(&moderator_kp, &v1_user_takedown_path).await?;
    assert!(!ModeratedUser::is_moderated(&user_seller_id).await?);
    assert_listed(&user_seller_id, &user_listing_id).await;

    for (kp, listing, shop) in [
        (&listing_seller_kp, &listing_path, &listing_shop_path),
        (&user_seller_kp, &user_listing_path, &user_shop_path),
    ] {
        test.del(kp, listing).await?;
        test.del(kp, shop).await?;
        test.cleanup_user(kp).await?;
    }
    test.del(&moderator_kp, &profile_path()).await?;
    Ok(())
}
