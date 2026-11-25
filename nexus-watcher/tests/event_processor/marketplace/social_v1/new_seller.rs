use super::super::moderation::{assert_hidden, assert_listed, seller_profile};
use super::super::utils::{test_listing, test_shop};
use super::{graph_epoch, graph_user_exists, profile_path, social_profile};
use crate::event_processor::utils::watcher::{HomeserverPath, WatcherTest};
use anyhow::Result;
use nexus_common::models::marketplace::ShopDetails;
use nexus_common::models::user::UserDetails;
use pubky::{Keypair, ResourcePath};
use pubky_app_specs::{traits::HasPath, PubkyAppListingCondition, PubkyAppShop, PubkyAppUser};
use pubky_social_specs::{PubkySocialUser, PubkySocialUserLink};

/// A seller who signed up with a Social v1 client has only `/pub/social/v1/profile.json`. That
/// profile creates their user at `v1`, so their shop and listing index without waiting for a
/// `pubky.app` profile that never comes; their later profile edits apply, and a `pubky.app`
/// profile written by a stale client is ignored.
#[tokio_shared_rt::test(shared)]
async fn a_social_v1_only_seller_is_indexed_and_sells() -> Result<()> {
    let mut test = WatcherTest::setup().await?;
    let seller_kp = Keypair::random();
    let seller_id = seller_kp.public_key().to_z32();
    test.register_user(&seller_kp).await?;
    assert!(!graph_user_exists(&seller_id).await?);

    let profile = PubkySocialUser::new(
        "Watcher:SocialV1:NewSeller".to_string(),
        Some("Boots and bags".to_string()),
        Some("https://example.com/avatar.png".to_string()),
        Some(vec![PubkySocialUserLink::new(
            "Shop site".to_string(),
            "https://example.com/".to_string(),
        )]),
        Some("open".to_string()),
    );
    test.put(&seller_kp, &profile_path(), &profile).await?;

    let details = UserDetails::get_by_id(&seller_id)
        .await?
        .expect("the social/v1 profile creates the user");
    assert_eq!(details.name, "Watcher:SocialV1:NewSeller");
    assert_eq!(details.bio.as_deref(), Some("Boots and bags"));
    assert_eq!(
        details.image.as_deref(),
        Some("https://example.com/avatar.png")
    );
    assert_eq!(details.status.as_deref(), Some("open"));
    let links = details.links.expect("the profile links are indexed");
    assert_eq!(links.len(), 1);
    assert_eq!(links[0].title, "Shop site");
    assert_eq!(links[0].url, "https://example.com/");
    assert_eq!(graph_epoch(&seller_id).await?.as_deref(), Some("v1"));

    // Commerce records index for the v1 seller: no missing dependency on a pubky.app profile.
    let shop_path: ResourcePath = PubkyAppShop::create_path().parse()?;
    test.put(&seller_kp, &shop_path, &test_shop(&seller_id))
        .await?;
    assert!(ShopDetails::get_from_graph(&seller_id).await?.is_some());
    assert!(ShopDetails::get_from_index(&seller_id).await?.is_some());
    let listing = test_listing(
        &seller_id,
        "Social v1 boots",
        "fashion",
        PubkyAppListingCondition::New,
        9_000,
    );
    let (listing_id, listing_path) = test.create_listing(&seller_kp, &listing).await?;
    assert_listed(&seller_id, &listing_id).await;

    // A profile edit through social/v1 applies
    test.put(
        &seller_kp,
        &profile_path(),
        social_profile("Watcher:SocialV1:NewSeller:Edited"),
    )
    .await?;
    let details = UserDetails::get_by_id(&seller_id).await?.unwrap();
    assert_eq!(details.name, "Watcher:SocialV1:NewSeller:Edited");
    assert_eq!(details.bio, None);

    // A pubky.app profile from a client that predates the migration is never read again
    test.put(
        &seller_kp,
        &PubkyAppUser::hs_path(),
        seller_profile("Watcher:SocialV1:NewSeller:Stale"),
    )
    .await?;
    let details = UserDetails::get_by_id(&seller_id).await?.unwrap();
    assert_eq!(details.name, "Watcher:SocialV1:NewSeller:Edited");
    assert_eq!(graph_epoch(&seller_id).await?.as_deref(), Some("v1"));

    test.del(&seller_kp, &listing_path).await?;
    assert_hidden(&seller_id, &listing_id).await;
    test.del(&seller_kp, &shop_path).await?;
    assert!(ShopDetails::get_from_graph(&seller_id).await?.is_none());
    test.del(&seller_kp, &PubkyAppUser::hs_path()).await?;
    assert!(UserDetails::get_by_id(&seller_id).await?.is_some());
    // The social/v1 profile DEL is the one that removes the user
    test.del(&seller_kp, &profile_path()).await?;
    assert!(UserDetails::get_by_id(&seller_id).await?.is_none());
    assert!(!graph_user_exists(&seller_id).await?);
    Ok(())
}

/// A listing published before its seller's first social/v1 profile is the same missing
/// dependency a pubky.app seller's early listing is; the profile then indexes the seller and
/// the seller's next listing write indexes.
#[tokio_shared_rt::test(shared)]
async fn a_social_v1_profile_satisfies_the_seller_dependency_of_later_listings() -> Result<()> {
    let mut test = WatcherTest::setup().await?;
    let seller_kp = Keypair::random();
    let seller_id = seller_kp.public_key().to_z32();
    test.register_user(&seller_kp).await?;
    let listing = test_listing(
        &seller_id,
        "Before the profile",
        "fashion",
        PubkyAppListingCondition::Good,
        4_000,
    );
    let (listing_id, listing_path) = test.create_listing(&seller_kp, &listing).await?;
    assert_hidden(&seller_id, &listing_id).await;

    test.put(
        &seller_kp,
        &profile_path(),
        social_profile("Watcher:SocialV1:LateProfile"),
    )
    .await?;
    let mut republished = listing.clone();
    republished.listing_id = listing_id.clone();
    test.put(&seller_kp, &listing_path, &republished).await?;
    assert_listed(&seller_id, &listing_id).await;

    test.del(&seller_kp, &listing_path).await?;
    test.del(&seller_kp, &profile_path()).await?;
    Ok(())
}
