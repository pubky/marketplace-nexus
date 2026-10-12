use super::moderation::{
    assert_hidden, assert_listed, moderator_keypair, seller_profile, seller_stream_ids,
    tag_event_uri, tag_on_listing, MODERATED_LABEL,
};
use super::utils::{test_listing, test_shop};
use crate::event_processor::utils::default_moderation_tests;
use crate::event_processor::utils::watcher::{
    retrieve_and_handle_event_line, HomeserverHashIdPath, HomeserverPath, WatcherTest,
};
use anyhow::Result;
use chrono::Utc;
use nexus_common::models::event::EventProcessorError;
use nexus_common::models::marketplace::{
    ListingDetails, ListingModerationState, ModeratedListing, ModeratedUser, ShopDetails,
};
use nexus_common::models::user::UserDetails;
use nexus_watcher::events::handlers;
use pubky::Keypair;
use pubky_app_specs::{
    listing_uri_builder, shop_uri_builder, traits::HashId, user_uri_builder,
    PubkyAppListingCondition, PubkyAppShop, PubkyAppTag, PubkyAppUser, PubkyId,
};
use std::sync::Arc;

fn tag_on_user(user_id: &str, label: &str) -> PubkyAppTag {
    PubkyAppTag {
        uri: user_uri_builder(user_id.to_string()),
        label: label.to_string(),
        created_at: Utc::now().timestamp_millis(),
    }
}

fn pubky_id(id: &str) -> PubkyId {
    PubkyId::try_from(id).expect("valid pubky id")
}

/// Asserts no part of the index serves the shop.
async fn assert_shop_hidden(owner_id: &str) {
    assert!(ShopDetails::get_from_graph(owner_id)
        .await
        .unwrap()
        .is_none());
    assert!(ShopDetails::get_from_index(owner_id)
        .await
        .unwrap()
        .is_none());
    assert!(ShopDetails::get_by_id(owner_id).await.unwrap().is_none());
}

/// Asserts the shop is in the graph and the details cache.
async fn assert_shop_listed(owner_id: &str) {
    assert!(ShopDetails::get_from_graph(owner_id)
        .await
        .unwrap()
        .is_some());
    assert!(ShopDetails::get_from_index(owner_id)
        .await
        .unwrap()
        .is_some());
}

/// Tolerates what the processor would put in the retry queue: a tag handled
/// before the user it names is indexed, and an event whose owner is not
/// indexed yet.
async fn replay(line: &str) -> Result<()> {
    match retrieve_and_handle_event_line(line, Arc::new(default_moderation_tests())).await {
        Ok(()) | Err(EventProcessorError::SkipIndexing) => Ok(()),
        Err(e) if format!("{e:?}").contains("MissingDependency") => Ok(()),
        Err(e) => Err(anyhow::anyhow!("{e:?}")),
    }
}

struct Seller {
    kp: Keypair,
    id: String,
}

async fn seller_with_shop_and_listings(
    test: &mut WatcherTest,
    name: &str,
    listing_count: usize,
) -> Result<(Seller, Vec<(String, pubky::ResourcePath)>)> {
    let kp = Keypair::random();
    let id = test.create_user(&kp, &seller_profile(name)).await?;
    test.put(&kp, &PubkyAppShop::hs_path(), &test_shop(&id))
        .await?;
    let mut listings = Vec::new();
    for index in 0..listing_count {
        let listing = test_listing(
            &id,
            &format!("{name} boots {index}"),
            "fashion",
            PubkyAppListingCondition::New,
            10_000 + index as i64,
        );
        listings.push(test.create_listing(&kp, &listing).await?);
    }
    Ok((Seller { kp, id }, listings))
}

#[tokio_shared_rt::test(shared)]
async fn moderating_a_user_removes_their_shop_and_every_listing_and_keeps_them_out() -> Result<()> {
    let mut test = WatcherTest::setup().await?;
    let (seller, listings) =
        seller_with_shop_and_listings(&mut test, "Watcher:ModUser:Seller", 2).await?;
    let (bystander, bystander_listings) =
        seller_with_shop_and_listings(&mut test, "Watcher:ModUser:Bystander", 1).await?;
    let moderator_kp = moderator_keypair().await;
    let moderator_id = test
        .create_user(&moderator_kp, &seller_profile("Watcher:ModUser:Moderator"))
        .await?;

    assert_shop_listed(&seller.id).await;
    for (listing_id, _) in &listings {
        assert_listed(&seller.id, listing_id).await;
    }

    // The moderator tags the seller's profile
    let moderation_tag = tag_on_user(&seller.id, MODERATED_LABEL);
    test.put(&moderator_kp, &moderation_tag.hs_path(), &moderation_tag)
        .await?;

    assert_shop_hidden(&seller.id).await;
    for (listing_id, _) in &listings {
        assert_hidden(&seller.id, listing_id).await;
    }
    assert!(seller_stream_ids(&seller.id).await.is_empty());
    assert!(ModeratedUser::is_moderated(&seller.id).await?);
    assert!(UserDetails::get_by_id(&seller.id).await?.is_none());
    assert_eq!(
        ListingModerationState::get(&seller.id, &listings[0].0).await?,
        ListingModerationState {
            moderated: true,
            listing_moderated: false,
            seller_moderated: true,
        }
    );

    // Other sellers are untouched
    assert_shop_listed(&bystander.id).await;
    assert_listed(&bystander.id, &bystander_listings[0].0).await;
    assert_eq!(
        ListingModerationState::get(&bystander.id, &bystander_listings[0].0).await?,
        ListingModerationState {
            moderated: false,
            listing_moderated: false,
            seller_moderated: false,
        }
    );

    // The seller keeps publishing: a new profile, an edited shop, an edited
    // listing and a new listing. None of the shop or listings comes back.
    test.put(
        &seller.kp,
        &PubkyAppUser::hs_path(),
        &seller_profile("Watcher:ModUser:Seller again"),
    )
    .await?;
    let mut shop = test_shop(&seller.id);
    shop.name = "Edited while moderated".to_string();
    shop.revision = 2;
    shop.updated_at = "2025-01-02T00:00:00Z".to_string();
    test.put(&seller.kp, &PubkyAppShop::hs_path(), &shop)
        .await?;
    let mut edited = test_listing(
        &seller.id,
        "Edited while moderated",
        "fashion",
        PubkyAppListingCondition::New,
        1_000,
    );
    edited.listing_id = listings[0].0.clone();
    edited.revision = 2;
    edited.updated_at = "2025-01-02T00:00:00Z".to_string();
    test.put(&seller.kp, &listings[0].1, &edited).await?;
    let fresh = test_listing(
        &seller.id,
        "Published while moderated",
        "fashion",
        PubkyAppListingCondition::New,
        2_000,
    );
    let (fresh_id, fresh_path) = test.create_listing(&seller.kp, &fresh).await?;

    assert_shop_hidden(&seller.id).await;
    assert_hidden(&seller.id, &listings[0].0).await;
    assert_hidden(&seller.id, &fresh_id).await;
    assert!(seller_stream_ids(&seller.id).await.is_empty());
    assert!(
        ListingModerationState::get(&seller.id, &fresh_id)
            .await?
            .moderated,
        "a listing the index never held is still moderated through its seller"
    );

    // The moderator removes the tag: the seller's latest records are back
    test.del(&moderator_kp, &moderation_tag.hs_path()).await?;
    assert!(!ModeratedUser::is_moderated(&seller.id).await?);
    let restored_shop = ShopDetails::get_by_id(&seller.id)
        .await?
        .expect("restored shop");
    assert_eq!(restored_shop.name, "Edited while moderated");
    assert_listed(&seller.id, &listings[0].0).await;
    assert_listed(&seller.id, &listings[1].0).await;
    assert_listed(&seller.id, &fresh_id).await;
    assert_eq!(
        ListingDetails::get_by_id(&seller.id, &listings[0].0)
            .await?
            .expect("restored listing")
            .title,
        "Edited while moderated"
    );
    assert_eq!(
        UserDetails::get_by_id(&seller.id)
            .await?
            .expect("restored user")
            .name,
        "Watcher:ModUser:Seller again"
    );
    assert!(
        !ListingModerationState::get(&seller.id, &fresh_id)
            .await?
            .moderated
    );

    test.del(&seller.kp, &fresh_path).await?;
    test.del(&seller.kp, &PubkyAppShop::hs_path()).await?;
    for (_, path) in &listings {
        test.del(&seller.kp, path).await?;
    }
    for (_, path) in &bystander_listings {
        test.del(&bystander.kp, path).await?;
    }
    test.del(&bystander.kp, &PubkyAppShop::hs_path()).await?;
    let _ = moderator_id;
    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn a_moderated_user_stays_out_when_the_index_is_rebuilt_in_any_event_order() -> Result<()> {
    let mut test = WatcherTest::setup().await?;
    let (seller, listings) =
        seller_with_shop_and_listings(&mut test, "Watcher:ModUserRebuild:Seller", 2).await?;
    let moderator_kp = moderator_keypair().await;
    let moderator_id = test
        .create_user(
            &moderator_kp,
            &seller_profile("Watcher:ModUserRebuild:Moderator"),
        )
        .await?;
    let moderation_tag = tag_on_user(&seller.id, MODERATED_LABEL);
    test.put(&moderator_kp, &moderation_tag.hs_path(), &moderation_tag)
        .await?;
    assert_shop_hidden(&seller.id).await;

    let profile_line = format!("PUT {}", user_uri_builder(seller.id.clone()));
    let shop_line = format!("PUT {}", shop_uri_builder(seller.id.clone()));
    let listing_lines: Vec<String> = listings
        .iter()
        .map(|(listing_id, _)| {
            format!(
                "PUT {}",
                listing_uri_builder(seller.id.clone(), listing_id.clone())
            )
        })
        .collect();
    let tag_line = format!("PUT {}", tag_event_uri(&moderator_id, &moderation_tag));
    let tag_id = moderation_tag.create_id();

    let mut in_order = vec![profile_line.clone(), shop_line.clone()];
    in_order.extend(listing_lines.clone());
    in_order.push(tag_line.clone());

    // The order the homeserver recorded them in, the reverse, and the tag first
    // and last among the seller's records
    let mut reversed = in_order.clone();
    reversed.reverse();
    let mut tag_first = vec![tag_line.clone(), profile_line.clone(), shop_line.clone()];
    tag_first.extend(listing_lines.clone());
    let mut tag_between = vec![profile_line.clone(), tag_line.clone(), shop_line.clone()];
    tag_between.extend(listing_lines.clone());

    for order in [in_order, reversed, tag_first, tag_between] {
        // A rebuild starts from clean databases: wipe the marker, the shop, the
        // listings and the user
        ModeratedUser::delete(&moderator_id, &tag_id).await?;
        for (listing_id, _) in &listings {
            handlers::listing::del(pubky_id(&seller.id), listing_id.clone()).await?;
        }
        handlers::shop::del(pubky_id(&seller.id)).await?;
        let _ = handlers::user::del(pubky_id(&seller.id)).await;
        assert!(!ModeratedUser::is_moderated(&seller.id).await?);

        for line in &order {
            replay(line).await?;
        }
        // The retry queue runs the events that waited for the user again
        for line in order.iter().filter(|line| *line != &tag_line) {
            replay(line).await?;
        }

        assert!(
            ModeratedUser::is_moderated(&seller.id).await?,
            "the marker is rebuilt from the moderator's tag: {order:?}"
        );
        assert_shop_hidden(&seller.id).await;
        for (listing_id, _) in &listings {
            assert_hidden(&seller.id, listing_id).await;
        }
        assert!(seller_stream_ids(&seller.id).await.is_empty());
    }

    test.del(&moderator_kp, &moderation_tag.hs_path()).await?;
    assert_shop_listed(&seller.id).await;
    for (listing_id, _) in &listings {
        assert_listed(&seller.id, listing_id).await;
    }
    test.del(&seller.kp, &PubkyAppShop::hs_path()).await?;
    for (_, path) in &listings {
        test.del(&seller.kp, path).await?;
    }
    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn a_user_stays_hidden_until_every_moderation_tag_is_removed() -> Result<()> {
    let mut test = WatcherTest::setup().await?;
    let (seller, listings) =
        seller_with_shop_and_listings(&mut test, "Watcher:ModUserTwo:Seller", 1).await?;
    let moderator_kp = moderator_keypair().await;
    let moderator_id = test
        .create_user(
            &moderator_kp,
            &seller_profile("Watcher:ModUserTwo:Moderator"),
        )
        .await?;
    let moderation_tag = tag_on_user(&seller.id, MODERATED_LABEL);
    test.put(&moderator_kp, &moderation_tag.hs_path(), &moderation_tag)
        .await?;
    assert_shop_hidden(&seller.id).await;

    // A second moderation record for the same user (another tag id)
    ModeratedUser::put(
        &seller.id,
        &user_uri_builder(seller.id.clone()),
        &moderator_id,
        "second-tag",
        "moderated",
        Utc::now().timestamp_millis(),
    )
    .await?;

    test.del(&moderator_kp, &moderation_tag.hs_path()).await?;
    assert!(ModeratedUser::is_moderated(&seller.id).await?);
    assert_shop_hidden(&seller.id).await;
    assert_hidden(&seller.id, &listings[0].0).await;

    assert!(handlers::user::release_moderation(&pubky_id(&moderator_id), "second-tag").await?);
    assert!(!ModeratedUser::is_moderated(&seller.id).await?);
    assert_shop_listed(&seller.id).await;
    assert_listed(&seller.id, &listings[0].0).await;

    test.del(&seller.kp, &PubkyAppShop::hs_path()).await?;
    test.del(&seller.kp, &listings[0].1).await?;
    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn releasing_a_user_keeps_a_listing_hidden_by_its_own_moderator_tag() -> Result<()> {
    let mut test = WatcherTest::setup().await?;
    let (seller, listings) =
        seller_with_shop_and_listings(&mut test, "Watcher:ModUserBoth:Seller", 2).await?;
    let moderator_kp = moderator_keypair().await;
    test.create_user(
        &moderator_kp,
        &seller_profile("Watcher:ModUserBoth:Moderator"),
    )
    .await?;

    let listing_tag = tag_on_listing(&seller.id, &listings[1].0, MODERATED_LABEL);
    test.put(&moderator_kp, &listing_tag.hs_path(), &listing_tag)
        .await?;
    let user_tag = tag_on_user(&seller.id, MODERATED_LABEL);
    test.put(&moderator_kp, &user_tag.hs_path(), &user_tag)
        .await?;
    assert_shop_hidden(&seller.id).await;
    assert_hidden(&seller.id, &listings[0].0).await;
    assert_hidden(&seller.id, &listings[1].0).await;

    // Releasing the user restores the shop and the unmoderated listing only
    test.del(&moderator_kp, &user_tag.hs_path()).await?;
    assert_shop_listed(&seller.id).await;
    assert_listed(&seller.id, &listings[0].0).await;
    assert_hidden(&seller.id, &listings[1].0).await;
    assert!(ModeratedListing::is_moderated(&seller.id, &listings[1].0).await?);
    assert_eq!(
        seller_stream_ids(&seller.id).await,
        vec![listings[0].0.clone()]
    );

    // Releasing the listing restores it
    test.del(&moderator_kp, &listing_tag.hs_path()).await?;
    assert_listed(&seller.id, &listings[1].0).await;

    test.del(&seller.kp, &PubkyAppShop::hs_path()).await?;
    for (_, path) in &listings {
        test.del(&seller.kp, path).await?;
    }
    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn releasing_a_listing_does_not_resurrect_it_while_its_seller_is_moderated() -> Result<()> {
    let mut test = WatcherTest::setup().await?;
    let (seller, listings) =
        seller_with_shop_and_listings(&mut test, "Watcher:ModUserSeller:Seller", 1).await?;
    let moderator_kp = moderator_keypair().await;
    let moderator_id = test
        .create_user(
            &moderator_kp,
            &seller_profile("Watcher:ModUserSeller:Moderator"),
        )
        .await?;

    let user_tag = tag_on_user(&seller.id, MODERATED_LABEL);
    test.put(&moderator_kp, &user_tag.hs_path(), &user_tag)
        .await?;
    ModeratedListing::put(
        &seller.id,
        &listings[0].0,
        &listing_uri_builder(seller.id.clone(), listings[0].0.clone()),
        &moderator_id,
        "listing-tag",
        "moderated",
        Utc::now().timestamp_millis(),
    )
    .await?;

    assert!(handlers::listing::release_moderation(&pubky_id(&moderator_id), "listing-tag").await?);
    assert!(!ModeratedListing::is_moderated(&seller.id, &listings[0].0).await?);
    assert!(ModeratedUser::is_moderated(&seller.id).await?);
    assert_hidden(&seller.id, &listings[0].0).await;

    test.del(&moderator_kp, &user_tag.hs_path()).await?;
    assert_listed(&seller.id, &listings[0].0).await;

    test.del(&seller.kp, &PubkyAppShop::hs_path()).await?;
    test.del(&seller.kp, &listings[0].1).await?;
    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn a_user_tag_that_is_not_the_moderators_hides_nothing() -> Result<()> {
    let mut test = WatcherTest::setup().await?;
    let (seller, listings) =
        seller_with_shop_and_listings(&mut test, "Watcher:ModUserNone:Seller", 1).await?;
    let other_kp = Keypair::random();
    test.create_user(&other_kp, &seller_profile("Watcher:ModUserNone:Other"))
        .await?;

    let tag = tag_on_user(&seller.id, MODERATED_LABEL);
    test.put(&other_kp, &tag.hs_path(), &tag).await?;

    assert!(!ModeratedUser::is_moderated(&seller.id).await?);
    assert_shop_listed(&seller.id).await;
    assert_listed(&seller.id, &listings[0].0).await;

    test.del(&other_kp, &tag.hs_path()).await?;
    test.del(&seller.kp, &PubkyAppShop::hs_path()).await?;
    test.del(&seller.kp, &listings[0].1).await?;
    Ok(())
}
