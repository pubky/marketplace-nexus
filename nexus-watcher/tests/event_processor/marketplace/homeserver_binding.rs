use super::moderation::{
    assert_hidden, assert_listed, moderator_keypair, seller_profile, tag_event_uri, tag_on_listing,
    MODERATED_LABEL,
};
use super::utils::test_listing;
use crate::event_processor::utils::default_moderation_tests;
use crate::event_processor::utils::watcher::{
    retrieve_and_handle_event_line, HomeserverHashIdPath, WatcherTest,
};
use anyhow::Result;
use chrono::Utc;
use nexus_common::get_files_dir_test_pathbuf;
use nexus_common::models::homeserver::Homeserver;
use nexus_common::models::marketplace::{ModeratedListing, ModeratedUser};
use nexus_common::models::user::UserDetails;
use nexus_watcher::service::{EventProcessor, HomeserverPollBackoff};
use pubky::Keypair;
use pubky_app_specs::{
    listing_uri_builder, traits::HashId, user_uri_builder, PubkyAppListingCondition, PubkyAppTag,
    PubkyId,
};
use std::sync::Arc;

/// An event processor polling a homeserver that hosts none of the test's users.
async fn processor_for_another_homeserver(test: &mut WatcherTest) -> Result<EventProcessor> {
    let other_hs = test.testnet.create_random_homeserver().await?.public_key();
    let other_hs_id = PubkyId::try_from(other_hs.z32().as_str()).map_err(anyhow::Error::msg)?;
    Homeserver::persist_if_unknown(other_hs_id.clone()).await?;
    let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    Ok(EventProcessor {
        homeserver: Homeserver::new(other_hs_id),
        limit: 1000,
        files_path: get_files_dir_test_pathbuf(),
        tracer_name: String::from("watcher.test"),
        moderation: Arc::new(default_moderation_tests()),
        shutdown_rx,
        poll_backoff: Arc::new(HomeserverPollBackoff::default()),
    })
}

fn tag_on_user(user_id: &str) -> PubkyAppTag {
    PubkyAppTag {
        uri: user_uri_builder(user_id.to_string()),
        label: MODERATED_LABEL.to_string(),
        created_at: Utc::now().timestamp_millis(),
    }
}

#[tokio_shared_rt::test(shared)]
async fn a_homeserver_feed_only_applies_events_of_the_users_it_hosts() -> Result<()> {
    let mut test = WatcherTest::setup().await?;
    let seller_kp = Keypair::random();
    let seller_id = test
        .create_user(&seller_kp, &seller_profile("Watcher:Binding:Seller"))
        .await?;
    let moderator_kp = moderator_keypair().await;
    let moderator_id = test
        .create_user(&moderator_kp, &seller_profile("Watcher:Binding:Moderator"))
        .await?;

    let listing = test_listing(
        &seller_id,
        "Bound boots",
        "fashion",
        PubkyAppListingCondition::New,
        4_000,
    );
    let (listing_id, listing_path) = test.create_listing(&seller_kp, &listing).await?;
    let moderated = test_listing(
        &seller_id,
        "Moderated boots",
        "fashion",
        PubkyAppListingCondition::New,
        5_000,
    );
    let (moderated_id, moderated_path) = test.create_listing(&seller_kp, &moderated).await?;
    let moderation_tag = tag_on_listing(&seller_id, &moderated_id, MODERATED_LABEL);
    test.put(&moderator_kp, &moderation_tag.hs_path(), &moderation_tag)
        .await?;
    assert_listed(&seller_id, &listing_id).await;
    assert_hidden(&seller_id, &moderated_id).await;

    let other = processor_for_another_homeserver(&mut test).await?;
    other
        .process_event_lines(vec![
            format!(
                "DEL {}",
                listing_uri_builder(seller_id.clone(), listing_id.clone())
            ),
            format!("DEL {}", user_uri_builder(seller_id.clone())),
            format!("DEL {}", tag_event_uri(&moderator_id, &moderation_tag)),
        ])
        .await?;

    assert_listed(&seller_id, &listing_id).await;
    assert!(UserDetails::get_by_id(&seller_id).await?.is_some());
    assert!(ModeratedListing::is_moderated(&seller_id, &moderated_id).await?);
    assert_hidden(&seller_id, &moderated_id).await;

    // The homeserver that hosts them still has its events applied
    test.del(&moderator_kp, &moderation_tag.hs_path()).await?;
    assert_listed(&seller_id, &moderated_id).await;
    test.del(&seller_kp, &listing_path).await?;
    assert_hidden(&seller_id, &listing_id).await;
    test.del(&seller_kp, &moderated_path).await?;
    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn a_moderator_tag_del_lifts_nothing_while_the_tag_is_on_its_homeserver() -> Result<()> {
    let mut test = WatcherTest::setup().await?;
    let seller_kp = Keypair::random();
    let seller_id = test
        .create_user(&seller_kp, &seller_profile("Watcher:KeepMod:Seller"))
        .await?;
    let moderator_kp = moderator_keypair().await;
    let moderator_id = test
        .create_user(&moderator_kp, &seller_profile("Watcher:KeepMod:Moderator"))
        .await?;

    let listing = test_listing(
        &seller_id,
        "Kept down",
        "fashion",
        PubkyAppListingCondition::New,
        6_000,
    );
    let (listing_id, listing_path) = test.create_listing(&seller_kp, &listing).await?;
    let listing_tag = tag_on_listing(&seller_id, &listing_id, MODERATED_LABEL);
    let user_tag = tag_on_user(&seller_id);
    test.put(&moderator_kp, &listing_tag.hs_path(), &listing_tag)
        .await?;
    test.put(&moderator_kp, &user_tag.hs_path(), &user_tag)
        .await?;
    assert!(ModeratedListing::is_moderated(&seller_id, &listing_id).await?);
    assert!(ModeratedUser::is_moderated(&seller_id).await?);

    let moderation = Arc::new(default_moderation_tests());
    for tag in [&listing_tag, &user_tag] {
        retrieve_and_handle_event_line(
            &format!("DEL {}", tag_event_uri(&moderator_id, tag)),
            moderation.clone(),
        )
        .await?;
    }
    assert!(
        ModeratedListing::listings_of_tag(&moderator_id, &listing_tag.create_id())
            .await?
            .contains(&(seller_id.clone(), listing_id.clone())),
        "the listing marker stays while its tag is on the moderator's homeserver"
    );
    assert!(
        ModeratedUser::users_of_tag(&moderator_id, &user_tag.create_id())
            .await?
            .contains(&seller_id),
        "the user marker stays while its tag is on the moderator's homeserver"
    );
    assert_hidden(&seller_id, &listing_id).await;

    // Once the moderator removes the tags, the same DELs lift the moderation
    test.del(&moderator_kp, &user_tag.hs_path()).await?;
    assert!(!ModeratedUser::is_moderated(&seller_id).await?);
    test.del(&moderator_kp, &listing_tag.hs_path()).await?;
    assert!(!ModeratedListing::is_moderated(&seller_id, &listing_id).await?);
    assert_listed(&seller_id, &listing_id).await;

    test.del(&seller_kp, &listing_path).await?;
    Ok(())
}
