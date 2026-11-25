use super::super::moderation::{seller_profile, MODERATED_LABEL};
use super::{
    assert_stale_pubky_app_writes_ignored, assert_v1_tombstone, create_social_user, graph_epoch,
    own_moderator, profile_path, social_profile, tag,
};
use crate::event_processor::utils::watcher::WatcherTest;
use anyhow::Result;
use nexus_common::models::marketplace::ModeratedUser;
use nexus_common::models::user::UserDetails;
use pubky::Keypair;

/// A user at `v1` who deletes their `social/v1` profile, with nothing in the graph pointing at
/// them, keeps their node as the `[DELETED]` tombstone at `v1`: a removed node would read as
/// `v0` and let the stale `pubky.app` profile, follow and tag a client that predates the
/// migration still writes index the user again. Their next `social/v1` profile restores them.
#[tokio_shared_rt::test(shared)]
async fn a_social_v1_profile_del_keeps_the_user_as_a_v1_tombstone() -> Result<()> {
    let mut test = WatcherTest::setup().await?;
    let user_kp = Keypair::random();
    let user_id = create_social_user(&mut test, &user_kp, "Watcher:SocialV1:Tombstone").await?;
    let other_kp = Keypair::random();
    let other_id = test
        .create_user(
            &other_kp,
            &seller_profile("Watcher:SocialV1:TombstoneOther"),
        )
        .await?;

    test.del(&user_kp, &profile_path()).await?;
    assert_v1_tombstone(&user_id).await?;
    assert_stale_pubky_app_writes_ignored(&mut test, &user_kp, &other_id).await?;

    test.put(
        &user_kp,
        &profile_path(),
        social_profile("Watcher:SocialV1:TombstoneBack"),
    )
    .await?;
    assert_eq!(
        UserDetails::get_by_id(&user_id).await?.unwrap().name,
        "Watcher:SocialV1:TombstoneBack"
    );
    assert_eq!(graph_epoch(&user_id).await?.as_deref(), Some("v1"));

    test.del(&user_kp, &profile_path()).await?;
    test.cleanup_user(&other_kp).await?;
    Ok(())
}

/// The moderator's takedown of a user at `v1` with nothing in the graph pointing at them keeps
/// their node as the tombstone at `v1`, so the stale `pubky.app` writes stay ignored while
/// they are hidden; the release restores them from their `social/v1` profile, still at `v1`.
#[tokio_shared_rt::test(shared)]
async fn a_takedown_of_an_edge_free_social_v1_user_keeps_them_at_v1() -> Result<()> {
    let mut test = WatcherTest::setup().await?;
    let (moderator_kp, _) = own_moderator(&mut test);
    create_social_user(
        &mut test,
        &moderator_kp,
        "Watcher:SocialV1:TombstoneModerator",
    )
    .await?;
    let user_kp = Keypair::random();
    let user_id = create_social_user(&mut test, &user_kp, "Watcher:SocialV1:TakenDown").await?;
    let other_kp = Keypair::random();
    let other_id = test
        .create_user(
            &other_kp,
            &seller_profile("Watcher:SocialV1:TakenDownOther"),
        )
        .await?;

    let (_, takedown_path) = tag(
        &mut test,
        &moderator_kp,
        &pubky_social_specs::user_uri_builder(user_id.clone()),
        MODERATED_LABEL,
    )
    .await?;
    assert!(ModeratedUser::is_moderated(&user_id).await?);
    assert_v1_tombstone(&user_id).await?;
    assert_stale_pubky_app_writes_ignored(&mut test, &user_kp, &other_id).await?;
    assert!(ModeratedUser::is_moderated(&user_id).await?);

    test.del(&moderator_kp, &takedown_path).await?;
    assert!(!ModeratedUser::is_moderated(&user_id).await?);
    assert_eq!(
        UserDetails::get_by_id(&user_id).await?.unwrap().name,
        "Watcher:SocialV1:TakenDown"
    );
    assert_eq!(graph_epoch(&user_id).await?.as_deref(), Some("v1"));

    test.del(&user_kp, &profile_path()).await?;
    test.del(&moderator_kp, &profile_path()).await?;
    test.cleanup_user(&other_kp).await?;
    Ok(())
}
