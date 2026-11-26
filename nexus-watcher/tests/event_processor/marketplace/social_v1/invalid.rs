use super::super::moderation::seller_profile;
use super::{
    assert_refused, create_social_user, event_line, follow_path, follows_edge, graph_user_exists,
    profile_path, social_tag, tag_edge_ids, tag_path,
};
use crate::event_processor::users::utils::find_user_counts;
use crate::event_processor::utils::watcher::{retrieve_and_handle_event_line, WatcherTest};
use anyhow::Result;
use nexus_common::models::user::UserDetails;
use pubky::{Keypair, ResourcePath};
use pubky_social_specs::traits::{HasIdPath, HashId};
use pubky_social_specs::PubkySocialTag;

/// Writes `body` at `path` without running the watcher, then handles its PUT line directly
/// and returns the outcome.
async fn put_and_handle(
    test: &mut WatcherTest,
    keypair: &Keypair,
    path: &ResourcePath,
    body: Vec<u8>,
) -> std::result::Result<(), nexus_common::models::event::EventProcessorError> {
    let user_id = keypair.public_key().to_z32();
    test.create_file_from_body(keypair, &path.to_string(), body)
        .await
        .expect("the test homeserver stores the record");
    retrieve_and_handle_event_line(
        &event_line("PUT", &user_id, path),
        test.event_processor_runner.moderation.clone(),
    )
    .await
}

/// Profiles the `social/v1` reader rejects never create or change a user.
#[tokio_shared_rt::test(shared)]
async fn invalid_social_v1_profiles_are_refused() -> Result<()> {
    let mut test = WatcherTest::setup().await?;
    let new_kp = Keypair::random();
    test.register_user(&new_kp).await?;
    let new_id = new_kp.public_key().to_z32();
    let long_name = "x".repeat(51);
    let bodies = [
        "not json".to_string(),
        r#"{"bio":"no name"}"#.to_string(),
        format!(r#"{{"name":"{long_name}"}}"#),
        r#"{"name":"Ann","image":"not a uri"}"#.to_string(),
        format!(r#"{{"name":"Ann","image":"pubky://{new_id}/priv/social/v1/files/x.png"}}"#),
        r#"{"name":"Ann","links":[{"title":"x","url":"javascript:alert(1)"}]}"#.to_string(),
        r#"{"name":"Ann","name":"Bob"}"#.to_string(),
    ];
    for body in &bodies {
        let result = put_and_handle(&mut test, &new_kp, &profile_path(), body.clone().into()).await;
        assert_refused(result, "SpecValidation", body);
        assert!(!graph_user_exists(&new_id).await?, "{body}");
    }

    // A v1 user's invalid profile edit leaves their profile as it was
    let user_kp = Keypair::random();
    let user_id = create_social_user(&mut test, &user_kp, "Watcher:SocialV1:Valid").await?;
    for body in &bodies {
        let result =
            put_and_handle(&mut test, &user_kp, &profile_path(), body.clone().into()).await;
        assert_refused(result, "SpecValidation", body);
        assert_eq!(
            UserDetails::get_by_id(&user_id).await?.unwrap().name,
            "Watcher:SocialV1:Valid",
            "{body}"
        );
    }
    test.del(&user_kp, &profile_path()).await?;
    Ok(())
}

/// Follows and tags the `social/v1` reader rejects are never indexed: a bad follow body, a tag
/// whose file name is not its hash, an unfolded or invalid label, a private or versioned
/// target.
#[tokio_shared_rt::test(shared)]
async fn invalid_social_v1_follows_and_tags_are_refused() -> Result<()> {
    let mut test = WatcherTest::setup().await?;
    let user_kp = Keypair::random();
    let user_id = create_social_user(&mut test, &user_kp, "Watcher:SocialV1:BadWriter").await?;
    let other_kp = Keypair::random();
    let other_id = test
        .create_user(&other_kp, &seller_profile("Watcher:SocialV1:BadTarget"))
        .await?;

    for body in [
        r#"{"created_at":"yesterday"}"#,
        "[]",
        "{}",
        r#"{"created_at":9007199254740993}"#,
    ] {
        let result =
            put_and_handle(&mut test, &user_kp, &follow_path(&other_id), body.into()).await;
        assert_refused(result, "SpecValidation", body);
        assert!(!follows_edge(&user_id, &other_id).await?, "{body}");
    }

    let profile = pubky_social_specs::user_uri_builder(other_id.clone());
    let valid = social_tag(&profile, "fine");
    let wrong_id: ResourcePath =
        PubkySocialTag::create_path(&social_tag(&profile, "other").create_id()).parse()?;
    let result = put_and_handle(&mut test, &user_kp, &wrong_id, serde_json::to_vec(&valid)?).await;
    assert_refused(result, "SpecValidation", "a tag file named by another hash");

    let unfolded = PubkySocialTag {
        label: "Fine".to_string(),
        ..valid.clone()
    };
    let spaced = PubkySocialTag {
        label: "fi ne".to_string(),
        ..valid.clone()
    };
    let private = PubkySocialTag {
        uri: format!("pubky://{other_id}/priv/social/v1/bookmarks/x.json"),
        ..valid.clone()
    };
    let versioned = PubkySocialTag {
        uri: format!("pubky://{other_id}/pub/social/v1/posts/0032SSN7Q4EVG/0032SSN7Q4EVJ.json"),
        ..valid.clone()
    };
    for (what, tag) in [
        ("an unfolded label", unfolded),
        ("a label with whitespace", spaced),
        ("a private target", private),
        ("a post version target", versioned),
    ] {
        let path: ResourcePath = PubkySocialTag::create_path(&tag.create_id()).parse()?;
        let result = put_and_handle(&mut test, &user_kp, &path, serde_json::to_vec(&tag)?).await;
        assert_refused(result, "SpecValidation", what);
        assert!(
            tag_edge_ids(&user_id, &tag.label).await?.is_empty(),
            "{what}"
        );
    }
    assert_eq!(find_user_counts(&user_id).await.tagged, 0);
    assert_eq!(find_user_counts(&user_id).await.following, 0);

    test.cleanup_user(&other_kp).await?;
    test.del(&user_kp, &profile_path()).await?;
    Ok(())
}

/// Valid tags on an address that is never a tag target are refused: a follow, mute, bookmark,
/// feed, tag or last-read address in either epoch.
#[tokio_shared_rt::test(shared)]
async fn social_v1_tags_on_prohibited_targets_are_refused() -> Result<()> {
    let mut test = WatcherTest::setup().await?;
    let user_kp = Keypair::random();
    let user_id = create_social_user(&mut test, &user_kp, "Watcher:SocialV1:Prohibited").await?;
    let other_kp = Keypair::random();
    let other_id = test
        .create_user(
            &other_kp,
            &seller_profile("Watcher:SocialV1:ProhibitedOwner"),
        )
        .await?;
    let hash = "8Z8CWH8NVYQY39ZEBFGKQWWEKG";
    let targets = [
        format!("pubky://{other_id}/pub/social/v1/follows/{user_id}.json"),
        format!("pubky://{other_id}/pub/pubky.app/follows/{user_id}"),
        format!("pubky://{other_id}/pub/pubky.app/mutes/{user_id}"),
        format!("pubky://{other_id}/pub/pubky.app/bookmarks/0032SSN7Q4EVG"),
        format!("pubky://{other_id}/pub/social/v1/feeds/{hash}.json"),
        format!("pubky://{other_id}/pub/pubky.app/feeds/0032SSN7Q4EVG"),
        format!("pubky://{other_id}/pub/social/v1/tags/{hash}.json"),
        format!("pubky://{other_id}/pub/pubky.app/tags/{hash}"),
        format!("pubky://{other_id}/pub/pubky.app/last_read"),
    ];
    for target in &targets {
        let tag = social_tag(target, "nope");
        let result = put_and_handle(
            &mut test,
            &user_kp,
            &tag_path(&tag),
            serde_json::to_vec(&tag)?,
        )
        .await;
        assert_refused(result, "UnsupportedTarget", target);
    }
    assert!(tag_edge_ids(&user_id, "nope").await?.is_empty());
    assert_eq!(find_user_counts(&user_id).await.tagged, 0);

    test.cleanup_user(&other_kp).await?;
    test.del(&user_kp, &profile_path()).await?;
    Ok(())
}
