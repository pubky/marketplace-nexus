use super::super::moderation::seller_profile;
use super::super::utils::{test_listing, test_shop};
use super::{follow, follows_edge, graph_epoch, profile_path, social_profile, tag, tag_edge_ids};
use crate::event_processor::posts::utils::find_post_counts;
use crate::event_processor::users::utils::find_user_counts;
use crate::event_processor::utils::watcher::{HomeserverHashIdPath, WatcherTest};
use anyhow::Result;
use chrono::Utc;
use nexus_common::models::tag::listing::TagListing;
use nexus_common::models::tag::post::TagPost;
use nexus_common::models::tag::shop::TagShop;
use nexus_common::models::tag::traits::TagCollection;
use nexus_common::models::tag::user::TagUser;
use pubky::{Keypair, ResourcePath};
use pubky_app_specs::traits::{HasIdPath, HasPath, HashId};
use pubky_app_specs::{
    listing_uri_builder, post_uri_builder, shop_uri_builder, user_uri_builder, PubkyAppFollow,
    PubkyAppListingCondition, PubkyAppPost, PubkyAppPostKind, PubkyAppShop, PubkyAppTag,
};
use pubky_social_specs::traits::HashId as _;
use pubky_social_specs::PubkySocialTag;

fn unique(label: &str) -> String {
    format!("{label}{}", Utc::now().timestamp_micros() % 1_000_000)
}

fn pubky_app_tag(uri: String, label: &str) -> PubkyAppTag {
    PubkyAppTag {
        uri,
        label: label.to_string(),
        created_at: Utc::now().timestamp_millis(),
    }
}

/// The taggers count Redis serves for `label` on a target; 0 when the label is gone.
async fn taggers_count<T: TagCollection>(
    owner_id: &str,
    extra: Option<&str>,
    label: &str,
) -> Result<usize> {
    Ok(
        T::get_from_index(owner_id, extra, None, None, None, None, false)
            .await?
            .unwrap_or_default()
            .iter()
            .find(|tag| tag.label == label)
            .map_or(0, |tag| tag.taggers_count),
    )
}

/// The four tags of the migrating user, by target, with their labels.
struct Labels {
    user: String,
    post: String,
    listing: String,
    shop: String,
}

impl Labels {
    fn all(&self) -> [&str; 4] {
        [&self.user, &self.post, &self.listing, &self.shop]
    }
}

/// Asserts every count the tags and the follow drive, in Neo4j and in Redis, is `expected`.
async fn assert_counted(
    tagger_id: &str,
    friend_id: &str,
    post_id: &str,
    labels: &Labels,
    expected: usize,
    when: &str,
) -> Result<()> {
    for label in labels.all() {
        assert_eq!(
            tag_edge_ids(tagger_id, label).await?.len(),
            expected,
            "{when}: TAGGED edges with {label}"
        );
    }
    assert_eq!(
        follows_edge(tagger_id, friend_id).await?,
        expected == 1,
        "{when}: FOLLOWS edge"
    );
    let n = expected as u32;
    let tagger = find_user_counts(tagger_id).await;
    assert_eq!(tagger.tagged, 4 * n, "{when}: tagger's tagged count");
    assert_eq!(tagger.following, n, "{when}: tagger's following count");
    let friend = find_user_counts(friend_id).await;
    assert_eq!(friend.followers, n, "{when}: friend's followers count");
    assert_eq!(friend.tags, n, "{when}: friend's tags count");
    assert_eq!(friend.unique_tags, n, "{when}: friend's unique tags count");
    let post = find_post_counts(friend_id, post_id).await;
    assert_eq!(post.tags, n, "{when}: post's tags count");
    assert_eq!(post.unique_tags, n, "{when}: post's unique tags count");
    assert_eq!(
        taggers_count::<TagUser>(friend_id, None, &labels.user).await?,
        expected,
        "{when}: user tag taggers"
    );
    assert_eq!(
        taggers_count::<TagPost>(friend_id, Some(post_id), &labels.post).await?,
        expected,
        "{when}: post tag taggers"
    );
    Ok(())
}

/// A `pubky.app` user who follows and tags a friend's profile, post, listing and shop
/// migrates. The migrator's `social/v1` copies land on both sides of the flip: some before it
/// (ignored), the rest after it, and a rerun puts every copy again. Each copy finds the edge
/// the flip left at the copy's id, so every count stays exactly one in Neo4j and Redis; the
/// `pubky.app` DELs are ignored; the `social/v1` DELs, including those of copies the watcher
/// never read after the flip, take every count to exactly zero.
#[tokio_shared_rt::test(shared)]
async fn the_migrators_copies_count_once_and_their_deletes_count_to_zero() -> Result<()> {
    let mut test = WatcherTest::setup().await?;
    let labels = Labels {
        user: unique("copyuser"),
        post: unique("copypost"),
        listing: unique("copylisting"),
        shop: unique("copyshop"),
    };

    let friend_kp = Keypair::random();
    let friend_id = test
        .create_user(&friend_kp, &seller_profile("Watcher:SocialV1:CopiedFriend"))
        .await?;
    let post = PubkyAppPost {
        content: "A post tagged before its tagger migrates".to_string(),
        kind: PubkyAppPostKind::Short,
        parent: None,
        embed: None,
        attachments: None,
        lock: None,
    };
    let (post_id, post_path) = test.create_post(&friend_kp, &post).await?;
    let shop_path: ResourcePath = PubkyAppShop::create_path().parse()?;
    test.put(&friend_kp, &shop_path, &test_shop(&friend_id))
        .await?;
    let listing = test_listing(
        &friend_id,
        "Tagged before the migration",
        "fashion",
        PubkyAppListingCondition::New,
        4_000,
    );
    let (listing_id, listing_path) = test.create_listing(&friend_kp, &listing).await?;

    let tagger_kp = Keypair::random();
    let tagger_id = test
        .create_user(
            &tagger_kp,
            &seller_profile("Watcher:SocialV1:CopyingTagger"),
        )
        .await?;

    // Everything indexed from pubky.app while the tagger is v0
    let v0_follow_path: ResourcePath = PubkyAppFollow::create_path(&friend_id).parse()?;
    let v0_follow = PubkyAppFollow {
        created_at: Utc::now().timestamp_millis(),
    };
    test.put(&tagger_kp, &v0_follow_path, &v0_follow).await?;
    let v0_tags = [
        pubky_app_tag(user_uri_builder(friend_id.clone()), &labels.user),
        pubky_app_tag(
            post_uri_builder(friend_id.clone(), post_id.clone()),
            &labels.post,
        ),
        pubky_app_tag(
            listing_uri_builder(friend_id.clone(), listing_id.clone()),
            &labels.listing,
        ),
        pubky_app_tag(shop_uri_builder(friend_id.clone()), &labels.shop),
    ];
    for v0_tag in &v0_tags {
        test.put(&tagger_kp, &v0_tag.hs_path(), v0_tag).await?;
    }
    assert_counted(&tagger_id, &friend_id, &post_id, &labels, 1, "v0").await?;
    assert_eq!(
        taggers_count::<TagListing>(&friend_id, Some(&listing_id), &labels.listing).await?,
        1
    );
    assert_eq!(
        taggers_count::<TagShop>(&friend_id, None, &labels.shop).await?,
        1
    );

    // The social/v1 copies, by target, as the migrator writes them
    let copy_targets = [
        (
            pubky_social_specs::user_uri_builder(friend_id.clone()),
            &labels.user,
        ),
        (
            pubky_social_specs::post_uri_builder(friend_id.clone(), post_id.clone()),
            &labels.post,
        ),
        (
            listing_uri_builder(friend_id.clone(), listing_id.clone()),
            &labels.listing,
        ),
        (shop_uri_builder(friend_id.clone()), &labels.shop),
    ];

    // The post and shop copies land before the flip: ignored
    let mut copies: Vec<(PubkySocialTag, ResourcePath)> = Vec::new();
    for (target, label) in [&copy_targets[1], &copy_targets[3]] {
        copies.push(tag(&mut test, &tagger_kp, target, label).await?);
    }
    assert_counted(
        &tagger_id,
        &friend_id,
        &post_id,
        &labels,
        1,
        "copies before the flip",
    )
    .await?;

    // The profile flips the tagger
    test.put(
        &tagger_kp,
        &profile_path(),
        social_profile("Watcher:SocialV1:CopiedTagger"),
    )
    .await?;
    assert_eq!(graph_epoch(&tagger_id).await?.as_deref(), Some("v1"));

    // The follow, user and listing copies land after the flip
    let v1_follow_path = follow(&mut test, &tagger_kp, &friend_id).await?;
    copies.push(tag(&mut test, &tagger_kp, &copy_targets[0].0, copy_targets[0].1).await?);
    copies.push(tag(&mut test, &tagger_kp, &copy_targets[2].0, copy_targets[2].1).await?);
    assert_counted(
        &tagger_id,
        &friend_id,
        &post_id,
        &labels,
        1,
        "copies after the flip",
    )
    .await?;

    // Each edge carries the id of its copy, so the copy's DEL finds it
    for (copy, _) in &copies {
        assert_eq!(
            tag_edge_ids(&tagger_id, &copy.label).await?,
            [copy.create_id()],
            "the {} edge carries the id of its social/v1 copy",
            copy.label
        );
    }

    // A rerun of the migrator puts every copy again
    follow(&mut test, &tagger_kp, &friend_id).await?;
    for (copy, path) in &copies {
        test.put(&tagger_kp, path, copy).await?;
    }
    assert_counted(
        &tagger_id,
        &friend_id,
        &post_id,
        &labels,
        1,
        "migrator rerun",
    )
    .await?;
    assert_eq!(
        taggers_count::<TagListing>(&friend_id, Some(&listing_id), &labels.listing).await?,
        1
    );
    assert_eq!(
        taggers_count::<TagShop>(&friend_id, None, &labels.shop).await?,
        1
    );

    // Deleting the pubky.app copies changes nothing for a v1 user
    test.del(&tagger_kp, &v0_follow_path).await?;
    for v0_tag in &v0_tags {
        test.del(&tagger_kp, &v0_tag.hs_path()).await?;
    }
    assert_counted(
        &tagger_id,
        &friend_id,
        &post_id,
        &labels,
        1,
        "pubky.app DELs",
    )
    .await?;

    // Deleting the social/v1 copies removes everything
    test.del(&tagger_kp, &v1_follow_path).await?;
    for (_, path) in &copies {
        test.del(&tagger_kp, path).await?;
    }
    assert_counted(
        &tagger_id,
        &friend_id,
        &post_id,
        &labels,
        0,
        "social/v1 DELs",
    )
    .await?;
    assert_eq!(
        taggers_count::<TagListing>(&friend_id, Some(&listing_id), &labels.listing).await?,
        0
    );
    assert_eq!(
        taggers_count::<TagShop>(&friend_id, None, &labels.shop).await?,
        0
    );

    test.del(&tagger_kp, &profile_path()).await?;
    test.del(&friend_kp, &listing_path).await?;
    test.del(&friend_kp, &shop_path).await?;
    test.cleanup_post(&friend_kp, &post_path).await?;
    test.cleanup_user(&friend_kp).await?;
    Ok(())
}

/// A tag and a follow the user deletes after the migrator copied them but before the flip:
/// the client deletes both copies, the `pubky.app` DELs apply while the user is `v0` and the
/// `social/v1` DELs are ignored. The flip brings nothing back.
#[tokio_shared_rt::test(shared)]
async fn a_tag_deleted_before_the_flip_stays_deleted() -> Result<()> {
    let mut test = WatcherTest::setup().await?;
    let label = unique("deletedcopy");
    let friend_kp = Keypair::random();
    let friend_id = test
        .create_user(&friend_kp, &seller_profile("Watcher:SocialV1:Untagged"))
        .await?;
    let tagger_kp = Keypair::random();
    let tagger_id = test
        .create_user(&tagger_kp, &seller_profile("Watcher:SocialV1:Untagging"))
        .await?;

    let v0_follow_path: ResourcePath = PubkyAppFollow::create_path(&friend_id).parse()?;
    let v0_follow = PubkyAppFollow {
        created_at: Utc::now().timestamp_millis(),
    };
    test.put(&tagger_kp, &v0_follow_path, &v0_follow).await?;
    let v0_tag = pubky_app_tag(user_uri_builder(friend_id.clone()), &label);
    test.put(&tagger_kp, &v0_tag.hs_path(), &v0_tag).await?;
    let v1_follow_path = follow(&mut test, &tagger_kp, &friend_id).await?;
    let (_, v1_tag_path) = tag(
        &mut test,
        &tagger_kp,
        &pubky_social_specs::user_uri_builder(friend_id.clone()),
        &label,
    )
    .await?;
    assert_eq!(
        tag_edge_ids(&tagger_id, &label).await?,
        [v0_tag.create_id()]
    );
    assert_eq!(find_user_counts(&friend_id).await.tags, 1);

    test.del(&tagger_kp, &v0_tag.hs_path()).await?;
    test.del(&tagger_kp, &v1_tag_path).await?;
    test.del(&tagger_kp, &v0_follow_path).await?;
    test.del(&tagger_kp, &v1_follow_path).await?;
    test.put(
        &tagger_kp,
        &profile_path(),
        social_profile("Watcher:SocialV1:Untagged"),
    )
    .await?;
    assert_eq!(graph_epoch(&tagger_id).await?.as_deref(), Some("v1"));

    assert!(tag_edge_ids(&tagger_id, &label).await?.is_empty());
    assert!(!follows_edge(&tagger_id, &friend_id).await?);
    let tagger = find_user_counts(&tagger_id).await;
    assert_eq!(tagger.tagged, 0);
    assert_eq!(tagger.following, 0);
    let friend = find_user_counts(&friend_id).await;
    assert_eq!(friend.tags, 0);
    assert_eq!(friend.unique_tags, 0);
    assert_eq!(friend.followers, 0);
    assert_eq!(taggers_count::<TagUser>(&friend_id, None, &label).await?, 0);

    test.del(&tagger_kp, &profile_path()).await?;
    test.cleanup_user(&friend_kp).await?;
    Ok(())
}
