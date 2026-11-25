use super::super::moderation::seller_profile;
use super::super::utils::{test_listing, test_shop};
use super::{create_social_user, profile_path, tag, tag_edge_ids};
use crate::event_processor::users::utils::find_user_counts;
use crate::event_processor::utils::watcher::WatcherTest;
use anyhow::Result;
use chrono::Utc;
use nexus_common::models::marketplace::ListingsByTagSearch;
use nexus_common::models::tag::listing::TagListing;
use nexus_common::models::tag::search::TagSearch;
use nexus_common::models::tag::shop::TagShop;
use nexus_common::models::tag::traits::TagCollection;
use nexus_common::types::Pagination;
use pubky::{Keypair, ResourcePath};
use pubky_app_specs::{
    listing_uri_builder, shop_uri_builder, traits::HasPath, PubkyAppListingCondition, PubkyAppShop,
};
use pubky_social_specs::traits::HashId;

fn unique(label: &str) -> String {
    format!("{label}{}", Utc::now().timestamp_micros() % 1_000_000)
}

/// A Social v1 user's tags on a `pubky.app` seller's listing and shop are their community
/// tags, indexed as `pubky.app` tags are; the tag DELs remove them from every index.
#[tokio_shared_rt::test(shared)]
async fn social_v1_tags_on_listings_and_shops_index_and_untag() -> Result<()> {
    let mut test = WatcherTest::setup().await?;
    let seller_kp = Keypair::random();
    let seller_id = test
        .create_user(&seller_kp, &seller_profile("Watcher:SocialV1:TaggedSeller"))
        .await?;
    let shop_path: ResourcePath = PubkyAppShop::create_path().parse()?;
    test.put(&seller_kp, &shop_path, &test_shop(&seller_id))
        .await?;
    let listing = test_listing(
        &seller_id,
        "Tagged in v1",
        "fashion",
        PubkyAppListingCondition::New,
        5_000,
    );
    let (listing_id, listing_path) = test.create_listing(&seller_kp, &listing).await?;
    let tagger_kp = Keypair::random();
    let tagger_id = create_social_user(&mut test, &tagger_kp, "Watcher:SocialV1:Tagger").await?;

    let listing_label = unique("v1listing");
    let (_, listing_tag_path) = tag(
        &mut test,
        &tagger_kp,
        &listing_uri_builder(seller_id.clone(), listing_id.clone()),
        &listing_label,
    )
    .await?;
    let tags =
        TagListing::get_from_index(&seller_id, Some(&listing_id), None, None, None, None, false)
            .await?
            .expect("the listing tag is indexed");
    assert_eq!(tags.len(), 1);
    assert_eq!(tags[0].label, listing_label);
    assert_eq!(tags[0].taggers, std::slice::from_ref(&tagger_id));
    let listing_key = format!("{seller_id}:{listing_id}");
    assert!(
        ListingsByTagSearch::get_by_label(&listing_label, Pagination::default())
            .await?
            .is_some_and(|entries| entries.iter().any(|e| e.listing_key == listing_key))
    );
    assert!(
        TagSearch::get_by_label(&listing_label, &Pagination::default())
            .await?
            .is_some_and(|labels| !labels.is_empty())
    );

    let shop_label = unique("v1shop");
    let (_, shop_tag_path) = tag(
        &mut test,
        &tagger_kp,
        &shop_uri_builder(seller_id.clone()),
        &shop_label,
    )
    .await?;
    let tags = TagShop::get_from_index(&seller_id, None, None, None, None, None, false)
        .await?
        .expect("the shop tag is indexed");
    assert!(tags
        .iter()
        .any(|tag| tag.label == shop_label && tag.taggers == [tagger_id.clone()]));
    assert_eq!(find_user_counts(&tagger_id).await.tagged, 2);

    test.del(&tagger_kp, &listing_tag_path).await?;
    test.del(&tagger_kp, &shop_tag_path).await?;
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
    .unwrap_or_default()
    .is_empty());
    assert!(
        !ListingsByTagSearch::get_by_label(&listing_label, Pagination::default())
            .await?
            .unwrap_or_default()
            .iter()
            .any(|e| e.listing_key == listing_key)
    );
    assert!(
        !TagShop::get_from_index(&seller_id, None, None, None, None, None, false)
            .await?
            .unwrap_or_default()
            .iter()
            .any(|tag| tag.label == shop_label)
    );
    assert_eq!(find_user_counts(&tagger_id).await.tagged, 0);

    test.del(&seller_kp, &listing_path).await?;
    test.del(&seller_kp, &shop_path).await?;
    test.cleanup_user(&seller_kp).await?;
    test.del(&tagger_kp, &profile_path()).await?;
    Ok(())
}

/// A Social v1 tag on a user names them by their `social/v1` profile, their `pubky.app`
/// profile or their bare key: each is a tag on that user, keyed by the tag file's id.
#[tokio_shared_rt::test(shared)]
async fn social_v1_tags_on_users_index_under_every_user_address() -> Result<()> {
    let mut test = WatcherTest::setup().await?;
    let tagged_kp = Keypair::random();
    let tagged_id = test
        .create_user(&tagged_kp, &seller_profile("Watcher:SocialV1:TaggedUser"))
        .await?;
    let tagger_kp = Keypair::random();
    let tagger_id =
        create_social_user(&mut test, &tagger_kp, "Watcher:SocialV1:UserTagger").await?;

    for (target, label) in [
        (
            pubky_social_specs::user_uri_builder(tagged_id.clone()),
            unique("v1profile"),
        ),
        (
            pubky_app_specs::user_uri_builder(tagged_id.clone()),
            unique("v0profile"),
        ),
        (format!("pubky://{tagged_id}"), unique("barekey")),
    ] {
        let (social_tag, path) = tag(&mut test, &tagger_kp, &target, &label).await?;
        assert_eq!(
            tag_edge_ids(&tagger_id, &label).await?,
            [social_tag.create_id()],
            "{target}"
        );
        assert_eq!(find_user_counts(&tagged_id).await.tags, 1, "{target}");
        test.del(&tagger_kp, &path).await?;
        assert!(
            tag_edge_ids(&tagger_id, &label).await?.is_empty(),
            "{target}"
        );
        assert_eq!(find_user_counts(&tagged_id).await.tags, 0, "{target}");
    }

    test.cleanup_user(&tagged_kp).await?;
    test.del(&tagger_kp, &profile_path()).await?;
    Ok(())
}
