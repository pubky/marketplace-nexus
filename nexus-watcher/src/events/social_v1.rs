//! The `social/v1` reader for the three public objects the marketplace indexer reads (the
//! profile, a follow and a tag), over `pubky-social-specs`, and the resolution of the address a
//! tag names in either epoch.

use nexus_common::models::event::EventProcessorError;
use pubky_app_specs::{ParsedUri, PubkyId, Resource};
use pubky_social_specs::traits::Validatable;
use pubky_social_specs::{
    ParsedUri as SocialParsedUri, PubkySocialFollow, PubkySocialTag, PubkySocialUser,
    Resource as SocialResource, Visibility, PUB_CTX,
};

/// The `social/v1` profile in `blob`, read by the spec reader.
pub fn read_profile(blob: &[u8]) -> Result<PubkySocialUser, EventProcessorError> {
    <PubkySocialUser as Validatable>::try_from(blob, "", &PUB_CTX)
        .map_err(EventProcessorError::SpecValidation)
}

/// The `social/v1` follow of `followee_id` in `blob`, read by the spec reader.
pub fn read_follow(
    blob: &[u8],
    followee_id: &str,
) -> Result<PubkySocialFollow, EventProcessorError> {
    <PubkySocialFollow as Validatable>::try_from(blob, followee_id, &PUB_CTX)
        .map_err(EventProcessorError::SpecValidation)
}

/// The `social/v1` tag `tag_id` in `blob`, read by the spec reader: its id must hash its target
/// and label, its label must be stored folded and its target must be a canonical public address.
pub fn read_tag(blob: &[u8], tag_id: &str) -> Result<PubkySocialTag, EventProcessorError> {
    <PubkySocialTag as Validatable>::try_from(blob, tag_id, &PUB_CTX)
        .map_err(EventProcessorError::SpecValidation)
}

/// What the address a tag names is, whichever epoch's address names it.
#[derive(Debug, Clone)]
pub enum TaggedResource {
    /// A resource of the `pubky.app` vocabulary: a user, a post, a tag, a `pubky.app` file or a
    /// marketplace record. A `social/v1` profile, post or tag address and a bare `pubky://<pk>`
    /// resolve to the same resource as the `pubky.app` address of it.
    Resource(Box<ParsedUri>),
    /// A `social/v1` file, content-addressed media this indexer does not keep.
    SocialFile,
    /// An address that is never a tag target: a follow, mute, bookmark, feed, last-read cursor
    /// or any private path. The value names it.
    Prohibited(&'static str),
    /// An external URL, another app's path, or a path no reader recognises.
    Unindexed,
}

impl PartialEq for TaggedResource {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Resource(a), Self::Resource(b)) => {
                a.user_id == b.user_id && a.resource == b.resource
            }
            (Self::SocialFile, Self::SocialFile) | (Self::Unindexed, Self::Unindexed) => true,
            (Self::Prohibited(a), Self::Prohibited(b)) => a == b,
            _ => false,
        }
    }
}

/// Resolves the target of a tag. The `social` namespace is read by the `social/v1` parser, every
/// other public pubky path by the `pubky.app` parser.
pub fn resolve_tag_target(uri: &str) -> TaggedResource {
    if is_private(uri) {
        return TaggedResource::Prohibited("private");
    }
    if let Ok(parsed) = SocialParsedUri::try_from(uri) {
        if parsed.visibility == Visibility::Private {
            return TaggedResource::Prohibited("private");
        }
        let Ok(user_id) = PubkyId::try_from(parsed.user_id.as_ref()) else {
            return TaggedResource::Unindexed;
        };
        let resource = match parsed.resource {
            SocialResource::User => Resource::User,
            SocialResource::Post { id, .. } => Resource::Post(id),
            SocialResource::Tag(tag_id) => Resource::Tag(tag_id),
            SocialResource::File(_) => return TaggedResource::SocialFile,
            SocialResource::Follow(_) => return TaggedResource::Prohibited("follow"),
            SocialResource::Feed(_) => return TaggedResource::Prohibited("feed"),
            SocialResource::Foreign { ref namespace, .. } if namespace == PUBKY_APP_NAMESPACE => {
                return resolve_pubky_app(uri)
            }
            SocialResource::Foreign { .. }
            | SocialResource::UnsupportedVersion { .. }
            | SocialResource::Unknown => return TaggedResource::Unindexed,
            _ => return TaggedResource::Unindexed,
        };
        return TaggedResource::Resource(Box::new(ParsedUri { user_id, resource }));
    }
    resolve_pubky_app(uri)
}

const PUBKY_APP_NAMESPACE: &str = "pubky.app";

fn resolve_pubky_app(uri: &str) -> TaggedResource {
    let Ok(parsed) = ParsedUri::try_from(uri) else {
        return TaggedResource::Unindexed;
    };
    match parsed.resource {
        Resource::Follow(_) => TaggedResource::Prohibited("follow"),
        Resource::Mute(_) => TaggedResource::Prohibited("mute"),
        Resource::Bookmark(_) => TaggedResource::Prohibited("bookmark"),
        Resource::Feed(_) => TaggedResource::Prohibited("feed"),
        Resource::LastRead => TaggedResource::Prohibited("last-read"),
        Resource::Blob(_) | Resource::Unknown => TaggedResource::Unindexed,
        Resource::User
        | Resource::Post(_)
        | Resource::Tag(_)
        | Resource::File(_)
        | Resource::Shop
        | Resource::Listing(_)
        | Resource::Drop(_)
        | Resource::MarketplaceReview(_)
        | Resource::ReviewResponse(_)
        | Resource::AttestorProfile
        | Resource::AttestorAnnotation(_) => TaggedResource::Resource(Box::new(parsed)),
    }
}

/// Whether `uri` is a pubky address under the private root, whatever its namespace.
fn is_private(uri: &str) -> bool {
    uri.strip_prefix("pubky://")
        .and_then(|rest| rest.find('/').map(|slash| &rest[slash..]))
        .is_some_and(|path| path == "/priv" || path.starts_with("/priv/"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use pubky_app_specs::{
        file_uri_builder, listing_uri_builder, marketplace_review_uri_builder,
        post_uri_builder as v0_post_uri_builder, shop_uri_builder, tag_uri_builder as v0_tag_uri,
        user_uri_builder as v0_user_uri_builder,
    };
    use pubky_social_specs::{
        follow_uri_builder, post_uri_builder, tag_uri_builder, user_uri_builder,
    };

    const USER: &str = "operrr8wsbpr3ue9d4qj41ge1kcc6r7fdiy6o3ugjrrhi4y77rdo";
    const OTHER: &str = "8pinxxgqs41n4aididenw5apqp1urfmzdztr8jt4abrkdn435ewo";
    const POST_ID: &str = "0032SSN7Q4EVG";
    const TAG_ID: &str = "8Z8CWH8NVYQY39ZEBFGKQWWEKG";

    fn resource(user: &str, resource: Resource) -> TaggedResource {
        TaggedResource::Resource(Box::new(ParsedUri {
            user_id: PubkyId::try_from(user).unwrap(),
            resource,
        }))
    }

    #[test]
    fn a_user_resolves_from_either_profile_address_and_the_bare_key() {
        for uri in [
            user_uri_builder(OTHER.into()),
            v0_user_uri_builder(OTHER.into()),
            format!("pubky://{OTHER}"),
        ] {
            assert_eq!(
                resolve_tag_target(&uri),
                resource(OTHER, Resource::User),
                "{uri}"
            );
        }
    }

    #[test]
    fn a_post_and_a_tag_resolve_from_either_epoch() {
        for uri in [
            post_uri_builder(OTHER.into(), POST_ID.into()),
            v0_post_uri_builder(OTHER.into(), POST_ID.into()),
        ] {
            assert_eq!(
                resolve_tag_target(&uri),
                resource(OTHER, Resource::Post(POST_ID.into())),
                "{uri}"
            );
        }
        for uri in [
            tag_uri_builder(OTHER.into(), TAG_ID.into()),
            v0_tag_uri(OTHER.into(), TAG_ID.into()),
        ] {
            assert_eq!(
                resolve_tag_target(&uri),
                resource(OTHER, Resource::Tag(TAG_ID.into())),
                "{uri}"
            );
        }
    }

    #[test]
    fn marketplace_records_resolve_through_the_pubky_app_reader() {
        assert_eq!(
            resolve_tag_target(&listing_uri_builder(OTHER.into(), POST_ID.into())),
            resource(OTHER, Resource::Listing(POST_ID.into()))
        );
        assert_eq!(
            resolve_tag_target(&shop_uri_builder(OTHER.into())),
            resource(OTHER, Resource::Shop)
        );
        let review_id = "0".repeat(32);
        assert_eq!(
            resolve_tag_target(&marketplace_review_uri_builder(
                OTHER.into(),
                review_id.clone()
            )),
            resource(OTHER, Resource::MarketplaceReview(review_id))
        );
        assert_eq!(
            resolve_tag_target(&file_uri_builder(OTHER.into(), POST_ID.into())),
            resource(OTHER, Resource::File(POST_ID.into()))
        );
    }

    #[test]
    fn follows_mutes_bookmarks_feeds_last_read_and_private_paths_are_prohibited() {
        for (uri, kind) in [
            (follow_uri_builder(USER.into(), OTHER.into()), "follow"),
            (
                format!("pubky://{USER}/pub/pubky.app/follows/{OTHER}"),
                "follow",
            ),
            (
                format!("pubky://{USER}/pub/pubky.app/mutes/{OTHER}"),
                "mute",
            ),
            (
                format!("pubky://{USER}/pub/pubky.app/bookmarks/{POST_ID}"),
                "bookmark",
            ),
            (
                format!("pubky://{USER}/pub/pubky.app/feeds/{POST_ID}"),
                "feed",
            ),
            (
                format!("pubky://{USER}/pub/pubky.app/last_read"),
                "last-read",
            ),
            (
                format!("pubky://{USER}/priv/social/v1/mutes/{OTHER}.json"),
                "private",
            ),
            (
                format!("pubky://{USER}/priv/pubky.app/marketplace/v2/s/x"),
                "private",
            ),
        ] {
            assert_eq!(
                resolve_tag_target(&uri),
                TaggedResource::Prohibited(kind),
                "{uri}"
            );
        }
    }

    #[test]
    fn files_external_urls_and_other_apps_are_not_indexed() {
        assert_eq!(
            resolve_tag_target(&format!("pubky://{OTHER}/pub/social/v1/files/{TAG_ID}.png")),
            TaggedResource::SocialFile
        );
        for uri in [
            "https://example.com/item".to_string(),
            "nostr:npub1abc".to_string(),
            format!("pubky://{OTHER}/pub/other.app/items/1"),
            format!("pubky://{OTHER}/pub/social/v2/profile.json"),
        ] {
            assert_eq!(resolve_tag_target(&uri), TaggedResource::Unindexed, "{uri}");
        }
    }
}
