use std::path::PathBuf;

use crate::events::handlers;
use crate::events::social_v1::TaggedResource;
use nexus_common::models::event::EventProcessorError;
use nexus_common::models::marketplace::ReviewModerationState;
use pubky_app_specs::{PubkyId, Resource};
use tracing::{info, warn};

pub struct Moderation {
    /// Moderator trusted user id
    pub id: PubkyId,
    /// Tags to be moderated (tagged content is deleted; a tagged review's text
    /// is hidden)
    pub tags: Vec<String>,
    /// Tags that, on a review, leave it out of the aggregates and, on a user,
    /// leave out every review the user wrote
    pub review_exclude_tags: Vec<String>,
}

impl Moderation {
    /// Whether a tag with `label` on `target` is a moderation action: placed by
    /// the moderator with a moderated label, or with a review-exclusion label
    /// on a review or a user. Tags of either epoch are judged alike.
    pub fn is_moderation(&self, label: &str, target: &TaggedResource, tagger_id: &PubkyId) -> bool {
        if tagger_id != &self.id {
            return false;
        }
        if self.excludes_reviews(label, target) {
            return true;
        }
        self.tags.iter().any(|tag| tag == label)
    }

    /// Whether the moderator's tag is a review-exclusion label on a review or
    /// a user. Elsewhere such a label is an ordinary tag.
    fn excludes_reviews(&self, label: &str, target: &TaggedResource) -> bool {
        self.review_exclude_tags.iter().any(|tag| tag == label)
            && matches!(
                target,
                TaggedResource::Resource(parsed)
                    if matches!(parsed.resource, Resource::MarketplaceReview(_) | Resource::User)
            )
    }

    /// Applies a moderation tag with `label` on `tagged_uri`, which resolves
    /// to `target`. `moderator_id` and `tag_id` identify the moderator's tag
    /// record, which a listing's, user's or review's replay-safe marker is
    /// keyed by.
    pub async fn apply_moderation(
        &self,
        label: &str,
        tagged_uri: &str,
        target: TaggedResource,
        moderator_id: &PubkyId,
        tag_id: &str,
        files_path: PathBuf,
    ) -> Result<(), EventProcessorError> {
        let excludes_reviews = self.excludes_reviews(label, &target);
        let parsed_uri = match target {
            TaggedResource::Resource(parsed_uri) => *parsed_uri,
            TaggedResource::SocialFile => {
                warn!("Moderation tag '{label}' on {tagged_uri} not applied: social/v1 files are not indexed here");
                return Ok(());
            }
            TaggedResource::Prohibited(_) | TaggedResource::Unindexed => {
                warn!("Moderation tag '{label}' on {tagged_uri} not applied: it names nothing this indexer removes");
                return Ok(());
            }
        };
        let user_id = parsed_uri.user_id;

        match parsed_uri.resource {
            Resource::Post(post_id) => {
                // Delete the post and return the result
                info!(
                    "Moderation tag '{}' detected. Deleting post {}:{}",
                    label, user_id, post_id
                );
                handlers::post::sync_del(user_id, post_id).await
            }
            Resource::Tag(tag_id) => {
                // Delete the tag and return the result
                info!(
                    "Moderation tag '{}' detected. Deleting tag {}:{}",
                    label, user_id, tag_id
                );
                handlers::tag::del(user_id, tag_id).await
            }
            Resource::User if excludes_reviews => {
                handlers::review::exclude_reviewer(user_id, tagged_uri, moderator_id, tag_id, label)
                    .await
            }
            Resource::User => {
                // Hide the user: their profile is deleted, their shop and
                // listings are taken out of the index and kept out, and their
                // reviews' text is hidden
                info!(
                    "Moderation tag '{}' detected. Hiding user {}",
                    label, user_id
                );
                handlers::user::moderate(user_id, tagged_uri, moderator_id, tag_id, label).await
            }
            Resource::File(file_id) => {
                // Delete the file and return the result
                info!(
                    "Moderation tag '{}' detected. Deleting file {}:{}",
                    label, user_id, file_id
                );
                handlers::file::del(&user_id, file_id, files_path).await
            }
            Resource::Listing(listing_id) => {
                handlers::listing::moderate(user_id, listing_id, moderator_id, tag_id, label).await
            }
            Resource::MarketplaceReview(review_id) => {
                let state = if excludes_reviews {
                    ReviewModerationState::Excluded
                } else {
                    ReviewModerationState::TextHidden
                };
                handlers::review::moderate(
                    user_id,
                    review_id,
                    tagged_uri,
                    moderator_id,
                    tag_id,
                    label,
                    state,
                )
                .await
            }
            _ => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Moderation;
    use crate::events::social_v1::{resolve_tag_target, TaggedResource};
    use pubky_app_specs::{
        listing_uri_builder, marketplace_review_uri_builder, post_uri_builder, user_uri_builder,
        PubkyId,
    };

    const MODERATOR: &str = "uo7jgkykft4885n8cruizwy6khw71mnu5pq3ay9i8pw1ymcn85ko";
    const AUTHOR: &str = "8um71us3fyw6h8wbcxb5ar3rwusy1a6u49956ikzojg3gcwd1dty";

    fn moderation() -> Moderation {
        Moderation {
            id: PubkyId::try_from(MODERATOR).unwrap(),
            tags: vec!["hatespeech".to_string()],
            review_exclude_tags: vec!["review_fraud".to_string()],
        }
    }

    fn target(uri: &str) -> TaggedResource {
        resolve_tag_target(uri)
    }

    fn review_uri() -> String {
        marketplace_review_uri_builder(AUTHOR.to_string(), "0".repeat(32))
    }

    #[test]
    fn review_exclusion_labels_moderate_only_reviews_and_users() {
        let moderation = moderation();
        let moderator = PubkyId::try_from(MODERATOR).unwrap();
        assert!(moderation.is_moderation("review_fraud", &target(&review_uri()), &moderator));
        for user_uri in [
            user_uri_builder(AUTHOR.to_string()),
            pubky_social_specs::user_uri_builder(AUTHOR.to_string()),
            format!("pubky://{AUTHOR}"),
        ] {
            assert!(
                moderation.is_moderation("review_fraud", &target(&user_uri), &moderator),
                "{user_uri}"
            );
        }
        assert!(!moderation.is_moderation(
            "review_fraud",
            &target(&post_uri_builder(
                AUTHOR.to_string(),
                "0033SSE3B1FQ0".to_string()
            )),
            &moderator
        ));
        assert!(!moderation.is_moderation(
            "review_fraud",
            &target(&listing_uri_builder(
                AUTHOR.to_string(),
                "0033SSE3B1FQ0".to_string()
            )),
            &moderator
        ));
    }

    #[test]
    fn moderated_labels_moderate_reviews_and_other_labels_do_not() {
        let moderation = moderation();
        let moderator = PubkyId::try_from(MODERATOR).unwrap();
        assert!(moderation.is_moderation("hatespeech", &target(&review_uri()), &moderator));
        assert!(!moderation.is_moderation("helpful", &target(&review_uri()), &moderator));
    }

    #[test]
    fn only_the_configured_moderator_moderates() {
        let moderation = moderation();
        let other = PubkyId::try_from(AUTHOR).unwrap();
        assert!(!moderation.is_moderation("hatespeech", &target(&review_uri()), &other));
        assert!(!moderation.is_moderation("review_fraud", &target(&review_uri()), &other));
    }
}
