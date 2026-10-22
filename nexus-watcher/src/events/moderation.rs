use std::path::PathBuf;

use crate::events::handlers;
use nexus_common::models::event::EventProcessorError;
use nexus_common::models::marketplace::ReviewModerationState;
use pubky_app_specs::{ParsedUri, PubkyAppTag, PubkyId, Resource};
use tracing::info;

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
    /// Whether the tag is a moderation action: placed by the moderator with a
    /// moderated label, or with a review-exclusion label on a review or a user.
    pub fn is_moderation(&self, tag: &PubkyAppTag, tagger_id: &PubkyId) -> bool {
        if tagger_id != &self.id {
            return false;
        }
        if self.excludes_reviews(tag) {
            return true;
        }
        self.tags.contains(&tag.label)
    }

    /// Whether the moderator's tag is a review-exclusion label on a review or
    /// a user URI. Elsewhere such a label is an ordinary tag.
    fn excludes_reviews(&self, tag: &PubkyAppTag) -> bool {
        self.review_exclude_tags.contains(&tag.label)
            && ParsedUri::try_from(tag.uri.as_str()).is_ok_and(|parsed| {
                matches!(
                    parsed.resource,
                    Resource::MarketplaceReview(_) | Resource::User
                )
            })
    }

    /// Applies a moderation tag. `moderator_id` and `tag_id` identify the
    /// moderator's tag record, which a listing's, user's or review's
    /// replay-safe marker is keyed by.
    pub async fn apply_moderation(
        &self,
        moderator_tag: PubkyAppTag,
        moderator_id: &PubkyId,
        tag_id: &str,
        files_path: PathBuf,
    ) -> Result<(), EventProcessorError> {
        let excludes_reviews = self.excludes_reviews(&moderator_tag);
        // Parse the embeded URI to extract author_id and post_id using parse_tagged_post_uri
        let parsed_uri = ParsedUri::try_from(moderator_tag.uri.as_str())
            .map_err(EventProcessorError::generic)?;
        let user_id = parsed_uri.user_id;

        match parsed_uri.resource {
            Resource::Post(post_id) => {
                // Delete the post and return the result
                info!(
                    "Moderation tag '{}' detected. Deleting post {}:{}",
                    moderator_tag.label, user_id, post_id
                );
                handlers::post::sync_del(user_id, post_id).await
            }
            Resource::Tag(tag_id) => {
                // Delete the tag and return the result
                info!(
                    "Moderation tag '{}' detected. Deleting tag {}:{}",
                    moderator_tag.label, user_id, tag_id
                );
                handlers::tag::del(user_id, tag_id).await
            }
            Resource::User if excludes_reviews => {
                handlers::review::exclude_reviewer(
                    user_id,
                    &moderator_tag.uri,
                    moderator_id,
                    tag_id,
                    &moderator_tag.label,
                )
                .await
            }
            Resource::User => {
                // Hide the user: their profile is deleted, their shop and
                // listings are taken out of the index and kept out, and their
                // reviews' text is hidden
                info!(
                    "Moderation tag '{}' detected. Hiding user {}",
                    moderator_tag.label, user_id
                );
                handlers::user::moderate(
                    user_id,
                    &moderator_tag.uri,
                    moderator_id,
                    tag_id,
                    &moderator_tag.label,
                )
                .await
            }
            Resource::File(file_id) => {
                // Delete the file and return the result
                info!(
                    "Moderation tag '{}' detected. Deleting file {}:{}",
                    moderator_tag.label, user_id, file_id
                );
                handlers::file::del(&user_id, file_id, files_path).await
            }
            Resource::Listing(listing_id) => {
                handlers::listing::moderate(
                    user_id,
                    listing_id,
                    moderator_id,
                    tag_id,
                    &moderator_tag.label,
                )
                .await
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
                    &moderator_tag.uri,
                    moderator_id,
                    tag_id,
                    &moderator_tag.label,
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
    use pubky_app_specs::{
        listing_uri_builder, marketplace_review_uri_builder, post_uri_builder, user_uri_builder,
        PubkyAppTag, PubkyId,
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

    fn tag(uri: String, label: &str) -> PubkyAppTag {
        PubkyAppTag {
            uri,
            label: label.to_string(),
            created_at: 0,
        }
    }

    fn review_uri() -> String {
        marketplace_review_uri_builder(AUTHOR.to_string(), "0".repeat(32))
    }

    #[test]
    fn review_exclusion_labels_moderate_only_reviews_and_users() {
        let moderation = moderation();
        let moderator = PubkyId::try_from(MODERATOR).unwrap();
        assert!(moderation.is_moderation(&tag(review_uri(), "review_fraud"), &moderator));
        assert!(moderation.is_moderation(
            &tag(user_uri_builder(AUTHOR.to_string()), "review_fraud"),
            &moderator
        ));
        assert!(!moderation.is_moderation(
            &tag(
                post_uri_builder(AUTHOR.to_string(), "0033SSE3B1FQ0".to_string()),
                "review_fraud"
            ),
            &moderator
        ));
        assert!(!moderation.is_moderation(
            &tag(
                listing_uri_builder(AUTHOR.to_string(), "0033SSE3B1FQ0".to_string()),
                "review_fraud"
            ),
            &moderator
        ));
    }

    #[test]
    fn moderated_labels_moderate_reviews_and_other_labels_do_not() {
        let moderation = moderation();
        let moderator = PubkyId::try_from(MODERATOR).unwrap();
        assert!(moderation.is_moderation(&tag(review_uri(), "hatespeech"), &moderator));
        assert!(!moderation.is_moderation(&tag(review_uri(), "helpful"), &moderator));
    }

    #[test]
    fn only_the_configured_moderator_moderates() {
        let moderation = moderation();
        let other = PubkyId::try_from(AUTHOR).unwrap();
        assert!(!moderation.is_moderation(&tag(review_uri(), "hatespeech"), &other));
        assert!(!moderation.is_moderation(&tag(review_uri(), "review_fraud"), &other));
    }
}
