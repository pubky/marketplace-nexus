use epoch_gate::Gate;
use handlers::listing::HomeserverRecord;
use nexus_common::models::event::{Event, EventProcessorError, EventType};
use nexus_common::models::marketplace::{
    ExcludedReviewer, ModeratedListing, ModeratedReview, ModeratedUser,
};
use nexus_common::models::user::Epoch;
use pubky_app_specs::{PubkyAppObject, PubkyId, Resource, VALIDATION_LIMITS};
use remote_body::{fetch_record_capped, MAX_RECORD_BYTES};
use retry::queue::RetryQueue;
use std::path::PathBuf;
use std::sync::Arc;
use tracing::{debug, warn};

mod directory_page;
pub mod epoch_gate;
pub mod epoch_metrics;
pub mod flip;
pub mod handlers;
mod moderation;
pub mod remote_body;
pub mod retry;
pub mod social_v1;

pub use moderation::Moderation;

/// What [`handle_gated`] did with an event it did not fail on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Handled {
    Applied,
    /// The epoch gate ignored it.
    Ignored,
}

/// Handles one event after the epoch gate (see [`epoch_gate`]), as [`handle_gated`] does,
/// without saying whether the gate ignored it.
pub async fn handle(
    event: &Event,
    moderation: Arc<Moderation>,
    retry_queue: &RetryQueue,
) -> Result<(), EventProcessorError> {
    handle_gated(event, moderation, retry_queue).await.map(drop)
}

/// Handles one event after the epoch gate (see [`epoch_gate`]). An ignored event is skipped
/// without a fetch, an error or a stored event line. A deferred one is the
/// [`EventProcessorError::DeferredUntilFlip`] the caller parks in `retry_queue`, and a flip
/// that fails is the [`EventProcessorError::FlipFailed`] the caller queues there.
pub async fn handle_gated(
    event: &Event,
    moderation: Arc<Moderation>,
    retry_queue: &RetryQueue,
) -> Result<Handled, EventProcessorError> {
    match epoch_gate::check(event, retry_queue).await? {
        Gate::Ignore => return Ok(Handled::Ignored),
        Gate::Defer => {
            return Err(EventProcessorError::DeferredUntilFlip {
                flip_uri: epoch_gate::flip_profile_uri(&event.parsed_uri.user_id),
            })
        }
        Gate::Flip => Box::pin(flip::flip_and_settle(event, retry_queue)).await?,
        Gate::Handle => match event.event_type {
            EventType::Put => Box::pin(handle_put_event(event, moderation)).await?,
            EventType::Del => Box::pin(handle_del_event(event)).await?,
        },
    }

    event.store_event().await?;
    Ok(Handled::Applied)
}

/// Handles a PUT of one of the `social/v1` objects the marketplace indexer reads, its record
/// read by the spec reader.
async fn handle_social_v1_put(
    event: &Event,
    moderation: Arc<Moderation>,
) -> Result<(), EventProcessorError> {
    let user_id = event.parsed_uri.user_id.clone();
    match &event.parsed_uri.resource {
        Resource::User => flip::put_profile(event).await,
        Resource::Follow(followee_id) => {
            let Some(blob) = fetch_record_capped(&event.uri, MAX_RECORD_BYTES).await? else {
                debug!(
                    "PUT record {} is already gone from its homeserver; skipping",
                    event.uri
                );
                return Ok(());
            };
            social_v1::read_follow(&blob, followee_id)?;
            handlers::follow::sync_put(user_id, followee_id.clone()).await
        }
        Resource::Tag(tag_id) => {
            let Some(blob) = fetch_record_capped(&event.uri, MAX_RECORD_BYTES).await? else {
                debug!(
                    "PUT record {} is already gone from its homeserver; skipping",
                    event.uri
                );
                return Ok(());
            };
            let tag = social_v1::read_tag(&blob, tag_id)?;
            put_tag(
                &tag.label,
                &tag.uri,
                user_id,
                tag_id.clone(),
                &moderation,
                event.files_path.clone(),
            )
            .await
        }
        other => Err(EventProcessorError::InvalidEventLine(format!(
            "social/v1 resource {other} has no handler: {}",
            event.uri
        ))),
    }
}

/// Indexes a tag of either epoch, or applies it when it is a moderation action.
async fn put_tag(
    label: &str,
    tagged_uri: &str,
    tagger_id: PubkyId,
    tag_id: String,
    moderation: &Moderation,
    files_path: PathBuf,
) -> Result<(), EventProcessorError> {
    let target = social_v1::resolve_tag_target(tagged_uri);
    if moderation.is_moderation(label, &target, &tagger_id) {
        moderation
            .apply_moderation(label, tagged_uri, target, &tagger_id, &tag_id, files_path)
            .await
    } else {
        handlers::tag::sync_put(label, tagged_uri, target, tagger_id, tag_id).await
    }
}

pub async fn handle_put_event(
    event: &Event,
    moderation: Arc<Moderation>,
) -> Result<(), EventProcessorError> {
    debug!("Handling PUT event for URI: {}", event.uri);

    if event.epoch == Epoch::V1 {
        return handle_social_v1_put(event, moderation).await;
    }

    let resource = event.parsed_uri.resource.clone();
    let max_bytes = if matches!(resource, Resource::Blob(_)) {
        VALIDATION_LIMITS.max_blob_size_bytes
    } else {
        MAX_RECORD_BYTES
    };
    let Some(blob) = fetch_record_capped(&event.uri, max_bytes).await? else {
        debug!(
            "PUT record {} is already gone from its homeserver; skipping",
            event.uri
        );
        return Ok(());
    };
    if matches!(resource, Resource::Listing(_)) {
        handlers::listing::validate_public_listing_blob(&blob)?;
    }
    // Attestor records verify against the URI's owner, which `from_resource` doesn't see.
    if matches!(
        resource,
        Resource::AttestorProfile | Resource::AttestorAnnotation(_)
    ) {
        let user_id = event.parsed_uri.user_id.clone();
        return handlers::attestor::sync_put(&event.uri, &blob, user_id).await;
    }

    // Use the new importer from pubky-app-specs
    let pubky_object =
        PubkyAppObject::from_resource(&resource, &blob).map_err(EventProcessorError::generic)?;

    let user_id = event.parsed_uri.user_id.clone();
    match (pubky_object, resource) {
        (PubkyAppObject::User(user), Resource::User) => {
            handlers::user::sync_put(user, user_id).await?
        }
        (PubkyAppObject::Post(post), Resource::Post(post_id)) => {
            handlers::post::sync_put(post, user_id, post_id).await?
        }
        (PubkyAppObject::Follow(_follow), Resource::Follow(followee_id)) => {
            handlers::follow::sync_put(user_id, followee_id).await?
        }
        (PubkyAppObject::Mute(_), Resource::Mute(_)) => {
            debug!("Mute events are no longer handled by nexus");
        }
        (PubkyAppObject::Bookmark(bookmark), Resource::Bookmark(bookmark_id)) => {
            handlers::bookmark::sync_put(user_id, bookmark, bookmark_id).await?
        }
        (PubkyAppObject::Tag(tag), Resource::Tag(tag_id)) => {
            put_tag(
                &tag.label,
                &tag.uri,
                user_id,
                tag_id,
                &moderation,
                event.files_path.clone(),
            )
            .await?
        }
        (PubkyAppObject::File(file), Resource::File(file_id)) => {
            handlers::file::sync_put(
                file,
                event.uri.clone(),
                user_id,
                file_id,
                event.files_path.clone(),
            )
            .await?
        }
        (PubkyAppObject::Shop(shop), Resource::Shop) => {
            handlers::shop::sync_put(shop, user_id).await?
        }
        (PubkyAppObject::Listing(listing), Resource::Listing(listing_id)) => {
            handlers::listing::sync_put(*listing, user_id, listing_id).await?
        }
        (PubkyAppObject::Drop(drop), Resource::Drop(drop_id)) => {
            handlers::drop::sync_put(drop, user_id, drop_id).await?
        }
        (PubkyAppObject::MarketplaceReview(review), Resource::MarketplaceReview(review_id)) => {
            handlers::review::sync_put(review, user_id, review_id).await?
        }
        (PubkyAppObject::ReviewResponse(response), Resource::ReviewResponse(review_id)) => {
            handlers::review_response::sync_put(response, user_id, review_id).await?
        }
        other => debug!("Event type not handled, Resource: {other:?}"),
    }
    Ok(())
}

/// Handles a PUT event by fetching the blob from the homeserver
/// and using the importer to convert it to a PubkyAppObject.
pub async fn handle_del_event(event: &Event) -> Result<(), EventProcessorError> {
    debug!("Handling DEL event for URI: {}", event.uri);

    let user_id = event.parsed_uri.user_id.clone();
    match &event.parsed_uri.resource {
        Resource::User => handlers::user::del(user_id).await?,
        Resource::Post(post_id) => handlers::post::del(user_id, post_id.clone()).await?,
        Resource::Follow(followee_id) => {
            handlers::follow::del(user_id, followee_id.clone()).await?
        }
        Resource::Mute(_) => debug!("Mute events are no longer handled by nexus"),
        Resource::Bookmark(bookmark_id) => {
            handlers::bookmark::del(user_id, bookmark_id.clone()).await?
        }
        Resource::Tag(tag_id) => {
            // A DEL line alone never lifts moderation: the marker goes only once the
            // moderator's own homeserver answers that the tag record is gone.
            if tag_has_moderation_markers(&user_id, tag_id).await?
                && handlers::listing::record_on_homeserver(&event.uri).await?
                    == HomeserverRecord::Present
            {
                warn!(
                    "Moderator tag {} is still on its homeserver; keeping its moderation",
                    event.uri
                );
                return Ok(());
            }
            // A moderator's tag on a listing, a user or a review is never
            // indexed as a tag: removing it only lifts the moderation marker.
            let released = handlers::listing::release_moderation(&user_id, tag_id).await?;
            let released = handlers::user::release_moderation(&user_id, tag_id).await? || released;
            let released =
                handlers::review::release_moderation(&user_id, tag_id).await? || released;
            match handlers::tag::del(user_id, tag_id.clone()).await {
                Err(EventProcessorError::SkipIndexing) if released => {}
                other => other?,
            }
        }
        Resource::File(file_id) => {
            handlers::file::del(&user_id, file_id.clone(), event.files_path.clone()).await?
        }
        Resource::Shop => handlers::shop::del(user_id).await?,
        Resource::Listing(listing_id) => {
            handlers::listing::del(user_id, listing_id.clone()).await?
        }
        Resource::Drop(drop_id) => handlers::drop::del(user_id, drop_id.clone()).await?,
        Resource::MarketplaceReview(review_id) => {
            handlers::review::del(user_id, review_id.clone()).await?
        }
        Resource::ReviewResponse(review_id) => {
            handlers::review_response::del(user_id, review_id.clone()).await?
        }
        Resource::AttestorProfile => handlers::attestor::del_profile(user_id).await?,
        Resource::AttestorAnnotation(order_ref) => {
            handlers::attestor::del_annotation(user_id, order_ref.clone()).await?
        }
        other => debug!("DEL event type not handled for resource: {other:?}"),
    }
    Ok(())
}

async fn tag_has_moderation_markers(
    tagger_id: &PubkyId,
    tag_id: &str,
) -> Result<bool, EventProcessorError> {
    if !ModeratedListing::listings_of_tag(tagger_id, tag_id)
        .await?
        .is_empty()
    {
        return Ok(true);
    }
    if !ModeratedUser::users_of_tag(tagger_id, tag_id)
        .await?
        .is_empty()
    {
        return Ok(true);
    }
    if !ModeratedReview::reviews_of_tag(tagger_id, tag_id)
        .await?
        .is_empty()
    {
        return Ok(true);
    }
    Ok(!ExcludedReviewer::users_of_tag(tagger_id, tag_id)
        .await?
        .is_empty())
}
