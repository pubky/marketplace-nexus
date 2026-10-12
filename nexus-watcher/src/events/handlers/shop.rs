use crate::events::retry::event::RetryEvent;
use crate::events::EventProcessorError;
use nexus_common::db::OperationOutcome;
use nexus_common::models::marketplace::{ModeratedUser, ShopDetails};
use pubky_app_specs::{PubkyAppShop, PubkyId};
use tracing::{debug, info};

pub async fn sync_put(shop: PubkyAppShop, user_id: PubkyId) -> Result<(), EventProcessorError> {
    sync_put_with_moderation(shop, user_id, true).await
}

/// Indexes a shop. With `enforce_moderation` set (every ingest path), the shop
/// of a user a moderator tag hides is not indexed, and anything of it still
/// indexed is removed (the removal is idempotent): the check runs before the
/// owner-exists precondition, so a moderated shop neither lands nor waits in
/// the retry queue for its owner, and runs again after the writes, which
/// closes the window against a moderator tag handled by another processor
/// while this shop was being written (the tag writes its marker before it
/// removes the shop).
///
/// Only the release of a user moderation passes `false`, to index the shop
/// while the marker it is releasing is still recorded.
pub(crate) async fn sync_put_with_moderation(
    shop: PubkyAppShop,
    user_id: PubkyId,
    enforce_moderation: bool,
) -> Result<(), EventProcessorError> {
    debug!("Indexing new shop: {}", user_id);

    if enforce_moderation && ModeratedUser::is_moderated(&user_id).await? {
        info!("User {user_id} is moderated; not indexing their shop");
        return del(user_id).await;
    }

    // Create ShopDetails object
    let shop_details = ShopDetails::from_homeserver(shop, &user_id);

    // SAVE TO GRAPH: only if the owner user exists
    match shop_details.put_to_graph().await? {
        OperationOutcome::CreatedOrDeleted | OperationOutcome::Updated => (),
        OperationOutcome::MissingDependency => {
            let key = RetryEvent::generate_index_key_from_uri(&user_id.to_uri());
            return Err(EventProcessorError::missing_dependencies(vec![key]));
        }
    }

    // SAVE TO INDEX
    shop_details.put_to_index().await?;

    if enforce_moderation && ModeratedUser::is_moderated(&user_id).await? {
        info!("User {user_id} was moderated while their shop was being indexed; removing it");
        del(user_id).await?;
    }

    Ok(())
}

pub async fn del(user_id: PubkyId) -> Result<(), EventProcessorError> {
    debug!("Deleting shop: {}", user_id);

    super::tag::del_tagged_target(super::tag::TagTarget::Shop { owner_id: &user_id }).await?;
    ShopDetails::delete_indexes(&user_id).await?;

    Ok(())
}
