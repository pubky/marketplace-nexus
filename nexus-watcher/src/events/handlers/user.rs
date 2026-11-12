use super::listing::{self, ModerationChecks};
use super::{review, shop};
use crate::events::directory_page::{entry_name, fetch_directory_page, next_cursor};
use crate::events::remote_body::{fetch_record_capped, within, MAX_RECORD_BYTES};
use crate::events::EventProcessorError;

use nexus_common::db::queries::get::user_is_safe_to_delete;
use nexus_common::db::{execute_graph_operation, OperationOutcome, PubkyConnector};
use nexus_common::models::{
    marketplace::{ListingDetails, ModeratedUser},
    traits::Collection,
    user::{UserCounts, UserDetails, UserSearch, USER_DELETED_SENTINEL},
};
use pubky::PubkyResource;
use pubky_app_specs::{
    listing_uri_builder, shop_uri_builder, user_uri_builder, PubkyAppObject, PubkyAppUser, PubkyId,
    Resource,
};
use tracing::{debug, info};

/// Listing directory entries read per LIST page when a released user's
/// listings are restored.
const LISTING_RESTORE_PAGE: u16 = 500;

pub async fn sync_put(user: PubkyAppUser, user_id: PubkyId) -> Result<(), EventProcessorError> {
    debug!("Indexing new user profile: {}", user_id);

    // Step 1: Create `UserDetails` object
    let user_details = UserDetails::from_homeserver(user, &user_id);

    // Step 2: Save to graph
    user_details.put_to_graph().await?;

    // Step 3: Run in parallel the cache process: SAVE TO INDEX
    let indexing_results = tokio::join!(
        async {
            UserSearch::put_to_index(&[&user_details]).await?;
            Ok::<(), EventProcessorError>(())
        },
        async {
            // TODO: Use SCARD on a set for unique tag count to avoid race conditions in parallel processing
            // If new user (no existing counts), save a new `UserCounts`
            if UserCounts::get_from_index(&user_id).await?.is_none() {
                UserCounts::default().put_to_index(&user_id).await?;
            }
            Ok::<(), EventProcessorError>(())
        },
        async {
            UserDetails::put_to_index(&[&user_details.id], vec![Some(user_details.clone())])
                .await?;
            Ok::<(), EventProcessorError>(())
        }
    );

    indexing_results.0?;
    indexing_results.1?;
    indexing_results.2?;
    Ok(())
}

pub async fn del(user_id: PubkyId) -> Result<(), EventProcessorError> {
    debug!("Deleting user profile:  {}", user_id);

    // 1. Graph query to check if there is any edge at all to this user.
    let query = user_is_safe_to_delete(&user_id);

    // 2. If there is no relationships (OperationOutcome::CreatedOrDeleted), delete from graph and redis.
    // 3. But if there is any relationship (OperationOutcome::Updated), then we simply update the user with empty profile
    // and keyword username [DELETED].
    // A deleted user is a user whose profile is empty and has username `"[DELETED]"`
    match execute_graph_operation(query)
        .await
        .map_err(EventProcessorError::graph_query_failed)?
    {
        OperationOutcome::CreatedOrDeleted => {
            // UserSearch::delete reads UserDetails from the index to find the username,
            // so it must complete before UserDetails::delete runs.
            UserSearch::delete(&user_id).await?;
            let indexing_results =
                tokio::join!(UserDetails::delete(&user_id), UserCounts::delete(&user_id));
            indexing_results.0?;
            indexing_results.1?;
        }
        OperationOutcome::Updated => {
            let deleted_user = PubkyAppUser {
                name: USER_DELETED_SENTINEL.to_string(),
                bio: None,
                status: None,
                links: None,
                image: None,
            };

            sync_put(deleted_user, user_id).await?;
        }
        OperationOutcome::MissingDependency => return Err(EventProcessorError::SkipIndexing),
    }

    // TODO notifications for deleted user

    Ok(())
}

/// Hides a user, their shop and all their listings because the configured
/// moderator tagged the user's profile with a moderated label, and hides the
/// text of every review they wrote (their ratings still count).
///
/// The marker is recorded first: it is what keeps the shop and the listings
/// out when their events arrive later or are replayed again, what hides the
/// text of reviews indexed later, and what the transaction service reads to
/// refuse sales (see [`ModeratedUser`]). Then the user's reviews are marked,
/// every listing and the shop are removed from the graph, the details cache
/// and every stream, and the user profile is removed as a moderated profile
/// always was. Every step is idempotent, so a failure after the marker is
/// written is finished by the retried tag event.
pub async fn moderate(
    user_id: PubkyId,
    uri: &str,
    moderator_id: &PubkyId,
    tag_id: &str,
    label: &str,
) -> Result<(), EventProcessorError> {
    info!("Moderation tag '{label}' on user {user_id}; hiding their shop and listings");
    ModeratedUser::put(
        &user_id,
        uri,
        moderator_id,
        tag_id,
        label,
        chrono::Utc::now().timestamp_millis(),
    )
    .await?;
    review::refresh_reviewer_moderation(&user_id).await?;
    remove_marketplace_content(&user_id).await?;
    del(user_id).await
}

/// Removes every listing the user has in the graph and their shop.
async fn remove_marketplace_content(user_id: &PubkyId) -> Result<(), EventProcessorError> {
    for listing_id in ListingDetails::ids_of_owner(user_id).await? {
        listing::del(user_id.clone(), listing_id).await?;
    }
    shop::del(user_id.clone()).await
}

/// Handles the moderator deleting one of its moderation tags. Returns whether
/// the tag was hiding a user.
///
/// A user no other marker hides gets their profile, shop and listings indexed
/// again from their homeserver (a record the user has deleted stays gone, and
/// a listing hidden by its own moderator tag stays hidden), and the text of
/// their reviews shown again unless another marker hides it. The marker is
/// first flagged as being released, which review moderation skips everywhere
/// (see [`review::release_moderation`]), and removed only after that, so a
/// failed fetch leaves the marker in place and the retried tag deletion starts
/// over.
pub async fn release_moderation(
    moderator_id: &PubkyId,
    tag_id: &str,
) -> Result<bool, EventProcessorError> {
    let users = ModeratedUser::users_of_tag(moderator_id, tag_id).await?;
    if users.is_empty() {
        return Ok(false);
    }
    ModeratedUser::mark_releasing(moderator_id, tag_id).await?;

    for user in users {
        let user_id = PubkyId::try_from(user.as_str()).map_err(EventProcessorError::generic)?;
        let hidden_by_another_tag =
            ModeratedUser::is_moderated_besides(&user, moderator_id, tag_id).await?;
        if !hidden_by_another_tag {
            info!("Moderation tag removed; restoring user {user}");
            restore_from_homeserver(&user_id).await?;
        }
        review::refresh_reviewer_moderation(&user).await?;
        ModeratedUser::delete(moderator_id, tag_id).await?;
        settle_after_release(&user_id, hidden_by_another_tag).await?;
    }
    Ok(true)
}

/// After a marker is gone. A moderator tag that appeared while the user was
/// being restored removes their shop and listings again. When
/// `restore_if_absent` is set, the release skipped the restore because
/// another marker was still there; if that marker was released concurrently
/// and skipped its own restore for the same reason, the user is restored
/// here.
async fn settle_after_release(
    user_id: &PubkyId,
    restore_if_absent: bool,
) -> Result<(), EventProcessorError> {
    if ModeratedUser::is_moderated(user_id).await? {
        return remove_marketplace_content(user_id).await;
    }
    if restore_if_absent {
        let hidden = UserDetails::get_by_id(user_id)
            .await?
            .is_none_or(|details| details.name == USER_DELETED_SENTINEL);
        if hidden {
            restore_from_homeserver(user_id).await?;
        }
    }
    Ok(())
}

/// Indexes the user's profile, shop and listings again from their homeserver
/// while the marker being released is still recorded: the profile first, since
/// the shop and the listings need their owner indexed. A user whose profile is
/// gone from the homeserver has nothing to index.
async fn restore_from_homeserver(user_id: &PubkyId) -> Result<(), EventProcessorError> {
    within(
        RESTORE_TIMEOUT,
        &format!("restoring {user_id} from their homeserver"),
        restore_steps(user_id),
    )
    .await
}

async fn restore_steps(user_id: &PubkyId) -> Result<(), EventProcessorError> {
    let Some(blob) = fetch_record(&user_uri_builder(user_id.to_string())).await? else {
        info!("User {user_id} has no profile on their homeserver; nothing to restore");
        return Ok(());
    };
    match PubkyAppObject::from_resource(&Resource::User, &blob)
        .map_err(EventProcessorError::generic)?
    {
        PubkyAppObject::User(profile) => sync_put(profile, user_id.clone()).await?,
        _ => {
            return Err(EventProcessorError::generic(format!(
                "Expected a profile record for {user_id}"
            )))
        }
    }

    if let Some(blob) = fetch_record(&shop_uri_builder(user_id.to_string())).await? {
        match PubkyAppObject::from_resource(&Resource::Shop, &blob)
            .map_err(EventProcessorError::generic)?
        {
            PubkyAppObject::Shop(shop_record) => {
                shop::sync_put_with_moderation(shop_record, user_id.clone(), false).await?
            }
            _ => {
                return Err(EventProcessorError::generic(format!(
                    "Expected a shop record for {user_id}"
                )))
            }
        }
    }

    for listing_id in homeserver_listing_ids(user_id).await? {
        listing::reindex_from_homeserver_with_moderation(
            user_id,
            &listing_id,
            ModerationChecks::RELEASING_OWNER,
        )
        .await?;
    }
    Ok(())
}

/// The record at `uri`, or `None` when the homeserver answers 404. Every
/// other failure is an error: an unreachable homeserver never reads as a
/// deleted record.
async fn fetch_record(uri: &str) -> Result<Option<Vec<u8>>, EventProcessorError> {
    fetch_record_capped(uri, MAX_RECORD_BYTES).await
}

/// Most listings restored for one seller. A directory listing is cursor-paged
/// by a homeserver the seller chooses, so its length is bounded here.
const MAX_RESTORED_LISTINGS: usize = 10_000;

/// Budget for paging a whole listings directory.
const LISTING_DIRECTORY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(300);

/// Accumulates the entries of a listings directory, refusing a homeserver
/// that serves more ids than [MAX_RESTORED_LISTINGS].
#[derive(Default)]
struct ListingIdPages {
    ids: Vec<String>,
    seen: std::collections::HashSet<String>,
}

impl ListingIdPages {
    /// Adds the listing ids of one page of entries.
    fn add_page(&mut self, entries: &[PubkyResource]) -> Result<(), EventProcessorError> {
        for listing_id in entries.iter().filter_map(entry_name) {
            if self.seen.insert(listing_id.to_string()) {
                if self.ids.len() == MAX_RESTORED_LISTINGS {
                    return Err(EventProcessorError::InvalidEventLine(format!(
                        "listings directory has more than {MAX_RESTORED_LISTINGS} entries"
                    )));
                }
                self.ids.push(listing_id.to_string());
            }
        }
        Ok(())
    }
}

/// Budget for restoring one user: profile, shop, the listings directory and
/// every listing record. Each step has its own tighter deadline.
const RESTORE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(1800);

/// The ids of every listing record in the user's listings directory on their
/// homeserver (cursor-paginated, bounded by [MAX_RESTORED_LISTINGS],
/// [LISTING_DIRECTORY_TIMEOUT] and a byte budget per page). A user with no
/// directory has none.
async fn homeserver_listing_ids(user_id: &PubkyId) -> Result<Vec<String>, EventProcessorError> {
    within(
        LISTING_DIRECTORY_TIMEOUT,
        &format!("listing the listings of {user_id}"),
        async {
            let directory: PubkyResource =
                listing_uri_builder(user_id.to_string(), String::new()).parse()?;
            let pubky = PubkyConnector::get()?;
            let mut pages = ListingIdPages::default();
            let mut cursor: Option<String> = None;
            loop {
                let Some(entries) = fetch_directory_page(
                    pubky.client(),
                    &directory,
                    LISTING_RESTORE_PAGE,
                    cursor.as_deref(),
                )
                .await?
                else {
                    return Ok(pages.ids);
                };
                pages.add_page(&entries)?;
                cursor = next_cursor(&entries, LISTING_RESTORE_PAGE);
                if cursor.is_none() {
                    return Ok(pages.ids);
                }
            }
        },
    )
    .await
}

#[cfg(test)]
mod listing_id_pages_tests {
    use super::*;

    fn page(prefix: &str, count: usize) -> Vec<PubkyResource> {
        let owner = pubky::Keypair::random().public_key();
        (0..count)
            .map(|i| {
                format!(
                    "pubky://{}/pub/pubky.app/listings/{prefix}{i:05}",
                    owner.z32()
                )
                .parse()
                .unwrap()
            })
            .collect()
    }

    #[test]
    fn listing_ids_are_the_entry_names() {
        let mut pages = ListingIdPages::default();
        pages.add_page(&page("a", 3)).unwrap();
        pages.add_page(&page("b", 2)).unwrap();
        assert_eq!(
            pages.ids,
            ["a00000", "a00001", "a00002", "b00000", "b00001"]
        );
    }

    #[test]
    fn repeated_ids_are_kept_once() {
        let mut pages = ListingIdPages::default();
        let mut entries = page("a", 2);
        entries.extend(page("a", 2));
        pages.add_page(&entries).unwrap();
        assert_eq!(pages.ids, ["a00000", "a00001"]);
    }

    #[test]
    fn more_ids_than_the_budget_are_refused() {
        let mut pages = ListingIdPages::default();
        let mut result = Ok(());
        for chunk in 0..=(MAX_RESTORED_LISTINGS / LISTING_RESTORE_PAGE as usize) {
            result = pages.add_page(&page(&format!("c{chunk}-"), LISTING_RESTORE_PAGE as usize));
            if result.is_err() {
                break;
            }
        }
        let error = result.expect_err("an endless directory must be cut off");
        assert!(error.to_string().contains("more than"), "{error}");
        assert_eq!(pages.ids.len(), MAX_RESTORED_LISTINGS);
    }
}
