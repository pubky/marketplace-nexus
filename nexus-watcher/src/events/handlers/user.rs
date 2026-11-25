use super::listing::{self, ModerationChecks};
use super::{review, shop};
use crate::events::directory_page::{DirectoryPager, PageStep};
use crate::events::remote_body::{fetch_record_capped, within, MAX_RECORD_BYTES};
use crate::events::{flip, social_v1, EventProcessorError};

use nexus_common::db::queries::get::user_is_safe_to_delete;
use nexus_common::db::{execute_graph_operation, OperationOutcome, PubkyConnector};
use nexus_common::models::{
    marketplace::{ListingDetails, ModeratedUser},
    traits::Collection,
    user::{Epoch, UserCounts, UserDetails, UserSearch, USER_DELETED_SENTINEL},
};
use pubky::PubkyResource;
use pubky_app_specs::{
    listing_uri_builder, shop_uri_builder, user_uri_builder, PubkyAppObject, PubkyAppUser, PubkyId,
    Resource,
};
use tracing::{debug, info, warn};

/// Listing directory entries read per LIST page when a released user's
/// listings are restored.
const LISTING_RESTORE_PAGE: u16 = 500;

pub async fn sync_put(user: PubkyAppUser, user_id: PubkyId) -> Result<(), EventProcessorError> {
    sync_put_details(UserDetails::from_homeserver(user, &user_id)).await
}

/// Indexes a profile of either epoch. The node keeps its epoch.
pub async fn sync_put_details(user_details: UserDetails) -> Result<(), EventProcessorError> {
    let user_id = user_details.id.clone();
    debug!("Indexing new user profile: {}", user_id);

    // Save to graph
    user_details.put_to_graph().await?;

    // Run in parallel the cache process: SAVE TO INDEX
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
    let outcome = execute_graph_operation(query)
        .await
        .map_err(EventProcessorError::graph_query_failed)?;
    // A user at `v1` always keeps their node, as the tombstone: the epoch on it is what keeps
    // their stale `pubky.app` events ignored, and a removed node reads as `v0`.
    let outcome = match outcome {
        OperationOutcome::CreatedOrDeleted
            if Epoch::get_from_graph_if_present(&user_id).await? == Some(Epoch::V1) =>
        {
            OperationOutcome::Updated
        }
        outcome => outcome,
    };
    match outcome {
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
    if !restore_profile(user_id).await? {
        info!("User {user_id} has no profile on their homeserver; nothing to restore");
        return Ok(());
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

/// Indexes the user's profile again from the epoch they are read through, and returns whether
/// they had one. A `social/v1` profile is read first: it is the last file the migrator writes,
/// so a user still at `v0` who has one migrated while hidden (their node, and the epoch on it,
/// may have been removed with their profile) and flips here as their profile PUT would have
/// flipped them. A `social/v1` profile that is gone or fails the reader is not there to restore
/// from: a user at `v0` or out of the graph is restored from `pubky.app`, and a user at `v1`,
/// who is never restored from `pubky.app`, keeps their tombstone until their next valid profile
/// PUT, while the release of the marker still completes.
async fn restore_profile(user_id: &PubkyId) -> Result<bool, EventProcessorError> {
    let epoch = Epoch::get_from_graph_if_present(user_id).await?;
    if let Some(details) = social_profile_to_restore(user_id).await? {
        match epoch {
            Some(Epoch::V1) => sync_put_details(details).await?,
            Some(Epoch::V0) => {
                flip::move_to_v1(user_id).await?;
                sync_put_details(details).await?;
            }
            None => {
                sync_put_details(details).await?;
                Epoch::set_v1_in_graph(user_id).await?;
            }
        }
        return Ok(true);
    }
    if epoch == Some(Epoch::V1) {
        return Ok(false);
    }
    let Some(blob) = fetch_record(&user_uri_builder(user_id.to_string())).await? else {
        return Ok(false);
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
    Ok(true)
}

/// The user's `social/v1` profile, `None` when it is gone from their homeserver or fails the
/// reader. A homeserver that cannot be read is an error, never a missing profile.
async fn social_profile_to_restore(
    user_id: &PubkyId,
) -> Result<Option<UserDetails>, EventProcessorError> {
    let uri = pubky_social_specs::user_uri_builder(user_id.to_string());
    let Some(blob) = fetch_record(&uri).await? else {
        return Ok(None);
    };
    match social_v1::read_profile(&blob) {
        Ok(profile) => Ok(Some(UserDetails::from_social_v1(profile, user_id))),
        Err(EventProcessorError::SpecValidation(reason)) => {
            warn!(%user_id, %reason, "Social v1 profile fails the reader; not restored from it");
            Ok(None)
        }
        Err(error) => Err(error),
    }
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

/// Budget for restoring one user: profile, shop, the listings directory and
/// every listing record. Each step has its own tighter deadline.
const RESTORE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(1800);

/// The ids of every listing record in the user's listings directory on their
/// homeserver (cursor-paginated, bounded by [MAX_RESTORED_LISTINGS] distinct
/// ids, [LISTING_DIRECTORY_TIMEOUT] and a byte budget per page). A user with no
/// directory has none, and a 404 on a later page ends the listing like a
/// deleted directory; a directory with more than [MAX_RESTORED_LISTINGS]
/// entries, or one that does not end within the pager's page bound, is
/// refused.
async fn homeserver_listing_ids(user_id: &PubkyId) -> Result<Vec<String>, EventProcessorError> {
    within(
        LISTING_DIRECTORY_TIMEOUT,
        &format!("listing the listings of {user_id}"),
        async {
            let directory: PubkyResource =
                listing_uri_builder(user_id.to_string(), String::new()).parse()?;
            let pubky = PubkyConnector::get()?;
            let mut pager =
                DirectoryPager::new(directory, LISTING_RESTORE_PAGE, MAX_RESTORED_LISTINGS);
            let mut ids = Vec::new();
            loop {
                let step = pager.next(pubky.client()).await?;
                if add_listing_page(&mut ids, step)? {
                    return Ok(ids);
                }
            }
        },
    )
    .await
}

/// Adds the ids one listings directory [PageStep] found to `ids`. Returns
/// whether the listing is complete: after its last page, a 404 (even after
/// earlier pages, read as a deleted directory) or its end at the cap. A
/// directory past the cap or the page bound is an error.
fn add_listing_page(ids: &mut Vec<String>, step: PageStep) -> Result<bool, EventProcessorError> {
    match step {
        PageStep::Names { names, last } => {
            ids.extend(names);
            Ok(last)
        }
        PageStep::Gone | PageStep::End => Ok(true),
        PageStep::Truncated => Err(EventProcessorError::InvalidEventLine(format!(
            "listings directory has more than {MAX_RESTORED_LISTINGS} entries"
        ))),
        PageStep::PageBoundReached => Err(EventProcessorError::InvalidEventLine(
            "listings directory did not end within its page bound (subdirectories or repeated entries)"
                .to_string(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn names(names: &[&str], last: bool) -> PageStep {
        PageStep::Names {
            names: names.iter().map(|name| name.to_string()).collect(),
            last,
        }
    }

    #[test]
    fn a_full_page_adds_its_ids_and_the_last_page_completes_the_listing() {
        let mut ids = Vec::new();
        assert!(!add_listing_page(&mut ids, names(&["a", "b"], false)).unwrap());
        assert!(add_listing_page(&mut ids, names(&["c"], true)).unwrap());
        assert_eq!(ids, ["a", "b", "c"]);
    }

    #[test]
    fn a_404_or_the_end_at_the_cap_completes_the_listing_with_the_ids_so_far() {
        for step in [PageStep::Gone, PageStep::End] {
            let mut ids = vec!["a".to_string()];
            assert!(add_listing_page(&mut ids, step).unwrap());
            assert_eq!(ids, ["a"]);
        }
    }

    #[test]
    fn a_directory_past_the_cap_or_the_page_bound_is_refused() {
        for (step, message) in [
            (PageStep::Truncated, "more than 10000 entries"),
            (
                PageStep::PageBoundReached,
                "did not end within its page bound",
            ),
        ] {
            let mut ids = vec!["a".to_string()];
            let error = add_listing_page(&mut ids, step)
                .expect_err("a truncated listings directory must not restore part of it");
            assert!(
                matches!(&error, EventProcessorError::InvalidEventLine(text) if text.contains(message)),
                "{error:?}"
            );
        }
    }
}
