use super::listing::{self, ModerationChecks};
use super::{review, shop};
use crate::events::remote_body::{
    event_feed_budget, fetch_record_capped, percent_encode_query_value, read_capped, within,
    MAX_RECORD_BYTES,
};
use crate::events::EventProcessorError;

use nexus_common::db::queries::get::user_is_safe_to_delete;
use nexus_common::db::{execute_graph_operation, OperationOutcome, PubkyConnector};
use nexus_common::models::{
    marketplace::{ListingDetails, ModeratedUser},
    traits::Collection,
    user::{UserCounts, UserDetails, UserSearch, USER_DELETED_SENTINEL},
};
use pubky::Method;
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

/// Accumulates the pages of a listings directory, refusing a homeserver that
/// repeats its cursor or serves more ids than [MAX_RESTORED_LISTINGS].
#[derive(Default)]
struct ListingIdPages {
    ids: Vec<String>,
    seen: std::collections::HashSet<String>,
    cursor: Option<String>,
}

impl ListingIdPages {
    /// Adds one page of entry URLs. Returns the cursor to resume from, or
    /// `None` when the page was the last.
    fn add_page(&mut self, urls: &[String]) -> Result<Option<String>, EventProcessorError> {
        for url in urls {
            let Some(listing_id) = url.rsplit('/').next().filter(|id| !id.is_empty()) else {
                continue;
            };
            if self.seen.insert(listing_id.to_string()) {
                if self.ids.len() == MAX_RESTORED_LISTINGS {
                    return Err(EventProcessorError::InvalidEventLine(format!(
                        "listings directory has more than {MAX_RESTORED_LISTINGS} entries"
                    )));
                }
                self.ids.push(listing_id.to_string());
            }
        }
        if urls.len() < LISTING_RESTORE_PAGE as usize {
            return Ok(None);
        }
        let next = urls.last().cloned();
        if next == self.cursor {
            return Err(EventProcessorError::InvalidEventLine(
                "listings directory page did not advance its cursor".to_string(),
            ));
        }
        self.cursor = next.clone();
        Ok(next)
    }
}

/// Budget for restoring one user: profile, shop, the listings directory and
/// every listing record. Each step has its own tighter deadline.
const RESTORE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(1800);

/// One page of a shallow directory listing as `pubky://<owner>/<path>` URLs.
///
/// A homeserver answers with one resource per line, as `pubky://` URLs or
/// `https://_pubky.<owner>/<path>` transport URLs. More lines than the page
/// size means the homeserver ignored the `limit`, so the page is refused.
fn parse_directory_page(text: &str) -> Result<Vec<String>, EventProcessorError> {
    let mut urls = Vec::new();
    for line in text.lines().map(str::trim).filter(|line| !line.is_empty()) {
        if urls.len() == LISTING_RESTORE_PAGE as usize {
            return Err(EventProcessorError::InvalidEventLine(format!(
                "directory page has more than {LISTING_RESTORE_PAGE} entries"
            )));
        }
        let url = if line.starts_with("pubky://") {
            line.to_string()
        } else if let Some(rest) = line
            .strip_prefix("https://")
            .or_else(|| line.strip_prefix("http://"))
        {
            let (host, path) = rest.split_once('/').unwrap_or((rest, ""));
            let owner = host.strip_prefix("_pubky.").ok_or_else(|| {
                EventProcessorError::InvalidEventLine(format!(
                    "directory entry is not a Pubky resource: {line}"
                ))
            })?;
            format!("pubky://{owner}/{path}")
        } else {
            return Err(EventProcessorError::InvalidEventLine(format!(
                "directory entry is not a URL: {line}"
            )));
        };
        urls.push(url);
    }
    Ok(urls)
}

/// One page of the user's listings directory, or `None` when the homeserver
/// answers 404. The body is read against a byte budget.
async fn list_directory_page(
    directory: &str,
    cursor: Option<&str>,
) -> Result<Option<Vec<String>>, EventProcessorError> {
    let resource = directory.strip_prefix("pubky://").unwrap_or(directory);
    let mut url = format!("https://_pubky.{resource}?shallow&limit={LISTING_RESTORE_PAGE}");
    if let Some(cursor) = cursor {
        url.push_str("&cursor=");
        url.push_str(&percent_encode_query_value(cursor));
    }
    let response = PubkyConnector::get()?
        .client()
        .request(Method::GET, &url)
        .send()
        .await
        .map_err(|e| EventProcessorError::client_error(e.to_string()))?;
    let status = response.status();
    if status == pubky::StatusCode::NOT_FOUND {
        return Ok(None);
    }
    if !status.is_success() {
        return Err(EventProcessorError::client_error(format!(
            "listing {directory} answered {status}"
        )));
    }
    let body = read_capped(
        response,
        event_feed_budget(LISTING_RESTORE_PAGE as u32),
        "Listings directory page",
    )
    .await?;
    parse_directory_page(&String::from_utf8_lossy(&body)).map(Some)
}

/// The ids of every listing record in the user's listings directory on their
/// homeserver (cursor-paginated, bounded by [MAX_RESTORED_LISTINGS],
/// [LISTING_DIRECTORY_TIMEOUT] and a byte budget per page). A user with no
/// directory has none.
async fn homeserver_listing_ids(user_id: &PubkyId) -> Result<Vec<String>, EventProcessorError> {
    within(
        LISTING_DIRECTORY_TIMEOUT,
        &format!("listing the listings of {user_id}"),
        async {
            let directory = listing_uri_builder(user_id.to_string(), String::new());
            let mut pages = ListingIdPages::default();
            let mut cursor: Option<String> = None;
            loop {
                let Some(urls) = list_directory_page(&directory, cursor.as_deref()).await? else {
                    return Ok(pages.ids);
                };
                match pages.add_page(&urls)? {
                    Some(next) => cursor = Some(next),
                    None => return Ok(pages.ids),
                }
            }
        },
    )
    .await
}

#[cfg(test)]
mod listing_id_pages_tests {
    use super::*;

    const OWNER: &str = "8pinxxgqs41n4aididenw5apqp1urfmzdztr8jt4abrkdn435ewo";

    #[test]
    fn directory_pages_accept_pubky_and_transport_urls() {
        let text = format!(
            "pubky://{OWNER}/pub/x/a\nhttps://_pubky.{OWNER}/pub/x/b\n\nhttp://_pubky.{OWNER}/pub/x/dir/\n"
        );
        assert_eq!(
            parse_directory_page(&text).unwrap(),
            [
                format!("pubky://{OWNER}/pub/x/a"),
                format!("pubky://{OWNER}/pub/x/b"),
                format!("pubky://{OWNER}/pub/x/dir/"),
            ]
        );
    }

    #[test]
    fn directory_pages_refuse_other_lines() {
        for line in ["garbage", "https://example.com/pub/x", "ftp://a/b"] {
            assert!(
                matches!(
                    parse_directory_page(line),
                    Err(EventProcessorError::InvalidEventLine(_))
                ),
                "{line}"
            );
        }
    }

    #[test]
    fn a_page_longer_than_the_limit_is_refused() {
        let line = format!("pubky://{OWNER}/pub/x/a\n");
        let page = line.repeat(LISTING_RESTORE_PAGE as usize);
        assert_eq!(
            parse_directory_page(&page).unwrap().len(),
            LISTING_RESTORE_PAGE as usize
        );
        let over = line.repeat(LISTING_RESTORE_PAGE as usize + 1);
        assert!(parse_directory_page(&over).is_err());
    }

    fn page(prefix: &str, count: usize) -> Vec<String> {
        (0..count)
            .map(|i| format!("pubky://owner/pub/pubky.app/listings/{prefix}{i:05}"))
            .collect()
    }

    #[test]
    fn a_short_page_ends_the_listing() {
        let mut pages = ListingIdPages::default();
        assert_eq!(pages.add_page(&page("a", 3)).unwrap(), None);
        assert_eq!(pages.ids.len(), 3);
        assert_eq!(pages.ids[0], "a00000");
    }

    #[test]
    fn a_full_page_returns_its_last_entry_as_the_cursor() {
        let mut pages = ListingIdPages::default();
        let full = page("a", LISTING_RESTORE_PAGE as usize);
        let next = pages.add_page(&full).unwrap();
        assert_eq!(next.as_deref(), full.last().map(String::as_str));
        assert_eq!(pages.add_page(&page("b", 2)).unwrap(), None);
        assert_eq!(pages.ids.len(), LISTING_RESTORE_PAGE as usize + 2);
    }

    #[test]
    fn a_homeserver_that_repeats_its_cursor_is_refused() {
        let mut pages = ListingIdPages::default();
        let full = page("a", LISTING_RESTORE_PAGE as usize);
        pages.add_page(&full).unwrap();
        let error = pages
            .add_page(&full)
            .expect_err("a repeated final entry must not loop forever");
        assert!(error.to_string().contains("did not advance"), "{error}");
    }

    #[test]
    fn repeated_ids_are_kept_once() {
        let mut pages = ListingIdPages::default();
        let mut urls = page("a", 2);
        urls.extend(page("a", 2));
        pages.add_page(&urls).unwrap();
        assert_eq!(pages.ids, ["a00000", "a00001"]);
    }

    #[test]
    fn more_ids_than_the_budget_are_refused() {
        let mut pages = ListingIdPages::default();
        let mut result = Ok(None);
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
