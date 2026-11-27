use crate::db::kv::RedisResult;
use crate::db::{
    exec_single_row, fetch_all_rows_from_graph, fetch_row_from_graph, get_redis_conn, queries,
    GraphError, GraphResult,
};
use crate::models::error::ModelResult;
use deadpool_redis::redis::{AsyncCommands, Script};
use pubky::Keypair;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;
use std::time::Duration;
use tokio::time::{timeout_at, Instant};
use tracing::warn;
use utoipa::ToSchema;

/// Short TTL (seconds) for cached listing and seller moderation flags in Redis.
/// Bounds Redis memory and caps staleness if markers are mutated outside the
/// standard watcher write path.
pub const MODERATION_CACHE_TTL_SECS: i64 = 15;

/// TTL (seconds) for per-target miss-fill lease tokens. Strictly exceeds
/// [`MAX_CACHE_FILL_ELAPSED`] and is deleted atomically on every moderation
/// write or delete so a stale miss read cannot write back after invalidation,
/// expiry, eviction, or Redis restart.
const MODERATION_LEASE_TTL_SECS: i64 = 30;

/// Maximum wall-clock budget (from cache lookup start through Redis fill
/// completion) for a miss read to populate Redis.
const MAX_CACHE_FILL_ELAPSED: Duration = Duration::from_secs(10);

/// Maximum byte length of an `owner_id` or `listing_id` component eligible for
/// Redis caching. Valid Pubky z32 IDs (52 bytes) and Crockford32 IDs are well
/// under this bound; oversized path segments fall through directly to Neo4j.
const MAX_CACHED_ID_BYTES: usize = 128;

static PROCESS_LEASE_PREFIX: OnceLock<String> = OnceLock::new();
static NEXT_LEASE_SEQ: AtomicU64 = AtomicU64::new(1);

fn next_lease_token() -> String {
    let prefix = PROCESS_LEASE_PREFIX.get_or_init(|| Keypair::random().public_key().to_z32());
    let seq = NEXT_LEASE_SEQ.fetch_add(1, Ordering::Relaxed);
    format!("{prefix}:{seq}")
}

fn is_cacheable_id(id: &str) -> bool {
    !id.is_empty() && id.len() <= MAX_CACHED_ID_BYTES
}

/// Redis key caching whether a listing has any `ModeratedListing` marker.
/// Length-prefixed on `owner_id` so arbitrary strings cannot collide on `:`.
pub fn listing_moderation_cache_key(owner_id: &str, listing_id: &str) -> String {
    format!(
        "Marketplace:Moderation:Listing:{}:{owner_id}:{listing_id}",
        owner_id.len()
    )
}

/// Redis key holding the active miss-fill lease token for one listing.
pub fn listing_moderation_lease_key(owner_id: &str, listing_id: &str) -> String {
    format!(
        "Marketplace:Moderation:ListingLease:{}:{owner_id}:{listing_id}",
        owner_id.len()
    )
}

/// Redis key caching whether a seller has any `ModeratedUser` marker.
pub fn user_moderation_cache_key(user_id: &str) -> String {
    format!("Marketplace:Moderation:User:{user_id}")
}

fn user_moderation_lease_key(user_id: &str) -> String {
    format!("Marketplace:Moderation:UserLease:{user_id}")
}

fn encode_cached_bool(value: bool) -> &'static str {
    if value {
        "1"
    } else {
        "0"
    }
}

fn parse_cached_bool(raw: Option<&str>) -> Option<bool> {
    match raw {
        Some("1") => Some(true),
        Some("0") => Some(false),
        _ => None,
    }
}

/// Atomically deletes both the cached flag and its miss-fill lease token so
/// any concurrent cache-miss graph read that began before this write cannot
/// populate Redis with a stale value.
async fn invalidate_cache_entry(cache_key: &str, lease_key: &str) -> RedisResult<()> {
    let mut conn = get_redis_conn().await?;
    let script = Script::new(
        r#"
        redis.call('DEL', KEYS[1], KEYS[2])
        return 1
        "#,
    );
    let _: i64 = script
        .key(cache_key)
        .key(lease_key)
        .invoke_async(&mut conn)
        .await?;
    Ok(())
}

async fn invalidate_listing_cache(owner_id: &str, listing_id: &str) -> RedisResult<()> {
    if !is_cacheable_id(owner_id) || !is_cacheable_id(listing_id) {
        return Ok(());
    }
    invalidate_cache_entry(
        &listing_moderation_cache_key(owner_id, listing_id),
        &listing_moderation_lease_key(owner_id, listing_id),
    )
    .await
}

async fn invalidate_user_cache(user_id: &str) -> RedisResult<()> {
    if !is_cacheable_id(user_id) {
        return Ok(());
    }
    invalidate_cache_entry(
        &user_moderation_cache_key(user_id),
        &user_moderation_lease_key(user_id),
    )
    .await
}

struct CachedFlagWrite<'a> {
    cache_key: &'a str,
    lease_key: &'a str,
    value: bool,
    expected_lease: &'a str,
}

async fn read_cached_moderation_pair(
    listing_key: &str,
    user_key: &str,
) -> RedisResult<(Option<bool>, Option<bool>)> {
    let mut conn = get_redis_conn().await?;
    let values: Vec<Option<String>> = conn.mget(&[listing_key, user_key]).await?;
    if values.len() != 2 {
        return Ok((None, None));
    }
    Ok((
        parse_cached_bool(values[0].as_deref()),
        parse_cached_bool(values[1].as_deref()),
    ))
}

/// Seeds a fresh non-empty lease token if none is live and returns the active
/// token. Must be called before querying Neo4j on a cache miss.
async fn acquire_lease_token(lease_key: &str) -> RedisResult<String> {
    let mut conn = get_redis_conn().await?;
    let candidate = next_lease_token();
    let script = Script::new(
        r#"
        redis.call('SET', KEYS[1], ARGV[1], 'NX', 'EX', tonumber(ARGV[2]))
        return redis.call('GET', KEYS[1]) or ''
        "#,
    );
    let token: String = script
        .key(lease_key)
        .arg(&candidate)
        .arg(MODERATION_LEASE_TTL_SECS)
        .invoke_async(&mut conn)
        .await?;
    Ok(token)
}

/// Seeds and returns active lease tokens for both listing and seller in one
/// Redis roundtrip before querying Neo4j on a cold miss.
async fn acquire_both_lease_tokens(
    listing_lease_key: &str,
    user_lease_key: &str,
) -> RedisResult<(String, String)> {
    let mut conn = get_redis_conn().await?;
    let listing_candidate = next_lease_token();
    let user_candidate = next_lease_token();
    let script = Script::new(
        r#"
        local ttl = tonumber(ARGV[3])
        redis.call('SET', KEYS[1], ARGV[1], 'NX', 'EX', ttl)
        redis.call('SET', KEYS[2], ARGV[2], 'NX', 'EX', ttl)
        return { redis.call('GET', KEYS[1]) or '', redis.call('GET', KEYS[2]) or '' }
        "#,
    );
    let tokens: Vec<String> = script
        .key(listing_lease_key)
        .key(user_lease_key)
        .arg(&listing_candidate)
        .arg(&user_candidate)
        .arg(MODERATION_LEASE_TTL_SECS)
        .invoke_async(&mut conn)
        .await?;
    if tokens.len() != 2 {
        return Ok((String::new(), String::new()));
    }
    Ok((tokens[0].clone(), tokens[1].clone()))
}

/// Populates one cached flag only when `expected_lease` is non-empty and still
/// matches the live lease token in Redis. Returns whether the entry was written.
async fn put_cached_flag_if_lease_matches(
    cache_key: &str,
    lease_key: &str,
    value: bool,
    expected_lease: &str,
) -> RedisResult<bool> {
    if expected_lease.is_empty() {
        return Ok(false);
    }
    let mut conn = get_redis_conn().await?;
    let script = Script::new(
        r#"
        if ARGV[2] == '' then
            return 0
        end
        if redis.call('GET', KEYS[2]) == ARGV[2] then
            redis.call('SET', KEYS[1], ARGV[1], 'EX', tonumber(ARGV[3]))
            return 1
        end
        return 0
        "#,
    );
    let written: i64 = script
        .key(cache_key)
        .key(lease_key)
        .arg(encode_cached_bool(value))
        .arg(expected_lease)
        .arg(MODERATION_CACHE_TTL_SECS)
        .invoke_async(&mut conn)
        .await?;
    Ok(written == 1)
}

/// Populates both listing and seller cached flags in one Redis roundtrip,
/// requiring each target's live lease token to match its non-empty expected
/// token independently.
async fn put_both_cached_flags_if_lease_matches(
    listing: CachedFlagWrite<'_>,
    user: CachedFlagWrite<'_>,
) -> RedisResult<()> {
    if listing.expected_lease.is_empty() && user.expected_lease.is_empty() {
        return Ok(());
    }
    let mut conn = get_redis_conn().await?;
    let script = Script::new(
        r#"
        local ttl = tonumber(ARGV[5])
        if ARGV[2] ~= '' and redis.call('GET', KEYS[2]) == ARGV[2] then
            redis.call('SET', KEYS[1], ARGV[1], 'EX', ttl)
        end
        if ARGV[4] ~= '' and redis.call('GET', KEYS[4]) == ARGV[4] then
            redis.call('SET', KEYS[3], ARGV[3], 'EX', ttl)
        end
        return 1
        "#,
    );
    let _: i64 = script
        .key(listing.cache_key)
        .key(listing.lease_key)
        .key(user.cache_key)
        .key(user.lease_key)
        .arg(encode_cached_bool(listing.value))
        .arg(listing.expected_lease)
        .arg(encode_cached_bool(user.value))
        .arg(user.expected_lease)
        .arg(MODERATION_CACHE_TTL_SECS)
        .invoke_async(&mut conn)
        .await?;
    Ok(())
}

#[cfg(feature = "test-utils")]
pub async fn acquire_listing_lease_for_test(
    owner_id: &str,
    listing_id: &str,
) -> RedisResult<String> {
    acquire_lease_token(&listing_moderation_lease_key(owner_id, listing_id)).await
}

#[cfg(feature = "test-utils")]
pub async fn populate_listing_cache_if_lease_matches(
    owner_id: &str,
    listing_id: &str,
    value: bool,
    expected_lease: &str,
) -> RedisResult<bool> {
    put_cached_flag_if_lease_matches(
        &listing_moderation_cache_key(owner_id, listing_id),
        &listing_moderation_lease_key(owner_id, listing_id),
        value,
        expected_lease,
    )
    .await
}

/// Moderation marker hiding a marketplace listing.
///
/// A marker is written when the configured moderator tags a listing URI with a
/// moderated label, and is keyed by the moderator's tag record, so the
/// moderator removing that tag removes exactly that marker. It lives in the
/// graph as a standalone node that references the listing only by
/// `(owner_id, listing_id)`: it does not depend on the listing node or on the
/// seller `User` node, so it holds whether the tag event is replayed before or
/// after the listing event, and it survives deletion and re-creation of the
/// listing. A listing with at least one marker is never indexed.
pub struct ModeratedListing;

impl ModeratedListing {
    /// Records (or refreshes) the marker written by one moderator tag and
    /// invalidates the listing moderation cache immediately.
    pub async fn put(
        owner_id: &str,
        listing_id: &str,
        uri: &str,
        moderator_id: &str,
        tag_id: &str,
        label: &str,
        moderated_at: i64,
    ) -> ModelResult<()> {
        let row = fetch_row_from_graph(queries::put::put_moderated_listing(
            owner_id,
            listing_id,
            uri,
            moderator_id,
            tag_id,
            label,
            moderated_at,
        ))
        .await?;
        let prev_target = if let Some(row) = row {
            let prev_owner: Option<String> = row.get("prev_owner_id")?;
            let prev_listing: Option<String> = row.get("prev_listing_id")?;
            match (prev_owner, prev_listing) {
                (Some(prev_owner), Some(prev_listing))
                    if prev_owner != owner_id || prev_listing != listing_id =>
                {
                    Some((prev_owner, prev_listing))
                }
                _ => None,
            }
        } else {
            None
        };
        // Invalidate the newly moderated target first so a transient Redis error
        // on an old retargeted key never skips protecting the new target.
        let new_result = invalidate_listing_cache(owner_id, listing_id).await;
        let prev_result = if let Some((prev_owner, prev_listing)) = prev_target {
            invalidate_listing_cache(&prev_owner, &prev_listing).await
        } else {
            Ok(())
        };
        new_result?;
        prev_result?;
        Ok(())
    }

    /// Whether any moderator tag currently hides the listing.
    pub async fn is_moderated(owner_id: &str, listing_id: &str) -> GraphResult<bool> {
        let row = fetch_row_from_graph(queries::get::listing_moderation_marker(
            owner_id, listing_id,
        ))
        .await?;
        Ok(row.is_some())
    }

    /// Whether a moderator tag other than `(moderator_id, tag_id)` hides the listing.
    pub async fn is_moderated_besides(
        owner_id: &str,
        listing_id: &str,
        moderator_id: &str,
        tag_id: &str,
    ) -> GraphResult<bool> {
        let row = fetch_row_from_graph(queries::get::listing_moderation_marker_besides(
            owner_id,
            listing_id,
            moderator_id,
            tag_id,
        ))
        .await?;
        Ok(row.is_some())
    }

    /// The `(owner_id, listing_id)` pairs hidden by one moderator tag record.
    pub async fn listings_of_tag(
        moderator_id: &str,
        tag_id: &str,
    ) -> GraphResult<Vec<(String, String)>> {
        let rows = fetch_all_rows_from_graph(queries::get::moderated_listings_of_tag(
            moderator_id,
            tag_id,
        ))
        .await?;
        let mut listings = Vec::with_capacity(rows.len());
        for row in rows {
            if let (Some(owner_id), Some(listing_id)) =
                (row.get("owner_id")?, row.get("listing_id")?)
            {
                listings.push((owner_id, listing_id));
            }
        }
        Ok(listings)
    }

    /// Removes the marker written by one moderator tag record and invalidates
    /// the listing moderation cache immediately.
    pub async fn delete(moderator_id: &str, tag_id: &str) -> ModelResult<()> {
        let rows =
            fetch_all_rows_from_graph(queries::del::delete_moderated_listing(moderator_id, tag_id))
                .await?;
        for row in rows {
            let owner_id: Option<String> = row.get("owner_id")?;
            let listing_id: Option<String> = row.get("listing_id")?;
            if let (Some(owner_id), Some(listing_id)) = (owner_id, listing_id) {
                if let Err(err) = invalidate_listing_cache(&owner_id, &listing_id).await {
                    warn!(
                        %owner_id,
                        %listing_id,
                        ?err,
                        "Failed to invalidate listing moderation cache after delete"
                    );
                }
            }
        }
        Ok(())
    }
}

/// Moderation marker hiding a user's marketplace shop and listings.
///
/// Written when the configured moderator tags a user's profile URI with a
/// moderated label. It is keyed by the moderator's tag record, so the
/// moderator removing that tag removes exactly that marker, and it is a
/// standalone graph node that references the user only by id: it holds
/// whether the tag event is replayed before or after the user's profile,
/// shop and listing events. A shop or listing whose owner has at least one
/// marker is never indexed.
pub struct ModeratedUser;

impl ModeratedUser {
    /// Records (or refreshes) the marker written by one moderator tag and
    /// invalidates the seller moderation cache immediately.
    pub async fn put(
        user_id: &str,
        uri: &str,
        moderator_id: &str,
        tag_id: &str,
        label: &str,
        moderated_at: i64,
    ) -> ModelResult<()> {
        let row = fetch_row_from_graph(queries::put::put_moderated_user(
            user_id,
            uri,
            moderator_id,
            tag_id,
            label,
            moderated_at,
        ))
        .await?;
        let prev_user = if let Some(row) = row {
            let prev_user: Option<String> = row.get("prev_user_id")?;
            prev_user.filter(|prev| prev != user_id)
        } else {
            None
        };
        // Invalidate the newly moderated seller first so a transient Redis error
        // on a retargeted prior seller never skips invalidating the new seller.
        let new_result = invalidate_user_cache(user_id).await;
        let prev_result = if let Some(prev_user) = prev_user {
            invalidate_user_cache(&prev_user).await
        } else {
            Ok(())
        };
        new_result?;
        prev_result?;
        Ok(())
    }

    /// Whether any moderator tag currently hides the user's shop and listings.
    pub async fn is_moderated(user_id: &str) -> GraphResult<bool> {
        let row = fetch_row_from_graph(queries::get::user_moderation_marker(user_id)).await?;
        Ok(row.is_some())
    }

    /// Whether a moderator tag other than `(moderator_id, tag_id)` hides the user.
    pub async fn is_moderated_besides(
        user_id: &str,
        moderator_id: &str,
        tag_id: &str,
    ) -> GraphResult<bool> {
        let row = fetch_row_from_graph(queries::get::user_moderation_marker_besides(
            user_id,
            moderator_id,
            tag_id,
        ))
        .await?;
        Ok(row.is_some())
    }

    /// The ids of the users hidden by one moderator tag record.
    pub async fn users_of_tag(moderator_id: &str, tag_id: &str) -> GraphResult<Vec<String>> {
        let rows =
            fetch_all_rows_from_graph(queries::get::moderated_users_of_tag(moderator_id, tag_id))
                .await?;
        let mut users = Vec::with_capacity(rows.len());
        for row in rows {
            if let Some(user_id) = row.get("user_id")? {
                users.push(user_id);
            }
        }
        Ok(users)
    }

    /// Flags the marker of one moderator tag record as being released: review
    /// moderation stops applying it, the shop and listings stay hidden until
    /// [`Self::delete`].
    pub async fn mark_releasing(moderator_id: &str, tag_id: &str) -> GraphResult<()> {
        exec_single_row(queries::put::mark_moderated_user_releasing(
            moderator_id,
            tag_id,
        ))
        .await
    }

    /// Removes the marker written by one moderator tag record and invalidates
    /// the seller moderation cache immediately.
    pub async fn delete(moderator_id: &str, tag_id: &str) -> ModelResult<()> {
        let rows =
            fetch_all_rows_from_graph(queries::del::delete_moderated_user(moderator_id, tag_id))
                .await?;
        for row in rows {
            let user_id: Option<String> = row.get("user_id")?;
            if let Some(user_id) = user_id {
                if let Err(err) = invalidate_user_cache(&user_id).await {
                    warn!(
                        %user_id,
                        ?err,
                        "Failed to invalidate seller moderation cache after delete"
                    );
                }
            }
        }
        Ok(())
    }
}

/// Moderation marker on one marketplace review.
///
/// Written when the configured moderator tags a review URI with a moderated
/// label (the review's text is hidden) or a review-exclusion label (the review
/// leaves the aggregates). Like the listing marker it is keyed by the
/// moderator's tag record and is a standalone node that references the review
/// only by `(reviewer_id, review_id)`: it holds whether the tag event is
/// replayed before or after the review, and across edits of the review.
/// Nothing is deleted; the review's `moderation` is derived from the markers.
pub struct ModeratedReview;

impl ModeratedReview {
    /// Records (or refreshes) the marker written by one moderator tag.
    #[allow(clippy::too_many_arguments)]
    pub async fn put(
        reviewer_id: &str,
        review_id: &str,
        uri: &str,
        moderator_id: &str,
        tag_id: &str,
        label: &str,
        state: ReviewModerationState,
        moderated_at: i64,
    ) -> GraphResult<()> {
        exec_single_row(queries::put::put_moderated_review(
            reviewer_id,
            review_id,
            uri,
            moderator_id,
            tag_id,
            label,
            state.as_str(),
            moderated_at,
        ))
        .await
    }

    /// The `(reviewer_id, review_id)` pairs marked by one moderator tag record.
    pub async fn reviews_of_tag(
        moderator_id: &str,
        tag_id: &str,
    ) -> GraphResult<Vec<(String, String)>> {
        let rows =
            fetch_all_rows_from_graph(queries::get::moderated_reviews_of_tag(moderator_id, tag_id))
                .await?;
        let mut reviews = Vec::with_capacity(rows.len());
        for row in rows {
            if let (Some(reviewer_id), Some(review_id)) =
                (row.get("reviewer_id")?, row.get("review_id")?)
            {
                reviews.push((reviewer_id, review_id));
            }
        }
        Ok(reviews)
    }

    /// Flags the marker of one moderator tag record as being released: review
    /// moderation stops applying it.
    pub async fn mark_releasing(moderator_id: &str, tag_id: &str) -> GraphResult<()> {
        exec_single_row(queries::put::mark_moderated_review_releasing(
            moderator_id,
            tag_id,
        ))
        .await
    }

    /// Removes the marker written by one moderator tag record.
    pub async fn delete(moderator_id: &str, tag_id: &str) -> GraphResult<()> {
        exec_single_row(queries::del::delete_moderated_review(moderator_id, tag_id)).await
    }
}

/// Moderation marker excluding every review a user wrote from the aggregates
/// (a fraud ring), written when the configured moderator tags the user's
/// profile URI with a review-exclusion label. It does not hide the user, their
/// shop or their listings. Keyed by the moderator's tag record and standalone,
/// like the other markers, so it also holds for reviews indexed after it.
pub struct ExcludedReviewer;

impl ExcludedReviewer {
    /// Records (or refreshes) the marker written by one moderator tag.
    pub async fn put(
        user_id: &str,
        uri: &str,
        moderator_id: &str,
        tag_id: &str,
        label: &str,
        moderated_at: i64,
    ) -> GraphResult<()> {
        exec_single_row(queries::put::put_excluded_reviewer(
            user_id,
            uri,
            moderator_id,
            tag_id,
            label,
            moderated_at,
        ))
        .await
    }

    /// The ids of the users whose reviews one moderator tag record excludes.
    pub async fn users_of_tag(moderator_id: &str, tag_id: &str) -> GraphResult<Vec<String>> {
        let rows = fetch_all_rows_from_graph(queries::get::excluded_reviewers_of_tag(
            moderator_id,
            tag_id,
        ))
        .await?;
        let mut users = Vec::with_capacity(rows.len());
        for row in rows {
            if let Some(user_id) = row.get("user_id")? {
                users.push(user_id);
            }
        }
        Ok(users)
    }

    /// Flags the marker of one moderator tag record as being released: review
    /// moderation stops applying it.
    pub async fn mark_releasing(moderator_id: &str, tag_id: &str) -> GraphResult<()> {
        exec_single_row(queries::put::mark_excluded_reviewer_releasing(
            moderator_id,
            tag_id,
        ))
        .await
    }

    /// Removes the marker written by one moderator tag record.
    pub async fn delete(moderator_id: &str, tag_id: &str) -> GraphResult<()> {
        exec_single_row(queries::del::delete_excluded_reviewer(moderator_id, tag_id)).await
    }
}

/// What moderation does to a review.
#[derive(Serialize, Deserialize, ToSchema, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ReviewModerationState {
    /// The review's text is withheld; its ratings still count.
    TextHidden,
    /// The review is left out of every aggregate and its text is withheld.
    Excluded,
}

impl ReviewModerationState {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::TextHidden => "text_hidden",
            Self::Excluded => "excluded",
        }
    }

    fn from_graph(value: &str) -> Option<Self> {
        match value {
            "text_hidden" => Some(Self::TextHidden),
            "excluded" => Some(Self::Excluded),
            _ => None,
        }
    }
}

/// The moderation applied to a review: its state and the moderator's label as
/// the reason.
#[derive(Serialize, Deserialize, ToSchema, Clone, Debug, PartialEq, Eq)]
pub struct ReviewModeration {
    pub state: ReviewModerationState,
    /// The moderator tag's label, e.g. `hatespeech` or `review_fraud`.
    pub reason: String,
}

impl ReviewModeration {
    /// Derives a review's moderation from the markers that apply to it: a
    /// marker on the review, a review-exclusion marker on its reviewer, or a
    /// moderation marker hiding its reviewer (which hides the text, ratings
    /// kept). Exclusion wins over hidden text; among markers of one state the
    /// lowest tag id gives the reason, so a replay picks the same one.
    /// Markers flagged as being released are left out, in every process, from
    /// the moment the release starts.
    pub async fn get(reviewer_id: &str, review_id: &str) -> GraphResult<Option<Self>> {
        let row =
            fetch_row_from_graph(queries::get::review_moderation(reviewer_id, review_id)).await?;
        let Some(row) = row else {
            return Ok(None);
        };
        let state: String = row.get("state")?;
        let reason: String = row.get("reason")?;
        Ok(ReviewModerationState::from_graph(&state).map(|state| Self { state, reason }))
    }

    /// Derives a review's moderation like [`Self::get`] and stores it on the
    /// review edge, as one locked step per review (see
    /// [`queries::put::derive_review_moderation`]). This is the only path that
    /// stores a review's moderation; the details cache is written from its
    /// result with [`DerivedReviewModeration::version`].
    pub async fn derive_and_store(
        reviewer_id: &str,
        review_id: &str,
    ) -> GraphResult<DerivedReviewModeration> {
        let row = fetch_row_from_graph(queries::put::derive_review_moderation(
            reviewer_id,
            review_id,
        ))
        .await?
        .ok_or_else(|| {
            GraphError::Generic(format!(
                "derive_review_moderation returned no row for {reviewer_id}/{review_id}"
            ))
        })?;
        let moderation = Self::from_row(
            row.get::<Option<String>>("state")?,
            row.get::<Option<String>>("reason")?,
        );
        let previous = Self::from_row(
            row.get::<Option<String>>("previous_state")?,
            row.get::<Option<String>>("previous_reason")?,
        );
        let indexed: bool = row.get("indexed")?;
        Ok(DerivedReviewModeration {
            version: row.get("version")?,
            changed: indexed && previous != moderation,
            moderation,
        })
    }

    fn from_row(state: Option<String>, reason: Option<String>) -> Option<Self> {
        let state = ReviewModerationState::from_graph(&state?)?;
        Some(Self {
            state,
            reason: reason?,
        })
    }
}

/// A review's moderation as one [`ReviewModeration::derive_and_store`] stored it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DerivedReviewModeration {
    /// Orders this derivation among every derivation of the review: a higher
    /// version read the markers later.
    pub version: i64,
    pub moderation: Option<ReviewModeration>,
    /// Whether the review edge exists and its stored moderation changed, so
    /// the aggregates that read it must be recomputed.
    pub changed: bool,
}

/// Whether the moderation of pubky.app's moderator keeps a listing out of the
/// marketplace index, and which moderator tag does it.
#[derive(Serialize, Deserialize, ToSchema, Clone, Copy, Debug, PartialEq, Eq)]
pub struct ListingModerationState {
    /// The listing, or its seller, is moderated. A moderated listing must
    /// not be sold, bid on or offered on.
    pub moderated: bool,
    /// A moderator tag on the listing itself hides it.
    pub listing_moderated: bool,
    /// A moderator tag on the seller hides the seller's shop and every listing.
    pub seller_moderated: bool,
}

impl ListingModerationState {
    pub fn new(listing_moderated: bool, seller_moderated: bool) -> Self {
        Self {
            moderated: listing_moderated || seller_moderated,
            listing_moderated,
            seller_moderated,
        }
    }

    /// Reads both listing and seller moderation flags directly from Neo4j in
    /// one roundtrip.
    pub async fn get_from_graph(owner_id: &str, listing_id: &str) -> GraphResult<Self> {
        let row =
            fetch_row_from_graph(queries::get::listing_moderation_state(owner_id, listing_id))
                .await?
                .ok_or_else(|| {
                    GraphError::Generic(format!(
                        "listing_moderation_state returned no row for {owner_id}/{listing_id}"
                    ))
                })?;
        let listing_moderated: bool = row.get("listing_moderated")?;
        let seller_moderated: bool = row.get("seller_moderated")?;
        Ok(Self::new(listing_moderated, seller_moderated))
    }

    /// Reads the markers for a listing. It answers from the moderation
    /// markers only, so it is the same whether or not the listing is indexed:
    /// a listing the index never saw, or never will, can still be moderated.
    ///
    /// Serves bursts from a short-TTL Redis cache invalidated immediately by
    /// [`ModeratedListing`] and [`ModeratedUser`] writes, and consolidates
    /// cold misses into a single Neo4j query.
    pub async fn get(owner_id: &str, listing_id: &str) -> ModelResult<Self> {
        if !is_cacheable_id(owner_id) || !is_cacheable_id(listing_id) {
            return Ok(Self::get_from_graph(owner_id, listing_id).await?);
        }

        let listing_key = listing_moderation_cache_key(owner_id, listing_id);
        let listing_lease_key = listing_moderation_lease_key(owner_id, listing_id);
        let user_key = user_moderation_cache_key(owner_id);
        let user_lease_key = user_moderation_lease_key(owner_id);
        let deadline = Instant::now() + MAX_CACHE_FILL_ELAPSED;

        let (cached_listing, cached_seller) =
            match read_cached_moderation_pair(&listing_key, &user_key).await {
                Ok(pair) => pair,
                Err(err) => {
                    warn!(
                        %owner_id,
                        %listing_id,
                        ?err,
                        "Failed to read listing moderation cache; falling back to Neo4j"
                    );
                    return Ok(Self::get_from_graph(owner_id, listing_id).await?);
                }
            };

        match (cached_listing, cached_seller) {
            (Some(listing_moderated), Some(seller_moderated)) => {
                Ok(Self::new(listing_moderated, seller_moderated))
            }
            (None, None) => {
                let (listing_lease, user_lease) =
                    match acquire_both_lease_tokens(&listing_lease_key, &user_lease_key).await {
                        Ok(leases) => leases,
                        Err(err) => {
                            warn!(
                                %owner_id,
                                %listing_id,
                                ?err,
                                "Failed to acquire listing moderation cache leases; skipping fill"
                            );
                            (String::new(), String::new())
                        }
                    };
                let state = Self::get_from_graph(owner_id, listing_id).await?;
                if !listing_lease.is_empty() || !user_lease.is_empty() {
                    match timeout_at(
                        deadline,
                        put_both_cached_flags_if_lease_matches(
                            CachedFlagWrite {
                                cache_key: &listing_key,
                                lease_key: &listing_lease_key,
                                value: state.listing_moderated,
                                expected_lease: &listing_lease,
                            },
                            CachedFlagWrite {
                                cache_key: &user_key,
                                lease_key: &user_lease_key,
                                value: state.seller_moderated,
                                expected_lease: &user_lease,
                            },
                        ),
                    )
                    .await
                    {
                        Ok(Ok(())) => {}
                        Ok(Err(err)) => {
                            warn!(
                                %owner_id,
                                %listing_id,
                                ?err,
                                "Failed to populate listing moderation cache"
                            );
                        }
                        Err(_) => {
                            warn!(
                                %owner_id,
                                %listing_id,
                                "Timed out populating listing moderation cache"
                            );
                        }
                    }
                }
                Ok(state)
            }
            (None, Some(seller_moderated)) => {
                let listing_lease = match acquire_lease_token(&listing_lease_key).await {
                    Ok(lease) => lease,
                    Err(err) => {
                        warn!(
                            %owner_id,
                            %listing_id,
                            ?err,
                            "Failed to acquire listing moderation cache lease; skipping fill"
                        );
                        String::new()
                    }
                };
                let listing_moderated =
                    ModeratedListing::is_moderated(owner_id, listing_id).await?;
                if !listing_lease.is_empty() {
                    match timeout_at(
                        deadline,
                        put_cached_flag_if_lease_matches(
                            &listing_key,
                            &listing_lease_key,
                            listing_moderated,
                            &listing_lease,
                        ),
                    )
                    .await
                    {
                        Ok(Ok(_)) => {}
                        Ok(Err(err)) => {
                            warn!(
                                %owner_id,
                                %listing_id,
                                ?err,
                                "Failed to populate listing moderation cache entry"
                            );
                        }
                        Err(_) => {
                            warn!(
                                %owner_id,
                                %listing_id,
                                "Timed out populating listing moderation cache entry"
                            );
                        }
                    }
                }
                Ok(Self::new(listing_moderated, seller_moderated))
            }
            (Some(listing_moderated), None) => {
                let user_lease = match acquire_lease_token(&user_lease_key).await {
                    Ok(lease) => lease,
                    Err(err) => {
                        warn!(
                            %owner_id,
                            ?err,
                            "Failed to acquire seller moderation cache lease; skipping fill"
                        );
                        String::new()
                    }
                };
                let seller_moderated = ModeratedUser::is_moderated(owner_id).await?;
                if !user_lease.is_empty() {
                    match timeout_at(
                        deadline,
                        put_cached_flag_if_lease_matches(
                            &user_key,
                            &user_lease_key,
                            seller_moderated,
                            &user_lease,
                        ),
                    )
                    .await
                    {
                        Ok(Ok(_)) => {}
                        Ok(Err(err)) => {
                            warn!(
                                %owner_id,
                                ?err,
                                "Failed to populate seller moderation cache entry"
                            );
                        }
                        Err(_) => {
                            warn!(
                                %owner_id,
                                "Timed out populating seller moderation cache entry"
                            );
                        }
                    }
                }
                Ok(Self::new(listing_moderated, seller_moderated))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        is_cacheable_id, listing_moderation_cache_key, next_lease_token, parse_cached_bool,
        ListingModerationState, MAX_CACHED_ID_BYTES,
    };

    #[test]
    fn a_listing_is_moderated_when_either_marker_hides_it() {
        assert!(!ListingModerationState::new(false, false).moderated);
        assert!(ListingModerationState::new(true, false).moderated);
        assert!(ListingModerationState::new(false, true).moderated);
        assert!(ListingModerationState::new(true, true).moderated);
    }

    #[test]
    fn cache_keys_do_not_collide_across_colon_boundaries() {
        assert_ne!(
            listing_moderation_cache_key("a:b", "c"),
            listing_moderation_cache_key("a", "b:c")
        );
    }

    #[test]
    fn parse_cached_bool_accepts_only_zero_and_one() {
        assert_eq!(parse_cached_bool(Some("0")), Some(false));
        assert_eq!(parse_cached_bool(Some("1")), Some(true));
        assert_eq!(parse_cached_bool(Some("true")), None);
        assert_eq!(parse_cached_bool(Some("")), None);
        assert_eq!(parse_cached_bool(None), None);
    }

    #[test]
    fn is_cacheable_id_bounds_key_size() {
        assert!(!is_cacheable_id(""));
        assert!(is_cacheable_id(&"a".repeat(MAX_CACHED_ID_BYTES)));
        assert!(!is_cacheable_id(&"a".repeat(MAX_CACHED_ID_BYTES + 1)));
    }

    #[test]
    fn lease_tokens_are_non_empty_and_distinct() {
        let first = next_lease_token();
        let second = next_lease_token();
        assert!(!first.is_empty());
        assert!(!second.is_empty());
        assert_ne!(first, second);
    }
}
