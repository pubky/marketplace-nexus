use crate::db::kv::RedisResult;
use crate::db::{
    exec_single_row, fetch_all_rows_from_graph, fetch_row_from_graph, get_redis_conn, queries,
    GraphError, GraphResult,
};
use crate::models::error::ModelResult;
use deadpool_redis::redis::{AsyncCommands, Script};
use serde::{Deserialize, Serialize};
use std::time::{Duration, Instant};
use tracing::warn;
use utoipa::ToSchema;

/// Short TTL (seconds) for cached listing and seller moderation flags in Redis.
/// Bounds Redis memory and caps staleness if markers are mutated outside the
/// standard watcher write path.
pub const MODERATION_CACHE_TTL_SECS: i64 = 15;

/// TTL (seconds) for per-target invalidation generation counters. Strictly
/// exceeds [`MAX_CACHE_FILL_ELAPSED`] so an in-flight miss read cannot outlive
/// a concurrent invalidation's generation bump.
const MODERATION_GEN_TTL_SECS: i64 = 120;

/// Maximum elapsed time for a cache-miss graph read that is still eligible to
/// populate Redis. Prevents ABA wraparound against [`MODERATION_GEN_TTL_SECS`].
const MAX_CACHE_FILL_ELAPSED: Duration = Duration::from_secs(10);

/// Maximum byte length of an `owner_id` or `listing_id` component eligible for
/// Redis caching. Valid Pubky z32 IDs (52 bytes) and Crockford32 IDs are well
/// under this bound; oversized path segments fall through directly to Neo4j.
const MAX_CACHED_ID_BYTES: usize = 128;

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

fn listing_moderation_gen_key(owner_id: &str, listing_id: &str) -> String {
    format!(
        "Marketplace:Moderation:ListingGen:{}:{owner_id}:{listing_id}",
        owner_id.len()
    )
}

/// Redis key caching whether a seller has any `ModeratedUser` marker.
pub fn user_moderation_cache_key(user_id: &str) -> String {
    format!("Marketplace:Moderation:User:{user_id}")
}

fn user_moderation_gen_key(user_id: &str) -> String {
    format!("Marketplace:Moderation:UserGen:{user_id}")
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

/// Atomically deletes the cached flag and bumps its generation counter so any
/// concurrent cache-miss graph read that began before this write cannot
/// populate Redis with a stale value.
async fn invalidate_cache_entry(cache_key: &str, gen_key: &str) -> RedisResult<()> {
    let mut conn = get_redis_conn().await?;
    let script = Script::new(
        r#"
        redis.call('DEL', KEYS[1])
        redis.call('INCR', KEYS[2])
        redis.call('EXPIRE', KEYS[2], tonumber(ARGV[1]))
        return 1
        "#,
    );
    let _: i64 = script
        .key(cache_key)
        .key(gen_key)
        .arg(MODERATION_GEN_TTL_SECS)
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
        &listing_moderation_gen_key(owner_id, listing_id),
    )
    .await
}

async fn invalidate_user_cache(user_id: &str) -> RedisResult<()> {
    if !is_cacheable_id(user_id) {
        return Ok(());
    }
    invalidate_cache_entry(
        &user_moderation_cache_key(user_id),
        &user_moderation_gen_key(user_id),
    )
    .await
}

struct CachedFlagWrite<'a> {
    cache_key: &'a str,
    gen_key: &'a str,
    value: bool,
    expected_gen: &'a str,
}

struct CachedModerationLookup {
    listing: Option<bool>,
    seller: Option<bool>,
    listing_gen: String,
    seller_gen: String,
}

async fn read_cached_moderation_pair(
    listing_key: &str,
    user_key: &str,
    listing_gen_key: &str,
    user_gen_key: &str,
) -> RedisResult<CachedModerationLookup> {
    let mut conn = get_redis_conn().await?;
    let values: Vec<Option<String>> = conn
        .mget(&[listing_key, user_key, listing_gen_key, user_gen_key])
        .await?;
    if values.len() != 4 {
        return Ok(CachedModerationLookup {
            listing: None,
            seller: None,
            listing_gen: String::new(),
            seller_gen: String::new(),
        });
    }
    Ok(CachedModerationLookup {
        listing: parse_cached_bool(values[0].as_deref()),
        seller: parse_cached_bool(values[1].as_deref()),
        listing_gen: values[2].clone().unwrap_or_default(),
        seller_gen: values[3].clone().unwrap_or_default(),
    })
}

/// Populates one cached flag only when its generation counter still equals
/// `expected_gen`. Returns whether the cache entry was written.
async fn put_cached_flag_if_gen_matches(
    cache_key: &str,
    gen_key: &str,
    value: bool,
    expected_gen: &str,
) -> RedisResult<bool> {
    let mut conn = get_redis_conn().await?;
    let script = Script::new(
        r#"
        local current = redis.call('GET', KEYS[2]) or ''
        if current == ARGV[2] then
            redis.call('SET', KEYS[1], ARGV[1], 'EX', tonumber(ARGV[3]))
            return 1
        end
        return 0
        "#,
    );
    let written: i64 = script
        .key(cache_key)
        .key(gen_key)
        .arg(encode_cached_bool(value))
        .arg(expected_gen)
        .arg(MODERATION_CACHE_TTL_SECS)
        .invoke_async(&mut conn)
        .await?;
    Ok(written == 1)
}

/// Populates both listing and seller cached flags in one Redis roundtrip,
/// checking each target's generation counter independently.
async fn put_both_cached_flags_if_gen_matches(
    listing: CachedFlagWrite<'_>,
    user: CachedFlagWrite<'_>,
) -> RedisResult<()> {
    let mut conn = get_redis_conn().await?;
    let script = Script::new(
        r#"
        local ttl = tonumber(ARGV[5])
        if (redis.call('GET', KEYS[2]) or '') == ARGV[2] then
            redis.call('SET', KEYS[1], ARGV[1], 'EX', ttl)
        end
        if (redis.call('GET', KEYS[4]) or '') == ARGV[4] then
            redis.call('SET', KEYS[3], ARGV[3], 'EX', ttl)
        end
        return 1
        "#,
    );
    let _: i64 = script
        .key(listing.cache_key)
        .key(listing.gen_key)
        .key(user.cache_key)
        .key(user.gen_key)
        .arg(encode_cached_bool(listing.value))
        .arg(listing.expected_gen)
        .arg(encode_cached_bool(user.value))
        .arg(user.expected_gen)
        .arg(MODERATION_CACHE_TTL_SECS)
        .invoke_async(&mut conn)
        .await?;
    Ok(())
}

#[cfg(feature = "test-utils")]
pub async fn populate_listing_cache_if_gen_matches(
    owner_id: &str,
    listing_id: &str,
    value: bool,
    expected_gen: &str,
) -> RedisResult<bool> {
    put_cached_flag_if_gen_matches(
        &listing_moderation_cache_key(owner_id, listing_id),
        &listing_moderation_gen_key(owner_id, listing_id),
        value,
        expected_gen,
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
        if let Some(row) = row {
            let prev_owner: Option<String> = row.get("prev_owner_id")?;
            let prev_listing: Option<String> = row.get("prev_listing_id")?;
            if let (Some(prev_owner), Some(prev_listing)) = (prev_owner, prev_listing) {
                if prev_owner != owner_id || prev_listing != listing_id {
                    invalidate_listing_cache(&prev_owner, &prev_listing).await?;
                }
            }
        }
        invalidate_listing_cache(owner_id, listing_id).await?;
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
        if let Some(row) = row {
            let prev_user: Option<String> = row.get("prev_user_id")?;
            if let Some(prev_user) = prev_user {
                if prev_user != user_id {
                    invalidate_user_cache(&prev_user).await?;
                }
            }
        }
        invalidate_user_cache(user_id).await?;
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
        let listing_gen_key = listing_moderation_gen_key(owner_id, listing_id);
        let user_key = user_moderation_cache_key(owner_id);
        let user_gen_key = user_moderation_gen_key(owner_id);
        let started = Instant::now();

        let cached = match read_cached_moderation_pair(
            &listing_key,
            &user_key,
            &listing_gen_key,
            &user_gen_key,
        )
        .await
        {
            Ok(lookup) => lookup,
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

        match (cached.listing, cached.seller) {
            (Some(listing_moderated), Some(seller_moderated)) => {
                Ok(Self::new(listing_moderated, seller_moderated))
            }
            (None, None) => {
                let state = Self::get_from_graph(owner_id, listing_id).await?;
                if started.elapsed() < MAX_CACHE_FILL_ELAPSED {
                    if let Err(err) = put_both_cached_flags_if_gen_matches(
                        CachedFlagWrite {
                            cache_key: &listing_key,
                            gen_key: &listing_gen_key,
                            value: state.listing_moderated,
                            expected_gen: &cached.listing_gen,
                        },
                        CachedFlagWrite {
                            cache_key: &user_key,
                            gen_key: &user_gen_key,
                            value: state.seller_moderated,
                            expected_gen: &cached.seller_gen,
                        },
                    )
                    .await
                    {
                        warn!(
                            %owner_id,
                            %listing_id,
                            ?err,
                            "Failed to populate listing moderation cache"
                        );
                    }
                }
                Ok(state)
            }
            (None, Some(seller_moderated)) => {
                let listing_moderated =
                    ModeratedListing::is_moderated(owner_id, listing_id).await?;
                if started.elapsed() < MAX_CACHE_FILL_ELAPSED {
                    if let Err(err) = put_cached_flag_if_gen_matches(
                        &listing_key,
                        &listing_gen_key,
                        listing_moderated,
                        &cached.listing_gen,
                    )
                    .await
                    {
                        warn!(
                            %owner_id,
                            %listing_id,
                            ?err,
                            "Failed to populate listing moderation cache entry"
                        );
                    }
                }
                Ok(Self::new(listing_moderated, seller_moderated))
            }
            (Some(listing_moderated), None) => {
                let seller_moderated = ModeratedUser::is_moderated(owner_id).await?;
                if started.elapsed() < MAX_CACHE_FILL_ELAPSED {
                    if let Err(err) = put_cached_flag_if_gen_matches(
                        &user_key,
                        &user_gen_key,
                        seller_moderated,
                        &cached.seller_gen,
                    )
                    .await
                    {
                        warn!(
                            %owner_id,
                            ?err,
                            "Failed to populate seller moderation cache entry"
                        );
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
        is_cacheable_id, listing_moderation_cache_key, parse_cached_bool, ListingModerationState,
        MAX_CACHED_ID_BYTES,
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
}
