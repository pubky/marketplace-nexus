use crate::db::{fetch_all_rows_from_graph, fetch_row_from_graph, queries, RedisOps};
use crate::models::error::ModelResult;
use pubky_app_specs::{PubkyAppReviewRole, PubkyId};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use utoipa::ToSchema;

/// The compact reputation object embedded in listing stream entries and shop
/// views (ADR 0024 §9): cards render stars with zero additional requests.
///
/// Truth basis: `count` covers every indexed review of the scope;
/// `verified_count` covers the subset whose embedded purchase attestation
/// cryptographically verified (parse + Ed25519 signature against the `iss`
/// pubky + review binding). Which attestors to *trust* is the consumer's
/// policy — the full [`ReputationSummary`] breaks verified counts down per
/// attestor pubky. `avg` averages `ratings.overall` over ALL indexed
/// reviews (verified and labeled-unverified alike, ratified D5).
///
/// `buckets` and `unverified_count` carry the counting rules v2 figures:
/// a client sums the buckets of the attestors it trusts whose trust window
/// covers the bucket's `[first_iat, last_iat]`, and drops a bucket a window
/// cuts, so it never over-counts.
#[derive(Serialize, Deserialize, ToSchema, Clone, Debug, PartialEq)]
pub struct ReputationSnippet {
    pub avg: f64,
    pub count: u64,
    pub verified_count: u64,
    /// Counted verified reviews per attestor pubky (see [`ReputationBucket`]).
    #[serde(default)]
    pub buckets: BTreeMap<String, ReputationBucket>,
    /// Reviews whose attestation did not verify; never bucketed.
    #[serde(default)]
    pub unverified_count: u64,
}

/// The counting rules v2 figures of one set of counted reviews: a scope's
/// reviews signed by one attestor (a trust bucket), or, from the
/// `attestors=` parameter, a scope's reviews inside the given attestor
/// windows.
///
/// A counted review verified, is not superseded by its order-scoped twin and
/// is not excluded by moderation. Every figure is a sum or a count, so a
/// client can add the buckets of several trusted attestors.
/// - rating = `sum_buyer_means / buyers`: the mean over buyers of each
///   buyer's mean `overall`.
/// - weighted rating = `weighted_sum / weight_total`: per buyer,
///   m'_b = Σw·r / Σw over their reviews and W_b = their largest weight;
///   `weighted_sum` = Σ W_b·m'_b and `weight_total` = Σ W_b. The weight w is
///   the review's price weight (1-5).
/// - seller rating (subject scope of the buyer-reviewing-seller role only)
///   = `sum_seller_buyer_means / seller_buyers`: per buyer, the mean over
///   their counted orders of (shipping + communication) / 2, using the one
///   present when the other is missing; an order with neither does not
///   count, and a buyer without such an order is not in `seller_buyers`.
#[derive(Serialize, Deserialize, ToSchema, Clone, Debug, PartialEq, Default)]
pub struct ReputationBucket {
    /// Distinct attested orders `(attestor, order_ref)`.
    pub orders: u64,
    /// Distinct reviewers ("buyers" in the buyer-reviewing-seller role).
    pub buyers: u64,
    /// Σ over buyers of the buyer's mean `ratings.overall`.
    pub sum_buyer_means: f64,
    /// Σ over buyers of W_b·m'_b (see the type docs).
    pub weighted_sum: f64,
    /// Σ over buyers of W_b, the buyer's largest price weight.
    pub weight_total: u64,
    /// Star histogram of `ratings.overall` over the counted reviews: index 0
    /// holds 1-star counts.
    pub histogram: [u64; 5],
    /// Earliest attestation `iat` (seconds) among the counted reviews;
    /// `null` only when nothing counts.
    pub first_iat: Option<i64>,
    /// Latest attestation `iat` (seconds) among the counted reviews; `null`
    /// only when nothing counts.
    pub last_iat: Option<i64>,
    /// Buyers with at least one order carrying a seller rating. Subject
    /// scope of the buyer-reviewing-seller role only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub seller_buyers: Option<u64>,
    /// Σ over those buyers of their mean order seller score. Subject scope
    /// of the buyer-reviewing-seller role only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sum_seller_buyer_means: Option<f64>,
}

/// One counted verified review as the trust figures read it.
#[derive(Clone, Debug, PartialEq)]
pub struct CountedReview {
    pub reviewer_id: String,
    pub attestor_id: String,
    pub order_ref: String,
    pub rating_overall: i64,
    pub price_weight: u8,
    pub iat: i64,
    /// Whether this review carries its order's counted seller rating.
    pub seller_fields_counted: bool,
    pub rating_shipping: Option<i64>,
    pub rating_communication: Option<i64>,
}

impl CountedReview {
    /// The order's seller score from this copy: the mean of shipping and
    /// communication, or the one present; `None` with neither.
    fn seller_score(&self) -> Option<f64> {
        match (self.rating_shipping, self.rating_communication) {
            (Some(shipping), Some(communication)) => Some((shipping + communication) as f64 / 2.0),
            (Some(score), None) | (None, Some(score)) => Some(score as f64),
            (None, None) => None,
        }
    }
}

impl ReputationBucket {
    /// Computes the figures of a set of counted reviews. `seller_score`
    /// adds the seller rating fields.
    pub fn compute<'a>(
        reviews: impl IntoIterator<Item = &'a CountedReview>,
        seller_score: bool,
    ) -> Self {
        let mut bucket = ReputationBucket::default();
        let mut orders = BTreeSet::new();
        let mut by_buyer: BTreeMap<&str, Vec<&CountedReview>> = BTreeMap::new();
        for review in reviews {
            orders.insert((review.attestor_id.as_str(), review.order_ref.as_str()));
            if let Some(slot) = usize::try_from(review.rating_overall - 1)
                .ok()
                .and_then(|index| bucket.histogram.get_mut(index))
            {
                *slot += 1;
            }
            bucket.first_iat = Some(bucket.first_iat.map_or(review.iat, |t| t.min(review.iat)));
            bucket.last_iat = Some(bucket.last_iat.map_or(review.iat, |t| t.max(review.iat)));
            by_buyer
                .entry(review.reviewer_id.as_str())
                .or_default()
                .push(review);
        }
        bucket.orders = orders.len() as u64;
        bucket.buyers = by_buyer.len() as u64;

        let mut seller_buyers = 0u64;
        let mut sum_seller_buyer_means = 0.0;
        for reviews in by_buyer.values() {
            let count = reviews.len() as f64;
            let ratings: f64 = reviews.iter().map(|r| r.rating_overall as f64).sum();
            bucket.sum_buyer_means += ratings / count;

            let weights: f64 = reviews.iter().map(|r| f64::from(r.price_weight)).sum();
            let weighted: f64 = reviews
                .iter()
                .map(|r| f64::from(r.price_weight) * r.rating_overall as f64)
                .sum();
            let largest = reviews.iter().map(|r| r.price_weight).max().unwrap_or(1);
            bucket.weighted_sum += f64::from(largest) * weighted / weights;
            bucket.weight_total += u64::from(largest);

            if seller_score {
                let mut order_scores: BTreeMap<(&str, &str), f64> = BTreeMap::new();
                for review in reviews.iter().filter(|r| r.seller_fields_counted) {
                    if let Some(score) = review.seller_score() {
                        order_scores
                            .entry((review.attestor_id.as_str(), review.order_ref.as_str()))
                            .or_insert(score);
                    }
                }
                if !order_scores.is_empty() {
                    seller_buyers += 1;
                    sum_seller_buyer_means +=
                        order_scores.values().sum::<f64>() / order_scores.len() as f64;
                }
            }
        }
        if seller_score {
            bucket.seller_buyers = Some(seller_buyers);
            bucket.sum_seller_buyer_means = Some(sum_seller_buyer_means);
        }
        bucket
    }

    /// One bucket per attestor.
    pub fn per_attestor(reviews: &[CountedReview], seller_score: bool) -> BTreeMap<String, Self> {
        let attestors: BTreeSet<&str> = reviews.iter().map(|r| r.attestor_id.as_str()).collect();
        attestors
            .into_iter()
            .map(|attestor| {
                let bucket = Self::compute(
                    reviews.iter().filter(|r| r.attestor_id == attestor),
                    seller_score,
                );
                (attestor.to_string(), bucket)
            })
            .collect()
    }

    /// The figures of the reviews inside any of `windows`.
    pub fn within(
        reviews: &[CountedReview],
        windows: &[AttestorWindow],
        seller_score: bool,
    ) -> Self {
        Self::compute(
            reviews
                .iter()
                .filter(|review| windows.iter().any(|window| window.covers(review))),
            seller_score,
        )
    }
}

/// Most entries one `attestors=` parameter may carry.
pub const MAX_ATTESTOR_WINDOWS: usize = 20;

/// One entry of the `attestors=` parameter: an attestor pubky and an
/// optional trust window `[from, until)` over the attestation `iat`, in
/// seconds. A pubky may repeat with other windows (key epochs); a review
/// counts when any entry covers it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AttestorWindow {
    pub attestor_id: String,
    pub from: Option<i64>,
    pub until: Option<i64>,
}

impl AttestorWindow {
    /// Parses `<pubky>[:<from>:<until>],…`. `from` or `until` may be empty
    /// for an open end.
    pub fn parse_list(value: &str) -> Result<Vec<Self>, String> {
        let entries: Vec<&str> = value
            .split(',')
            .map(str::trim)
            .filter(|entry| !entry.is_empty())
            .collect();
        if entries.is_empty() {
            return Err("attestors must name at least one attestor".to_string());
        }
        if entries.len() > MAX_ATTESTOR_WINDOWS {
            return Err(format!(
                "attestors takes at most {MAX_ATTESTOR_WINDOWS} entries"
            ));
        }
        entries.into_iter().map(Self::parse).collect()
    }

    fn parse(entry: &str) -> Result<Self, String> {
        let parts: Vec<&str> = entry.split(':').collect();
        let (pubky, from, until) = match parts.as_slice() {
            [pubky] => (*pubky, None, None),
            [pubky, from, until] => (*pubky, parse_bound(from)?, parse_bound(until)?),
            _ => {
                return Err(format!(
                    "attestors entry {entry:?} must be <pubky> or <pubky>:<from>:<until>"
                ))
            }
        };
        PubkyId::try_from(pubky)
            .map_err(|_| format!("attestors entry {entry:?} has an invalid pubky"))?;
        if let (Some(from), Some(until)) = (from, until) {
            if from >= until {
                return Err(format!(
                    "attestors entry {entry:?} must have from before until"
                ));
            }
        }
        Ok(AttestorWindow {
            attestor_id: pubky.to_string(),
            from,
            until,
        })
    }

    fn covers(&self, review: &CountedReview) -> bool {
        review.attestor_id == self.attestor_id
            && self.from.is_none_or(|from| review.iat >= from)
            && self.until.is_none_or(|until| review.iat < until)
    }
}

fn parse_bound(bound: &str) -> Result<Option<i64>, String> {
    if bound.is_empty() {
        return Ok(None);
    }
    bound
        .parse::<i64>()
        .map(Some)
        .map_err(|_| format!("attestors window bound {bound:?} must be seconds since the epoch"))
}

/// The full reputation aggregate of one scope: a subject in one review role
/// (`Subject:{pubky}:{role}`) or a listing (`Listing:{owner}:{listing_id}`,
/// buyer reviews only).
///
/// Recomputed from the graph's `REVIEWED` edges on every review/response
/// event — the whole aggregate is reproducible from public records by any
/// third party (design §9); this index is a cache, not an authority.
#[derive(Serialize, Deserialize, ToSchema, Clone, Debug, PartialEq)]
pub struct ReputationSummary {
    /// Total indexed reviews in this scope, verified and unverified.
    pub count: u64,
    /// Reviews whose purchase attestation cryptographically verified.
    pub verified_count: u64,
    /// Mean of `ratings.overall` over all indexed reviews.
    pub avg: f64,
    /// Star histogram of `ratings.overall`: index 0 holds 1-star counts.
    pub histogram: [u64; 5],
    pub avg_item_accuracy: Option<f64>,
    pub avg_shipping: Option<f64>,
    pub avg_communication: Option<f64>,
    /// Reviews carrying a subject response record.
    pub response_count: u64,
    /// Reviews edited beyond the marketplace's 24h edit window.
    pub edited_late_count: u64,
    /// Verified reviews per attestor pubky. Verification proves the signer;
    /// whether a signer is a trusted attestor is the consumer's decision.
    pub attestors: BTreeMap<String, u64>,
    /// `created_at` of the most recent review in this scope.
    pub last_reviewed_at: Option<String>,
    /// Counted verified reviews per attestor pubky (see [`ReputationBucket`]).
    #[serde(default)]
    pub buckets: BTreeMap<String, ReputationBucket>,
    /// Reviews whose attestation did not verify; never bucketed.
    #[serde(default)]
    pub unverified_count: u64,
}

impl RedisOps for ReputationSummary {}

/// A scope's reputation as the `attestors=` parameter serves it: the stored
/// aggregate plus the exact figures of its reviews inside the given windows.
#[derive(Serialize, Deserialize, ToSchema, Clone, Debug, PartialEq)]
pub struct ScopedReputationSummary {
    #[serde(flatten)]
    pub summary: ReputationSummary,
    /// The counted reviews inside the `attestors=` windows, deduplicated
    /// across attestors (one buyer counts once). Present only with the
    /// parameter.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scoped: Option<ReputationBucket>,
}

pub const REPUTATION_SUBJECT_KEY: &str = "Subject";
pub const REPUTATION_LISTING_KEY: &str = "Listing";

impl ReputationSummary {
    pub fn snippet(&self) -> ReputationSnippet {
        ReputationSnippet {
            avg: self.avg,
            count: self.count,
            verified_count: self.verified_count,
            buckets: self.buckets.clone(),
            unverified_count: self.unverified_count,
        }
    }

    pub async fn get_by_subject(
        subject_id: &str,
        role: PubkyAppReviewRole,
    ) -> ModelResult<Option<Self>> {
        Ok(
            Self::try_from_index_json(&[REPUTATION_SUBJECT_KEY, subject_id, role.as_str()], None)
                .await?,
        )
    }

    pub async fn get_by_listing(
        listing_owner_id: &str,
        listing_id: &str,
    ) -> ModelResult<Option<Self>> {
        Ok(Self::try_from_index_json(
            &[REPUTATION_LISTING_KEY, listing_owner_id, listing_id],
            None,
        )
        .await?)
    }

    /// The exact figures of a subject's counted reviews inside `windows`,
    /// read from the graph.
    pub async fn scoped_by_subject(
        subject_id: &str,
        role: PubkyAppReviewRole,
        windows: &[AttestorWindow],
    ) -> ModelResult<ReputationBucket> {
        let reviews = counted_reviews(queries::get::subject_counting_rows(
            subject_id,
            role.as_str(),
        ))
        .await?;
        Ok(ReputationBucket::within(
            &reviews,
            windows,
            has_seller_score(role),
        ))
    }

    /// The exact figures of a listing's counted buyer reviews inside
    /// `windows`, read from the graph.
    pub async fn scoped_by_listing(
        listing_owner_id: &str,
        listing_id: &str,
        windows: &[AttestorWindow],
    ) -> ModelResult<ReputationBucket> {
        let reviews = counted_reviews(queries::get::listing_counting_rows(
            listing_owner_id,
            listing_id,
        ))
        .await?;
        Ok(ReputationBucket::within(&reviews, windows, false))
    }

    /// Recomputes the subject-scoped aggregate from the graph and stores it,
    /// removing the key when the last review of the scope is gone.
    pub async fn recompute_subject(
        subject_id: &str,
        role: PubkyAppReviewRole,
    ) -> ModelResult<Option<Self>> {
        let query = queries::get::subject_reputation(subject_id, role.as_str());
        let rows = queries::get::subject_counting_rows(subject_id, role.as_str());
        Self::recompute(
            query,
            rows,
            has_seller_score(role),
            &[REPUTATION_SUBJECT_KEY, subject_id, role.as_str()],
        )
        .await
    }

    /// Recomputes the listing-scoped aggregate (buyer reviews only) from the
    /// graph and stores it, removing the key when empty.
    pub async fn recompute_listing(
        listing_owner_id: &str,
        listing_id: &str,
    ) -> ModelResult<Option<Self>> {
        let query = queries::get::listing_reputation(listing_owner_id, listing_id);
        let rows = queries::get::listing_counting_rows(listing_owner_id, listing_id);
        Self::recompute(
            query,
            rows,
            false,
            &[REPUTATION_LISTING_KEY, listing_owner_id, listing_id],
        )
        .await
    }

    async fn recompute(
        query: crate::db::graph::Query,
        counting_rows: crate::db::graph::Query,
        seller_score: bool,
        key_parts: &[&str],
    ) -> ModelResult<Option<Self>> {
        let Some(row) = fetch_row_from_graph(query).await? else {
            Self::remove_from_index_multiple_json(&[key_parts]).await?;
            return Ok(None);
        };

        let count: i64 = row.get("total")?;
        if count == 0 {
            Self::remove_from_index_multiple_json(&[key_parts]).await?;
            return Ok(None);
        }

        let verified_count: i64 = row.get("verified_count")?;
        let avg: f64 = row.get::<Option<f64>>("average")?.unwrap_or(0.0);
        let histogram = [
            row.get::<i64>("stars_1")? as u64,
            row.get::<i64>("stars_2")? as u64,
            row.get::<i64>("stars_3")? as u64,
            row.get::<i64>("stars_4")? as u64,
            row.get::<i64>("stars_5")? as u64,
        ];
        let attestor_list: Vec<String> = row.get("attestors")?;
        let mut attestors: BTreeMap<String, u64> = BTreeMap::new();
        for attestor in attestor_list {
            *attestors.entry(attestor).or_default() += 1;
        }
        let counted = counted_reviews(counting_rows).await?;

        let summary = ReputationSummary {
            count: count as u64,
            verified_count: verified_count as u64,
            avg,
            histogram,
            avg_item_accuracy: row.get("avg_item_accuracy")?,
            avg_shipping: row.get("avg_shipping")?,
            avg_communication: row.get("avg_communication")?,
            response_count: row.get::<i64>("response_count")? as u64,
            edited_late_count: row.get::<i64>("edited_late_count")? as u64,
            attestors,
            last_reviewed_at: row.get("last_reviewed_at")?,
            buckets: ReputationBucket::per_attestor(&counted, seller_score),
            unverified_count: row.get::<i64>("unverified_count")? as u64,
        };

        summary.put_index_json(key_parts, None, None).await?;
        Ok(Some(summary))
    }

    /// Batch-fetches subject-scoped snippets (buyer-reviewing-seller role)
    /// for stream hydration. Returns entries aligned with the input order;
    /// scopes without an aggregate yield `None` (honest absence, never a
    /// fabricated 0.0).
    pub async fn snippets_by_subjects(
        subject_ids: &[&str],
    ) -> ModelResult<Vec<Option<ReputationSnippet>>> {
        let role = PubkyAppReviewRole::BuyerReviewingSeller.as_str();
        let key_slices: Vec<Vec<&str>> = subject_ids
            .iter()
            .map(|subject| vec![REPUTATION_SUBJECT_KEY, *subject, role])
            .collect();
        let key_refs: Vec<&[&str]> = key_slices.iter().map(|parts| &parts[..]).collect();
        let summaries = Self::try_from_index_multiple_json(&key_refs).await?;
        Ok(summaries
            .into_iter()
            .map(|summary| summary.map(|summary| summary.snippet()))
            .collect())
    }

    /// Batch-fetches listing-scoped snippets for stream hydration.
    pub async fn snippets_by_listings(
        listing_keys: &[(&str, &str)],
    ) -> ModelResult<Vec<Option<ReputationSnippet>>> {
        let key_slices: Vec<Vec<&str>> = listing_keys
            .iter()
            .map(|(owner, listing)| vec![REPUTATION_LISTING_KEY, *owner, *listing])
            .collect();
        let key_refs: Vec<&[&str]> = key_slices.iter().map(|parts| &parts[..]).collect();
        let summaries = Self::try_from_index_multiple_json(&key_refs).await?;
        Ok(summaries
            .into_iter()
            .map(|summary| summary.map(|summary| summary.snippet()))
            .collect())
    }
}

/// The seller rating (RV2) rates sellers, so only the subject scope of the
/// buyer-reviewing-seller role carries it.
fn has_seller_score(role: PubkyAppReviewRole) -> bool {
    role == PubkyAppReviewRole::BuyerReviewingSeller
}

async fn counted_reviews(query: crate::db::graph::Query) -> ModelResult<Vec<CountedReview>> {
    let rows = fetch_all_rows_from_graph(query).await?;
    let mut reviews = Vec::with_capacity(rows.len());
    for row in rows {
        let price_weight: i64 = row.get("price_weight")?;
        reviews.push(CountedReview {
            reviewer_id: row.get("reviewer_id")?,
            attestor_id: row.get("attestor_id")?,
            order_ref: row.get("order_ref")?,
            rating_overall: row.get("rating_overall")?,
            price_weight: u8::try_from(price_weight).unwrap_or(1),
            iat: row.get("iat")?,
            seller_fields_counted: row.get("seller_fields_counted")?,
            rating_shipping: row.get("rating_shipping")?,
            rating_communication: row.get("rating_communication")?,
        });
    }
    Ok(reviews)
}

#[cfg(test)]
mod tests {
    use super::*;

    const ATTESTOR_A: &str = "8um71us3fyw6h8wbcxb5ar3rwusy1a6u49956ikzojg3gcwd1dty";
    const ATTESTOR_B: &str = "uo7jgkykft4885n8cruizwy6khw71mnu5pq3ay9i8pw1ymcn85ko";

    #[allow(clippy::too_many_arguments)]
    fn review(
        reviewer: &str,
        attestor: &str,
        order: &str,
        overall: i64,
        weight: u8,
        iat: i64,
        seller: (Option<i64>, Option<i64>),
    ) -> CountedReview {
        CountedReview {
            reviewer_id: reviewer.to_string(),
            attestor_id: attestor.to_string(),
            order_ref: order.to_string(),
            rating_overall: overall,
            price_weight: weight,
            iat,
            seller_fields_counted: true,
            rating_shipping: seller.0,
            rating_communication: seller.1,
        }
    }

    #[test]
    fn one_buyer_with_many_orders_is_one_voice() {
        let reviews = vec![
            review("b1", ATTESTOR_A, "o1", 5, 1, 100, (None, None)),
            review("b1", ATTESTOR_A, "o2", 5, 3, 200, (None, None)),
            review("b1", ATTESTOR_A, "o3", 2, 1, 300, (None, None)),
            review("b2", ATTESTOR_A, "o4", 1, 2, 400, (None, None)),
        ];
        let bucket = ReputationBucket::compute(&reviews, false);
        assert_eq!(bucket.orders, 4);
        assert_eq!(bucket.buyers, 2);
        assert_eq!(bucket.sum_buyer_means, 4.0 + 1.0);
        assert_eq!(bucket.histogram, [1, 1, 0, 0, 2]);
        // b1: m' = (5 + 15 + 2) / 5 = 4.4 at W = 3; b2: 1 at W = 2.
        assert!((bucket.weighted_sum - (3.0 * 4.4 + 2.0)).abs() < 1e-9);
        assert_eq!(bucket.weight_total, 5);
        assert_eq!((bucket.first_iat, bucket.last_iat), (Some(100), Some(400)));
        assert_eq!(bucket.seller_buyers, None);
    }

    #[test]
    fn buckets_split_by_attestor_and_windows_dedupe_buyers() {
        let reviews = vec![
            review("b1", ATTESTOR_A, "o1", 4, 1, 100, (None, None)),
            review("b1", ATTESTOR_B, "o2", 2, 1, 200, (None, None)),
            review("b2", ATTESTOR_B, "o3", 5, 1, 300, (None, None)),
        ];
        let buckets = ReputationBucket::per_attestor(&reviews, false);
        assert_eq!(buckets.len(), 2);
        assert_eq!(buckets[ATTESTOR_A].buyers, 1);
        assert_eq!(buckets[ATTESTOR_B].buyers, 2);

        let both = [
            AttestorWindow {
                attestor_id: ATTESTOR_A.to_string(),
                from: None,
                until: None,
            },
            AttestorWindow {
                attestor_id: ATTESTOR_B.to_string(),
                from: None,
                until: None,
            },
        ];
        let scoped = ReputationBucket::within(&reviews, &both, false);
        assert_eq!(scoped.buyers, 2, "b1 counts once across attestors");
        assert_eq!(scoped.sum_buyer_means, 3.0 + 5.0);
        assert_eq!(scoped.orders, 3);
    }

    #[test]
    fn a_window_counts_only_the_reviews_it_covers() {
        let reviews = vec![
            review("b1", ATTESTOR_A, "o1", 5, 1, 100, (None, None)),
            review("b2", ATTESTOR_A, "o2", 3, 1, 200, (None, None)),
            review("b3", ATTESTOR_A, "o3", 1, 1, 300, (None, None)),
        ];
        let windows = AttestorWindow::parse_list(&format!("{ATTESTOR_A}:200:300")).unwrap();
        let scoped = ReputationBucket::within(&reviews, &windows, false);
        assert_eq!(scoped.buyers, 1);
        assert_eq!(scoped.histogram, [0, 0, 1, 0, 0]);
        assert_eq!((scoped.first_iat, scoped.last_iat), (Some(200), Some(200)));

        let open_end = AttestorWindow::parse_list(&format!("{ATTESTOR_A}:200:")).unwrap();
        assert_eq!(
            ReputationBucket::within(&reviews, &open_end, false).buyers,
            2
        );
        let open_start = AttestorWindow::parse_list(&format!("{ATTESTOR_A}::200")).unwrap();
        assert_eq!(
            ReputationBucket::within(&reviews, &open_start, false).buyers,
            1
        );

        // Two epochs of one key count as a union.
        let epochs =
            AttestorWindow::parse_list(&format!("{ATTESTOR_A}::150,{ATTESTOR_A}:250:")).unwrap();
        let scoped = ReputationBucket::within(&reviews, &epochs, false);
        assert_eq!(scoped.buyers, 2);
        assert_eq!(scoped.sum_buyer_means, 5.0 + 1.0);

        let none = ReputationBucket::within(&reviews, &[], false);
        assert_eq!(none, ReputationBucket::default());
    }

    #[test]
    fn seller_score_uses_the_present_field_and_skips_orders_with_neither() {
        let mut reviews = vec![
            // b1: order o1 rated 5 and 3 -> 4; order o2 shipping only -> 2.
            review("b1", ATTESTOR_A, "o1", 5, 1, 100, (Some(5), Some(3))),
            review("b1", ATTESTOR_A, "o2", 4, 1, 200, (Some(2), None)),
            // b2: communication only.
            review("b2", ATTESTOR_A, "o3", 4, 1, 300, (None, Some(5))),
            // b3: a legacy review with neither field.
            review("b3", ATTESTOR_A, "o4", 3, 1, 400, (None, None)),
        ];
        // o1's second item review: its copy of the seller rating does not count.
        let mut copy = review("b1", ATTESTOR_A, "o1", 1, 1, 100, (Some(1), Some(1)));
        copy.seller_fields_counted = false;
        reviews.push(copy);

        let bucket = ReputationBucket::compute(&reviews, true);
        assert_eq!(bucket.buyers, 3);
        assert_eq!(bucket.seller_buyers, Some(2));
        assert_eq!(bucket.sum_seller_buyer_means, Some(3.0 + 5.0));

        let legacy_only = ReputationBucket::compute(&reviews[3..4], true);
        assert_eq!(legacy_only.buyers, 1);
        assert_eq!(legacy_only.seller_buyers, Some(0));
        assert_eq!(legacy_only.sum_seller_buyer_means, Some(0.0));
    }

    #[test]
    fn attestor_parameter_parsing() {
        let windows = AttestorWindow::parse_list(&format!(
            "{ATTESTOR_A}, {ATTESTOR_B}:1790000000:1800000000"
        ))
        .unwrap();
        assert_eq!(
            windows,
            vec![
                AttestorWindow {
                    attestor_id: ATTESTOR_A.to_string(),
                    from: None,
                    until: None,
                },
                AttestorWindow {
                    attestor_id: ATTESTOR_B.to_string(),
                    from: Some(1_790_000_000),
                    until: Some(1_800_000_000),
                },
            ]
        );
        for bad in [
            String::new(),
            ",".to_string(),
            "not-a-pubky".to_string(),
            format!("{ATTESTOR_A}:1"),
            format!("{ATTESTOR_A}:x:"),
            format!("{ATTESTOR_A}:200:100"),
            format!("{ATTESTOR_A}:200:200"),
            vec![ATTESTOR_A; MAX_ATTESTOR_WINDOWS + 1].join(","),
        ] {
            assert!(AttestorWindow::parse_list(&bad).is_err(), "{bad:?}");
        }
    }
}
