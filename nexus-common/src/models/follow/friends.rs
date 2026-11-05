use crate::db::RedisOps;
use crate::models::error::ModelResult;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use utoipa::ToSchema;

use super::followers::Followers;
use super::following::Following;
use super::traits::UserFollows;

/// The ids in `following` that are also in `followers`, in `following`'s order.
fn mutual_ids(following: Vec<String>, followers: Vec<String>) -> Vec<String> {
    let followers: HashSet<String> = followers.into_iter().collect();
    following
        .into_iter()
        .filter(|user_id| followers.contains(user_id))
        .collect()
}

#[derive(Serialize, Deserialize, ToSchema, Default)]
pub struct Friends(pub Vec<String>);

impl Friends {
    pub async fn get_by_id(
        user_id: &str,
        skip: Option<usize>,
        limit: Option<usize>,
    ) -> ModelResult<Option<Self>> {
        // Fetch following and followers, limit to 10K
        let following = Following::get_by_id(user_id, None, Some(10000))
            .await?
            .unwrap_or_default()
            .0;

        let followers = Followers::get_by_id(user_id, None, Some(10000))
            .await?
            .unwrap_or_default()
            .0;

        // Find intersection of following and followers (mutual friends)
        let mut friends = mutual_ids(following, followers);

        if friends.is_empty() {
            return Ok(None);
        }

        if let Some(skip) = skip {
            friends = friends.into_iter().skip(skip).collect();
        }
        if let Some(limit) = limit {
            friends.truncate(limit);
        }

        Ok(Some(Self(friends)))
    }

    // Checks whether user_a and user_b are friends
    pub async fn check(user_a_id: &str, user_b_id: &str) -> ModelResult<bool> {
        let user_a_key_parts = &[user_a_id][..];
        let user_b_key_parts = &[user_b_id][..];

        let ((_, a_follows_b), (_, b_follows_a)) = tokio::try_join!(
            Following::check_set_member(user_a_key_parts, user_b_id),
            Following::check_set_member(user_b_key_parts, user_a_id),
        )?;

        Ok(a_follows_b && b_follows_a)
    }
}

#[cfg(test)]
mod tests {
    use super::mutual_ids;

    fn ids(range: std::ops::Range<usize>) -> Vec<String> {
        range.map(|i| format!("user{i}")).collect()
    }

    #[test]
    fn mutual_ids_keep_the_order_of_the_following_list() {
        let following = vec!["c".to_string(), "a".to_string(), "b".to_string()];
        let followers = vec!["b".to_string(), "c".to_string(), "z".to_string()];
        assert_eq!(mutual_ids(following, followers), ["c", "b"]);
    }

    #[test]
    fn no_overlap_yields_no_friends() {
        assert!(mutual_ids(ids(0..10), ids(10..20)).is_empty());
    }

    #[test]
    fn two_full_ten_thousand_entry_lists_intersect_without_a_quadratic_scan() {
        let started = std::time::Instant::now();
        let friends = mutual_ids(ids(0..10_000), ids(5_000..15_000));
        assert_eq!(friends.len(), 5_000);
        assert_eq!(friends[0], "user5000");
        assert!(started.elapsed() < std::time::Duration::from_secs(2));
    }
}
