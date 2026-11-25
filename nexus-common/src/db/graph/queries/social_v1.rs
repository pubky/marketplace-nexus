//! Queries of the two-epoch Social v1 indexing: the spec epoch a user is read through, and the
//! rewrite, at a user's flip from `pubky.app` to `social/v1`, of the graph records whose id
//! hashes an address the migrator rewrote.

use crate::db::graph::Query;

/// The epoch stored on `user_id`'s node: one row with `epoch` (null when the node has none),
/// or no row when the user is not in the graph.
pub fn get_user_epoch(user_id: &str) -> Query {
    Query::new(
        "get_user_epoch",
        "MATCH (u:User {id: $user_id})
         RETURN u.epoch AS epoch",
    )
    .param("user_id", user_id)
}

/// Stores `epoch` on `user_id`'s node and returns how many nodes it changed in `changed`.
pub fn set_user_epoch(user_id: &str, epoch: &str) -> Query {
    Query::new(
        "set_user_epoch",
        "MATCH (u:User {id: $user_id})
         SET u.epoch = $epoch
         RETURN count(u) AS changed",
    )
    .param("user_id", user_id)
    .param("epoch", epoch)
}

/// One keyset page of the `TAGGED` edges `tagger_id` wrote on users and posts, ordered by the
/// edge's element id, starting after `after` (an empty string starts at the first edge).
///
/// Each row has the edge's element id `edge`, its `id` and `label`, the target `kind` (`user` or
/// `post`), the target's `target_id`, and for a post its `author_id` (null when the post has no
/// `AUTHORED` author). Edges on listings and shops are not read: the migrator keeps their target
/// address, so their id is the same in both epochs.
pub fn tagger_social_edges_after(tagger_id: &str, after: &str, limit: usize) -> Query {
    Query::new(
        "tagger_social_edges_after",
        "MATCH (tagger:User {id: $tagger_id})-[t:TAGGED]->(target)
         WHERE (target:User OR target:Post) AND elementId(t) > $after
         WITH t, target
         ORDER BY elementId(t)
         LIMIT $limit
         OPTIONAL MATCH (author:User)-[:AUTHORED]->(target:Post)
         RETURN elementId(t) AS edge,
                t.id AS id,
                t.label AS label,
                CASE WHEN target:User THEN 'user' ELSE 'post' END AS kind,
                target.id AS target_id,
                author.id AS author_id
         ORDER BY edge",
    )
    .param("tagger_id", tagger_id)
    .param("after", after)
    .param("limit", limit as i64)
}

/// What a user's flip rewrites, as rows of strings for the one statement of [`flip_to_v1`].
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct FlipRewrite {
    /// `[edge element id, id the edge holds, new id]` for each of the user's `TAGGED` edges.
    pub edges: Vec<Vec<String>>,
    /// `[old tag id, new tag id, new uri, "move" or "drop"]` for each of the user's
    /// `ModeratedUser` markers, as the moderator.
    pub moderated_users: Vec<Vec<String>>,
    /// The same rows for the user's `ExcludedReviewer` markers.
    pub excluded_reviewers: Vec<Vec<String>>,
}

/// Rewrites `rewrite` and moves `user_id` to `v1`, in one statement and so in one transaction:
/// either every edge id, every marker and the epoch change, or none does. Returns `changed`,
/// 1 when the user's node was found, and no row when it was not.
///
/// An edge changes only while it still holds the id it was read with. A marker moves to its new
/// tag id, unless a marker is already recorded under that id or its row says `drop` (another
/// row moves a marker to the same id); then it is removed, so the `(moderator_id, tag_id)`
/// uniqueness holds and a rerun finds nothing left to move.
pub fn flip_to_v1(user_id: &str, rewrite: FlipRewrite) -> Query {
    let markers = |label: &str, rows: &str| {
        format!(
            "CALL () {{
                 UNWIND ${rows} AS row
                 MATCH (m:{label} {{moderator_id: $user_id, tag_id: row[0]}})
                 OPTIONAL MATCH (kept:{label} {{moderator_id: $user_id, tag_id: row[1]}})
                 WITH m, row, kept IS NOT NULL OR row[3] = 'drop' AS dropped
                 FOREACH (_ IN CASE WHEN dropped THEN [] ELSE [1] END |
                     SET m.tag_id = row[1], m.uri = row[2])
                 FOREACH (_ IN CASE WHEN dropped THEN [1] ELSE [] END |
                     DETACH DELETE m)
                 RETURN count(*) AS {rows}
             }}"
        )
    };
    Query::new(
        "flip_to_v1",
        format!(
            "MATCH (u:User {{id: $user_id}})
             CALL (u) {{
                 UNWIND $edges AS row
                 MATCH (u)-[t:TAGGED]->()
                 WHERE elementId(t) = row[0] AND t.id = row[1]
                 SET t.id = row[2]
                 RETURN count(t) AS edges
             }}
             {}
             {}
             SET u.epoch = 'v1'
             RETURN count(u) AS changed",
            markers("ModeratedUser", "moderated_users"),
            markers("ExcludedReviewer", "excluded_reviewers"),
        ),
    )
    .param("user_id", user_id)
    .param("edges", rewrite.edges)
    .param("moderated_users", rewrite.moderated_users)
    .param("excluded_reviewers", rewrite.excluded_reviewers)
}

/// Every `ModeratedUser` and `ExcludedReviewer` marker written by `moderator_id`'s tags, with
/// its node `kind`, `tag_id`, the tagged `uri` and the tag's `label`.
pub fn moderator_user_markers(moderator_id: &str) -> Query {
    Query::new(
        "moderator_user_markers",
        "MATCH (m:ModeratedUser {moderator_id: $moderator_id})
         RETURN 'ModeratedUser' AS kind, m.tag_id AS tag_id, m.uri AS uri, m.label AS label
         UNION ALL
         MATCH (m:ExcludedReviewer {moderator_id: $moderator_id})
         RETURN 'ExcludedReviewer' AS kind, m.tag_id AS tag_id, m.uri AS uri, m.label AS label",
    )
    .param("moderator_id", moderator_id)
}
