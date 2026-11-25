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

/// Sets the id of each of `tagger_id`'s `TAGGED` edges named in `rows`, each
/// `[edge element id, new id]`, and returns how many edges it changed in `changed`.
pub fn set_tagger_edge_ids(tagger_id: &str, rows: Vec<Vec<String>>) -> Query {
    Query::new(
        "set_tagger_edge_ids",
        "UNWIND $rows AS row
         MATCH (:User {id: $tagger_id})-[t:TAGGED]->()
         WHERE elementId(t) = row[0]
         SET t.id = row[1]
         RETURN count(t) AS changed",
    )
    .param("tagger_id", tagger_id)
    .param("rows", rows)
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

/// Moves each of `moderator_id`'s `ModeratedUser` markers named in `rows`, each
/// `[old tag id, new tag id, new uri]`, to the new tag id and uri. A marker already recorded
/// under the new id keeps its own state and the old one is removed, so a rerun never trips the
/// `(moderator_id, tag_id)` uniqueness.
pub fn rekey_moderated_user_markers(moderator_id: &str, rows: Vec<Vec<String>>) -> Query {
    rekey_markers(
        "rekey_moderated_user_markers",
        "ModeratedUser",
        moderator_id,
        rows,
    )
}

/// [`rekey_moderated_user_markers`] for `ExcludedReviewer` markers.
pub fn rekey_excluded_reviewer_markers(moderator_id: &str, rows: Vec<Vec<String>>) -> Query {
    rekey_markers(
        "rekey_excluded_reviewer_markers",
        "ExcludedReviewer",
        moderator_id,
        rows,
    )
}

fn rekey_markers(
    name: &'static str,
    label: &'static str,
    moderator_id: &str,
    rows: Vec<Vec<String>>,
) -> Query {
    Query::new(
        name,
        format!(
            "UNWIND $rows AS row
             MATCH (m:{label} {{moderator_id: $moderator_id, tag_id: row[0]}})
             OPTIONAL MATCH (kept:{label} {{moderator_id: $moderator_id, tag_id: row[1]}})
             FOREACH (_ IN CASE WHEN kept IS NULL THEN [1] ELSE [] END |
                 SET m.tag_id = row[1], m.uri = row[2])
             FOREACH (_ IN CASE WHEN kept IS NULL THEN [] ELSE [1] END |
                 DETACH DELETE m)"
        ),
    )
    .param("moderator_id", moderator_id)
    .param("rows", rows)
}
