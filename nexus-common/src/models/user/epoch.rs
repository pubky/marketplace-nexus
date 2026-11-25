use crate::db::{fetch_key_from_graph, fetch_row_from_graph, queries, GraphError, GraphResult};
use serde::{Deserialize, Serialize};
use std::fmt;

/// The spec epoch a user's social data is read through: `pubky.app` (`v0`) or `social/v1`
/// (`v1`). Every user starts at `v0`, the absence of the `epoch` property included, and moves
/// to `v1` once, when the watcher reads their `/pub/social/v1/profile.json` (the flip).
/// Marketplace records under `/pub/pubky.app/marketplace/` are not social data and are read in
/// every epoch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Epoch {
    V0,
    V1,
}

impl Epoch {
    pub const fn as_str(self) -> &'static str {
        match self {
            Epoch::V0 => "v0",
            Epoch::V1 => "v1",
        }
    }

    /// The epoch a stored `epoch` property names; `None` for a value no build writes.
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "v0" => Some(Epoch::V0),
            "v1" => Some(Epoch::V1),
            _ => None,
        }
    }

    /// The epoch of `user_id` in the graph: `v0` for a user who is not in the graph or has no
    /// epoch stored. A stored value no build writes is an error, never read as either epoch.
    pub async fn get_from_graph(user_id: &str) -> GraphResult<Epoch> {
        Ok(Self::get_from_graph_if_present(user_id)
            .await?
            .unwrap_or(Epoch::V0))
    }

    /// The epoch of `user_id` in the graph, `None` when the user is not in the graph. A node
    /// with no epoch stored is at `v0`.
    pub async fn get_from_graph_if_present(user_id: &str) -> GraphResult<Option<Epoch>> {
        let Some(row) = fetch_row_from_graph(queries::social_v1::get_user_epoch(user_id)).await?
        else {
            return Ok(None);
        };
        match row.get::<Option<String>>("epoch")? {
            None => Ok(Some(Epoch::V0)),
            Some(value) => Self::parse(&value).map(Some).ok_or_else(|| {
                GraphError::Generic(format!("User {user_id} has an unknown epoch {value:?}"))
            }),
        }
    }

    /// Moves `user_id` to `v1` in the graph. The user's node must exist: the flip writes the
    /// profile first, so a missing node is an error.
    pub async fn set_v1_in_graph(user_id: &str) -> GraphResult<()> {
        let changed: Option<i64> = fetch_key_from_graph(
            queries::social_v1::set_user_epoch(user_id, Epoch::V1.as_str()),
            "changed",
        )
        .await?;
        if changed.unwrap_or(0) == 0 {
            return Err(GraphError::Generic(format!(
                "User {user_id} is not in the graph; its epoch cannot move to v1"
            )));
        }
        Ok(())
    }
}

impl fmt::Display for Epoch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::Epoch;

    #[test]
    fn stored_values_round_trip_and_unknown_ones_are_refused() {
        for epoch in [Epoch::V0, Epoch::V1] {
            assert_eq!(Epoch::parse(epoch.as_str()), Some(epoch));
        }
        for value in ["", "V1", "v2", "1"] {
            assert_eq!(Epoch::parse(value), None, "{value:?}");
        }
    }
}
