use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    db::{kv::RedisError, GraphError},
    models::error::ModelError,
};

#[derive(Error, Debug, Clone, Serialize, Deserialize)]
pub enum EventProcessorError {
    /// Failed to execute query in the graph database
    #[error("GraphQueryFailed: {0}")]
    GraphQueryFailed(String),

    /// The event could not be indexed due to missing graph dependencies
    #[error("MissingDependency: Could not be indexed")]
    MissingDependency { dependency: Vec<String> },

    /// Failed to complete indexing due to a Redis operation error
    #[error("IndexOperationFailed: Indexing incomplete due to Redis error - {0}")]
    IndexOperationFailed(String),

    /// The event appears to be unindexed. Verify the event in the retry queue
    #[error("SkipIndexing: The PUT event appears to be unindexed, so we cannot delete an object that doesn't exist")]
    SkipIndexing,

    /// The event could not be parsed from a line
    #[error("InvalidEventLine: {0}")]
    InvalidEventLine(String),

    /// The record's bytes fail the `social/v1` reader. The same bytes fail the same way, so the
    /// event is never retried.
    #[error("SpecValidation: {0}")]
    SpecValidation(String),

    /// An ordinary tag whose target is never indexed as a tag: a follow, mute, bookmark, feed,
    /// tag, last-read or private address, or a resource this indexer keeps no tags for. The
    /// configured moderator's tag on a tag address is not one: it deletes that tag. Never
    /// retried.
    #[error("UnsupportedTarget: {0}")]
    UnsupportedTarget(String),

    /// A `v0` user's flip did not land: they stay at `v0`. Their `social/v1` profile PUT waits
    /// in the watcher's retry queue, retried without limit while they are at `v0`, and their
    /// `social/v1` writes are deferred behind it.
    #[error("FlipFailed: {0}")]
    FlipFailed(Box<EventProcessorError>),

    /// A `v0` user's `social/v1` write, held until their flip, queued at `flip_uri`, lands.
    #[error("DeferredUntilFlip: waits for the flip at {flip_uri}")]
    DeferredUntilFlip { flip_uri: String },

    /// The Pubky client could not resolve the pubky
    #[error("PubkyClientError: {0}")]
    PubkyClientError(#[from] crate::db::PubkyClientError),

    #[error("MediaProcessor: {0}")]
    MediaProcessorError(String),

    #[error("Internal error: {0}")]
    InternalError(String),

    #[error("StaticSaveFailed: {0}")]
    StaticSaveFailed(String),

    /// Catch-all for miscellaneous errors in the processor layer
    #[error("Generic error: {0}")]
    Generic(String),
}

impl From<ModelError> for EventProcessorError {
    fn from(e: ModelError) -> Self {
        match e {
            ModelError::GraphOperationFailed(source) => {
                EventProcessorError::GraphQueryFailed(source.to_string())
            }
            ModelError::KvOperationFailed(source) => {
                EventProcessorError::IndexOperationFailed(source.to_string())
            }
            ModelError::MediaProcessorError(source) => {
                EventProcessorError::MediaProcessorError(source.to_string())
            }
            ModelError::FileOperationFailed(source) => {
                EventProcessorError::InternalError(source.to_string())
            }
            ModelError::Generic(message) => EventProcessorError::Generic(message),
        }
    }
}

impl From<pubky::Error> for EventProcessorError {
    fn from(e: pubky::Error) -> Self {
        EventProcessorError::client_error(e.to_string())
    }
}

impl From<std::io::Error> for EventProcessorError {
    fn from(e: std::io::Error) -> Self {
        EventProcessorError::InternalError(e.to_string())
    }
}

impl From<RedisError> for EventProcessorError {
    fn from(e: RedisError) -> Self {
        EventProcessorError::IndexOperationFailed(e.to_string())
    }
}

impl From<GraphError> for EventProcessorError {
    fn from(e: GraphError) -> Self {
        EventProcessorError::GraphQueryFailed(e.to_string())
    }
}

impl EventProcessorError {
    pub fn missing_dependencies(dependency_uris: Vec<String>) -> Self {
        Self::MissingDependency {
            dependency: dependency_uris,
        }
    }

    pub fn client_error(message: String) -> Self {
        Self::PubkyClientError(crate::db::PubkyClientError::ClientError(message))
    }

    pub fn index_operation_failed(source: impl std::fmt::Display) -> Self {
        Self::IndexOperationFailed(source.to_string())
    }

    pub fn static_save_failed(source: impl std::fmt::Display) -> Self {
        Self::StaticSaveFailed(source.to_string())
    }

    pub fn graph_query_failed(source: impl std::fmt::Display) -> Self {
        Self::GraphQueryFailed(source.to_string())
    }

    pub fn generic(source: impl std::fmt::Display) -> Self {
        Self::Generic(source.to_string())
    }

    /// Whether the same event fails the same way on every attempt: its line, its record's
    /// bytes or its tag target is refused. Such an event is never retried.
    pub fn is_permanent(&self) -> bool {
        matches!(
            self,
            Self::InvalidEventLine(_) | Self::SpecValidation(_) | Self::UnsupportedTarget(_)
        )
    }

    /// Whether Neo4j or Redis failed the operation, rather than the event itself.
    pub fn is_store_failure(&self) -> bool {
        matches!(
            self,
            Self::GraphQueryFailed(_) | Self::IndexOperationFailed(_)
        )
    }
}

#[cfg(test)]
mod tests {
    use super::EventProcessorError;

    #[test]
    fn only_refused_lines_records_and_targets_are_permanent() {
        let permanent = [
            EventProcessorError::InvalidEventLine("line".into()),
            EventProcessorError::SpecValidation("bytes".into()),
            EventProcessorError::UnsupportedTarget("target".into()),
        ];
        for error in &permanent {
            assert!(error.is_permanent(), "{error}");
        }
        let retried = [
            EventProcessorError::GraphQueryFailed("down".into()),
            EventProcessorError::IndexOperationFailed("down".into()),
            EventProcessorError::missing_dependencies(vec!["pubky://dep".into()]),
            EventProcessorError::SkipIndexing,
            EventProcessorError::client_error("unreachable".into()),
            EventProcessorError::FlipFailed(Box::new(EventProcessorError::SpecValidation(
                "bytes".into(),
            ))),
            EventProcessorError::DeferredUntilFlip {
                flip_uri: "pubky://user/pub/social/v1/profile.json".into(),
            },
        ];
        for error in &retried {
            assert!(!error.is_permanent(), "{error}");
        }
    }

    #[test]
    fn a_failed_flip_keeps_its_cause_through_serde() {
        let error = EventProcessorError::FlipFailed(Box::new(
            EventProcessorError::GraphQueryFailed("missing label".into()),
        ));
        let json = serde_json::to_string(&error).unwrap();
        let back: EventProcessorError = serde_json::from_str(&json).unwrap();
        assert_eq!(
            back.to_string(),
            "FlipFailed: GraphQueryFailed: missing label"
        );
    }

    #[test]
    fn only_neo4j_and_redis_failures_are_store_failures() {
        assert!(EventProcessorError::GraphQueryFailed("down".into()).is_store_failure());
        assert!(EventProcessorError::IndexOperationFailed("down".into()).is_store_failure());
        for error in [
            EventProcessorError::SkipIndexing,
            EventProcessorError::client_error("unreachable".into()),
            EventProcessorError::FlipFailed(Box::new(EventProcessorError::GraphQueryFailed(
                "down".into(),
            ))),
        ] {
            assert!(!error.is_store_failure(), "{error}");
        }
    }
}
