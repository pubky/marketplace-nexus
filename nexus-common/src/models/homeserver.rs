use crate::db::exec_single_row;
use crate::db::fetch_key_from_graph;
use crate::db::kv::RedisError;
use crate::db::kv::RedisResult;
use crate::db::queries;
use crate::db::GraphError;
use crate::db::GraphResult;
use crate::db::{PubkyConnector, RedisOps};
use crate::models::error::ModelError;
use crate::models::error::ModelResult;
use crate::models::user::UserDetails;

use pubky::PublicKey;
use pubky_app_specs::ParsedUri;
use pubky_app_specs::PubkyId;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::fmt;
use std::sync::OnceLock;
use tracing::{info, warn};

static ALLOWED_HOMESERVERS: OnceLock<HomeserverAllowlist> = OnceLock::new();

/// The homeservers this process may watch ([crate::StackConfig::allowed_homeservers]).
/// Empty allows every homeserver.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct HomeserverAllowlist(BTreeSet<String>);

impl HomeserverAllowlist {
    pub fn new<I, S>(ids: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        Self(ids.into_iter().map(|id| id.as_ref().to_string()).collect())
    }

    pub fn is_restricted(&self) -> bool {
        !self.0.is_empty()
    }

    pub fn allows(&self, homeserver_id: &str) -> bool {
        self.0.is_empty() || self.0.contains(homeserver_id)
    }

    /// Makes this the process-wide allowlist. The first install wins: the API and the watcher of
    /// one `nexusd` share a [crate::StackConfig], so a second install with other contents is a
    /// misconfiguration and is reported, not applied.
    pub fn install(self) {
        if let Err(rejected) = ALLOWED_HOMESERVERS.set(self) {
            if Some(&rejected) != ALLOWED_HOMESERVERS.get() {
                warn!("Ignoring a second, different homeserver allowlist: {rejected}");
            }
        }
    }

    /// The installed allowlist, or the empty (allow-all) one before [Self::install].
    pub fn current() -> &'static HomeserverAllowlist {
        static ALLOW_ALL: OnceLock<HomeserverAllowlist> = OnceLock::new();
        ALLOWED_HOMESERVERS
            .get()
            .unwrap_or_else(|| ALLOW_ALL.get_or_init(HomeserverAllowlist::default))
    }
}

impl fmt::Display for HomeserverAllowlist {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.0.is_empty() {
            return f.write_str("all");
        }
        let ids: Vec<&str> = self.0.iter().map(String::as_str).collect();
        f.write_str(&ids.join(", "))
    }
}

/// Represents a homeserver with its public key, URL, and cursor.
#[derive(Serialize, Deserialize, Debug)]
pub struct Homeserver {
    pub id: PubkyId,

    // We persist this field only in the cache, but not in the graph.
    // Redis has regular snapshots, which ensures we get a recent state in case of RAM data loss (system crash).
    pub cursor: String,
}

impl RedisOps for Homeserver {}

impl Homeserver {
    /// Instantiates a new homeserver with default cursor
    pub fn new(id: PubkyId) -> Self {
        Homeserver {
            id,
            cursor: "0000000000000".to_string(),
        }
    }

    /// Creates a new homeserver instance with the specified cursor
    pub fn try_from_cursor<T: Into<String>>(id: PubkyId, cursor: T) -> ModelResult<Self> {
        let cursor = cursor.into();
        if cursor.is_empty() {
            return Err(ModelError::from_generic(
                "Cannot create a homeserver from an empty cursor",
            ));
        }

        Ok(Homeserver { id, cursor })
    }

    /// Stores this homeserver in the graph.
    pub async fn put_to_graph(&self) -> GraphResult<()> {
        let query = queries::put::create_homeserver(&self.id);
        exec_single_row(query).await
    }

    /// Retrieves a homeserver from Neo4j.
    ///
    /// Note that the cursor in the returned homeserver will have the default value, as it is not persisted in the graph.
    pub async fn get_from_graph(id: &str) -> GraphResult<Option<Homeserver>> {
        let query = queries::get::get_homeserver_by_id(id);

        let maybe_id = fetch_key_from_graph(query, "id").await?;
        let maybe_hs = maybe_id.map(Homeserver::new);

        Ok(maybe_hs)
    }

    /// Retrieves the homeserver from Redis.
    pub async fn get_from_index(id: &str) -> RedisResult<Option<Self>> {
        Self::try_from_index_json(&[id], None).await
    }

    /// Stores this homeserver in Redis.
    pub async fn put_to_index(&self) -> RedisResult<()> {
        if self.cursor.is_empty() {
            return Err(RedisError::InvalidInput(
                "Cannot save to index a homeserver with an empty cursor".into(),
            ));
        }
        self.put_index_json(&[&self.id], None, None).await
    }

    pub async fn get_by_id(homeserver_id: PubkyId) -> ModelResult<Option<Homeserver>> {
        match Homeserver::get_from_index(&homeserver_id).await? {
            Some(hs) => Ok(Some(hs)),
            None => match Self::get_from_graph(&homeserver_id).await? {
                Some(hs_from_graph) => {
                    hs_from_graph.put_to_index().await?;
                    Ok(Some(hs_from_graph))
                }
                None => Ok(None),
            },
        }
    }

    /// Verifies if homeserver exists in the graph, or persists it if missing and allowed by the
    /// installed [HomeserverAllowlist]. Every new homeserver enters through here (the configured
    /// default at watcher start, and discovery from events and `/v0/bootstrap`).
    pub async fn persist_if_unknown(homeserver_id: PubkyId) -> ModelResult<()> {
        Self::persist_if_unknown_in(homeserver_id, HomeserverAllowlist::current())
            .await
            .map(|_| ())
    }

    /// [Self::persist_if_unknown] against an explicit allowlist. Returns whether the homeserver is
    /// now known: `false` when the allowlist refused a homeserver the graph did not have.
    pub async fn persist_if_unknown_in(
        homeserver_id: PubkyId,
        allowlist: &HomeserverAllowlist,
    ) -> ModelResult<bool> {
        if Self::get_from_graph(&homeserver_id).await?.is_some() {
            return Ok(true);
        }
        if !allowlist.allows(homeserver_id.as_ref()) {
            info!("Not watching homeserver {homeserver_id}: not in the allowed homeservers");
            return Ok(false);
        }
        info!("Persisting new homeserver: {homeserver_id}");
        let homeserver = Homeserver::new(homeserver_id);
        homeserver.put_to_graph().await?;
        homeserver.put_to_index().await?;
        Ok(true)
    }

    /// Retrieves all homeservers from the graph.
    ///
    /// # Returns
    /// A list of all known homeserver IDs.
    ///
    /// # Errors
    /// Throws an error if no homeservers are found.
    pub async fn get_all_from_graph() -> GraphResult<Vec<String>> {
        let query = queries::get::get_all_homeservers();
        let maybe_hs_ids = fetch_key_from_graph(query, "homeservers_list").await?;
        let hs_ids: Vec<String> = maybe_hs_ids.unwrap_or_default();

        match hs_ids.is_empty() {
            true => Err(GraphError::Generic("No homeservers found in graph".into())),
            false => Ok(hs_ids),
        }
    }

    /// If a referenced post is hosted on a new, unknown homeserver, this method triggers ingestion of that homeserver.
    ///
    /// ### Arguments
    ///
    /// - `referenced_post_uri`: The parent post (if current post is a reply to it), or a reposted post (if current post is a Repost)
    pub async fn maybe_ingest_for_post(referenced_post_uri: &ParsedUri) -> ModelResult<()> {
        let ref_post_author_id = referenced_post_uri.user_id.as_ref();

        Self::maybe_ingest_for_user(ref_post_author_id).await
    }

    /// If a referenced user is using a new, unknown homeserver, this method triggers ingestion of that homeserver.
    ///
    /// ### Arguments
    ///
    /// - `referenced_user_id`: The URI of the referenced user
    pub async fn maybe_ingest_for_user(referenced_user_id: &str) -> ModelResult<()> {
        let pubky = PubkyConnector::get().map_err(ModelError::from_generic)?;

        if UserDetails::get_by_id(referenced_user_id).await?.is_some() {
            tracing::debug!(
                "Skipping homeserver ingestion: author {referenced_user_id} already known"
            );
            return Ok(());
        }

        let ref_post_author_pk = referenced_user_id
            .parse::<PublicKey>()
            .map_err(ModelError::from_generic)?;
        let Some(ref_post_author_hs) = pubky.get_homeserver_of(&ref_post_author_pk).await else {
            tracing::warn!("Skipping homeserver ingestion: author {ref_post_author_pk} has no published homeserver");
            return Ok(());
        };

        // The specs crate's `From<pubky::PublicKey>` impl targets a newer
        // pubky crate than the one this workspace pins; convert through the
        // bare z-base-32 form, which is version-independent. (`to_string()`
        // would render the `pubky`-prefixed form, which PubkyId rejects.)
        let hs_pk = PubkyId::try_from(ref_post_author_hs.z32().as_str())
            .map_err(ModelError::from_generic)?;
        Self::persist_if_unknown_in(hs_pk.clone(), HomeserverAllowlist::current())
            .await
            .inspect(|known| {
                if *known {
                    tracing::info!("Ingested homeserver {hs_pk}")
                }
            })
            .inspect_err(|e| tracing::error!("Failed to ingest homeserver {hs_pk}: {e}"))
            .map(|_| ())
    }
}

#[cfg(test)]
mod tests {
    use pubky::Keypair;
    use pubky_app_specs::PubkyId;

    use crate::{types::DynError, StackConfig, StackManager};

    use super::*;

    #[tokio_shared_rt::test(shared)]
    async fn test_put_to_get_from_graph() -> Result<(), DynError> {
        StackManager::setup("unit-hs-test", &StackConfig::default()).await?;

        let keys = Keypair::random();
        let id = PubkyId::try_from(&keys.public_key().to_z32())?;

        let hs = Homeserver::new(id.clone());
        hs.put_to_graph()
            .await
            .expect("Failed to put homeserver to graph");

        let hs_from_graph = Homeserver::get_from_graph(&id)
            .await
            .ok()
            .flatten()
            .expect("Failed to get homeserver from graph");

        assert_eq!(id, hs_from_graph.id);

        Ok(())
    }

    #[tokio_shared_rt::test(shared)]
    async fn test_put_to_get_from_index() -> Result<(), DynError> {
        StackManager::setup("unit-hs-test", &StackConfig::default()).await?;

        let keys = Keypair::random();
        let id = PubkyId::try_from(&keys.public_key().to_z32())?;

        let hs = Homeserver::new(id.clone());
        hs.put_to_index()
            .await
            .expect("Failed to put homeserver to index");

        let hs_from_index = Homeserver::get_from_index(&id)
            .await
            .ok()
            .flatten()
            .expect("Failed to get homeserver from index");

        assert_eq!(id, hs_from_index.id);

        Ok(())
    }

    fn random_id() -> PubkyId {
        PubkyId::try_from(&Keypair::random().public_key().to_z32())
            .expect("random key is a pubky id")
    }

    #[test]
    fn empty_allowlist_allows_every_homeserver() {
        let allowlist = HomeserverAllowlist::default();
        assert!(!allowlist.is_restricted());
        assert!(allowlist.allows(random_id().as_ref()));
        assert_eq!(allowlist.to_string(), "all");
    }

    #[test]
    fn restricted_allowlist_allows_only_listed_homeservers() {
        let (listed, other) = (random_id(), random_id());
        let allowlist = HomeserverAllowlist::new([&listed]);
        assert!(allowlist.is_restricted());
        assert!(allowlist.allows(listed.as_ref()));
        assert!(!allowlist.allows(other.as_ref()));
        assert_eq!(allowlist.to_string(), listed.to_string());
    }

    #[tokio_shared_rt::test(shared)]
    async fn restricted_allowlist_refuses_an_unknown_homeserver() -> Result<(), DynError> {
        StackManager::setup("unit-hs-test", &StackConfig::default()).await?;
        let (listed, other) = (random_id(), random_id());
        let allowlist = HomeserverAllowlist::new([&listed]);

        assert!(!Homeserver::persist_if_unknown_in(other.clone(), &allowlist).await?);
        assert!(Homeserver::get_from_graph(&other).await?.is_none());
        assert!(Homeserver::get_from_index(&other).await?.is_none());

        assert!(Homeserver::persist_if_unknown_in(listed.clone(), &allowlist).await?);
        assert!(Homeserver::get_from_graph(&listed).await?.is_some());
        assert!(Homeserver::get_from_index(&listed).await?.is_some());
        Ok(())
    }

    #[tokio_shared_rt::test(shared)]
    async fn allowlist_keeps_a_homeserver_the_graph_already_has() -> Result<(), DynError> {
        StackManager::setup("unit-hs-test", &StackConfig::default()).await?;
        let known = random_id();
        Homeserver::new(known.clone()).put_to_graph().await?;

        let allowlist = HomeserverAllowlist::new([random_id()]);
        assert!(Homeserver::persist_if_unknown_in(known.clone(), &allowlist).await?);
        assert!(Homeserver::get_from_graph(&known).await?.is_some());
        Ok(())
    }

    #[tokio_shared_rt::test(shared)]
    async fn empty_allowlist_persists_an_unknown_homeserver() -> Result<(), DynError> {
        StackManager::setup("unit-hs-test", &StackConfig::default()).await?;
        let id = random_id();
        assert!(
            Homeserver::persist_if_unknown_in(id.clone(), &HomeserverAllowlist::default()).await?
        );
        assert!(Homeserver::get_from_graph(&id).await?.is_some());
        Ok(())
    }
}
