use async_trait::async_trait;
use chrono::Utc;
use futures::TryStreamExt;
use nexus_common::{
    db::{get_neo4j_graph, graph::Query, GraphOps},
    types::DynError,
};
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::collections::HashSet;
use std::sync::Arc;
use tracing::info;

use crate::migrations::utils::{self, generate_template};

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MigrationPhase {
    DualWrite,
    Backfill,
    Cutover,
    Cleanup,
    Done,
}

impl MigrationPhase {
    fn next(&self) -> Option<MigrationPhase> {
        match self {
            MigrationPhase::DualWrite => Some(MigrationPhase::Backfill),
            MigrationPhase::Backfill => Some(MigrationPhase::Cutover),
            MigrationPhase::Cutover => Some(MigrationPhase::Cleanup),
            MigrationPhase::Cleanup => Some(MigrationPhase::Done),
            MigrationPhase::Done => None,
        }
    }

    fn to_string(&self) -> &str {
        match self {
            MigrationPhase::DualWrite => "dual_write",
            MigrationPhase::Backfill => "backfill",
            MigrationPhase::Cutover => "cutover",
            MigrationPhase::Cleanup => "cleanup",
            MigrationPhase::Done => "done",
        }
    }
}

#[async_trait]
pub trait Migration {
    fn id(&self) -> &'static str;
    /*
     * Should be marked as true if the migration is multi-staged.
     * Alternatively you can return false if the migration is single staged, this will cause the migration
     * to only run the backfill phase.
     */
    fn is_multi_staged(&self) -> bool;
    /*
     * This method should be implemented to write data to the new source.
     * For redis, if your struct is doing impl RedisOps for ExampleSturct,
     * you can add a impl RedisOps for MigrationExampleStruct in the migration file,
     * and use that to write to the new redis source.
     */
    async fn dual_write(data: Box<dyn Any + Send + 'static>) -> Result<(), DynError>
    where
        Self: Sized;
    /* Backfill is where the data is copied from the old source to the new source.
     * This is the most important phase of the migration.
     * You should make sure after this phase, the data in the new source is consistent with the old source.
     */
    async fn backfill(&self) -> Result<(), DynError>;
    /* This phase is where the cutover is done. This is where the application starts reading from the new source.
     * For graph, this might mean changing the application layer code, and removing the dual_write calls.
     * For redis, most of the time a simple rename command from the new key to the old key is enough.
     */
    async fn cutover(&self) -> Result<(), DynError>;
    /* This phase is where the old source is cleaned up.
     * For graph, this might mean deleting the old nodes and relationships.
     * For redis, this might mean deleting the old keys, if any is left.
     */
    async fn cleanup(&self) -> Result<(), DynError>;
}

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct MigrationNode {
    id: String,
    phase: MigrationPhase,
    created_at: i64,
    updated_at: i64,
}

const MIGRATION_PATH: &str = "nexusd/src/migrations/migrations_list/";

/// What `migration run` does with one id listed in `backfill_ready`.
#[derive(Debug, PartialEq)]
enum BackfillReadiness {
    /// The stored migration is at `dual_write`: move it to `backfill`.
    Advance,
    /// The stored migration is at another phase: leave it alone.
    Ignore(MigrationPhase),
    /// No migration with this id is stored in the database.
    Unknown,
}

/// Decides what a `backfill_ready` id means for the migrations stored in the
/// database. Only a migration at `dual_write` is advanced: it is the one phase
/// that waits for an operator, and every later phase is moved on by the runner
/// itself.
fn backfill_readiness(id: &str, stored_migrations: &[MigrationNode]) -> BackfillReadiness {
    match stored_migrations.iter().find(|m| m.id == id) {
        None => BackfillReadiness::Unknown,
        Some(m) if m.phase == MigrationPhase::DualWrite => BackfillReadiness::Advance,
        Some(m) => BackfillReadiness::Ignore(m.phase.clone()),
    }
}

pub struct MigrationManager {
    graph: Arc<dyn GraphOps>,
    migrations: Vec<Box<dyn Migration>>,
}

impl Default for MigrationManager {
    fn default() -> Self {
        let graph = match get_neo4j_graph() {
            Ok(graph) => graph,
            Err(e) => panic!("Could not initialise migration manager: {e:?}"),
        };
        Self {
            graph,
            migrations: Vec::new(),
        }
    }
}

impl MigrationManager {
    pub async fn dual_write<T: Migration>(data: Box<dyn Any + Send>) -> Result<(), DynError> {
        T::dual_write(data).await
    }

    pub async fn new_migration(name: String) -> Result<(), DynError> {
        let now = Utc::now().timestamp();
        let snake_case_name = utils::to_snake_case(&name);
        let migration_file_name = format!("{snake_case_name}_{now}");
        let migration_template = generate_template(&migration_file_name);
        let file_path = format!("{}{}.rs", MIGRATION_PATH, migration_file_name);
        tokio::fs::write(file_path.clone(), migration_template)
            .await
            .map_err(|err| {
                format!(
                    "Failed to create migration file at {}: error: {}",
                    file_path.as_str(),
                    err
                )
            })?;

        // append to migrations_list/mod.rs
        let mod_file_path = format!("{MIGRATION_PATH}mod.rs");
        let mod_file_content = format!("pub mod {migration_file_name};\n");
        let mut mod_file = tokio::fs::OpenOptions::new()
            .append(true)
            .open(mod_file_path)
            .await?;
        tokio::io::AsyncWriteExt::write_all(&mut mod_file, mod_file_content.as_bytes()).await?;
        println!("Migration file created at {file_path}");
        Ok(())
    }

    pub fn register(&mut self, migration: Box<dyn Migration>) {
        self.migrations.push(migration);
    }

    pub async fn run(&mut self, migrations_backfill_ready: &[String]) -> Result<(), DynError> {
        // get all migrations from the database
        let stored_migrations = self.get_migrations().await?;
        // move the migrations marked as ready for backfill out of dual_write
        self.advance_backfill_ready(migrations_backfill_ready, &stored_migrations)
            .await?;

        // get all migrations from the database
        let stored_migrations = self.get_migrations().await?;
        // perform pending migrations
        for migration in &self.migrations {
            let migration_id = migration.id();
            let is_migration_multi_staged = migration.is_multi_staged();
            let mut stored_migration: Option<MigrationNode> = stored_migrations
                .iter()
                .find(|m| m.id == migration_id)
                .cloned();
            if stored_migration.is_none() {
                info!("Storing new migration {}...", migration_id);
                self.store_new_migration(migration_id, is_migration_multi_staged)
                    .await?;
                if is_migration_multi_staged {
                    continue;
                } else {
                    let now = Utc::now().timestamp_millis();
                    let new_migration_node = MigrationNode {
                        id: migration_id.to_string(),
                        phase: MigrationPhase::Backfill,
                        created_at: now,
                        updated_at: now,
                    };
                    stored_migration = Some(new_migration_node.clone());
                }
            }
            let stored_migration = stored_migration.unwrap();
            if stored_migration.phase == MigrationPhase::Done {
                info!("Migration {} is already done", migration_id);
                continue;
            }
            info!(
                "Migration {} is at phase {}",
                migration_id,
                stored_migration.phase.to_string()
            );

            match stored_migration.phase {
                MigrationPhase::Backfill => migration.backfill().await?,
                MigrationPhase::Cutover => {
                    migration.cutover().await?;
                }
                MigrationPhase::Cleanup => migration.cleanup().await?,
                _ => continue,
            }
            info!(
                "Migration {} completed phase {} successfully!",
                migration_id,
                stored_migration.phase.to_string()
            );
            let next_phase = match is_migration_multi_staged {
                true => stored_migration.phase.next(),
                false => Some(MigrationPhase::Done),
            };
            if let Some(next_phase) = next_phase {
                self.update_migration_phase(migration_id, &next_phase)
                    .await?;
            }
        }
        Ok(())
    }

    /// Moves each `backfill_ready` migration that is at `dual_write` to
    /// `backfill`, once. An id that is not at `dual_write`, or not stored at
    /// all, is logged and left as it is.
    async fn advance_backfill_ready(
        &self,
        migrations_backfill_ready: &[String],
        stored_migrations: &[MigrationNode],
    ) -> Result<(), DynError> {
        let mut seen = HashSet::new();
        for id in migrations_backfill_ready {
            if !seen.insert(id.as_str()) {
                continue;
            }
            match backfill_readiness(id, stored_migrations) {
                BackfillReadiness::Advance => {
                    if self.advance_dual_write_to_backfill(id).await? {
                        info!("Migration {} moved from dual_write to backfill", id);
                    } else {
                        info!(
                            "Migration {} is listed in backfill_ready but left dual_write before it was advanced; left alone",
                            id
                        );
                    }
                }
                BackfillReadiness::Ignore(phase) => info!(
                    "Migration {} is listed in backfill_ready but is at phase {}; only dual_write migrations are advanced, so it is left alone",
                    id,
                    phase.to_string()
                ),
                BackfillReadiness::Unknown => info!(
                    "Migration {} is listed in backfill_ready but has no stored phase; ignored",
                    id
                ),
            }
        }
        Ok(())
    }

    /// Moves one migration from `dual_write` to `backfill` in a single
    /// statement and reports whether it moved. The statement takes the node's
    /// lock before it tests the phase, so a concurrent runner that already
    /// advanced the migration, or finished it, is seen and its phase is not
    /// overwritten.
    ///
    /// The lock comes from `SET m:Migration`, a write that changes no value. A
    /// predicate inside the `MATCH` is tested before the lock is taken, so a
    /// runner waiting on the lock would reset a migration the other runner just
    /// finished; and a lock taken by rewriting `m.updated_at` would put the
    /// value read before the lock over the other runner's.
    async fn advance_dual_write_to_backfill(&self, id: &str) -> Result<bool, DynError> {
        let query = Query::new(
            "advance_dual_write_to_backfill",
            "MATCH (m:Migration {id: $id})
            SET m:Migration
            WITH m
            WHERE m.phase = 'dual_write'
            SET m.phase = 'backfill', m.updated_at = timestamp()
            RETURN count(m) AS advanced",
        )
        .param("id", id);
        let mut result = self.graph.execute(query).await.map_err(|e| e.to_string())?;
        let row = result
            .try_next()
            .await
            .map_err(|e| format!("ADVANCE MIGRATION ERROR: {e:?}"))?
            .ok_or("ADVANCE MIGRATION ERROR: no result row")?;
        let advanced: i64 = row
            .get("advanced")
            .map_err(|e| format!("ADVANCE MIGRATION ROW ERROR: {e:?}"))?;
        Ok(advanced > 0)
    }

    async fn get_migrations(&self) -> Result<Vec<MigrationNode>, DynError> {
        let query = Query::new(
            "get_migrations",
            "MATCH (m:Migration) RETURN COLLECT(m) as migrations",
        );
        let mut result = self.graph.execute(query).await.map_err(|e| e.to_string())?;

        match result.try_next().await {
            Ok(row) => match row {
                Some(row) => match row.get::<Vec<MigrationNode>>("migrations") {
                    Ok(migrations) => Ok(migrations),
                    Err(e) => Err(format!("GET ROW ERROR: {e:?}, {row:?}").into()),
                },
                None => Err("Migration Not found".into()),
            },
            Err(e) => Err(format!("GET MIGRATION ERROR: {e:?}").into()),
        }
    }

    async fn store_new_migration(&self, id: &str, is_multi_stage: bool) -> Result<(), DynError> {
        let initial_phase = match is_multi_stage {
            true => MigrationPhase::DualWrite,
            false => MigrationPhase::Backfill,
        };
        let query = Query::new(
            "store_migration",
            "MERGE (m:Migration {id: $id, phase: $phase, created_at: timestamp(), updated_at: 0})",
        )
        .param("id", id)
        .param("phase", initial_phase.to_string());

        self.graph.run(query).await?;
        Ok(())
    }

    async fn update_migration_phase(
        &self,
        id: &str,
        phase: &MigrationPhase,
    ) -> Result<(), DynError> {
        let query = Query::new(
            "update_migration_phase",
            "MERGE (m:Migration {id: $id}) SET m.phase = $phase, m.updated_at = timestamp()",
        )
        .param("id", id)
        .param("phase", phase.to_string());

        self.graph.run(query).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stored(id: &str, phase: MigrationPhase) -> MigrationNode {
        MigrationNode {
            id: id.to_string(),
            phase,
            created_at: 1,
            updated_at: 1,
        }
    }

    #[test]
    fn a_listed_dual_write_migration_is_advanced() {
        let stored = [stored("A", MigrationPhase::DualWrite)];
        assert_eq!(backfill_readiness("A", &stored), BackfillReadiness::Advance);
    }

    #[test]
    fn a_listed_migration_in_any_other_phase_is_left_alone() {
        for phase in [
            MigrationPhase::Backfill,
            MigrationPhase::Cutover,
            MigrationPhase::Cleanup,
            MigrationPhase::Done,
        ] {
            let stored = [stored("A", phase.clone())];
            assert_eq!(
                backfill_readiness("A", &stored),
                BackfillReadiness::Ignore(phase)
            );
        }
    }

    #[test]
    fn an_unlisted_dual_write_migration_is_not_asked_about() {
        let stored = [
            stored("A", MigrationPhase::DualWrite),
            stored("B", MigrationPhase::DualWrite),
        ];
        let listed = ["B".to_string()];
        let advanced: Vec<&str> = listed
            .iter()
            .filter(|id| backfill_readiness(id, &stored) == BackfillReadiness::Advance)
            .map(String::as_str)
            .collect();
        assert_eq!(advanced, ["B"]);
    }

    #[test]
    fn an_id_unknown_to_the_database_is_ignored() {
        let stored = [stored("A", MigrationPhase::DualWrite)];
        assert_eq!(
            backfill_readiness("Missing", &stored),
            BackfillReadiness::Unknown
        );
        assert_eq!(backfill_readiness("A", &[]), BackfillReadiness::Unknown);
    }
}
