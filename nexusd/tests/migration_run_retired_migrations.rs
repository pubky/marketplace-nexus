use anyhow::Result;
use futures::TryStreamExt;
use nexus_common::db::get_neo4j_graph;
use nexus_common::db::graph::Query;
use nexus_watcher::service::NexusWatcher;
use nexusd::migrations::{
    import_migrations, registered_migration_ids, MigrationManager, RETIRED_MIGRATION_IDS,
};

type Stored = (String, String, i64, i64);

fn graph() -> Result<std::sync::Arc<dyn nexus_common::db::GraphOps>> {
    get_neo4j_graph().map_err(|e| anyhow::anyhow!("{e:?}"))
}

/// The `(id, phase, created_at, updated_at)` of every stored node with one of
/// these ids.
async fn stored(ids: &[&str]) -> Result<Vec<Stored>> {
    let mut rows = graph()?
        .execute(
            Query::new(
                "test_stored_migrations",
                "MATCH (m:Migration) WHERE m.id IN $ids
                 RETURN m.id AS id, m.phase AS phase, m.created_at AS created_at,
                        m.updated_at AS updated_at
                 ORDER BY m.id",
            )
            .param(
                "ids",
                ids.iter().map(|id| id.to_string()).collect::<Vec<_>>(),
            ),
        )
        .await?;
    let mut found = Vec::new();
    while let Some(row) = rows.try_next().await? {
        found.push((
            row.get::<String>("id")?,
            row.get::<String>("phase")?,
            row.get::<i64>("created_at")?,
            row.get::<i64>("updated_at")?,
        ));
    }
    Ok(found)
}

async fn replace(ids: &[&str], nodes: &[Stored]) -> Result<()> {
    graph()?
        .run(
            Query::new(
                "test_clear_migrations",
                "MATCH (m:Migration) WHERE m.id IN $ids DETACH DELETE m",
            )
            .param(
                "ids",
                ids.iter().map(|id| id.to_string()).collect::<Vec<_>>(),
            ),
        )
        .await?;
    for (id, phase, created_at, updated_at) in nodes {
        graph()?
            .run(
                Query::new(
                    "test_seed_migration",
                    "CREATE (:Migration {id: $id, phase: $phase, created_at: $created_at,
                                         updated_at: $updated_at})",
                )
                .param("id", id.as_str())
                .param("phase", phase.as_str())
                .param("created_at", *created_at)
                .param("updated_at", *updated_at),
            )
            .await?;
    }
    Ok(())
}

async fn run_the_registered_list() -> Result<()> {
    let mut manager = MigrationManager::default();
    import_migrations(&mut manager);
    manager.run(&[]).await.map_err(|e| anyhow::anyhow!("{e:?}"))
}

/// `migration run` with the full registered list does not touch the retired
/// migrations' stored nodes, whatever their phase, and does not create them
/// when they are absent. The other registered migrations are stored as `done`,
/// so the run changes nothing at all; a retired migration that was still
/// registered would run from `backfill` (and move its node to `done`) or store
/// a new node.
#[tokio_shared_rt::test(shared)]
async fn migration_run_leaves_retired_migration_nodes_alone() -> Result<()> {
    NexusWatcher::builder()
        .init_test_stack()
        .await
        .map_err(|e| anyhow::anyhow!("{e:?}"))?;

    let registered = registered_migration_ids();
    assert!(!registered.is_empty());
    let mut all_ids: Vec<&str> = registered.clone();
    all_ids.extend(RETIRED_MIGRATION_IDS);
    let before = stored(&all_ids).await?;

    let done_registered: Vec<Stored> = registered
        .iter()
        .map(|id| (id.to_string(), "done".to_string(), 10, 20))
        .collect();
    let retired_at = |phase: &str| -> Vec<Stored> {
        RETIRED_MIGRATION_IDS
            .iter()
            .map(|id| (id.to_string(), phase.to_string(), 30, 40))
            .collect()
    };

    for scenario in ["done", "backfill", "absent"] {
        let retired = match scenario {
            "absent" => Vec::new(),
            phase => retired_at(phase),
        };
        let mut seeded = done_registered.clone();
        seeded.extend(retired);
        seeded.sort();
        replace(&all_ids, &seeded).await?;

        run_the_registered_list().await?;
        assert_eq!(
            stored(&all_ids).await?,
            seeded,
            "a run over the registered list changes no node (retired ids {scenario})"
        );

        run_the_registered_list().await?;
        assert_eq!(
            stored(&all_ids).await?,
            seeded,
            "and neither does a second run (retired ids {scenario})"
        );
    }

    let mut restored = before;
    restored.sort();
    replace(&all_ids, &restored).await?;
    Ok(())
}
