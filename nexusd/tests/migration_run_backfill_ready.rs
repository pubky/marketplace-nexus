use anyhow::Result;
use futures::TryStreamExt;
use nexus_common::db::graph::Query;
use nexus_common::db::{get_neo4j_graph, Neo4JConfig};
use nexus_watcher::service::NexusWatcher;
use nexusd::migrations::MigrationManager;
use std::time::Duration;

const PHASES: [&str; 5] = ["dual_write", "backfill", "cutover", "cleanup", "done"];
const SEEDED_UPDATED_AT: i64 = 1_111;

/// Every test in this file seeds its own `Migration` nodes under its own id
/// prefix, so tests running side by side never see each other's nodes.
struct Seeds {
    prefix: &'static str,
}

impl Seeds {
    fn id(&self, name: &str) -> String {
        format!("{}_{name}", self.prefix)
    }

    async fn clear(&self) -> Result<()> {
        let graph = get_neo4j_graph().map_err(|e| anyhow::anyhow!("{e:?}"))?;
        graph
            .run(
                Query::new(
                    "test_clear_migrations",
                    "MATCH (m:Migration) WHERE m.id STARTS WITH $prefix DETACH DELETE m",
                )
                .param("prefix", self.prefix),
            )
            .await?;
        Ok(())
    }

    async fn seed(&self, name: &str, phase: &str) -> Result<String> {
        let id = self.id(name);
        let graph = get_neo4j_graph().map_err(|e| anyhow::anyhow!("{e:?}"))?;
        graph
            .run(
                Query::new(
                    "test_seed_migration",
                    "CREATE (:Migration {id: $id, phase: $phase, created_at: 1, updated_at: $updated_at})",
                )
                .param("id", id.as_str())
                .param("phase", phase)
                .param("updated_at", SEEDED_UPDATED_AT),
            )
            .await?;
        Ok(id)
    }

    /// `(phase, updated_at)` of the single node with this id, `None` if there is none.
    async fn state(&self, id: &str) -> Result<Option<(String, i64)>> {
        let graph = get_neo4j_graph().map_err(|e| anyhow::anyhow!("{e:?}"))?;
        let mut rows = graph
            .execute(
                Query::new(
                    "test_migration_state",
                    "MATCH (m:Migration {id: $id}) RETURN m.phase AS phase, m.updated_at AS updated_at",
                )
                .param("id", id),
            )
            .await?;
        let mut found = Vec::new();
        while let Some(row) = rows.try_next().await? {
            found.push((row.get::<String>("phase")?, row.get::<i64>("updated_at")?));
        }
        assert!(found.len() <= 1, "{id} is stored {} times", found.len());
        Ok(found.pop())
    }
}

async fn init() -> Result<()> {
    NexusWatcher::builder()
        .init_test_stack()
        .await
        .map_err(|e| anyhow::anyhow!("{e:?}"))
}

async fn run(listed: &[String]) -> Result<()> {
    let mut manager = MigrationManager::default();
    manager
        .run(listed)
        .await
        .map_err(|e| anyhow::anyhow!("{e:?}"))
}

/// A `backfill_ready` id moves a migration out of `dual_write` and nothing
/// else: `backfill`, `cutover`, `cleanup` and `done` keep their phase and their
/// `updated_at`, an unlisted `dual_write` migration stays, and an id the
/// database does not know creates nothing. Running again changes nothing.
#[tokio_shared_rt::test(shared)]
async fn backfill_ready_advances_only_a_dual_write_migration() -> Result<()> {
    init().await?;
    let seeds = Seeds {
        prefix: "BackfillReadyEachPhase",
    };
    seeds.clear().await?;

    let mut listed = Vec::new();
    for phase in PHASES {
        listed.push(seeds.seed(phase, phase).await?);
    }
    let unlisted = seeds.seed("unlisted", "dual_write").await?;
    listed.push(seeds.id("unknown"));

    run(&listed).await?;

    let advanced = seeds.state(&seeds.id("dual_write")).await?.expect("stored");
    assert_eq!(advanced.0, "backfill");
    assert!(
        advanced.1 > SEEDED_UPDATED_AT,
        "advancing records the time of the move"
    );
    for phase in &PHASES[1..] {
        assert_eq!(
            seeds.state(&seeds.id(phase)).await?,
            Some((phase.to_string(), SEEDED_UPDATED_AT)),
            "a {phase} migration is left alone"
        );
    }
    assert_eq!(
        seeds.state(&unlisted).await?,
        Some(("dual_write".to_string(), SEEDED_UPDATED_AT))
    );
    assert_eq!(seeds.state(&seeds.id("unknown")).await?, None);

    run(&listed).await?;

    assert_eq!(
        seeds.state(&seeds.id("dual_write")).await?,
        Some(advanced),
        "the second run does not advance it again"
    );
    assert_eq!(
        seeds.state(&seeds.id("done")).await?,
        Some(("done".to_string(), SEEDED_UPDATED_AT)),
        "a done migration listed on every run keeps its phase and its time"
    );
    seeds.clear().await?;
    Ok(())
}

/// Two `migration run`s that find the same `dual_write` migration both succeed
/// and it ends in `backfill`, once.
#[tokio_shared_rt::test(shared)]
async fn two_concurrent_runs_advance_a_dual_write_migration_once() -> Result<()> {
    init().await?;
    let seeds = Seeds {
        prefix: "BackfillReadyConcurrent",
    };
    for round in 0..10 {
        seeds.clear().await?;
        let id = seeds.seed("one", "dual_write").await?;
        let listed = [id.clone()];

        let (a, b) = tokio::join!(run(&listed), run(&listed));
        a?;
        b?;

        let (phase, updated_at) = seeds.state(&id).await?.expect("stored");
        assert_eq!(phase, "backfill", "round {round}");
        assert!(updated_at > SEEDED_UPDATED_AT, "round {round}");
    }
    seeds.clear().await?;
    Ok(())
}

/// A run that read a migration at `dual_write` and is waiting on the node's
/// lock must not move it back when the lock holder finishes the migration
/// first: it stays `done` with the holder's `updated_at`.
#[tokio_shared_rt::test(shared)]
async fn a_migration_finished_while_a_run_waits_stays_done() -> Result<()> {
    init().await?;
    let seeds = Seeds {
        prefix: "BackfillReadyFinishedWhileWaiting",
    };
    seeds.clear().await?;
    let id = seeds.seed("one", "dual_write").await?;

    let config = Neo4JConfig::default();
    let other = neo4rs::Graph::new(config.uri.as_str(), &config.user, &config.password).await?;
    let mut holder = other.start_txn().await?;
    holder
        .run(
            neo4rs::query("MATCH (m:Migration {id: $id}) SET m.updated_at = 2222")
                .param("id", id.as_str()),
        )
        .await?;

    let listed = [id.clone()];
    let waiting = run(&listed);

    let graph = get_neo4j_graph().map_err(|e| anyhow::anyhow!("{e:?}"))?;
    let finish_the_migration = async {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
        loop {
            let mut rows = graph
                .execute(Query::new(
                    "test_blocked_advance",
                    "SHOW TRANSACTIONS YIELD currentQuery, status
                     WHERE currentQuery CONTAINS 'advanced'
                       AND status STARTS WITH 'Blocked'
                     RETURN count(*) AS n",
                ))
                .await?;
            let blocked = match rows.try_next().await? {
                Some(row) => row.get::<i64>("n")?,
                None => 0,
            };
            if blocked >= 1 {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "the run never blocked on the migration"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        holder
            .run(
                neo4rs::query(
                    "MATCH (m:Migration {id: $id}) SET m.phase = 'done', m.updated_at = 3333",
                )
                .param("id", id.as_str()),
            )
            .await?;
        holder.commit().await?;
        anyhow::Ok(())
    };
    let (waited, finished) = tokio::join!(waiting, finish_the_migration);
    finished?;
    waited?;

    assert_eq!(
        seeds.state(&id).await?,
        Some(("done".to_string(), 3333)),
        "the migration the other runner finished is not moved back"
    );
    seeds.clear().await?;
    Ok(())
}
