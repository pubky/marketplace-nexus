use anyhow::Result;
use nexus_common::models::backfill_run::{BackfillClaim, BackfillRun, BackfillStatus};
use nexus_watcher::backfill::jobs::{DISCOVERY_JOB, TRUST_BUCKETS_JOB};
use nexus_watcher::service::NexusWatcher;
use nexusd::migrations::MigrationManager;

/// Listing a backfill job id in `backfill_ready` must not touch the job:
/// `migration run` moves only registered migrations, and the job's state node
/// is not a migration.
#[tokio_shared_rt::test(shared)]
async fn migration_run_ignores_backfill_job_ids_in_backfill_ready() -> Result<()> {
    NexusWatcher::builder()
        .init_test_stack()
        .await
        .map_err(|e| anyhow::anyhow!("{e:?}"))?;

    for job in [TRUST_BUCKETS_JOB, DISCOVERY_JOB] {
        BackfillRun::delete(job).await?;
        let claim =
            BackfillRun::claim(job, "fetch", "migration-isolation-test", 60_000, false).await?;
        assert!(matches!(claim, BackfillClaim::Acquired(_)));
    }
    let before: Vec<BackfillRun> = {
        let mut runs = Vec::new();
        for job in [TRUST_BUCKETS_JOB, DISCOVERY_JOB] {
            runs.push(BackfillRun::get(job).await?.expect("seeded"));
        }
        runs
    };

    let mut manager = MigrationManager::default();
    manager
        .run(&[TRUST_BUCKETS_JOB.to_string(), DISCOVERY_JOB.to_string()])
        .await
        .map_err(|e| anyhow::anyhow!("{e:?}"))?;

    for (job, before) in [TRUST_BUCKETS_JOB, DISCOVERY_JOB].iter().zip(&before) {
        let after = BackfillRun::get(job).await?.expect("still there");
        assert_eq!(after.status, BackfillStatus::Running);
        assert_eq!(after.lease_owner, before.lease_owner);
        assert_eq!(after.cursor, before.cursor);
        assert_eq!(after.counters, before.counters);
        BackfillRun::delete(job).await?;
    }
    Ok(())
}
