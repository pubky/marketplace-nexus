use async_trait::async_trait;

use crate::migrations::manager::Migration;
use nexus_common::types::DynError;
use nexus_watcher::events::handlers::review::backfill_trust_buckets;

/// Gives every review aggregate its per-attestor trust buckets. Verified
/// reviews indexed before the index kept their attestation's `iat` and price
/// weight are re-read from the reviewer's homeserver and re-ingested, then
/// every subject and listing aggregate is recomputed. Idempotent.
///
/// Requires the Pubky client, which `MigrationBuilder::init_stack`
/// initialises from the migration config (`testnet` / `testnet_host`,
/// mainnet by default).
pub struct ReviewTrustBucketsBackfill1792587206;

#[async_trait]
impl Migration for ReviewTrustBucketsBackfill1792587206 {
    fn id(&self) -> &'static str {
        "ReviewTrustBucketsBackfill1792587206"
    }

    fn is_multi_staged(&self) -> bool {
        false
    }

    async fn dual_write(_data: Box<dyn std::any::Any + Send + 'static>) -> Result<(), DynError> {
        Ok(())
    }

    async fn backfill(&self) -> Result<(), DynError> {
        let summary = backfill_trust_buckets().await?;
        tracing::info!(
            "Review trust-bucket backfill: {} review(s) re-read, {} gone, {} unreachable, {} failed, {} aggregate(s) recomputed",
            summary.refetched,
            summary.gone,
            summary.unreachable,
            summary.failed,
            summary.aggregates
        );
        // An unreachable homeserver must not block the migrations after this
        // one; a review that was read but failed to ingest is retried.
        if summary.failed > 0 {
            return Err(format!(
                "{} review(s) could not be re-ingested; re-run the migration to retry them",
                summary.failed
            )
            .into());
        }
        Ok(())
    }

    async fn cutover(&self) -> Result<(), DynError> {
        Ok(())
    }

    async fn cleanup(&self) -> Result<(), DynError> {
        Ok(())
    }
}
