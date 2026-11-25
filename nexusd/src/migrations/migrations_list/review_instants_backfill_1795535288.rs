use async_trait::async_trait;

use crate::migrations::manager::Migration;
use nexus_common::types::DynError;
use nexus_watcher::events::handlers::review::backfill_review_instants;

/// Stores the instants the review aggregates order by, settles each attested
/// order's counted seller rating, and recomputes every review aggregate. No
/// record is rewritten. Idempotent; needs only the databases.
pub struct ReviewInstantsBackfill1795535288;

#[async_trait]
impl Migration for ReviewInstantsBackfill1795535288 {
    fn id(&self) -> &'static str {
        "ReviewInstantsBackfill1795535288"
    }

    fn is_multi_staged(&self) -> bool {
        false
    }

    async fn dual_write(_data: Box<dyn std::any::Any + Send + 'static>) -> Result<(), DynError> {
        Ok(())
    }

    async fn backfill(&self) -> Result<(), DynError> {
        let summary = backfill_review_instants().await?;
        tracing::info!(
            "Review instants backfill: {} review(s) stored, {} order(s) settled, {} aggregate(s) recomputed",
            summary.reviews,
            summary.orders,
            summary.aggregates
        );
        Ok(())
    }

    async fn cutover(&self) -> Result<(), DynError> {
        Ok(())
    }

    async fn cleanup(&self) -> Result<(), DynError> {
        Ok(())
    }
}
