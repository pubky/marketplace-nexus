use async_trait::async_trait;

use crate::migrations::manager::Migration;
use nexus_common::types::DynError;
use nexus_watcher::events::handlers::review::recompute_per_item_counting;

/// Recounts reviews for per-item reviews: re-evaluates every review's order
/// twins with the listing-aware match, settles which item review carries each
/// attested order's counted seller rating, and recomputes every review
/// aggregate. No record is rewritten. Idempotent; needs only the databases.
pub struct ReviewPerItemRecompute1792584000;

#[async_trait]
impl Migration for ReviewPerItemRecompute1792584000 {
    fn id(&self) -> &'static str {
        "ReviewPerItemRecompute1792584000"
    }

    fn is_multi_staged(&self) -> bool {
        false
    }

    async fn dual_write(_data: Box<dyn std::any::Any + Send + 'static>) -> Result<(), DynError> {
        Ok(())
    }

    async fn backfill(&self) -> Result<(), DynError> {
        let summary = recompute_per_item_counting().await?;
        tracing::info!(
            "Review per-item recompute: {} review(s) re-evaluated, {} order(s) settled, {} aggregate(s) recomputed",
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
