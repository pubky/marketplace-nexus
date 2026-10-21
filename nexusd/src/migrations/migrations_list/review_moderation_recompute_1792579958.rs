use async_trait::async_trait;

use crate::migrations::manager::Migration;
use nexus_common::types::DynError;
use nexus_watcher::events::handlers::review::recompute_all_review_moderation;

/// Re-derives every indexed review's `moderation` from the moderation markers
/// and recomputes the aggregates that changed. Users moderated before review
/// moderation shipped already have a `ModeratedUser` marker, but their reviews
/// were indexed without moderation: this hides their text. Idempotent; needs
/// only the databases.
pub struct ReviewModerationRecompute1792579958;

#[async_trait]
impl Migration for ReviewModerationRecompute1792579958 {
    fn id(&self) -> &'static str {
        "ReviewModerationRecompute1792579958"
    }

    fn is_multi_staged(&self) -> bool {
        false
    }

    async fn dual_write(_data: Box<dyn std::any::Any + Send + 'static>) -> Result<(), DynError> {
        Ok(())
    }

    async fn backfill(&self) -> Result<(), DynError> {
        let changed = recompute_all_review_moderation().await?;
        tracing::info!("Review moderation recompute: {changed} review(s) changed");
        Ok(())
    }

    async fn cutover(&self) -> Result<(), DynError> {
        Ok(())
    }

    async fn cleanup(&self) -> Result<(), DynError> {
        Ok(())
    }
}
