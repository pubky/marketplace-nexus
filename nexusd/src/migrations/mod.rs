#[allow(unused_imports)]
use manager::Migration;

pub mod builder;
pub mod manager;
mod migrations_list;
mod utils;

pub use builder::MigrationBuilder;
pub use manager::MigrationManager;

use crate::migrations::migrations_list::listing_auction_terms_reindex_1787256279::ListingAuctionTermsReindex1787256279;
use crate::migrations::migrations_list::listing_reserve_scrub_1789805700::ListingReserveScrub1789805700;
use crate::migrations::migrations_list::remove_muted_1771718400::RemoveMuted1771718400;
use crate::migrations::migrations_list::review_moderation_recompute_1792579958::ReviewModerationRecompute1792579958;
use crate::migrations::migrations_list::review_per_item_recompute_1792584000::ReviewPerItemRecompute1792584000;
use crate::migrations::migrations_list::users_by_pk_reindex_1751635096::UsersByPkReindex1751635096;
/// Registers migrations with the `MigrationManager`
///
/// # Description
/// This function populates the migration manager with a list of migration tasks.
/// Each migration should be manually added to the `migrations` vector after it is
/// created in the `/db/migrations/migration_list` folder. The migration ID must be copied and
/// referenced in this function to ensure it is included in the execution process.
///
/// # Steps to Add a New Migration in pub fn import_migrations:
/// 1. Create a migration using the CLI: `cargo run -- db migration new DumpNotifications`
/// 2. Copy the migration struct name (e.g., `DumpNotifications1739459200`).
/// 3. Add it to the `migrations` vector as `Box::new(DumpNotifications1739459200)`.
/// 4. Ensure the migration is registered by calling `migration_manager.register(migration)`
///
/// # Example:
/// ```rust
/// let migrations: Vec<Box<dyn Migration>> = vec![
///     Box::new(DumpNotifications1739459200),
///     Box::new(AnotherMigration1739459201), // Add new migrations here
/// ];
/// ```
///
/// # Parameters
/// - `migration_manager`: A mutable reference to `MigrationManager` where migrations will be registered.
///
pub fn import_migrations(migration_manager: &mut MigrationManager) {
    for migration in all_migrations() {
        migration_manager.register(migration);
    }
}

/// Ids of migrations that were removed from the list. Their `Migration` nodes
/// may still be stored in a database; the manager only visits registered
/// migrations, so those nodes are inert and are left as they are. An id here
/// must not be registered again: it would run its unthrottled body again on any
/// database that lacks the node or has it at `backfill`.
///
/// Their work is `nexusd db backfill reviews --job review-trust-buckets` and
/// `--job review-discovery`.
pub const RETIRED_MIGRATION_IDS: [&str; 2] = [
    "ReviewBackfill1787905961",
    "ReviewTrustBucketsBackfill1792587206",
];

/// The ids of the registered migrations, in registration order.
pub fn registered_migration_ids() -> Vec<&'static str> {
    all_migrations()
        .iter()
        .map(|migration| migration.id())
        .collect()
}

fn all_migrations() -> Vec<Box<dyn Migration>> {
    vec![
        // Note: Add your migrations here to be picked up by the manager
        Box::new(UsersByPkReindex1751635096),
        Box::new(RemoveMuted1771718400),
        Box::new(ListingAuctionTermsReindex1787256279),
        Box::new(ListingReserveScrub1789805700),
        Box::new(ReviewModerationRecompute1792579958),
        Box::new(ReviewPerItemRecompute1792584000),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    use nexus_watcher::backfill::jobs::{DISCOVERY_JOB, TRUST_BUCKETS_JOB};

    /// The throttled review backfills are driven by `nexusd db backfill
    /// reviews` and never by `migration run`: no registered migration carries
    /// their ids, so listing those ids in `backfill_ready` cannot start them.
    #[test]
    fn the_throttled_backfill_jobs_are_not_registered_migrations() {
        let ids = registered_migration_ids();
        for job in [TRUST_BUCKETS_JOB, DISCOVERY_JOB] {
            assert!(!ids.contains(&job), "{job} must not be a migration");
        }
    }

    #[test]
    fn no_registered_migration_uses_a_retired_id() {
        let ids = registered_migration_ids();
        for retired in RETIRED_MIGRATION_IDS {
            assert!(
                !ids.contains(&retired),
                "{retired} is retired and must not be registered"
            );
        }
    }

    #[test]
    fn registered_migration_ids_are_unique() {
        let ids = registered_migration_ids();
        assert!(!ids.is_empty());
        let mut unique = ids.clone();
        unique.sort();
        unique.dedup();
        assert_eq!(unique.len(), ids.len(), "migration ids are unique");
    }
}
