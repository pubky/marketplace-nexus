//! The operator-visible signal for the epoch gate and the flip retry queue, as `pubky-nexus`'s
//! `epoch_metrics`: counters labelled by epoch, resource, reason, kind and outcome only, never
//! by user or object id, beside a structured log line naming the user and the address.
//!
//! - `watcher.epoch.skipped` {`epoch`, `resource`, `reason`}: `gated` for an event of an epoch
//!   its user is not read through, ignored, logged at debug (every migrator copy is one);
//!   `deferred` for a `v0` user's `social/v1` write held while their flip waits in the retry
//!   queue, logged at warn. `deferred` counts holds: a parked write its replay finds waiting
//!   on a flip queued anew counts again.
//! - `watcher.retry.events` {`kind`, `outcome`}: what the retry queue did with a flip or a
//!   deferred write; see [`RetryOutcome`].

use crate::events::retry::queue::RetryKind;
use nexus_common::models::event::Event;
use nexus_common::models::user::Epoch;
use opentelemetry::metrics::{Counter, Meter};
use opentelemetry::{global, KeyValue};
use pubky_app_specs::Resource;
use std::sync::LazyLock;
use tracing::{debug, warn};

/// The OpenTelemetry meter of the watcher's own instruments.
pub const METER_NAME: &str = "nexus.watcher";

/// What the retry queue did with one entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryOutcome {
    /// A failed flip entered the queue.
    Queued,
    /// A deferred write was parked behind its user's flip.
    Parked,
    /// A deferred write was refused: its user already holds the most parked writes.
    Overflow,
    /// A parked write was woken by its user's flip settling.
    Woken,
    /// A replay succeeded and left the queue.
    Applied,
    /// A replay failed and was scheduled again.
    Rescheduled,
    /// A replay failed for good, or the entry could not be replayed, and left the queue.
    Dropped,
    /// A deferred write used up its replays and left the queue.
    Exhausted,
}

impl RetryOutcome {
    fn label(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Parked => "parked",
            Self::Overflow => "overflow",
            Self::Woken => "woken",
            Self::Applied => "applied",
            Self::Rescheduled => "rescheduled",
            Self::Dropped => "dropped",
            Self::Exhausted => "exhausted",
        }
    }
}

/// `reason` of an event the gate ignored.
const GATED: &str = "gated";
/// `reason` of an event the gate deferred until its user's flip.
const DEFERRED: &str = "deferred";

/// The counters, built from one meter.
struct Instruments {
    skipped: Counter<u64>,
    retry: Counter<u64>,
}

impl Instruments {
    fn new(meter: &Meter) -> Self {
        Self {
            skipped: meter
                .u64_counter("watcher.epoch.skipped")
                .with_description(
                    "Events the epoch gate ignored or deferred before any fetch; \
                     a deferral counts on every hold",
                )
                .build(),
            retry: meter
                .u64_counter("watcher.retry.events")
                .with_description("Flips and deferred writes through the retry queue, by outcome")
                .build(),
        }
    }

    fn gated(&self, event: &Event, user_epoch: Epoch) {
        debug!(
            uri = %event.uri,
            event_epoch = epoch_label(event.epoch),
            user_epoch = epoch_label(user_epoch),
            "Ignored event: epochs do not match"
        );
        self.skipped.add(1, &skip_labels(event, GATED));
    }

    fn deferred(&self, event: &Event, flip_uri: &str) {
        warn!(
            uri = %event.uri,
            user_id = %event.parsed_uri.user_id,
            %flip_uri,
            "Deferred event: the user's flip waits in the retry queue"
        );
        self.skipped.add(1, &skip_labels(event, DEFERRED));
    }

    fn retry(&self, kind: RetryKind, outcome: RetryOutcome, count: u64) {
        if count > 0 {
            self.retry.add(
                count,
                &[
                    KeyValue::new("kind", kind_label(kind)),
                    KeyValue::new("outcome", outcome.label()),
                ],
            );
        }
    }
}

static INSTRUMENTS: LazyLock<Instruments> =
    LazyLock::new(|| Instruments::new(&global::meter(METER_NAME)));

/// Counts and logs `event`, which the gate ignored, its user being at `user_epoch`.
pub fn record_gated(event: &Event, user_epoch: Epoch) {
    INSTRUMENTS.gated(event, user_epoch);
}

/// Counts and logs `event`, which the gate deferred until the flip queued at `flip_uri`.
pub fn record_deferred(event: &Event, flip_uri: &str) {
    INSTRUMENTS.deferred(event, flip_uri);
}

/// Counts `count` entries of `kind` the retry queue settled with `outcome`.
pub fn record_retry(kind: RetryKind, outcome: RetryOutcome, count: u64) {
    INSTRUMENTS.retry(kind, outcome, count);
}

fn skip_labels(event: &Event, reason: &'static str) -> [KeyValue; 3] {
    [
        KeyValue::new("epoch", epoch_label(event.epoch)),
        KeyValue::new("resource", resource_label(&event.parsed_uri.resource)),
        KeyValue::new("reason", reason),
    ]
}

fn epoch_label(epoch: Epoch) -> &'static str {
    match epoch {
        Epoch::V0 => "v0",
        Epoch::V1 => "v1",
    }
}

fn kind_label(kind: RetryKind) -> &'static str {
    match kind {
        RetryKind::Flip => "flip",
        RetryKind::Deferred => "deferred",
    }
}

/// The `resource` label: the kind of record, never its id. Exhaustive, so a resource the specs
/// crate adds later gets a label of its own.
fn resource_label(resource: &Resource) -> &'static str {
    match resource {
        Resource::User => "profile",
        Resource::Post(_) => "post",
        Resource::Follow(_) => "follow",
        Resource::Mute(_) => "mute",
        Resource::Bookmark(_) => "bookmark",
        Resource::Tag(_) => "tag",
        Resource::File(_) => "file",
        Resource::Blob(_) => "blob",
        Resource::Feed(_) => "feed",
        Resource::LastRead => "last_read",
        Resource::Shop => "shop",
        Resource::Listing(_) => "listing",
        Resource::Drop(_) => "drop",
        Resource::MarketplaceReview(_) => "review",
        Resource::ReviewResponse(_) => "review_response",
        Resource::AttestorProfile => "attestor_profile",
        Resource::AttestorAnnotation(_) => "attestor_annotation",
        Resource::Unknown => "unknown",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry::metrics::MeterProvider as _;
    use opentelemetry::Value;
    use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData};
    use opentelemetry_sdk::metrics::{InMemoryMetricExporter, PeriodicReader, SdkMeterProvider};
    use std::collections::BTreeMap;
    use std::path::PathBuf;
    use tracing_test::traced_test;

    const USER: &str = "operrr8wsbpr3ue9d4qj41ge1kcc6r7fdiy6o3ugjrrhi4y77rdo";
    const TAG_ID: &str = "8Z8CWH8NVYQY39ZEBFGKQWWEKG";

    fn event(line: &str) -> Event {
        Event::parse_event(line, PathBuf::new())
            .unwrap_or_else(|e| panic!("{line}: {e}"))
            .unwrap_or_else(|| panic!("{line}: skipped"))
    }

    fn social_tag_put() -> Event {
        event(&format!(
            "PUT pubky://{USER}/pub/social/v1/tags/{TAG_ID}.json"
        ))
    }

    /// Instruments on their own provider, and the exporter that reads them back.
    fn instruments() -> (Instruments, SdkMeterProvider, InMemoryMetricExporter) {
        let exporter = InMemoryMetricExporter::default();
        let provider = SdkMeterProvider::builder()
            .with_reader(PeriodicReader::builder(exporter.clone()).build())
            .build();
        let instruments = Instruments::new(&provider.meter("test"));
        (instruments, provider, exporter)
    }

    /// Every data point of the counter `name`, by its sorted `key=value` labels.
    fn sums(
        provider: &SdkMeterProvider,
        exporter: &InMemoryMetricExporter,
        name: &str,
    ) -> BTreeMap<String, u64> {
        provider.force_flush().unwrap();
        let mut sums = BTreeMap::new();
        for resource in exporter.get_finished_metrics().unwrap() {
            for scope in resource.scope_metrics() {
                for metric in scope.metrics().filter(|m| m.name() == name) {
                    let AggregatedMetrics::U64(MetricData::Sum(sum)) = metric.data() else {
                        panic!("{name} is no u64 sum");
                    };
                    for point in sum.data_points() {
                        let mut labels: Vec<String> = point
                            .attributes()
                            .map(|kv| match &kv.value {
                                Value::String(s) => format!("{}={}", kv.key, s.as_str()),
                                other => format!("{}={other}", kv.key),
                            })
                            .collect();
                        labels.sort();
                        sums.insert(labels.join(","), point.value());
                    }
                }
            }
        }
        sums
    }

    #[traced_test]
    #[test]
    fn a_gated_event_counts_by_epoch_resource_and_reason_and_logs_at_debug() {
        let (instruments, provider, exporter) = instruments();
        instruments.gated(&social_tag_put(), Epoch::V0);
        instruments.gated(&social_tag_put(), Epoch::V0);
        assert_eq!(
            sums(&provider, &exporter, "watcher.epoch.skipped"),
            BTreeMap::from([("epoch=v1,reason=gated,resource=tag".to_string(), 2)])
        );
        assert!(logs_contain("Ignored event: epochs do not match"));
        assert!(logs_contain("user_epoch=\"v0\""));
    }

    #[traced_test]
    #[test]
    fn a_deferred_event_warns_with_its_user_and_flip_and_counts_without_them() {
        let (instruments, provider, exporter) = instruments();
        let flip_uri = format!("pubky://{USER}/pub/social/v1/profile.json");
        instruments.deferred(&social_tag_put(), &flip_uri);
        let counted = sums(&provider, &exporter, "watcher.epoch.skipped");
        assert_eq!(
            counted,
            BTreeMap::from([("epoch=v1,reason=deferred,resource=tag".to_string(), 1)])
        );
        assert!(counted.keys().all(|labels| !labels.contains(USER)));
        assert!(logs_contain("WARN"));
        assert!(logs_contain(
            "Deferred event: the user's flip waits in the retry queue"
        ));
        assert!(logs_contain(&format!("user_id={USER}")));
        assert!(logs_contain(&format!("flip_uri={flip_uri}")));
    }

    #[test]
    fn retry_outcomes_count_by_kind_and_outcome() {
        let (instruments, provider, exporter) = instruments();
        instruments.retry(RetryKind::Flip, RetryOutcome::Queued, 1);
        instruments.retry(RetryKind::Flip, RetryOutcome::Rescheduled, 1);
        instruments.retry(RetryKind::Flip, RetryOutcome::Rescheduled, 1);
        instruments.retry(RetryKind::Deferred, RetryOutcome::Woken, 3);
        instruments.retry(RetryKind::Deferred, RetryOutcome::Exhausted, 0);
        assert_eq!(
            sums(&provider, &exporter, "watcher.retry.events"),
            BTreeMap::from([
                ("kind=deferred,outcome=woken".to_string(), 3),
                ("kind=flip,outcome=queued".to_string(), 1),
                ("kind=flip,outcome=rescheduled".to_string(), 2),
            ])
        );
    }

    #[test]
    fn resource_labels_name_the_kind_of_record_only() {
        let profile = event(&format!("PUT pubky://{USER}/pub/social/v1/profile.json"));
        assert_eq!(resource_label(&profile.parsed_uri.resource), "profile");
        assert_eq!(resource_label(&social_tag_put().parsed_uri.resource), "tag");
        let follow = event(&format!(
            "PUT pubky://{USER}/pub/social/v1/follows/{USER}.json"
        ));
        assert_eq!(resource_label(&follow.parsed_uri.resource), "follow");
    }
}
