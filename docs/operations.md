# Operating the marketplace Nexus

A standalone Nexus instance that indexes the **official staging homeserver** so
the deployed Shop gets catalog discovery (`/v0/stream/listings`,
`/v0/shop/{seller_id}`, `/v0/listing/{seller_id}/{listing_id}`). It runs API and
watcher in one `nexusd` process with a `config.toml` in its `--config-dir`.
Deployment and host-specific configuration are not part of this repository; the
team deploys it through
[pubky/shop-migration](https://github.com/pubky/shop-migration).

## Configuration keys that need a deliberate value

- `[watcher] homeserver`: the homeserver the index follows.
- `[watcher] moderation_id`, `[watcher] moderated_tags`: see below. There is no
  safe default for the moderator key.
- `[watcher] review_exclude_tags`: labels that, placed by the moderator on a
  marketplace review, leave it out of every review aggregate (text withheld),
  and on a user's profile, leave out every review that user wrote (the user,
  shop and listings stay). Defaults to `review_fraud` when omitted; an empty
  list turns review exclusion off.
- `[watcher] events_limit`, `watcher_sleep`: replay tuning (events per poll,
  milliseconds between polls).
- `[stack] allowed_homeservers`: optional list of z-base-32 ids; when set, the
  watcher polls only these and never adds a homeserver it discovers.

### Moderation: launched setting

The Shop index applies the same moderation as pubky.app: content and users that
pubky.app's moderator account tags are removed from the index.

| Setting | `config.toml` key under `[watcher]` | Launched value | Where the value comes from |
|---|---|---|---|
| Moderator key | `moderation_id` | pubky.app's production moderator public key | The operators of pubky.app's production Nexus hold it in that deployment's watcher config (`moderation_id`). It is not in any public repository: upstream `pubky-nexus` ships only a test key (`uo7jgkykft4885n8cruizwy6khw71mnu5pq3ay9i8pw1ymcn85ko`, marked "test user key, overridden by config.toml value"), and the config generators' built-in fallback (`51y9w1skwcryb3iq4sia3x49qwpgstc5feo5tqon65gid7o99khy`) is an older upstream constant that was never confirmed as the live key. |
| Moderation labels | `moderated_tags` | pubky.app's production label list, comma separated | Same source (`moderated_tags` in that config). Upstream's defaults, to compare against and not to copy blindly: `hatespeech`, `harassement` (sic), `terrorism`, `violence`, `illegal_activities`, `il_adult_nu_sex_act`. This fork also moderates `moderated`, the label its proof-listing cleanup uses; keep it in the list only while the moderator account uses it. |

Who sets it: whoever operates the Shop index deployment (its `[watcher]` config), copying the two values from the
pubky.app production Nexus configuration. Nothing in this repository can supply
them. Do not put a personal or throwaway key here: a moderator tag from any
other key is an ordinary community tag.

After a deploy, read the effective watcher config: `moderation_id` must equal
pubky.app's production moderator key and `moderated_tags` its production label
list. The moderator account must be on a homeserver this Nexus watches, or
its tags are never seen (see the next section).

What the moderator's tags do on this index:

| Tag on | Effect |
|---|---|
| a marketplace listing | the listing leaves the graph, the details cache and every stream; stays out across a reindex (`ModeratedListing` marker) |
| a user's profile | the user, their shop and every listing leave the index; the shop and listings stay out across a reindex, and a listing or shop the seller publishes later is not indexed (`ModeratedUser` marker) |
| a post, file or tag | removed, as on pubky.app |
| a marketplace review, with a `moderated_tags` label | the review's `moderation` is `{state: "text_hidden", reason: <label>}`: its text is withheld, its ratings still count (`ModeratedReview` marker) |
| a marketplace review, with a `review_exclude_tags` label | `moderation` is `{state: "excluded", reason: <label>}`: text withheld and the review leaves every aggregate (`ModeratedReview` marker) |
| a user's profile, with a `review_exclude_tags` label | every review the user wrote, and any they write later, is `excluded`; the user is not hidden (`ExcludedReviewer` marker) |

A moderated user's reviews (a `moderated_tags` label on their profile) are
`text_hidden`, ratings kept. Review moderation deletes nothing: the review
stays indexed and listed, and holds across edits and a reindex in any event
order.

Removing the moderator's tag restores what it hid, from the seller's
homeserver (a review's moderation is lifted in place). `GET /v0/listing/{seller_id}/{listing_id}/moderation` reports
whether a listing is moderated, through the listing's own marker or its
seller's, for a listing the index holds or not; the transaction service reads
it before it sells, takes a bid on, or takes an offer on a listing.

### Index discovery: launched setting

| Setting | Launched value | Where |
|---|---|---|
| Default homeserver | exactly one | `homeserver` in the watcher config |
| Discovery limit | `50` | `monitored_homeservers_limit = 50` in the watcher config |

The watcher always polls the default homeserver. It adds other homeservers it
discovers through follows, tags, mentions and reviews, up to the discovery limit
of 50 monitored homeservers in total.

### Homeserver watching: no credentials, full history replay

The watcher polls the homeserver's **public** event stream
(`https://<homeserver_pubky>/events/?cursor=…&limit=…`, resolved via pkarr) —
no credentials or session are needed. The event stream has **no path filter**:
a new Nexus starts at cursor `0000000000000` and must replay the homeserver's
entire event history (profiles, posts, follows, files, … *and* marketplace
records). It cannot run scoped to marketplace paths only; listing indexing
also depends on the seller's user profile being indexed first, so the full
replay is what makes marketplace rows land correctly. The cursor is persisted
in Redis, so restarts resume where they left off.

Observed on first deploy (2026-08-21): the staging homeserver history was
~115,000 events, and the watcher processed roughly 100–160 events/minute (each
PUT costs at least one content fetch from the homeserver), i.e. a full cold
replay takes on the order of half a day. Progress can be tracked in the
service log by the "cursor for the next request" lines.

## Running the backfill migrations

One-shot migrations (see the runbook in `README.md`):

- `ListingAuctionTermsReindex1787256279` re-reads auction listings that
  predate the auction-term fields.
- `ReviewBackfill1787905961` discovers and indexes marketplace reviews
  published BEFORE the deployed watcher's replay cursor, by LISTing every
  indexed user's reviews directory on their homeserver and running the
  normal ingest (attestation verification and reputation recompute
  included).
- `ReviewModerationRecompute1792579958` re-derives every indexed review's
  `moderation` from the moderation markers and recomputes the aggregates that
  changed, so the reviews of users moderated before review moderation shipped
  have their text hidden. It needs only the databases; run it once after the
  release that adds review moderation.
- `ReviewPerItemRecompute1792584000` recounts reviews for per-item reviews:
  a legacy review that an order-scoped review of another item of the same
  order superseded counts again, each attested order's seller rating
  (shipping, communication) counts once, and every review aggregate is
  recomputed. No record is rewritten. It needs only the databases; run it
  once after the release that adds per-item reviews.
- `ReviewTrustBucketsBackfill1792587206` re-reads every verified review
  indexed without its attestation `iat` (indexed before per-attestor buckets
  shipped) from the reviewer's homeserver, then recomputes every review
  aggregate so each carries its per-attestor buckets. It needs outbound
  access to homeservers. A homeserver that cannot be reached is reported and
  skipped, not fatal: its reviews stay out of the buckets (never
  over-counted) until their next edit. Run it once, after
  `ReviewPerItemRecompute1792584000`, after the release that adds trust-scoped
  aggregates.

Run them where the databases are reachable, for example inside the `nexusd`
container:

```bash
# the migration reads ~/.pubky-nexus/migrations/config.toml, created with
# defaults on first run; point it at the deployment's databases (values below
# come from the service's own configuration):
mkdir -p ~/.pubky-nexus/migrations
cat > ~/.pubky-nexus/migrations/config.toml <<'EOF'
name = "nexusd.migration"
backfill_ready = ["ListingAuctionTermsReindex1787256279", "ReviewBackfill1787905961"]
testnet = false
testnet_host = "localhost"

[stack]
log_level = "info"
files_path = "<the service's files_path>"

[stack.db]
redis = "<redis url>"

[stack.db.neo4j]
uri = "<neo4j bolt uri>"
password = "<neo4j password>"
EOF
nexusd db migration run
```

Both migrations are idempotent. The review backfill treats a missing review
directory or record (homeserver 404) as gone. If a user's homeserver cannot be
reached while its review directory is listed, that user is logged and counted
as `unreachable` but skipped so one offline homeserver cannot permanently block
this migration or the migrations after it. Record-level fetch, parse, and
ingest failures still fail the migration and are retried on the next run;
already-indexed reviews are skipped. The watcher does not need to be stopped.
