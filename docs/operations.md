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

### Attestor records: the attestor's homeserver must be polled

The attestor publishes its profile
(`/pub/pubky.app/marketplace/v1/attestor/profile`) and one annotation per
attested order (`/pub/pubky.app/marketplace/v1/attestor/annotations/{order_ref}`)
on its own homeserver. Like every record, they are indexed only from a
homeserver the watcher polls: with `[stack] allowed_homeservers` set to the
deployment's own homeserver, an attestor whose account lives on any other
homeserver is never indexed, and `/v0/marketplace/attestor/...` answers 404 and
`null` entries for it.

There is no backfill for these records. A watcher older than the release that
indexes them skips their events, so the release must be live before the
attestor starts publishing; a record published earlier is indexed only when the
attestor publishes it again.

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
backfill_ready = []
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

`backfill_ready` is empty in the sample on purpose. It moves a multi-staged
migration from `dual_write` to `backfill`, once, and does nothing else: a listed
id whose stored migration is at any other phase (`backfill`, `cutover`,
`cleanup` or `done`) is left alone, and an id the database does not know is
ignored. Each ignored id is logged with its phase, so the log says why nothing
ran. A single-staged migration starts at `backfill`, so listing one does
nothing. Migrations that are not listed run once, when they are first seen, and
are then done.

Both migrations are idempotent. The review backfill treats a missing review
directory or record (homeserver 404) as gone. If a user's homeserver cannot be
reached while its review directory is listed, that user is logged and counted
as `unreachable` but skipped so one offline homeserver cannot permanently block
this migration or the migrations after it. Record-level fetch, parse, and
ingest failures still fail the migration and are retried on the next run;
already-indexed reviews are skipped. The watcher does not need to be stopped.

## Running the review backfills at a controlled rate

`ReviewBackfill1787905961` and `ReviewTrustBucketsBackfill1792587206` read from
every reviewer's homeserver as fast as they can, with no cap and no way to stop
and continue. On a deployment with many reviewers, run the same work with the
throttled command instead:

```bash
nexusd db backfill reviews --job review-trust-buckets --dry-run
nexusd db backfill reviews --job review-trust-buckets --rate 2 --max-duration 2h
```

The command reads the same `~/.pubky-nexus/migrations/config.toml` as
`migration run` and needs the same access. It is not a migration: it is not
registered with `migration run`, and putting its job ids in `backfill_ready`
does nothing.

Jobs (`--job`):

- `review-trust-buckets` re-reads each verified review indexed without its
  attestation `iat` (one request per review), through the normal ingest, then
  recomputes every review aggregate once. The same work as
  `ReviewTrustBucketsBackfill1792587206`.
- `review-discovery` lists each indexed user's reviews directory and ingests
  the records the graph lacks, the same work as `ReviewBackfill1787905961`.

Flags:

| Flag | Default | Meaning |
| --- | --- | --- |
| `--rate` | `2` | Homeserver requests per second across the whole run: every record GET and every directory LIST page counts. Above 0, at most 20. |
| `--concurrency` | `1` | Requests in flight at once, 1 to 4. |
| `--max-requests` | none | Stop cleanly (status `paused`) after this many requests. |
| `--max-duration` | none | Stop cleanly (status `paused`) after this long: `90s`, `45m`, `2h`. |
| `--max-consecutive-errors` | `20` | Abort (status `aborted`) after this many failed requests in a row. Failed requests back off from 1 s up to 60 s, with jitter. |
| `--aggregate-pause-ms` | `50` | Pause between aggregate recomputes, which only touch the databases. |
| `--dry-run` | off | Print the work left and the least time it takes at the chosen rate. Sends no request and writes nothing. |
| `--force-rerun` | off | Start a job that is already `done` again from the beginning. It never takes a lease another runner holds. |

Behavior:

- Once only. A job's state (status, cursor, counters) is kept in the graph. A
  finished job answers "already done" and sends nothing; `--force-rerun` is the
  way to run it again.
- Resumable. The cursor is saved after every item. After a pause, an abort, a
  crash or an interrupt (Ctrl-C ends the run cleanly as `paused`), run the same
  command again: it continues after the last finished item and never asks again
  for one already finished. An item that was cut short is started again.
- One runner at a time. The run holds a lease it renews while it works. A
  second runner is refused ("another runner holds the lease") while the lease is
  live; a crashed runner's lease expires after 60 seconds and the next run takes
  over.
- Unreachable and gone records are counted and not retried: a review or
  directory the homeserver answers 404 for is `gone`, a homeserver that cannot
  be reached is `unreachable`. Both leave the review out of the buckets until
  its next edit, as in the migrations.
- Concurrent feed changes are respected: a review that the watcher deleted or
  edited after the backfill read it is never overwritten with the stale
  record, and a deleted review is not brought back.

Exit codes:

| Code | Meaning |
| --- | --- |
| `0` | The job is done, already done, or paused by a budget or an interrupt. |
| `1` | Nothing ran: another runner holds the lease, the flags were refused, or the databases could not be reached. |
| `2` | The run aborted: the error breaker tripped, the lease was lost, or a database failed. The cursor is kept. |
| `3` | The job is done, but some records failed (`failed` above 0 in the totals). |

Scripts should treat `0` as "run it again if the job is not done yet" only for
a `paused` run (the last line printed says which), and any other nonzero code as
a stop that needs a look at the log.
