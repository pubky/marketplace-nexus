#!/bin/sh
# Fail if entrypoint-railway.sh stdout/stderr contains URL userinfo (://user:pass@).
# The on-disk config must still receive the real Redis URL and Neo4j password.
set -eu

ROOT="$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)"
ENTRYPOINT="$ROOT/entrypoint-railway.sh"
test -f "$ENTRYPOINT"

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

mkdir -p "$TMP/bin" "$TMP/data"
printf '%s\n' '#!/bin/sh' 'exit 0' > "$TMP/bin/nexusd"
chmod +x "$TMP/bin/nexusd"

export PATH="$TMP/bin:$PATH"
export NEXUS_CONFIG_DIR="$TMP/data"
export NEXUS_REDIS_URL='redis://ci-user:ci-super-secret@redis.example:6379/0'
export NEXUS_NEO4J_PASSWORD='neo4j-super-secret'
export NEXUS_NEO4J_URI='bolt://neo4j.example:7687'

OUT="$TMP/out.txt"
# Do not print captured output: it is the secret-bearing fixture path on failure.
if ! sh "$ENTRYPOINT" >"$OUT" 2>&1; then
	echo "FAIL: entrypoint exited non-zero" >&2
	exit 1
fi

if grep -Eq '://[^/]*:[^@]*@' "$OUT"; then
	echo "FAIL: entrypoint output matched ://user:pass@ userinfo" >&2
	exit 1
fi

if grep -Fqe 'ci-super-secret' "$OUT" || grep -Fqe 'neo4j-super-secret' "$OUT" || grep -Fqe 'ci-user' "$OUT"; then
	echo "FAIL: entrypoint output contained fixture secret" >&2
	exit 1
fi

CFG="$TMP/data/config.toml"
test -f "$CFG"
if ! grep -Fqe 'redis://ci-user:ci-super-secret@redis.example:6379/0' "$CFG"; then
	echo "FAIL: on-disk config lost the Redis URL" >&2
	exit 1
fi
if ! grep -Fqe 'neo4j-super-secret' "$CFG"; then
	echo "FAIL: on-disk config lost the Neo4j password" >&2
	exit 1
fi

if ! grep -Fxqe 'moderated_tags = ["moderated"]' "$CFG"; then
	echo "FAIL: on-disk config does not moderate the default label" >&2
	exit 1
fi

if ! grep -Fxqe 'allowed_homeservers = []' "$CFG"; then
	echo "FAIL: on-disk config restricts homeservers without NEXUS_ALLOWED_HOMESERVERS" >&2
	exit 1
fi

NEXUS_ALLOWED_HOMESERVERS=' 8um71us3fyw6h8wbcxb5ar3rwusy1a6u49956ikzojg3gcwd1dty ,0lv!,"ufibwbmed6jeq9k4p583go95wofakh9fwpp4k734trq79pd9u1uy"' sh "$ENTRYPOINT" >"$OUT" 2>&1
if ! grep -Fxqe 'allowed_homeservers = ["8um71us3fyw6h8wbcxb5ar3rwusy1a6u49956ikzojg3gcwd1dty", "ufibwbmed6jeq9k4p583go95wofakh9fwpp4k734trq79pd9u1uy"]' "$CFG"; then
	echo "FAIL: NEXUS_ALLOWED_HOMESERVERS did not produce the sanitized homeserver list" >&2
	exit 1
fi

NEXUS_MODERATED_TAGS=' spam, "bad label",,illegal_activities ' sh "$ENTRYPOINT" >"$OUT" 2>&1
if ! grep -Fxqe 'moderated_tags = ["spam", "badlabel", "illegal_activities"]' "$CFG"; then
	echo "FAIL: NEXUS_MODERATED_TAGS did not produce the sanitized label list" >&2
	exit 1
fi

NEXUS_MODERATED_TAGS='' sh "$ENTRYPOINT" >"$OUT" 2>&1
if ! grep -Fxqe 'moderated_tags = []' "$CFG"; then
	echo "FAIL: an empty NEXUS_MODERATED_TAGS did not turn moderation off" >&2
	exit 1
fi

if ! grep -Fqe 'WARNING: NEXUS_MODERATION_ID is not set' "$OUT"; then
	echo "FAIL: an unset NEXUS_MODERATION_ID did not warn" >&2
	exit 1
fi

NEXUS_MODERATION_ID='uo7jgkykft4885n8cruizwy6khw71mnu5pq3ay9i8pw1ymcn85ko' sh "$ENTRYPOINT" >"$OUT" 2>&1
if ! grep -Fxqe 'moderation_id = "uo7jgkykft4885n8cruizwy6khw71mnu5pq3ay9i8pw1ymcn85ko"' "$CFG"; then
	echo "FAIL: NEXUS_MODERATION_ID did not set the moderator key" >&2
	exit 1
fi
if grep -Fqe 'NEXUS_MODERATION_ID is not set' "$OUT"; then
	echo "FAIL: a set NEXUS_MODERATION_ID still warned" >&2
	exit 1
fi

echo "PASS: entrypoint echo has no URL userinfo; on-disk config keeps credentials"
