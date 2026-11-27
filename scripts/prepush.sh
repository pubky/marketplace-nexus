#!/usr/bin/env bash
# Portable pre-push gate (Linux and macOS).
# Last line on success: PREPUSH OK <sha> <seconds> <mode>
#
# fast (default): cargo fmt --check, then cargo check --all-targets for the
#   packages the change touches since the merge base (scripts/prepush-packages.sh).
#   A change to a workspace-wide file checks the whole workspace; a change to
#   no package (docs, CI, scripts) runs fmt only. Requires no Docker, Redis,
#   Neo4j, or machine-specific paths.
# full (PREPUSH_FULL=1): cargo fmt --check, cargo clippy --workspace --all-targets,
#   mock DB load, and the full parallel cargo nextest suite matching
#   .github/workflows/test.yml.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

start=$(date +%s)
# shellcheck source=prepush-stamp.sh
source "$ROOT/scripts/prepush-stamp.sh"
prepush_mode_init || exit 1
echo "prepush: mode ${PREPUSH_MODE}"

if [ ! -t 0 ]; then
  skip=1
  saw=0
  while read -r _local_ref local_sha _remote_ref remote_sha; do
    [ -n "${local_sha:-}" ] || continue
    saw=1
    if [ "$local_sha" = "0000000000000000000000000000000000000000" ]; then
      continue
    fi
    if [ "$remote_sha" = "0000000000000000000000000000000000000000" ]; then
      range="$local_sha"
    else
      range="${remote_sha}..${local_sha}"
    fi
    # grep -q under pipefail exits 141 when git log is still writing,
    # and the gate then skips every push. Read the subjects first.
    subjects="$(git log --format=%s "$range" || true)"
    if printf '%s\n' "$subjects" | grep -v '\[skip ci\]' >/dev/null; then
      skip=0
    fi
  done
  if [ "$saw" = 1 ] && [ "$skip" = 1 ]; then
    echo "prepush: every commit has [skip ci]; gate not run"
    exit 0
  fi
fi

sha="$(git rev-parse HEAD)"
if prepush_reuse "$sha"; then
  exit 0
fi

if [ -n "${PREPUSH_CARGO_TARGET:-}" ]; then
  export CARGO_TARGET_DIR="$PREPUSH_CARGO_TARGET"
fi

if [ "$PREPUSH_MODE" = fast ]; then
  packages=()
  base=""
  if [ -n "${PREPUSH_BASE:-}" ]; then
    base="$PREPUSH_BASE"
  elif git rev-parse --verify --quiet origin/main >/dev/null; then
    base="$(git merge-base HEAD origin/main)"
  elif git rev-parse --verify --quiet main >/dev/null; then
    base="$(git merge-base HEAD main)"
  fi

  if [ -z "$base" ]; then
    packages=(--workspace)
  else
    # shellcheck source=prepush-packages.sh
    source "$ROOT/scripts/prepush-packages.sh"
    # docker/ holds the test env (.env-sample) and the graph `db mock` loads.
    selected="$(prepush_changed_packages "$base" \
      Cargo.toml Cargo.lock rust-toolchain rust-toolchain.toml '.cargo/*' \
      clippy.toml .clippy.toml rustfmt.toml .rustfmt.toml \
      'docker/*')" || {
      echo "prepush: cannot diff against base ${base}" >&2
      exit 1
    }
    while IFS= read -r line; do
      [ -n "$line" ] && packages+=("$line")
    done <<< "$selected"
  fi

  if [ "${#packages[@]}" -eq 0 ]; then
    echo "prepush: packages (none changed)"
  else
    echo "prepush: packages ${packages[*]}"
  fi

  echo "prepush: cargo fmt"
  cargo fmt --check

  if [ "${#packages[@]}" -eq 0 ]; then
    echo "prepush: cargo check skipped (no package changed)"
  else
    check_scope=()
    if [ "${packages[0]}" = --workspace ]; then
      check_scope=(--workspace)
    else
      for pkg in "${packages[@]}"; do
        check_scope+=(-p "$pkg")
      done
    fi
    echo "prepush: cargo check"
    cargo check "${check_scope[@]}" --all-targets
  fi

  prepush_stamp "$sha"
  seconds="$(( $(date +%s) - start ))"
  echo "PREPUSH OK ${sha} ${seconds} ${PREPUSH_MODE}"
  exit 0
fi

# Full mode (PREPUSH_FULL=1): clippy and full parallel nextest suite like CI.
# shellcheck source=heavy-lock.sh
source "$ROOT/scripts/heavy-lock.sh"

port_open() {
  local port="$1"
  if command -v nc >/dev/null 2>&1; then
    nc -z 127.0.0.1 "$port" >/dev/null 2>&1
  else
    (exec 3<>"/dev/tcp/127.0.0.1/${port}") >/dev/null 2>&1
  fi
}

echo "prepush: packages --workspace"
echo "prepush: cargo fmt"
cargo fmt --check

if ! cargo nextest --version >/dev/null 2>&1; then
  echo "prepush: cargo-nextest is required for PREPUSH_FULL=1" >&2
  exit 1
fi

if ! docker info >/dev/null 2>&1; then
  echo "prepush: docker is required for the scram-sha-256 Postgres" >&2
  exit 1
fi

set -a
# shellcheck disable=SC1091
. docker/.env-sample
set +a

# A fresh container and data volume for this worktree every gate. Tests
# create a database per run, and a long-lived container kept thousands.
# shellcheck source=prepush-pg.sh
source "$ROOT/scripts/prepush-pg.sh"
nexus_pg_legacy_cleanup
name="${PREPUSH_PG_CONTAINER:-$(nexus_pg_name "$ROOT")}"
port="$(nexus_pg_recreate "$name" "$ROOT" "${PREPUSH_PG_PORT:-}")"
echo "prepush: postgres ${name} on 127.0.0.1:${port}"
export POSTGRES_PORT="$port"
export TEST_PUBKY_CONNECTION_STRING="postgres://${POSTGRES_USER}:${POSTGRES_PASSWORD}@127.0.0.1:${port}/${POSTGRES_DB}?pubky-test=true"

ready=0
for _ in $(seq 1 60); do
  if docker exec -e PGPASSWORD="$POSTGRES_PASSWORD" "$name" \
    psql -h 127.0.0.1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tAc 'SELECT 1' >/dev/null 2>&1; then
    ready=1
    break
  fi
  sleep 1
done
if [ "$ready" != 1 ]; then
  echo "prepush: Postgres did not become ready" >&2
  exit 1
fi

encryption="$(docker exec -e PGPASSWORD="$POSTGRES_PASSWORD" "$name" psql -h 127.0.0.1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tAc 'SHOW password_encryption' | tr -d '[:space:]')"
host_all="$(docker exec -e PGPASSWORD="$POSTGRES_PASSWORD" "$name" psql -h 127.0.0.1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tAc "SELECT auth_method FROM pg_hba_file_rules WHERE type = 'host' AND address = 'all'" | tr -d '[:space:]')"
if [ "$encryption" != "scram-sha-256" ] || [ "$host_all" != "scram-sha-256" ]; then
  echo "prepush: Postgres is not scram-sha-256 like CI (encryption=${encryption} host_all=${host_all})" >&2
  exit 1
fi

if ! port_open 6379; then
  echo "prepush: Redis is not accepting connections on 127.0.0.1:6379" >&2
  exit 1
fi

# db mock runs /test-graph/run-queries.sh via `docker exec neo4j`.
neo_name="${PREPUSH_NEO4J_CONTAINER:-neo4j}"
if ! port_open 7687; then
  if docker ps -a --format '{{.Names}}' | grep -qx "$neo_name"; then
    echo "prepush: Neo4j port 7687 is closed and container name ${neo_name} is already taken" >&2
    exit 1
  fi
  docker run -d --name "$neo_name" \
    -e NEO4J_AUTH="${NEO4J_DB_USERNAME}/${NEO4J_PASSWORD}" \
    -e NEO4J_server_memory_pagecache_size=1G \
    -e NEO4J_server_memory_heap_initial__size=2G \
    -e NEO4J_server_memory_heap_max__size=2G \
    -e NEO4J_dbms_usage__report_enabled=false \
    -e NEO4J_client_allow__telemetry=false \
    -p 127.0.0.1:7474:7474 \
    -p 127.0.0.1:7687:7687 \
    -v "$ROOT/docker/test-graph:/test-graph:ro" \
    neo4j:5.26.20-community >/dev/null
fi

neo_ready=0
for _ in $(seq 1 90); do
  if curl -sf http://127.0.0.1:7474 >/dev/null 2>&1; then
    neo_ready=1
    break
  fi
  sleep 2
done
if [ "$neo_ready" != 1 ]; then
  echo "prepush: Neo4j did not become ready" >&2
  exit 1
fi

test_cmd="cargo run -p nexusd -- db mock"
test_cmd+=" && cargo nextest run -p nexus-common --no-fail-fast"
test_cmd+=" && cargo nextest run -p nexus-webapi --no-fail-fast"
test_cmd+=" && cargo nextest run -p nexusd --no-fail-fast"
test_cmd+=" && cargo nextest run -p nexus-watcher --no-fail-fast"

echo "prepush: cargo clippy"
# Same lint set as .github/workflows/lint.yml.
run_heavy cargo cargo clippy --workspace --all-targets -- -D warnings -A clippy::double_must_use

echo "prepush: cargo nextest"
echo "  ${test_cmd}"
run_heavy cargo bash -c "$test_cmd"

prepush_stamp "$sha"
seconds="$(( $(date +%s) - start ))"
echo "PREPUSH OK ${sha} ${seconds} ${PREPUSH_MODE}"
