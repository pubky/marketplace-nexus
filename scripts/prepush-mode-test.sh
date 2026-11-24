#!/usr/bin/env bash
# Proves the fast and full modes of scripts/prepush.sh run the right steps,
# and that scripts/prepush-packages.sh maps a change to its packages.
# Runs the real scripts in a throwaway workspace. cargo, docker, nc and curl
# are stand-ins on PATH that log their arguments; metadata and clean go to the
# real cargo, and nothing is built or started.
set -euo pipefail

DIR="$(cd "$(dirname "$0")" && pwd)"
WORK="$(mktemp -d "${TMPDIR:-/tmp}/prepush-mode-test.XXXXXX")"
trap 'rm -rf "$WORK"' EXIT
# A caller's CARGO_TARGET_DIR would make the gate seed its private target by
# copying that directory; a caller's override would replace every case's target.
unset CARGO_TARGET_DIR
unset PREPUSH_CARGO_TARGET
unset PREPUSH_FULL

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

REAL_CARGO="$(command -v cargo)"
export REAL_CARGO
mkdir -p "$WORK/bin" "$WORK/broken"
# metadata and clean go to the real cargo, so package widening reads a real
# graph and member cleaning really removes copied workspace artifacts.
cat > "$WORK/bin/cargo" <<'EOF'
#!/usr/bin/env bash
if [ "${1:-}" = metadata ]; then
  if [ "${FAKE_METADATA_NO_DEPS_FAIL:-0}" = 1 ]; then
    for arg in "$@"; do
      [ "$arg" != --no-deps ] || exit 101
    done
  fi
  exec "$REAL_CARGO" "$@"
fi
printf '%s\n' "$*" >> "$FAKE_LOG/cargo"
if [ "${1:-}" = clean ]; then
  if [ "${FAKE_CLEAN_REQUIRE_NO_MARKER:-0}" = 1 ] && [ -e "${CARGO_TARGET_DIR:?}/.prepush-members-clean" ]; then
    echo 'fake cargo: clean found a copied marker' >&2
    exit 102
  fi
  if [ "${FAKE_CLEAN_FAIL:-0}" = 1 ]; then
    echo 'fake cargo: clean failed' >&2
    exit 101
  fi
  exec "$REAL_CARGO" "$@"
fi
EOF
cat > "$WORK/bin/docker" <<'EOF'
#!/usr/bin/env bash
printf '%s\n' "$*" >> "$FAKE_LOG/docker"
case "$*" in
  *password_encryption*|*pg_hba_file_rules*) echo scram-sha-256 ;;
esac
exit 0
EOF
for tool in nc curl; do
  printf '#!/usr/bin/env bash\nprintf "%%s\\n" "$*" >> "$FAKE_LOG/%s"\n' "$tool" > "$WORK/bin/$tool"
done
printf '#!/usr/bin/env bash\nexit 101\n' > "$WORK/broken/cargo"
chmod +x "$WORK/bin/"* "$WORK/broken/cargo"

# A real workspace with this repository's member graph: webapi and watcher
# depend on common; nexusd and the examples depend on all three.
manifest() {
  local dir="$1" name="$2" dep
  shift 2
  mkdir -p "$dir/src"
  printf '[package]\nname = "%s"\nversion = "0.1.0"\nedition = "2021"\n\n[dependencies]\n' "$name" > "$dir/Cargo.toml"
  for dep in "$@"; do
    printf '%s = { path = "../%s" }\n' "$dep" "$dep" >> "$dir/Cargo.toml"
  done
  printf '// lib\n' > "$dir/src/lib.rs"
  printf '// extra\n' > "$dir/src/extra.rs"
}

git init -q "$WORK/ws"
cd "$WORK/ws"
git config user.email test@example.invalid
git config user.name test
printf '[workspace]\nresolver = "2"\nmembers = ["nexus-webapi", "nexus-common", "nexus-watcher", "nexusd", "examples"]\n' > Cargo.toml
manifest nexus-common nexus-common
manifest nexus-webapi nexus-webapi nexus-common
manifest nexus-watcher nexus-watcher nexus-common
manifest nexusd nexusd nexus-common nexus-webapi nexus-watcher
mkdir -p examples/src
printf '[package]\nname = "nexus-examples"\nversion = "0.1.0"\nedition = "2021"\n\n[dependencies]\nnexus-common = { path = "../nexus-common" }\nnexus-webapi = { path = "../nexus-webapi" }\nnexus-watcher = { path = "../nexus-watcher" }\n' > examples/Cargo.toml
printf '// lib\n' > examples/src/lib.rs
"$REAL_CARGO" generate-lockfile --offline --quiet
mkdir -p docs docker/test-graph scripts
printf 'doc\n' > docs/readme.md
printf 'CREATE (n);\n' > docker/test-graph/mocks.cypher
cat > docker/.env-sample <<'EOF'
POSTGRES_USER=test_user
POSTGRES_PASSWORD=test_pass
POSTGRES_DB=postgres
NEO4J_DB_USERNAME=neo4j
NEO4J_PASSWORD=12345678
EOF
cp "$DIR/prepush.sh" "$DIR/prepush-stamp.sh" "$DIR/prepush-packages.sh" \
  "$DIR/prepush-pg.sh" "$DIR/heavy-lock.sh" scripts/
printf '/target\n' > .gitignore
git add -A
git commit -qm base
BASE="$(git rev-parse HEAD)"

# shellcheck source=prepush-packages.sh
source "$DIR/prepush-packages.sh"
pkgs() {
  prepush_changed_packages "$BASE" Cargo.toml Cargo.lock 'docker/*' | tr '\n' ' ' | sed 's/ $//'
}
reset_ws() {
  git reset -q --hard "$BASE"
  git clean -qfd
}
all_five="nexus-common nexus-examples nexus-watcher nexus-webapi nexusd"

echo "test: packages: no change selects nothing"
[ -z "$(pkgs)" ] || fail "clean tree: $(pkgs)"

echo "test: packages: a nexus-common change selects every package built on it"
printf '// x\n' >> nexus-common/src/lib.rs
git commit -qam common
[ "$(pkgs)" = "$all_five" ] || fail "common: $(pkgs)"
reset_ws

echo "test: packages: a watcher change adds nexusd and the examples, not webapi"
printf '// x\n' >> nexus-watcher/src/lib.rs
[ "$(pkgs)" = "nexus-examples nexus-watcher nexusd" ] || fail "watcher: $(pkgs)"
reset_ws

echo "test: packages: a webapi change adds nexusd and the examples, not the watcher"
printf '// x\n' >> nexus-webapi/src/lib.rs
[ "$(pkgs)" = "nexus-examples nexus-webapi nexusd" ] || fail "webapi: $(pkgs)"
reset_ws

echo "test: packages: nexusd and examples changes select only themselves"
printf '// x\n' >> nexusd/src/lib.rs
git add nexusd/src/lib.rs
printf '// x\n' >> examples/src/lib.rs
[ "$(pkgs)" = "nexus-examples nexusd" ] || fail "nexusd and examples: $(pkgs)"
reset_ws

echo "test: packages: an untracked file alone selects its package and dependents"
printf '// new\n' > nexus-watcher/src/new.rs
[ "$(pkgs)" = "nexus-examples nexus-watcher nexusd" ] || fail "untracked: $(pkgs)"
reset_ws

echo "test: packages: deletes and both sides of a rename count"
git mv nexusd/src/extra.rs nexus-webapi/src/moved.rs
[ "$(pkgs)" = "nexus-examples nexus-webapi nexusd" ] || fail "rename: $(pkgs)"
reset_ws

echo "test: packages: docs and scripts select nothing"
printf 'more\n' >> docs/readme.md
printf 'echo\n' >> scripts/prepush.sh
[ -z "$(pkgs)" ] || fail "docs and scripts: $(pkgs)"
reset_ws

echo "test: packages: docker/ and the root manifests select --workspace"
for file in docker/test-graph/mocks.cypher docker/.env-sample Cargo.toml Cargo.lock; do
  printf '\n' >> "$file"
  [ "$(pkgs)" = --workspace ] || fail "$file: $(pkgs)"
  reset_ws
done

echo "test: packages: an unreadable package graph selects --workspace"
printf '// x\n' >> nexusd/src/lib.rs
got="$(PATH="$WORK/broken:$PATH" pkgs 2>/dev/null)"
[ "$got" = --workspace ] || fail "failing cargo metadata: $got"
reset_ws

echo "test: packages: widening is transitive and counts dev-dependencies"
(
  git init -q "$WORK/chain"
  cd "$WORK/chain"
  printf '[workspace]\nresolver = "2"\nmembers = ["a", "b", "c", "d", "e"]\n' > Cargo.toml
  for spec in "a:" "b:a" "c:b" "e:"; do
    name="${spec%%:*}"
    dep="${spec#*:}"
    mkdir -p "$name/src"
    printf '[package]\nname = "%s"\nversion = "0.1.0"\nedition = "2021"\n' "$name" > "$name/Cargo.toml"
    [ -z "$dep" ] || printf '\n[dependencies]\n%s = { path = "../%s" }\n' "$dep" "$dep" >> "$name/Cargo.toml"
    printf '// lib\n' > "$name/src/lib.rs"
  done
  mkdir -p d/src
  printf '[package]\nname = "d"\nversion = "0.1.0"\nedition = "2021"\n\n[dev-dependencies]\na = { path = "../a" }\n' > d/Cargo.toml
  printf '// lib\n' > d/src/lib.rs
  "$REAL_CARGO" generate-lockfile --offline --quiet
  got="$(prepush_with_dependents a | tr '\n' ' ' | sed 's/ $//')"
  [ "$got" = "a b c d" ] || fail "chain from a: $got"
  got="$(prepush_with_dependents c e | tr '\n' ' ' | sed 's/ $//')"
  [ "$got" = "c e" ] || fail "leaves: $got"
)

echo "test: packages: a base git cannot read fails"
if prepush_changed_packages 0000000000000000000000000000000000000001 >/dev/null 2>&1; then
  fail "unknown base returned 0"
fi

echo "test: packages: this repository's graph widens nexus-common to every package"
got="$(cd "$DIR/.." && prepush_with_dependents nexus-common | tr '\n' ' ' | sed 's/ $//')"
[ "$got" = "$all_five" ] || fail "repository graph: $got"

# The real gate. Each case commits one change on top of the base, so the
# selection is that change alone and no stamp is reused.
gate() {
  rm -rf "$WORK/log"
  mkdir -p "$WORK/log"
  local st=0
  FAKE_LOG="$WORK/log" PATH="$WORK/bin:$PATH" PREPUSH_BASE="$BASE" \
    PREPUSH_CARGO_TARGET="${PREPUSH_CARGO_TARGET:-$WORK/target}" PREPUSH_PG_PORT=55999 \
    NEXUS_PG_LEGACY_NAME="prepush-mode-test-legacy-$$" \
    HEAVY_LOCK_DIR="$WORK/locks" HEAVY_DISK_VOLUMES="" \
    bash scripts/prepush.sh </dev/null >"$WORK/out" 2>&1 || st=$?
  printf '%s\n' "$st"
}
cargo_log() { cat "$WORK/log/cargo" 2>/dev/null || true; }
expect_ok() {
  [ "$1" = 0 ] || fail "$2: status $1: $(cat "$WORK/out")"
  tail -1 "$WORK/out" | grep -q "^PREPUSH OK $(git rev-parse HEAD) [0-9]* $3$" \
    || fail "$2: last line: $(tail -1 "$WORK/out")"
}
commit_change() {
  reset_ws
  printf '// %s %s\n' "$RANDOM" "$(date +%s)" >> "$1"
  git commit -qam "change $1"
}
expected_clean='clean -p nexus-webapi -p nexus-common -p nexus-watcher -p nexusd -p nexus-examples'
actual_members="$("$REAL_CARGO" metadata --no-deps --format-version 1 --locked | jq -r '
  .workspace_members as $members
  | .packages[]
  | select(.id as $id | $members | index($id))
  | .name
')"
[ "$actual_members" = $'nexus-webapi\nnexus-common\nnexus-watcher\nnexusd\nnexus-examples' ] \
  || fail "workspace members changed; update the literal clean expectation: $actual_members"

echo "test: gate: a seeded target cleans every workspace member before the gate"
commit_change nexus-common/src/lib.rs
mkdir -p "$WORK/shared-target/debug/.fingerprint/nexus-common-deadbeef" \
  "$WORK/shared-target/debug/deps"
printf 'registry artifact\n' > "$WORK/shared-target/artifact"
printf 'fingerprint\n' > "$WORK/shared-target/debug/.fingerprint/nexus-common-deadbeef/lib-nexus-common.json"
printf 'member\n' > "$WORK/shared-target/debug/deps/libnexus_common-deadbeef.rlib"
printf 'member\n' > "$WORK/shared-target/debug/deps/nexus_webapi-deadbeef.d"
printf 'registry\n' > "$WORK/shared-target/debug/deps/libserde-deadbeef.rlib"
st="$(CARGO_TARGET_DIR="$WORK/shared-target" PREPUSH_CARGO_TARGET="$WORK/seeded-target" gate)"
expect_ok "$st" "seeded target" fast
[ -f "$WORK/seeded-target/artifact" ] || fail "seeded target: shared artifact was not copied"
[ ! -e "$WORK/seeded-target/debug/.fingerprint/nexus-common-deadbeef" ] \
  || fail "seeded target: common fingerprint was not cleaned"
[ ! -e "$WORK/seeded-target/debug/deps/libnexus_common-deadbeef.rlib" ] \
  || fail "seeded target: common rlib was not cleaned"
[ ! -e "$WORK/seeded-target/debug/deps/nexus_webapi-deadbeef.d" ] \
  || fail "seeded target: webapi dep-info was not cleaned"
[ -f "$WORK/seeded-target/debug/deps/libserde-deadbeef.rlib" ] \
  || fail "seeded target: registry artifact was cleaned"
cargo_log | grep -Fqx "$expected_clean" || fail "seeded target: expected $expected_clean: $(cargo_log)"
[ "$(cargo_log | sed -n '1p')" = "$expected_clean" ] \
  || fail "seeded target: member clean did not precede fmt/clippy/test: $(cargo_log)"
[ -f "$WORK/seeded-target/.prepush-members-clean" ] || fail "seeded target: no clean marker"
[ "$(cat "$WORK/seeded-target/.prepush-members-clean")" = "$PWD" ] \
  || fail "seeded target: marker has the wrong root"

echo "test: gate: seeding follows a shared-target symlink without marking its target"
ln -s "$WORK/shared-target" "$WORK/shared-target-link"
commit_change nexusd/src/lib.rs
st="$(CARGO_TARGET_DIR="$WORK/shared-target-link" PREPUSH_CARGO_TARGET="$WORK/symlink-seeded-target" gate)"
expect_ok "$st" "symlinked shared target" fast
[ ! -L "$WORK/symlink-seeded-target" ] || fail "symlinked shared target: private target is a symlink"
[ -f "$WORK/symlink-seeded-target/artifact" ] || fail "symlinked shared target: artifact was not copied"
[ ! -e "$WORK/shared-target/.prepush-members-clean" ] \
  || fail "symlinked shared target: clean marker landed in the shared directory"
cargo_log | grep -Fqx "$expected_clean" || fail "symlinked shared target: expected $expected_clean: $(cargo_log)"

echo "test: gate: an existing unmarked target survives a failed clean"
mkdir -p "$WORK/uncleanable-target"
printf 'keep\n' > "$WORK/uncleanable-target/sentinel"
commit_change nexus-common/src/lib.rs
st="$(PREPUSH_CARGO_TARGET="$WORK/uncleanable-target" FAKE_CLEAN_FAIL=1 gate)"
[ "$st" -ne 0 ] || fail "failed existing-target clean returned zero: $(cat "$WORK/out")"
[ -d "$WORK/uncleanable-target" ] || fail "failed existing-target clean deleted the directory"
[ -f "$WORK/uncleanable-target/sentinel" ] || fail "failed existing-target clean deleted the sentinel"
grep -Fqx "prepush: cannot clean workspace members in $WORK/uncleanable-target; delete it or set PREPUSH_CARGO_TARGET to a new path" "$WORK/out" \
  || fail "failed existing-target clean: wrong error: $(cat "$WORK/out")"
grep -q '^fake cargo: clean failed$' "$WORK/out" \
  || fail "failed existing-target clean hid cargo stderr: $(cat "$WORK/out")"

echo "test: gate: a copied marker is removed before the seeded clean"
mkdir -p "$WORK/marked-shared-target"
printf '%s\n' "$PWD" > "$WORK/marked-shared-target/.prepush-members-clean"
commit_change nexusd/src/lib.rs
st="$(CARGO_TARGET_DIR="$WORK/marked-shared-target" PREPUSH_CARGO_TARGET="$WORK/copied-marker-target" \
  FAKE_CLEAN_REQUIRE_NO_MARKER=1 gate)"
expect_ok "$st" "copied marker" fast
if grep -q '^fake cargo: clean found a copied marker$' "$WORK/out"; then
  fail "copied marker survived until the seeded clean"
fi
[ "$(cargo_log | grep -Fxc "$expected_clean" || true)" = 1 ] \
  || fail "copied marker skipped the seeded clean: $(cargo_log)"
[ "$(cat "$WORK/copied-marker-target/.prepush-members-clean")" = "$PWD" ] \
  || fail "copied marker: marker has the wrong root"

echo "test: gate: a brand-new target is marked without a clean"
commit_change nexusd/src/lib.rs
st="$(gate)"
expect_ok "$st" "brand-new target" fast
if cargo_log | grep -q '^clean '; then
  fail "brand-new target was cleaned: $(cargo_log)"
fi
[ "$(cat "$WORK/target/.prepush-members-clean")" = "$PWD" ] \
  || fail "brand-new target: marker has the wrong root"

echo "test: gate: a marker from another root causes one clean and is updated"
mkdir -p "$WORK/other-root-target"
printf '/another/worktree\n' > "$WORK/other-root-target/.prepush-members-clean"
commit_change nexus-common/src/lib.rs
st="$(PREPUSH_CARGO_TARGET="$WORK/other-root-target" gate)"
expect_ok "$st" "other-root marker" fast
[ "$(cargo_log | grep -Fxc "$expected_clean" || true)" = 1 ] \
  || fail "other-root marker: expected one clean: $(cargo_log)"
[ "$(cat "$WORK/other-root-target/.prepush-members-clean")" = "$PWD" ] \
  || fail "other-root marker was not updated"

echo "test: gate: an unmarked target is cleaned once"
mkdir -p "$WORK/legacy-target"
commit_change nexus-common/src/lib.rs
st="$(PREPUSH_CARGO_TARGET="$WORK/legacy-target" gate)"
expect_ok "$st" "unmarked target" fast
[ "$(cargo_log | grep -Fxc "$expected_clean" || true)" = 1 ] \
  || fail "unmarked target: expected one member clean: $(cargo_log)"
[ "$(cat "$WORK/legacy-target/.prepush-members-clean")" = "$PWD" ] \
  || fail "unmarked target: marker has the wrong root"
commit_change nexusd/src/lib.rs
st="$(PREPUSH_CARGO_TARGET="$WORK/legacy-target" gate)"
expect_ok "$st" "marked target" fast
if cargo_log | grep -q '^clean '; then
  fail "marked target cleaned again: $(cargo_log)"
fi

echo "test: gate: a seeded target with a failed clean falls back to an empty target"
mkdir -p "$WORK/failing-shared-target"
printf 'discard\n' > "$WORK/failing-shared-target/artifact"
commit_change nexus-common/src/lib.rs
st="$(CARGO_TARGET_DIR="$WORK/failing-shared-target" PREPUSH_CARGO_TARGET="$WORK/failing-seeded-target" \
  FAKE_CLEAN_FAIL=1 gate)"
expect_ok "$st" "seeded clean failure" fast
[ -d "$WORK/failing-seeded-target" ] || fail "seeded clean failure: no fallback directory"
[ ! -e "$WORK/failing-seeded-target/artifact" ] || fail "seeded clean failure retained the seeded artifact"
[ "$(cat "$WORK/failing-seeded-target/.prepush-members-clean")" = "$PWD" ] \
  || fail "seeded clean failure: fallback marker has the wrong root"
grep -q '^prepush: could not clean workspace members; using an empty target$' "$WORK/out" \
  || fail "seeded clean failure: no fallback explanation: $(cat "$WORK/out")"
grep -q '^fake cargo: clean failed$' "$WORK/out" \
  || fail "seeded clean failure hid cargo stderr: $(cat "$WORK/out")"

echo "test: gate: a member metadata failure falls back to an empty target"
commit_change nexus-common/src/lib.rs
st="$(CARGO_TARGET_DIR="$WORK/shared-target" PREPUSH_CARGO_TARGET="$WORK/failed-target" \
  FAKE_METADATA_NO_DEPS_FAIL=1 gate)"
expect_ok "$st" "metadata failure" fast
[ ! -e "$WORK/failed-target/artifact" ] || fail "metadata failure retained the seeded artifact"
[ "$(cat "$WORK/failed-target/.prepush-members-clean")" = "$PWD" ] \
  || fail "metadata failure: fallback marker has the wrong root"
grep -q '^prepush: could not clean workspace members; using an empty target$' "$WORK/out" \
  || fail "metadata failure: no fallback explanation: $(cat "$WORK/out")"

echo "test: gate: fast tests only nexusd for a nexusd change"
commit_change nexusd/src/lib.rs
st="$(gate)"
expect_ok "$st" "nexusd fast" fast
[ "$(cargo_log)" = "fmt --check
clippy -p nexusd --all-targets -- -D warnings -A clippy::double_must_use
run -p nexusd -- db mock
test -p nexusd --lib --bins --tests --no-fail-fast -- --test-threads=1" ] \
  || fail "nexusd fast: cargo calls: $(cargo_log)"

echo "test: gate: fast widens a watcher change to nexusd and the examples"
commit_change nexus-watcher/src/lib.rs
st="$(gate)"
expect_ok "$st" "watcher fast" fast
[ "$(cargo_log)" = "fmt --check
clippy -p nexus-examples -p nexus-watcher -p nexusd --all-targets -- -D warnings -A clippy::double_must_use
run -p nexusd -- db mock
test -p nexus-examples -p nexusd --lib --bins --tests --no-fail-fast -- --test-threads=1
nextest run -p nexus-watcher --no-fail-fast -j 1" ] \
  || fail "watcher fast: cargo calls: $(cargo_log)"

echo "test: gate: fast widens a nexus-common change to every package, by name"
commit_change nexus-common/src/lib.rs
st="$(gate)"
expect_ok "$st" "common fast" fast
[ "$(cargo_log)" = "fmt --check
clippy -p nexus-common -p nexus-examples -p nexus-watcher -p nexus-webapi -p nexusd --all-targets -- -D warnings -A clippy::double_must_use
run -p nexusd -- db mock
test -p nexus-common -p nexus-examples -p nexus-webapi -p nexusd --lib --bins --tests --no-fail-fast -- --test-threads=1
nextest run -p nexus-watcher --no-fail-fast -j 1" ] \
  || fail "common fast: cargo calls: $(cargo_log)"

echo "test: gate: fast runs fmt only, and starts nothing, when no package changed"
commit_change docs/readme.md
st="$(gate)"
expect_ok "$st" "docs fast" fast
[ "$(cargo_log)" = 'fmt --check' ] || fail "docs fast: cargo calls: $(cargo_log)"
for tool in docker nc curl; do
  [ ! -e "$WORK/log/$tool" ] || fail "docs fast called $tool: $(cat "$WORK/log/$tool")"
done

echo "test: gate: fmt-only exits before creating or cleaning a fresh target"
commit_change docs/readme.md
st="$(PREPUSH_CARGO_TARGET="$WORK/fmt-only-target" FAKE_CLEAN_FAIL=1 gate)"
expect_ok "$st" "fmt-only fresh target" fast
[ "$(cargo_log)" = 'fmt --check' ] || fail "fmt-only fresh target: cargo calls: $(cargo_log)"
[ ! -e "$WORK/fmt-only-target" ] || fail "fmt-only fresh target: target path was created"

workspace_calls="fmt --check
clippy --workspace --all-targets -- -D warnings -A clippy::double_must_use
run -p nexusd -- db mock
test --workspace --exclude nexus-watcher --lib --bins --tests --no-fail-fast -- --test-threads=1
nextest run -p nexus-watcher --no-fail-fast -j 1"

echo "test: gate: fast runs the workspace for a docker/ change"
commit_change docker/test-graph/mocks.cypher
st="$(gate)"
expect_ok "$st" "docker fast" fast
[ "$(cargo_log)" = "$workspace_calls" ] || fail "docker fast: cargo calls: $(cargo_log)"

echo "test: gate: PREPUSH_FULL=1 runs the workspace and does not reuse a fast pass"
commit_change docs/readme.md
st="$(gate)"
expect_ok "$st" "docs fast before full" fast
st="$(PREPUSH_FULL=1 gate)"
expect_ok "$st" "docs full" full
[ "$(cargo_log)" = "$workspace_calls" ] || fail "docs full: cargo calls: $(cargo_log)"

echo "test: gate: a fast run after a full pass reuses it"
commit_change docs/readme.md
st="$(PREPUSH_FULL=1 gate)"
expect_ok "$st" "full before fast" full
st="$(gate)"
expect_ok "$st" "fast after full" "fast reused:full .*"
[ -z "$(cargo_log)" ] || fail "fast after full ran cargo: $(cargo_log)"

echo "test: gate: PREPUSH_FULL=yes is refused before any step"
commit_change docs/readme.md
st="$(PREPUSH_FULL=yes gate)"
[ "$st" = 1 ] || fail "PREPUSH_FULL=yes: status $st"
grep -q 'PREPUSH_FULL must be 1, 0 or unset' "$WORK/out" || fail "PREPUSH_FULL=yes: $(cat "$WORK/out")"
[ -z "$(cargo_log)" ] || fail "PREPUSH_FULL=yes ran cargo: $(cargo_log)"

echo "ALL OK"
