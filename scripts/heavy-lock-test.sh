#!/usr/bin/env bash
# Proves queue tickets disappear on INT/TERM, dead waiters and killed holders
# cannot block the queue, concurrent acquirers serialize cleanly, and unsafe
# lock directories or malformed tickets are rejected across all kernel lock
# backends (flock, python3, perl).
set -euo pipefail
# Background jobs ignore SIGINT unless job control is on. The real gate is
# foreground and does receive Ctrl-C. Monitor mode gives each job its own
# process group so the signals under test are delivered.
set -m

DIR="$(cd "$(dirname "$0")" && pwd)"
export HEAVY_LOCK_DIR
HEAVY_LOCK_DIR="$(umask 077 && mktemp -d "${TMPDIR:-/tmp}/heavy-lock-test.XXXXXX")"
# shellcheck source=heavy-lock.sh
source "$DIR/heavy-lock.sh"

pids=""
track() { pids="$pids $1"; }

cleanup() {
  local p
  for p in $pids; do
    kill -9 "$p" 2>/dev/null || true
  done
  wait 2>/dev/null || true
  rm -rf "$HEAVY_LOCK_DIR"
}
trap cleanup EXIT

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

process_gone() {
  local pid="$1" state
  if ! kill -0 "$pid" 2>/dev/null; then
    return 0
  fi
  state=$(ps -o stat= -p "$pid" 2>/dev/null | tr -d '[:space:]' || true)
  case "$state" in
    Z*) return 0 ;;
  esac
  return 1
}

ticket_for() {
  local pid="$1" f pid2 _epoch
  for f in "$HEAVY_LOCK_DIR"/vrt.q/*.ticket; do
    [ -e "$f" ] || continue
    pid2=""
    _epoch=""
    read -r pid2 _epoch < "$f" || true
    if [ "$pid2" = "$pid" ]; then
      printf '%s\n' "$f"
      return 0
    fi
  done
  return 1
}

wait_for() {
  local file="$1" needle="$2" _i
  for _i in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25; do
    if [ -f "$file" ] && grep -q "$needle" "$file"; then
      return 0
    fi
    sleep 0.2
  done
  return 1
}

reset_lock_dir() {
  rm -rf "$HEAVY_LOCK_DIR"
  (umask 077 && mkdir -p "$HEAVY_LOCK_DIR")
}

# One VRT slot. The holder occupies it. Two waiters queue. Killing the first
# must drop its ticket, and the second must run once the holder finishes.
interrupt_and_proceed() {
  local sig="$1" label="$2"
  local logdir holder w1 w2 t1 deadline
  reset_lock_dir
  logdir="$HEAVY_LOCK_DIR/logs"
  mkdir -p "$logdir"

  # shellcheck disable=SC2016
  run_heavy vrt bash -c 'printf hold > "$1"; sleep 4' _ "$logdir/holder" \
    >"$logdir/holder.out" 2>"$logdir/holder.err" &
  holder=$!
  track "$holder"
  wait_for "$logdir/holder" hold || fail "$label: holder did not start"

  # shellcheck disable=SC2016
  run_heavy vrt bash -c 'printf ran > "$1"' _ "$logdir/w1" \
    >"$logdir/w1.out" 2>"$logdir/w1.err" &
  w1=$!
  track "$w1"
  # shellcheck disable=SC2016
  run_heavy vrt bash -c 'printf ran > "$1"' _ "$logdir/w2" \
    >"$logdir/w2.out" 2>"$logdir/w2.err" &
  w2=$!
  track "$w2"
  wait_for "$logdir/w1.err" waiting || fail "$label: w1 did not wait"
  wait_for "$logdir/w2.err" waiting || fail "$label: w2 did not wait"
  t1="$(ticket_for "$w1")" || fail "$label: w1 has no ticket"

  # Signal the job's process group, as Ctrl-C does for a foreground gate.
  kill -s "$sig" -- "-$w1" 2>/dev/null || kill -s "$sig" "$w1" || true
  deadline=$(( $(date +%s) + 3 ))
  while [ -e "$t1" ]; do
    [ "$(date +%s)" -gt "$deadline" ] && fail "$label: ticket remained after $sig"
    sleep 0.1
  done
  deadline=$(( $(date +%s) + 3 ))
  while ! process_gone "$w1"; do
    [ "$(date +%s)" -gt "$deadline" ] && fail "$label: waiter still alive after $sig"
    sleep 0.1
  done
  if [ -f "$logdir/w2" ]; then
    fail "$label: next gate ran while the slot was still held"
  fi

  deadline=$(( $(date +%s) + 12 ))
  while [ ! -f "$logdir/w2" ]; do
    [ "$(date +%s)" -gt "$deadline" ] && fail "$label: next gate did not proceed"
    sleep 0.2
  done
  wait "$holder" || fail "$label: holder failed"
  wait "$w2" || fail "$label: next gate failed"
  wait "$w1" 2>/dev/null || true
  echo "$label ok"
}

run_security_suite() {
  local work pwn target
  work="$(umask 077 && mktemp -d "${TMPDIR:-/tmp}/heavy-lock-sec.XXXXXX")"
  pwn="$work/pwned"

  echo "test: security: refuses symlinked HEAVY_LOCK_DIR"
  (umask 077 && mkdir -p "$work/real")
  ln -s "$work/real" "$work/link"
  if HEAVY_LOCK_DIR="$work/link" run_heavy node true 2>"$work/err"; then
    rm -rf "$work"
    fail "security: accepted symlinked HEAVY_LOCK_DIR"
  fi
  grep -q 'symlink lock directory' "$work/err" || {
    rm -rf "$work"
    fail "security: wrong symlink error: $(cat "$work/err")"
  }

  echo "test: security: refuses group/other-writable HEAVY_LOCK_DIR"
  mkdir -p "$work/world"
  chmod 0777 "$work/world"
  if HEAVY_LOCK_DIR="$work/world" run_heavy node true 2>"$work/err"; then
    rm -rf "$work"
    fail "security: accepted world-writable HEAVY_LOCK_DIR"
  fi
  grep -q 'group/other-writable' "$work/err" || {
    rm -rf "$work"
    fail "security: wrong permission error: $(cat "$work/err")"
  }

  echo "test: security: rejects non-numeric ticket content and filenames without command execution"
  (umask 077 && mkdir -p "$work/safe/node.q")
  # shellcheck disable=SC2016
  printf '%s a[$(touch %s)]\n' "$$" "$pwn" > "$work/safe/node.q/00000000000000000001.ticket"
  # Filename cannot contain '/', so use a relative target while running in $work.
  # shellcheck disable=SC2016
  printf '%s %s\n' "$$" "$(date +%s)" > "$work/safe/node.q/1+a[\$(touch pwned)].ticket"
  target="$work/do-not-clobber"
  printf 'intact\n' > "$target"
  ln -s "$target" "$work/safe/node.q/00000000000000000002.ticket"

  (cd "$work" && HEAVY_LOCK_DIR="$work/safe" run_heavy node true) 2>"$work/err" || {
    rm -rf "$work"
    fail "security: run_heavy failed after adversarial tickets: $(cat "$work/err")"
  }
  [ ! -e "$pwn" ] || {
    rm -rf "$work"
    fail "security: arithmetic command injection executed"
  }
  [ "$(cat "$target")" = "intact" ] || {
    rm -rf "$work"
    fail "security: symlink ticket clobbered target file"
  }
  [ ! -e "$work/safe/node.q/00000000000000000001.ticket" ] || {
    rm -rf "$work"
    fail "security: malformed content ticket was not swept"
  }
  # shellcheck disable=SC2016
  [ ! -e "$work/safe/node.q/1+a[\$(touch pwned)].ticket" ] || {
    rm -rf "$work"
    fail "security: malformed filename ticket was not swept"
  }
  [ ! -L "$work/safe/node.q/00000000000000000002.ticket" ] || {
    rm -rf "$work"
    fail "security: symlink ticket was not removed"
  }

  rm -rf "$work"
  echo "security suite ok"
}

run_backend_suite() {
  local backend="$1"
  export HEAVY_LOCK_BACKEND="$backend"
  echo "=== backend: ${backend} ==="
  reset_lock_dir

  echo "test: command status is preserved"
  set +e
  run_heavy node bash -c 'exit 3'
  st=$?
  set -e
  [ "$st" = 3 ] || fail "${backend}: exit status $st"

  echo "test: previous EXIT trap is restored"
  restored="$(
    HEAVY_LOCK_DIR="$HEAVY_LOCK_DIR" HEAVY_LOCK_BACKEND="$backend" bash -c '
      source "$1"
      trap "echo ORIGINAL" EXIT
      run_heavy node true
      trap -p EXIT
    ' _ "$DIR/heavy-lock.sh"
  )"
  printf '%s\n' "$restored" | grep -q ORIGINAL || fail "${backend}: EXIT trap was not restored: $restored"

  echo "test: SIGINT drops the waiting ticket and the next gate proceeds"
  interrupt_and_proceed INT "${backend} SIGINT"

  echo "test: SIGTERM drops the waiting ticket and the next gate proceeds"
  interrupt_and_proceed TERM "${backend} SIGTERM"

  echo "test: a SIGKILL waiter ticket expires so the next gate proceeds"
  reset_lock_dir
  logdir="$HEAVY_LOCK_DIR/logs"
  mkdir -p "$logdir"
  # shellcheck disable=SC2016
  run_heavy vrt bash -c 'printf hold > "$1"; sleep 4' _ "$logdir/holder" \
    >"$logdir/holder.out" 2>"$logdir/holder.err" &
  holder=$!
  track "$holder"
  wait_for "$logdir/holder" hold || fail "${backend} SIGKILL waiter: holder did not start"
  # shellcheck disable=SC2016
  run_heavy vrt bash -c 'printf ran > "$1"' _ "$logdir/killed" \
    >"$logdir/killed.out" 2>"$logdir/killed.err" &
  killed=$!
  track "$killed"
  wait_for "$logdir/killed.err" waiting || fail "${backend} SIGKILL waiter: waiter did not wait"
  tleak="$(ticket_for "$killed")" || fail "${backend} SIGKILL waiter: no ticket"
  kill -9 "$killed" || true
  sleep 0.3
  [ -e "$tleak" ] || fail "${backend} SIGKILL waiter: ticket vanished without a scanner or a trap"
  # shellcheck disable=SC2016
  run_heavy vrt bash -c 'printf ran > "$1"' _ "$logdir/next" \
    >"$logdir/next.out" 2>"$logdir/next.err" &
  next=$!
  track "$next"
  deadline=$(( $(date +%s) + 3 ))
  while [ -e "$tleak" ]; do
    [ "$(date +%s)" -gt "$deadline" ] && fail "${backend} SIGKILL waiter: dead ticket was not expired"
    sleep 0.1
  done
  deadline=$(( $(date +%s) + 12 ))
  while [ ! -f "$logdir/next" ]; do
    [ "$(date +%s)" -gt "$deadline" ] && fail "${backend} SIGKILL waiter: next gate did not proceed"
    sleep 0.2
  done
  wait "$holder" || fail "${backend} SIGKILL waiter: holder failed"
  wait "$next" || fail "${backend} SIGKILL waiter: next gate failed"
  wait "$killed" 2>/dev/null || true
  echo "${backend} SIGKILL waiter ok"

  echo "test: a SIGKILL holder releases the kernel slot immediately so the waiter proceeds"
  reset_lock_dir
  logdir="$HEAVY_LOCK_DIR/logs"
  mkdir -p "$logdir"
  # shellcheck disable=SC2016
  run_heavy vrt bash -c 'printf hold > "$1"; sleep 60' _ "$logdir/holder" \
    >"$logdir/holder.out" 2>"$logdir/holder.err" &
  holder=$!
  track "$holder"
  wait_for "$logdir/holder" hold || fail "${backend} SIGKILL holder: holder did not start"
  # shellcheck disable=SC2016
  run_heavy vrt bash -c 'printf ran > "$1"' _ "$logdir/waiter" \
    >"$logdir/waiter.out" 2>"$logdir/waiter.err" &
  waiter=$!
  track "$waiter"
  wait_for "$logdir/waiter.err" waiting || fail "${backend} SIGKILL holder: waiter did not wait"
  # Kill the holder's process group so both the wrapper and sleep die immediately.
  kill -9 -- "-$holder" 2>/dev/null || kill -9 "$holder" || true
  deadline=$(( $(date +%s) + 6 ))
  while [ ! -f "$logdir/waiter" ]; do
    [ "$(date +%s)" -gt "$deadline" ] && fail "${backend} SIGKILL holder: waiter did not acquire slot after holder died"
    sleep 0.2
  done
  wait "$waiter" || fail "${backend} SIGKILL holder: waiter failed"
  wait "$holder" 2>/dev/null || true
  echo "${backend} SIGKILL holder ok"

  echo "test: concurrent acquirers serialize cleanly on one slot without overlap"
  reset_lock_dir
  logdir="$HEAVY_LOCK_DIR/logs"
  mkdir -p "$logdir"
  local w_pids=() idx
  for idx in 1 2 3 4 5 6; do
    # shellcheck disable=SC2016
    run_heavy vrt bash -c '
      active="$1"
      overlap="$2"
      done_dir="$3"
      id="$4"
      if ! mkdir "$active" 2>/dev/null; then
        printf "overlap %s\n" "$id" >> "$overlap"
      fi
      sleep 0.15
      rmdir "$active" 2>/dev/null || true
      printf "ok\n" > "$done_dir/$id"
    ' _ "$logdir/active" "$logdir/overlap" "$logdir" "$idx" \
      >"$logdir/c${idx}.out" 2>"$logdir/c${idx}.err" &
    w_pids+=("$!")
    track "$!"
  done
  for p in "${w_pids[@]}"; do
    wait "$p" || fail "${backend} concurrent: worker $p failed"
  done
  [ ! -e "$logdir/overlap" ] || fail "${backend} concurrent: overlap detected: $(cat "$logdir/overlap")"
  for idx in 1 2 3 4 5 6; do
    [ -f "$logdir/$idx" ] || fail "${backend} concurrent: worker $idx did not run"
  done
  echo "${backend} concurrent ok"
}

run_security_suite

for backend in flock python3 perl; do
  if command -v "$backend" >/dev/null 2>&1; then
    run_backend_suite "$backend"
  fi
done

echo "ALL OK"
