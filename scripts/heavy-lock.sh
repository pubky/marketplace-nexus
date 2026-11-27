#!/usr/bin/env bash
# Fair slots for optional full pre-push runs (PREPUSH_FULL=1).
#
# Each class has its own FIFO ticket queue. A process may run only when fewer
# than SLOTS older live tickets exist, then it holds one kernel-released slot
# lock while the command runs. Each scan drops a ticket whose PID is dead, a
# zombie, reused, or malformed. EXIT, INT, and TERM remove the current ticket
# immediately. SIGKILL cannot be caught; the kernel releases the slot lock
# immediately and the next scan expires the dead ticket.
#
# Locking uses kernel flock(2) via the `flock` binary when available, falling
# back to inline `python3` (fcntl.flock) or `perl` (Fcntl::flock) so both
# Linux and stock macOS use kernel-released locks with no directory reaping.
#
#   vrt    1 slot   Browser/container runs.
#   cargo  2 slots  Rust clippy and tests.
#   node   1 slot   Host Node checks.
#
# Classes do not exclude each other.
# HEAVY_LOCK_DIR overrides the lock directory (defaults to a user-owned dir
# under XDG_RUNTIME_DIR or TMPDIR).
# HEAVY_LOCK_BACKEND overrides lock backend selection (flock, python3, perl).

HEAVY_LOCK_DIR="${HEAVY_LOCK_DIR:-${XDG_RUNTIME_DIR:-${TMPDIR:-/tmp}}/marketplace-nexus-locks-$(id -u)}"

if [ "${BASH_SOURCE[0]}" = "$0" ]; then
  echo "source scripts/heavy-lock.sh" >&2
  exit 1
fi

heavy_slots() {
  case "$1" in
    vrt) echo 1 ;;
    cargo) echo 2 ;;
    node) echo 1 ;;
    *)
      echo "heavy-lock: unknown class: $1" >&2
      return 1
      ;;
  esac
}

heavy_lock_backend() {
  case "${HEAVY_LOCK_BACKEND:-}" in
    flock|python3|perl)
      printf '%s\n' "$HEAVY_LOCK_BACKEND"
      ;;
    "")
      if command -v flock >/dev/null 2>&1; then
        echo flock
      elif command -v python3 >/dev/null 2>&1; then
        echo python3
      elif command -v perl >/dev/null 2>&1; then
        echo perl
      else
        echo "heavy-lock: requires flock, python3, or perl for kernel file locking" >&2
        return 1
      fi
      ;;
    *)
      echo "heavy-lock: unknown HEAVY_LOCK_BACKEND: ${HEAVY_LOCK_BACKEND}" >&2
      return 1
      ;;
  esac
}

heavy_dir_mode() {
  local dir="$1" mode=""
  if mode=$(stat -c '%a' "$dir" 2>/dev/null) && [[ "$mode" =~ ^[0-7]+$ ]]; then
    printf '%s\n' "$mode"
    return 0
  fi
  if mode=$(stat -f '%Lp' "$dir" 2>/dev/null) && [[ "$mode" =~ ^[0-7]+$ ]]; then
    printf '%s\n' "$mode"
    return 0
  fi
  return 1
}

# Ensures a lock directory is a real, user-owned directory with no group or
# other write bits. Refuses symlinks, foreign owners, and shared-writable dirs.
heavy_verify_dir() {
  local dir="$1" mode perm
  if [ -L "$dir" ]; then
    echo "heavy-lock: refusing symlink lock directory: ${dir}" >&2
    return 1
  fi
  if [ ! -e "$dir" ]; then
    (umask 077 && mkdir -p "$dir") || return 1
  fi
  if [ -L "$dir" ] || [ ! -d "$dir" ]; then
    echo "heavy-lock: lock path is not a real directory: ${dir}" >&2
    return 1
  fi
  if [ ! -O "$dir" ]; then
    echo "heavy-lock: lock directory is not owned by current user: ${dir}" >&2
    return 1
  fi
  mode=$(heavy_dir_mode "$dir") || {
    echo "heavy-lock: could not inspect permissions of ${dir}" >&2
    return 1
  }
  perm=$((8#$mode))
  if [ $((perm & 0022)) -ne 0 ]; then
    echo "heavy-lock: refusing group/other-writable lock directory (mode ${mode}): ${dir}" >&2
    return 1
  fi
  return 0
}

# Executes a command while holding an exclusive kernel flock(2) on lockfile.
# nonblock=1 returns 75 on lock contention without starting the command.
heavy_flock_exec() {
  local nonblock="$1" lockfile="$2" backend
  shift 2
  backend=$(heavy_lock_backend) || return 1
  if [ -L "$lockfile" ]; then
    echo "heavy-lock: refusing symlink lock file: ${lockfile}" >&2
    return 1
  fi

  case "$backend" in
    flock)
      if [ "$nonblock" = 1 ]; then
        (umask 077 && exec flock -n "$lockfile" "$@")
      else
        (umask 077 && exec flock "$lockfile" "$@")
      fi
      ;;
    python3)
      python3 -c '
import errno, fcntl, os, sys
nonblock = sys.argv[1] == "1"
path = sys.argv[2]
cmd = sys.argv[3:]
flags = os.O_CREAT | os.O_RDWR | getattr(os, "O_NOFOLLOW", 0)
fd = os.open(path, flags, 0o600)
try:
    fcntl.flock(fd, fcntl.LOCK_EX | (fcntl.LOCK_NB if nonblock else 0))
except OSError as exc:
    if nonblock and exc.errno in (errno.EAGAIN, errno.EWOULDBLOCK):
        sys.exit(75)
    raise
os.set_inheritable(fd, True)
os.execvp(cmd[0], cmd)
' "$nonblock" "$lockfile" "$@"
      ;;
    perl)
      perl -e '
use strict;
use warnings;
use Errno qw(EAGAIN EWOULDBLOCK);
use Fcntl qw(:flock O_CREAT O_RDWR F_GETFD F_SETFD FD_CLOEXEC);
my $nonblock = shift @ARGV;
my $path = shift @ARGV;
my $flags = O_CREAT | O_RDWR;
my $nofollow = eval { Fcntl::O_NOFOLLOW() };
$flags |= $nofollow if defined $nofollow;
sysopen(my $fh, $path, $flags, 0600) or die "heavy-lock: open $path: $!\n";
my $op = $nonblock ? (LOCK_EX | LOCK_NB) : LOCK_EX;
if (!flock($fh, $op)) {
    exit 75 if $nonblock && ($! == EAGAIN || $! == EWOULDBLOCK);
    die "heavy-lock: flock $path: $!\n";
}
my $fd_flags = fcntl($fh, F_GETFD, 0);
if (defined $fd_flags) {
    fcntl($fh, F_SETFD, $fd_flags & ~FD_CLOEXEC) or die "heavy-lock: fcntl $path: $!\n";
}
exec { $ARGV[0] } @ARGV or die "heavy-lock: exec $ARGV[0]: $!\n";
' "$nonblock" "$lockfile" "$@"
      ;;
  esac
}

etime_to_seconds() {
  local t="$1" days=0 n a b c
  t="${t#-}"
  if [[ "$t" == *-* ]]; then
    days="${t%%-*}"
    t="${t#*-}"
  fi
  n=$(printf '%s\n' "$t" | awk -F: '{print NF}')
  IFS=: read -r a b c <<< "$t"
  a="${a:-0}"
  b="${b:-0}"
  c="${c:-0}"
  days="${days:-0}"
  if ! [[ "$a" =~ ^[0-9]+$ ]] || ! [[ "$b" =~ ^[0-9]+$ ]] || ! [[ "$c" =~ ^[0-9]+$ ]] || ! [[ "$days" =~ ^[0-9]+$ ]]; then
    echo 0
    return 1
  fi
  a=$((10#$a))
  b=$((10#$b))
  c=$((10#$c))
  days=$((10#$days))
  if [ "$n" -le 1 ]; then
    echo "$a"
  elif [ "$n" -eq 2 ]; then
    echo $((days * 86400 + a * 60 + b))
  else
    echo $((days * 86400 + a * 3600 + b * 60 + c))
  fi
}

heavy_release() {
  local ticket="${HEAVY_TICKET:-}"
  HEAVY_TICKET=""
  if [ -n "$ticket" ]; then
    rm -f "$ticket"
  fi
}

# Installed only while this shell owns a ticket. Previous handlers are put
# back when the command returns. A signal re-raises itself so the exit status
# stays the signal.
heavy_finish_exit() {
  local status=$?
  heavy_release
  trap - EXIT
  if [ -n "${HEAVY_SAVED_EXIT:-}" ]; then
    eval "$HEAVY_SAVED_EXIT"
  fi
  exit "$status"
}

heavy_finish_signal() {
  local sig="$1" child self
  self="${HEAVY_SELF:-$$}"
  heavy_release
  for child in $(pgrep -P "$self" 2>/dev/null || true); do
    kill -s "$sig" "$child" 2>/dev/null || true
  done
  trap - EXIT INT TERM
  kill -s "$sig" "$self"
}

heavy_arm_traps() {
  HEAVY_SAVED_EXIT=""
  HEAVY_SAVED_INT=""
  HEAVY_SAVED_TERM=""
  # In bash 4+, `trap -p` inside a subshell prints the parent shell's traps
  # even though the subshell cleared them; re-evaluating them on disarm would
  # fire the parent's EXIT trap inside a background job.
  if [ "${BASH_SUBSHELL:-0}" -eq 0 ]; then
    HEAVY_SAVED_EXIT=$(trap -p EXIT || true)
    HEAVY_SAVED_INT=$(trap -p INT || true)
    HEAVY_SAVED_TERM=$(trap -p TERM || true)
  fi
  trap 'heavy_finish_exit' EXIT
  trap 'heavy_finish_signal INT' INT
  trap 'heavy_finish_signal TERM' TERM
}

heavy_disarm_traps() {
  trap - EXIT INT TERM
  if [ -n "${HEAVY_SAVED_EXIT:-}" ]; then
    eval "$HEAVY_SAVED_EXIT"
  fi
  if [ -n "${HEAVY_SAVED_INT:-}" ]; then
    eval "$HEAVY_SAVED_INT"
  fi
  if [ -n "${HEAVY_SAVED_TERM:-}" ]; then
    eval "$HEAVY_SAVED_TERM"
  fi
  HEAVY_SAVED_EXIT=""
  HEAVY_SAVED_INT=""
  HEAVY_SAVED_TERM=""
}

heavy_issue() {
  local class="$1"
  local self_pid="$2"
  local qdir="$HEAVY_LOCK_DIR/${class}.q"
  local seqfile="$HEAVY_LOCK_DIR/${class}.seq"
  local issue="$HEAVY_LOCK_DIR/${class}.issue.lock"
  heavy_verify_dir "$qdir" || return 1
  if [ -L "$seqfile" ]; then
    echo "heavy-lock: refusing symlink sequence file: ${seqfile}" >&2
    return 1
  fi

  # shellcheck disable=SC2016
  heavy_flock_exec 0 "$issue" bash -c '
    set -euo pipefail
    umask 077
    seqfile="$1"
    qdir="$2"
    pid="$3"
    n=0
    if [ -f "$seqfile" ] && [ ! -L "$seqfile" ]; then
      n=$(tr -cd "0-9" < "$seqfile" || true)
    fi
    [ -n "$n" ] || n=0
    n=$((n + 1))
    tmp=$(mktemp "${seqfile}.XXXXXX")
    printf "%s\n" "$n" > "$tmp"
    mv -f "$tmp" "$seqfile"
    name=$(printf "%020d" "$n")
    ticket_tmp=$(mktemp "${qdir}/.ticket.XXXXXX")
    printf "%s %s\n" "$pid" "$(date +%s)" > "$ticket_tmp"
    mv -f "$ticket_tmp" "$qdir/${name}.ticket"
    printf "%s\n" "$n"
  ' _ "$seqfile" "$qdir" "$self_pid"
}

# 0 when the ticket must be removed: non-numeric PID or epoch, dead PID,
# zombie, or a reused PID whose process started after the ticket was written.
heavy_pid_dead() {
  local pid="$1" epoch="$2" state etime es now age
  if ! [[ "$pid" =~ ^[0-9]+$ ]]; then
    return 0
  fi
  if [ -n "$epoch" ] && ! [[ "$epoch" =~ ^[0-9]+$ ]]; then
    return 0
  fi
  if ! kill -0 "$pid" 2>/dev/null; then
    return 0
  fi
  state=$(ps -o stat= -p "$pid" 2>/dev/null | tr -d '[:space:]' || true)
  case "$state" in
    Z*) return 0 ;;
  esac
  if [ -z "$epoch" ]; then
    return 1
  fi
  etime=$(ps -o etime= -p "$pid" 2>/dev/null | tr -d '[:space:]' || true)
  if [ -z "$etime" ]; then
    return 1
  fi
  es=$(etime_to_seconds "$etime") || return 1
  now=$(date +%s)
  age=$((now - 10#$epoch))
  if [ "$age" -gt 3 ] && [ "$es" -lt $((age - 3)) ]; then
    return 0
  fi
  return 1
}

heavy_sweep() {
  local class="$1"
  local dir="$HEAVY_LOCK_DIR/${class}.q"
  local f base pid epoch extra
  [ -d "$dir" ] && [ ! -L "$dir" ] || return 0
  for f in "$dir"/*.ticket; do
    [ -e "$f" ] || [ -L "$f" ] || continue
    base=$(basename "$f")
    if [ -L "$f" ] || [ ! -f "$f" ] || ! [[ "$base" =~ ^[0-9]+\.ticket$ ]]; then
      rm -f "$f"
      continue
    fi
    pid=""
    epoch=""
    extra=""
    read -r pid epoch extra < "$f" || true
    if [ -n "$extra" ] || heavy_pid_dead "$pid" "$epoch"; then
      rm -f "$f"
    fi
  done
}

heavy_ahead() {
  local class="$1" myseq="$2"
  local dir="$HEAVY_LOCK_DIR/${class}.q"
  local ahead=0 f base seq
  [[ "$myseq" =~ ^[0-9]+$ ]] || return 1
  heavy_sweep "$class"
  for f in "$dir"/*.ticket; do
    [ -e "$f" ] || continue
    if [ ! -f "$f" ] || [ -L "$f" ]; then
      continue
    fi
    base=$(basename "$f")
    [[ "$base" =~ ^[0-9]+\.ticket$ ]] || continue
    seq=$((10#${base%.ticket}))
    if [ "$seq" -lt "$myseq" ]; then
      ahead=$((ahead + 1))
    fi
  done
  echo "$ahead"
}

# Run the command inside one slot. Returns 75 when the slot lock is busy
# and the command did not start. Otherwise returns the command status.
heavy_run_in_slot() {
  local slotfile="$1"
  local marker hold_msg slot_status=0
  shift
  hold_msg="$1"
  shift
  marker=$(umask 077 && mktemp "${HEAVY_LOCK_DIR}/ran.XXXXXX")
  : > "$marker"
  # `||` keeps a non-zero status under set -e. A bare assignment from $? after
  # `if` is wrong on bash 3.2: a failed if with no else reports status 0.
  # shellcheck disable=SC2016
  heavy_flock_exec 1 "$slotfile" bash -c '
    printf 1 > "$1"
    shift
    printf "%s\n" "$1" >&2
    shift
    "$@"
  ' _ "$marker" "$hold_msg" "$@" || slot_status=$?
  if [ ! -s "$marker" ]; then
    rm -f "$marker"
    return 75
  fi
  rm -f "$marker"
  return "$slot_status"
}

run_heavy() {
  local class="$1"
  shift
  local slots seq ahead last i st
  local self_pid pidfile
  slots=$(heavy_slots "$class") || return 1
  heavy_lock_backend >/dev/null || return 1
  heavy_verify_dir "$HEAVY_LOCK_DIR" || return 1
  # heavy_issue runs inside a command substitution, and that subshell exits
  # as soon as the ticket number is printed. Record this shell, not that
  # subshell. $$ is the outermost shell even inside a background job, and
  # bash 3.2 has no BASHPID, so a direct child reports this process.
  pidfile="$(umask 077 && mktemp "${HEAVY_LOCK_DIR}/self.XXXXXX")"
  sh -c 'echo $PPID' > "$pidfile"
  self_pid="$(tr -cd '0-9' < "$pidfile")"
  rm -f "$pidfile"
  if [ -z "$self_pid" ]; then
    echo "heavy-lock: could not read own pid" >&2
    return 1
  fi
  HEAVY_SELF=$self_pid
  seq=$(heavy_issue "$class" "$self_pid")
  if ! [[ "$seq" =~ ^[0-9]+$ ]]; then
    echo "heavy-lock: could not take a ${class} ticket" >&2
    return 1
  fi
  HEAVY_TICKET="$HEAVY_LOCK_DIR/${class}.q/$(printf '%020d' "$seq").ticket"
  heavy_arm_traps
  last=""
  while true; do
    ahead=$(heavy_ahead "$class" "$seq")
    if [ "$ahead" -lt "$slots" ]; then
      break
    fi
    if [ "$ahead" != "$last" ]; then
      echo "heavy-lock: waiting class=${class} ticket=${seq} ahead=${ahead} slots=${slots}" >&2
      last=$ahead
    fi
    sleep 0.25
  done
  while true; do
    i=1
    while [ "$i" -le "$slots" ]; do
      st=0
      heavy_run_in_slot "$HEAVY_LOCK_DIR/${class}.${i}.lock" \
        "heavy-lock: hold class=${class} slot=${i} ticket=${seq}" \
        "$@" || st=$?
      if [ "$st" -eq 0 ]; then
        heavy_release
        heavy_disarm_traps
        return 0
      fi
      if [ "$st" -ne 75 ]; then
        heavy_release
        heavy_disarm_traps
        return "$st"
      fi
      i=$((i + 1))
    done
    sleep 0.2
  done
}
