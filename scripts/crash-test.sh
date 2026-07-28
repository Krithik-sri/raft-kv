#!/usr/bin/env bash
# Kills a random node with SIGKILL mid-workload, restarts it, and checks that
# every acknowledged write is still readable. Repeats.
#
# Graceful shutdowns prove nothing: the interesting bugs live in the gap between
# "wrote to the page cache" and "actually on the disk".
#
# Works on Linux, macOS and Windows (Git Bash). bash 3.2 compatible, so no
# associative arrays -- acknowledged writes go in a plain file.

set -uo pipefail

ITERATIONS=20
WRITES_PER_BATCH=5
KEEP_LOGS=0
BASE_PORT=7301
SIZE=3

usage() {
    cat <<'EOF'
usage: crash-test.sh [options]

  -i, --iterations N        kill/restart cycles (default 20)
  -w, --writes-per-batch N  writes before and after each kill (default 5)
  -k, --keep-logs           keep the work directory on exit
  -h, --help
EOF
}

while [ $# -gt 0 ]; do
    case "$1" in
        -i|--iterations)       ITERATIONS="$2"; shift 2 ;;
        -w|--writes-per-batch) WRITES_PER_BATCH="$2"; shift 2 ;;
        -k|--keep-logs)        KEEP_LOGS=1; shift ;;
        -h|--help)             usage; exit 0 ;;
        *) echo "unknown option: $1" >&2; usage >&2; exit 2 ;;
    esac
done

REPO="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO"

GOEXE="$(go env GOEXE)"
WORK_DIR="$(mktemp -d)"
DATA_DIR="$WORK_DIR/data"
LOG_DIR="$WORK_DIR/logs"
BIN_DIR="$WORK_DIR/bin"
ACKED="$WORK_DIR/acked"

mkdir -p "$DATA_DIR" "$LOG_DIR" "$BIN_DIR"
: > "$ACKED"

SERVER="$BIN_DIR/raftnode$GOEXE"
KVCTL="$BIN_DIR/kvctl$GOEXE"

ALL_ADDRESSES=""
for i in $(seq 1 "$SIZE"); do
    [ -n "$ALL_ADDRESSES" ] && ALL_ADDRESSES="$ALL_ADDRESSES,"
    ALL_ADDRESSES="${ALL_ADDRESSES}localhost:$((BASE_PORT + i - 1))"
done

echo "building..."
go build -o "$SERVER" ./cmd/server || exit 1
go build -o "$KVCTL" ./cmd/kvctl || exit 1

# PIDS[i] holds the pid of node i, or empty if it is stopped. Indexed arrays
# work in bash 3.2; associative ones do not.
PIDS=""
for i in $(seq 1 "$SIZE"); do PIDS="$PIDS 0"; done
set -- $PIDS
PID_LIST=("$@")

pid_of() { echo "${PID_LIST[$(($1 - 1))]}"; }
set_pid() { PID_LIST[$(($1 - 1))]="$2"; }

peers_for() {
    local self="$1" i out=""
    for i in $(seq 1 "$SIZE"); do
        [ "$i" -eq "$self" ] && continue
        [ -n "$out" ] && out="$out,"
        out="${out}node${i}=localhost:$((BASE_PORT + i - 1))"
    done
    printf '%s' "$out"
}

start_node() {
    local index="$1" tag="$2"
    local port=$((BASE_PORT + index - 1))

    "$SERVER" \
        --id "node${index}" \
        --addr "localhost:${port}" \
        --peers "$(peers_for "$index")" \
        --data-dir "$DATA_DIR" \
        > "$LOG_DIR/node${index}-${tag}.log" 2>&1 &

    set_pid "$index" "$!"
}

stop_node() {
    local index="$1"
    local pid
    pid="$(pid_of "$index")"

    if [ "$pid" != "0" ]; then
        kill -9 "$pid" 2>/dev/null || true
        wait "$pid" 2>/dev/null || true
        set_pid "$index" 0
    fi
}

stop_all() {
    local i
    for i in $(seq 1 "$SIZE"); do stop_node "$i"; done
}

cleanup() {
    stop_all
    if [ "$KEEP_LOGS" -eq 1 ]; then
        echo "artifacts kept in $WORK_DIR"
    else
        rm -rf "$WORK_DIR"
    fi
}
trap cleanup EXIT INT TERM

put() {
    "$KVCTL" --peers "$ALL_ADDRESSES" --timeout 15s put "$1" "$2" > /dev/null 2>&1
}

get() {
    "$KVCTL" --peers "$ALL_ADDRESSES" --timeout 15s get "$1" 2>/dev/null | head -n 1
}

echo "starting cluster..."
for i in $(seq 1 "$SIZE"); do start_node "$i" initial; done
sleep 2

write_count=0
acked_count=0
failures=0

for iteration in $(seq 1 "$ITERATIONS"); do
    # Batch A: healthy cluster.
    for _ in $(seq 1 "$WRITES_PER_BATCH"); do
        write_count=$((write_count + 1))
        if put "k$write_count" "v$write_count"; then
            echo "k$write_count v$write_count" >> "$ACKED"
            acked_count=$((acked_count + 1))
        fi
    done

    # Kill one node hard, mid-workload.
    victim=$(( (RANDOM % SIZE) + 1 ))
    stop_node "$victim"

    # Batch B: degraded cluster, must still accept writes on the majority.
    for _ in $(seq 1 "$WRITES_PER_BATCH"); do
        write_count=$((write_count + 1))
        if put "k$write_count" "v$write_count"; then
            echo "k$write_count v$write_count" >> "$ACKED"
            acked_count=$((acked_count + 1))
        fi
    done

    # Restart the victim from its on-disk state.
    start_node "$victim" "iter$iteration"
    sleep 1.5

    # Every acknowledged write must still be readable.
    lost=0
    while read -r key want; do
        got="$(get "$key")"
        if [ "$got" != "$want" ]; then
            lost=$((lost + 1))
            if [ "$lost" -le 20 ]; then
                echo "  iteration $iteration: $key = '$got', expected '$want'"
            fi
        fi
    done < "$ACKED"

    if [ "$lost" -eq 0 ]; then
        status="ok"
    else
        status="LOST $lost"
        failures=$((failures + 1))
    fi

    printf 'iteration %3d/%d  killed node%d  acked %4d  verified %s\n' \
        "$iteration" "$ITERATIONS" "$victim" "$acked_count" "$status"

    [ "$lost" -gt 0 ] && break
done

stop_all

echo ""
if [ "$failures" -eq 0 ]; then
    echo "PASS: $acked_count acknowledged writes survived $ITERATIONS crash/restart cycles"
    exit 0
fi

echo "FAIL: committed writes were lost"
exit 1
