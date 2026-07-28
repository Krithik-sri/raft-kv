#!/usr/bin/env bash
# Starts a local cluster in the foreground. Ctrl-C stops every node.
#
# Works on Linux, macOS and Windows (Git Bash). Deliberately bash 3.2
# compatible, because that is what macOS still ships.

set -euo pipefail

SIZE=5
BASE_PORT=5001
DATA_DIR="data"
LOG_LEVEL="info"

usage() {
    cat <<'EOF'
usage: start-cluster.sh [options]

  -n, --nodes N        number of nodes (default 5)
  -p, --base-port N    first port, nodes take N..N+size-1 (default 5001)
  -d, --data-dir DIR   where nodes persist state (default ./data)
  -l, --log-level LVL  debug, info, warn or error (default info)
  -h, --help
EOF
}

while [ $# -gt 0 ]; do
    case "$1" in
        -n|--nodes)      SIZE="$2"; shift 2 ;;
        -p|--base-port)  BASE_PORT="$2"; shift 2 ;;
        -d|--data-dir)   DATA_DIR="$2"; shift 2 ;;
        -l|--log-level)  LOG_LEVEL="$2"; shift 2 ;;
        -h|--help)       usage; exit 0 ;;
        *) echo "unknown option: $1" >&2; usage >&2; exit 2 ;;
    esac
done

REPO="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO"

# .exe under Git Bash, empty everywhere else.
GOEXE="$(go env GOEXE)"
BIN_DIR="$(mktemp -d)"
SERVER="$BIN_DIR/raftnode$GOEXE"

mkdir -p "$DATA_DIR"

echo "building..."
go build -o "$SERVER" ./cmd/server

PIDS=""

cleanup() {
    echo ""
    echo "stopping cluster..."
    for pid in $PIDS; do
        kill "$pid" 2>/dev/null || true
    done
    for pid in $PIDS; do
        wait "$pid" 2>/dev/null || true
    done
    rm -rf "$BIN_DIR"
}
trap cleanup EXIT INT TERM

# peers_for <index> -> "id=host:port,..." for every node except <index>
peers_for() {
    local self="$1" i out=""
    for i in $(seq 1 "$SIZE"); do
        [ "$i" -eq "$self" ] && continue
        [ -n "$out" ] && out="$out,"
        out="${out}node${i}=localhost:$((BASE_PORT + i - 1))"
    done
    printf '%s' "$out"
}

ADDRESSES=""
for i in $(seq 1 "$SIZE"); do
    port=$((BASE_PORT + i - 1))
    [ -n "$ADDRESSES" ] && ADDRESSES="$ADDRESSES,"
    ADDRESSES="${ADDRESSES}localhost:${port}"
done

for i in $(seq 1 "$SIZE"); do
    port=$((BASE_PORT + i - 1))

    "$SERVER" \
        --id "node${i}" \
        --addr "localhost:${port}" \
        --peers "$(peers_for "$i")" \
        --data-dir "$DATA_DIR" \
        --log-level "$LOG_LEVEL" 2>&1 | sed "s/^/[node${i}] /" &

    PIDS="$PIDS $!"
    echo "started node${i} on localhost:${port}"
done

echo ""
echo "cluster is up. talk to it with:"
echo "  go run ./cmd/kvctl --peers $ADDRESSES put foo bar"
echo ""
echo "Ctrl-C to stop."

wait
