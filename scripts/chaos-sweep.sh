#!/usr/bin/env bash
# Runs the chaos test across a range of seeds.
#
# One green run means nothing: the seed controls fault scheduling, not goroutine
# timing, so a seed that passes once can fail on the third go. Sweep instead.
#
# Works on Linux, macOS and Windows (Git Bash). bash 3.2 compatible.

set -uo pipefail

SEEDS=20
DURATION="3s"
START_SEED=1
STRIDE=13

usage() {
    cat <<'EOF'
usage: chaos-sweep.sh [options]

  -n, --seeds N       how many seeds to run (default 20)
  -d, --duration D    fault injection time per seed (default 3s)
  -s, --start-seed N  first seed (default 1)
  -h, --help
EOF
}

while [ $# -gt 0 ]; do
    case "$1" in
        -n|--seeds)      SEEDS="$2"; shift 2 ;;
        -d|--duration)   DURATION="$2"; shift 2 ;;
        -s|--start-seed) START_SEED="$2"; shift 2 ;;
        -h|--help)       usage; exit 0 ;;
        *) echo "unknown option: $1" >&2; usage >&2; exit 2 ;;
    esac
done

REPO="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO"

OUT="$(mktemp)"
trap 'rm -f "$OUT"' EXIT INT TERM

passed=0
failed_seeds=""

for i in $(seq 0 $((SEEDS - 1))); do
    seed=$((START_SEED + i * STRIDE))

    go test ./raft/ -run TestChaos \
        "-chaos.seed=$seed" \
        "-chaos.duration=$DURATION" \
        -count 1 -v > "$OUT" 2>&1
    ok=$?

    # A SAFETY line means committed data was discarded. It should never appear.
    safety=$(grep -c "SAFETY" "$OUT" || true)
    summary=$(grep -o "invariant checks=.*" "$OUT" | head -n 1)

    if [ "$ok" -eq 0 ] && [ "$safety" -eq 0 ]; then
        passed=$((passed + 1))
        printf 'seed %-8s ok    %s\n' "$seed" "$summary"
    else
        failed_seeds="$failed_seeds $seed"
        printf 'seed %-8s FAIL  %s\n' "$seed" "$summary"
        grep -E "invariant violation|missing after|SAFETY" "$OUT" | head -n 5 | sed 's/^/    /'
    fi
done

echo ""
if [ -z "$failed_seeds" ]; then
    echo "PASS: $passed/$SEEDS seeds clean at $DURATION each"
    exit 0
fi

echo "FAIL: seeds violated an invariant:$failed_seeds"
echo "reproduce with: go test ./raft/ -run TestChaos -chaos.seed=<seed> -v"
exit 1
