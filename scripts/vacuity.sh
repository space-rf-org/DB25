#!/usr/bin/env bash
# How many tests assert NOTHING, counted every run instead of when someone remembers.
#
# bag_equal({}, {}) and seq_equal({}, {}) are both true, so a test whose every
# multiset/sequence comparison has both sides empty establishes nothing. The
# falsifiability gate cannot see this: it asks only whether SOME mutant makes a
# test fail, and an unrelated mutant may well do so while the comparison the
# test is named for stays vacuous.
#
# This is not hypothetical. Changing the USING/NATURAL merge predicate from `=`
# to `>` - every such join matching on greater-than - left the suite at 103/103
# PASSING, because users.id (1..5) and orders.id (100..106) are disjoint and so
# every USING/NATURAL comparison compared nothing to nothing.
#
# WHY A RATCHET AND NOT A LIMIT. An all-empty test is not automatically wrong:
# asserting emptiness against a LITERAL empty oracle (expect_empty) is a real
# assertion, and several of the counted tests are exactly that. So the count is
# not required to be zero - it is required NOT TO GROW. A new test that asserts
# nothing is caught on the commit that adds it, which is the only moment anyone
# can cheaply tell which kind it is.
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD="${1:-$ROOT/build}"
BASELINE="$ROOT/scripts/vacuity-baseline.txt"
HARNESS="$BUILD/db25_harness"

[ -x "$HARNESS" ] || { echo "vacuity: no harness at $HARNESS - build first"; exit 1; }

audit=$(DB25_AUDIT_EMPTY=1 "$HARNESS" 2>/dev/null | grep '^AUDIT') || true
[ -n "$audit" ] || { echo "vacuity: the harness produced no AUDIT lines; is DB25_AUDIT_EMPTY still wired?"; exit 1; }

echo "== tests whose EVERY comparison is empty == empty =="
echo "$audit" | awk -F'\t' '
    { split($3,c,"="); split($4,e,"=");
      if (c[2] > 0 && c[2] == e[2]) printf "  %-56s cmp=%s all empty\n", $2, c[2] }'

count=$(echo "$audit" | awk -F'\t' '
    { split($3,c,"="); split($4,e,"="); if (c[2] > 0 && c[2] == e[2]) n++ } END { print n+0 }')
total=$(echo "$audit" | wc -l | tr -d ' ')

echo
echo "  COUNT: $count of $total tests"

[ -r "$BASELINE" ] || { echo "vacuity: no baseline at $BASELINE"; exit 1; }
base=$(grep -oE '^[0-9]+' "$BASELINE" | head -1)
echo "  BASELINE: $base"

if [ "$count" -gt "$base" ]; then
    echo
    echo "VACUITY REGRESSION: $count tests assert nothing, baseline is $base."
    echo "  A test added since the baseline compares an empty result against an"
    echo "  empty expectation. If that IS the claim - the query must return no"
    echo "  rows - say so with expect_empty and raise the number here in the same"
    echo "  commit. If it is not, the fixture is the problem: pick data where the"
    echo "  comparison has rows to compare."
    exit 1
fi
if [ "$count" -lt "$base" ]; then
    echo "  IMPROVED: below baseline. Lower $BASELINE to $count to lock it in."
fi
echo "  OK: not growing."
