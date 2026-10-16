#!/usr/bin/env bash
# Line coverage for the DB25 frontend, recomputed from a clean instrumented run.
#
# WHY THIS EXISTS. Coverage for this project was measured for the first time
# long after the tests were written, and the first three numbers it produced
# were all misleading - not because gcov was wrong, but because they were read
# against the wrong suite. Measured against the umbrella's query-path harness,
# parser_ddl.cpp looked like 49.7% and parser_statements.cpp like 33.8%;
# measured in the repo whose tests cover that code, they are 81.8% and 77.1%.
# A coverage number is meaningless without the scope it was taken in, so this
# script states its scope in the output and the baseline records it.
#
# WHAT IT DOES NOT CLAIM. Coverage says what was REACHED, never what was
# CHECKED. This project has a worked example: changing the USING/NATURAL merge
# predicate from `=` to `>` left the whole suite green while running through
# fully-covered lines. The falsifiability gates decide whether reaching a line
# means anything; this only finds lines nothing reaches at all.
#
# THE RATCHET. CI fails if total frontend coverage falls below the baseline in
# scripts/coverage-baseline.txt. One number, not a per-file threshold, and it
# is a RECORD of what was measured rather than a target someone invented. To
# move it, move it deliberately in the same commit that changes coverage.
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD="${1:-$ROOT/build-coverage}"
BASELINE="$ROOT/scripts/coverage-baseline.txt"

command -v gcov >/dev/null 2>&1 || { echo "coverage: gcov not found"; exit 77; }

echo "== configuring instrumented build =="
cmake -S "$ROOT" -B "$BUILD" -DCMAKE_BUILD_TYPE=Debug \
      -DCMAKE_CXX_FLAGS="--coverage -O0 -g" \
      -DCMAKE_EXE_LINKER_FLAGS="--coverage" >/dev/null 2>&1 || exit 1
# Build output is discarded, stderr included: a coverage run that printed every
# -Wmissing-field-initializers would bury the one table it exists to produce.
cmake --build "$BUILD" -j"$(nproc)" >/dev/null 2>&1 || {
    echo "coverage: the instrumented build failed"; exit 1; }

# A stale .gcda from an earlier build silently mixes two runs together.
find "$BUILD" -name '*.gcda' -delete
echo "== running the suite =="
ctest --test-dir "$BUILD" --output-on-failure >/dev/null 2>&1 || {
    echo "coverage: the suite itself failed; coverage of a red suite means nothing"
    exit 1
}

echo
echo "== line coverage, umbrella suite =="
echo "   SCOPE: what db25's own ctest targets execute. A file whose tests live"
echo "   in another repo will read low here and be well covered there."
echo

report=$(for f in $(find "$BUILD" -name '*.gcda' | grep -E 'db25logicalplan|db25parser|db25arena|db25semantic'); do
    d=$(dirname "$f"); g=$(basename "$f")
    (cd "$d" && gcov -n "$g" 2>/dev/null) | awk '
        /^File / { f=$2; gsub(/'"'"'/,"",f); next }
        /^Lines executed/ && f ~ /\.cpp$/ && f !~ /\/usr\// { print f"\t"$0; f="" }'
done | sort -u)

echo "$report" | sed "s|$ROOT/external/||" | awk -F'\t' '
    { split($2,a,":"); split(a[2],b,"% of "); printf "  %-62s %6.2f%%  of %5d\n", $1, b[1], b[2] }' | sort -k2 -n

total=$(echo "$report" | awk -F'\t' '
    { split($2,a,":"); split(a[2],b,"% of "); cov += b[1]*b[2]/100; tot += b[2] }
    END { if (tot) printf "%.1f", 100*cov/tot; else print "0" }')
lines=$(echo "$report" | awk -F'\t' '{ split($2,a,":"); split(a[2],b,"% of "); tot += b[2] } END { print tot+0 }')

echo
echo "  TOTAL: ${total}% of ${lines} lines"

[ -r "$BASELINE" ] || { echo "coverage: no baseline at $BASELINE"; exit 1; }
base=$(grep -oE '^[0-9]+(\.[0-9]+)?' "$BASELINE" | head -1)
echo "  BASELINE: ${base}%"

if awk -v t="$total" -v b="$base" 'BEGIN { exit !(t < b) }'; then
    echo
    echo "COVERAGE REGRESSION: ${total}% is below the recorded baseline of ${base}%."
    echo "  Either the change removed tests, or it added code nothing reaches."
    echo "  If the drop is intended, move the number in $BASELINE in THIS commit"
    echo "  and say why - a baseline edited later is a baseline nobody believes."
    exit 1
fi
echo "  OK: at or above baseline."
