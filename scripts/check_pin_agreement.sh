#!/usr/bin/env bash
# The super-build turns db25-physical-plan's own tests OFF, on the grounds that
# they run in its own CI. That is only true if its CI tests it against the SAME
# db25-logical-plan this super-build links it against.
#
# Both repos vendor db25-logical-plan. The physical planner's CI builds it
# against ITS pin; the umbrella builds it against OURS and reuses the target. If
# the two pins drift, the umbrella ships a (physical-plan x logical-plan) pairing
# no CI has ever run - and the umbrella cannot notice, because the physical
# planner's thirteen test targets are switched off here.
#
# Two facts, each reasonable, that are jointly a hole. This closes it.
set -uo pipefail
root="${1:-.}"

# Exit 77 means SKIPPED, not PASSED. CMake maps it through SKIP_RETURN_CODE (see
# the add_test next to this script), so a run that could not perform the
# comparison is reported as skipped rather than counted among the passes.
#
# This distinction is the whole point of the status codes here. The check used to
# exit 0 whenever it could not read the pins, which made a NON-CHECK
# indistinguishable from a successful one: ctest printed "Passed", and the
# project's "18/18 green" included a test that had verified nothing. The skip is
# legitimate - a source archive has no git metadata and there are no pins to
# compare - but it must not be reported as a pass.
#
# Failing closed would be the wrong fix: it would break builds from a tarball,
# where nobody has done anything wrong and the comparison is simply unavailable.
readonly SKIP=77

if ! command -v git >/dev/null 2>&1 || ! git -C "$root" rev-parse --git-dir >/dev/null 2>&1; then
    echo "pin agreement: not a git checkout - SKIPPED (no pins exist to compare)"
    exit $SKIP
fi

pin_of() {  # repo-dir, submodule-path
    git -C "$1" ls-tree HEAD "$2" 2>/dev/null | awk '$2 == "commit" { print $3 }'
}

ours=$(pin_of "$root" external/db25-logical-plan)
theirs=$(pin_of "$root/external/db25-physical-plan" external/db25-logical-plan)

if [ -z "$ours" ] || [ -z "$theirs" ]; then
    echo "pin agreement: could not read both pins - SKIPPED"
    echo "  umbrella  external/db25-logical-plan  '$ours'"
    echo "  physical  external/db25-logical-plan  '$theirs'"
    echo "  One side is not a git checkout with that submodule recorded in HEAD -"
    echo "  a vendored or archived tree. Nothing was verified."
    exit $SKIP
fi

if [ "$ours" != "$theirs" ]; then
    cat <<MSG
PIN DISAGREEMENT - db25-logical-plan

  umbrella  external/db25-logical-plan               $ours
  physical  external/db25-logical-plan               $theirs

The physical planner is TESTED against its pin and SHIPPED against ours, so this
super-build is running a combination that no CI has exercised. Its own thirteen
test targets are off here (DB25_PHYSICAL_BUILD_TESTS), so nothing else will catch
a regression this introduces.

Fix by bumping one of the two so they agree, in the same change that moves the
other. If the divergence is deliberate and temporary, say so here rather than
disabling this check.
MSG
    exit 1
fi

echo "pin agreement: db25-logical-plan is ${ours:0:10} in both"

# ---------------------------------------------------------------------------
# Second pin, different kind. The semantic analyzer is consumed through a
# gitlink inside this chain, but its OWN CI builds against the parser it names
# in a workflow variable:
#
#     env:
#       PARSER_SHA: <sha>
#
# That job is the only thing that exercises the analyzer's find_package path -
# the path the rest of the chain does not take. If it builds against a
# different parser than the chain pins, it is still a green job, but it is no
# longer evidence about the combination that ships.
#
# A gitlink cannot go stale unnoticed: it diffs, and the comparison above reads
# it. A string in a YAML file can, and did - it sat four commits adrift while
# every gitlink in the chain stayed honest. This is that comparison, so the one
# pin in the project that is not a tree object is no longer the one pin nobody
# checks.
# ---------------------------------------------------------------------------
analyzer_ci="$root/external/db25-logical-plan/external/analyzer/.github/workflows/ci.yml"
chain_parser=$(pin_of "$root/external/db25-logical-plan" external/parser)

if [ ! -r "$analyzer_ci" ] || [ -z "$chain_parser" ]; then
    echo "parser pin: analyzer CI or chain pin unreadable - SKIPPED"
    echo "  Submodules may be uninitialised, or this is an archived tree."
    exit $SKIP
fi

# Every PARSER_SHA the workflow declares. One is the point of the companion
# change; more than one that DISAGREE is the defect that change removed, so
# catch a regression to three copies too.
# Both spellings on purpose: `PARSER_SHA: <sha>` is the workflow-level env,
# `PARSER_SHA=<sha>` the per-job shell assignment it replaced. Matching only the
# new form would make this check SKIP silently against the old one - a non-check
# that looks like a pass, which is the thing this file refuses to do.
mapfile -t declared < <(grep -oE 'PARSER_SHA[:=][[:space:]]*[0-9a-f]{7,40}' "$analyzer_ci" \
                        | grep -oE '[0-9a-f]{7,40}$' | sort -u)

if [ ${#declared[@]} -eq 0 ]; then
    echo "parser pin: no PARSER_SHA found in the analyzer's CI - SKIPPED"
    echo "  The workflow no longer declares one; nothing to compare."
    exit $SKIP
fi

if [ ${#declared[@]} -gt 1 ]; then
    echo "PARSER PIN DISAGREEMENT - within the analyzer's own CI"
    printf '  %s\n' "${declared[@]}"
    echo
    echo "The analyzer's workflow declares more than one distinct PARSER_SHA. Its"
    echo "jobs would build against different parsers, so a gate would not be"
    echo "gating the suite it claims to. Define it once at workflow level."
    exit 1
fi

theirs_parser="${declared[0]}"
# The workflow may abbreviate; compare on the shorter length, both directions.
n=${#theirs_parser}
if [ "${chain_parser:0:$n}" != "$theirs_parser" ]; then
    cat <<MSG
PARSER PIN DISAGREEMENT - db25-sql-parser

  chain     db25-logical-plan/external/parser   $chain_parser
  analyzer  .github/workflows/ci.yml PARSER_SHA $theirs_parser

The analyzer's CI is the only job that builds it through find_package against an
INSTALLED parser - the path this chain does not take and therefore cannot catch
a break in. Against a different parser than the chain pins, that job is green
and proves nothing about what ships.

Fix by setting PARSER_SHA to the chain's pin above. Bumping it to the parser's
main instead is the same mistake one commit later: the value that matters is
what this chain builds against, not what the parser has moved on to.
MSG
    exit 1
fi

echo "parser pin: analyzer CI and chain agree on ${theirs_parser:0:10}"
