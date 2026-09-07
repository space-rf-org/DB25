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

if ! command -v git >/dev/null 2>&1 || ! git -C "$root" rev-parse --git-dir >/dev/null 2>&1; then
    echo "pin agreement: not a git checkout, skipping (nothing to compare)"
    exit 0
fi

pin_of() {  # repo-dir, submodule-path
    git -C "$1" ls-tree HEAD "$2" 2>/dev/null | awk '$2 == "commit" { print $3 }'
}

ours=$(pin_of "$root" external/db25-logical-plan)
theirs=$(pin_of "$root/external/db25-physical-plan" external/db25-logical-plan)

if [ -z "$ours" ] || [ -z "$theirs" ]; then
    echo "pin agreement: could not read both pins (ours='$ours' theirs='$theirs'), skipping"
    exit 0
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
