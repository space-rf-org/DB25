#!/usr/bin/env bash
# Initialise exactly the submodules this super-build needs, and no more.
#
# WHY NOT `git submodule update --init --recursive`
#
# Both this repo and db25-physical-plan vendor db25-logical-plan, so a plain
# recursive init clones that whole subtree TWICE - db25-logical-plan, the
# analyzer, the parser and the tokenizer, once under external/ and again under
# external/db25-physical-plan/external/.
#
# Measured on clean clones of this repo:
#
#                      external/   whole checkout
#   --init --recursive      15 MB            41 MB
#   this script            7.6 MB            24 MB
#
# The duplicate working tree is 6.6 MB; the rest of the difference is the git
# objects behind it under .git/modules, which a recursive init also fetches.
#
# The second copy is never built. db25-physical-plan guards its
# add_subdirectory on `if(NOT TARGET db25logicalplan)`, and this super-build
# adds its own copy first, so the physical planner reuses that target and its
# own submodule is simply ignored. Cloning it buys nothing and invites someone
# to edit the copy that is not being compiled.
#
# Nothing is lost by leaving it uninitialised. scripts/check_pin_agreement.sh
# still compares the two pins, because it reads gitlinks out of git metadata
# (`git ls-tree HEAD`) rather than the working tree - so the drift guard works
# on a checkout where the second copy does not exist on disk.
#
# See docs/design/dependency-composition.md, section 4.
set -euo pipefail

root="${1:-$(git -C "$(dirname "${BASH_SOURCE[0]}")" rev-parse --show-toplevel)}"

if ! git -C "$root" rev-parse --git-dir >/dev/null 2>&1; then
    echo "bootstrap: $root is not a git checkout" >&2
    exit 1
fi

# The logical-plan stack, in full: it brings the analyzer, the parser and the
# tokenizer, all of which ARE compiled here.
git -C "$root" submodule update --init --recursive external/db25-logical-plan

# The physical planner, WITHOUT its own copy of the above.
git -C "$root" submodule update --init external/db25-physical-plan

# Fail loudly rather than leaving a half-initialised tree for CMake to trip on
# with a less obvious message.
for required in external/db25-logical-plan \
                external/db25-logical-plan/external/parser \
                external/db25-logical-plan/external/analyzer \
                external/db25-physical-plan; do
    if [ ! -f "$root/$required/CMakeLists.txt" ]; then
        echo "bootstrap: $required did not initialise (no CMakeLists.txt)" >&2
        exit 1
    fi
done

echo "bootstrap: ready. Configure with:"
echo "  cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=Debug"
