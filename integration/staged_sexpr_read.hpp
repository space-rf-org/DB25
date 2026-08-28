// Phase B: s-expression READERS for the staged-artifact harness.
//
// The writers (staged_sexpr.hpp) serialize a pipeline artifact to canonical
// s-expr. The readers do the inverse for the plan layer - parse that s-expr
// back into an owned LogicalNode tree - which enables:
//   * round-trip verification: write(read(golden)) == golden, proving the plan
//     s-expr is a lossless encoding of what the writer renders; and
//   * per-module INJECTION: feed a stage a hand-crafted or fuzzed plan directly,
//     independent of the binder that would normally produce it.
//
// Coverage spans every construct the staged corpus exercises (scan/filter/
// project/join, aggregate, window + OVER, sort, limit, set-op, recursive-CTE,
// values, and the full expression grammar incl. cast / like / between / isnull /
// boolean-test / case / in-list / quantified-subquery). Any construct the reader
// does not understand still produces a null result with a message in `error`,
// never a wrong tree.
#pragma once

#include <memory>
#include <string>
#include <string_view>

namespace db25::plan {
struct LogicalNode;
using LogicalNodePtr = std::unique_ptr<LogicalNode>;
}  // namespace db25::plan

namespace db25::staged {

// Parse canonical plan s-expr (as produced by plan_to_sexpr) into an owned
// LogicalNode tree. Returns null and sets `error` on a parse failure or an
// unsupported construct. Provenance ids ARE reconstructed: the writer renders a
// base column's (table_id, column_id) as :tid / :cid on schema columns and on
// colref / outerref expressions, and the reader restores them - so a read-back
// plan is FULL-fidelity (identical to the bound plan, not one with the ids
// dropped). The round-trip is exact for every rendered field.
[[nodiscard]] db25::plan::LogicalNodePtr plan_from_sexpr(std::string_view sexpr,
                                                         std::string& error);

}  // namespace db25::staged
