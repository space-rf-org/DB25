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
// Coverage is honest and partial: constructs the reader does not yet understand
// produce a null result with a message in `error`, never a wrong tree.
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
// unsupported construct. NOTE: provenance ids the writer does not render
// (ColumnSchema table_id/column_id, Expr ref_table_id/ref_column_id) are not
// reconstructed - the round-trip is exact for the RENDERED fields.
[[nodiscard]] db25::plan::LogicalNodePtr plan_from_sexpr(std::string_view sexpr,
                                                         std::string& error);

}  // namespace db25::staged
