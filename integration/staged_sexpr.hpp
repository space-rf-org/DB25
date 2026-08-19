// Canonical s-expression serialization of DB25 pipeline artifacts.
//
// This is the "writer" half of the staged-artifact test harness (see
// docs/layer-contracts.html): a faithful, deterministic projection of each
// artifact into the canonical s-expr grammar, so one generic runner can pin the
// output of every stage from a single SQL fixture. A rendering here must be
// LOSSLESS enough to round-trip - it carries the fields that make the artifact
// what it is (op / kind, positional slots, per-node type + nullability, output
// schema), never a lossy sketch.
//
// Phase A implements the plan layers (logical / optimized). The AST / resolved
// AST / token writers follow.
#pragma once

#include <string>

namespace db25::plan {
struct LogicalNode;
struct Expr;
}  // namespace db25::plan

namespace db25::ast {
struct ASTNode;
}  // namespace db25::ast

namespace db25::semantic {
class Analyzer;
}  // namespace db25::semantic

namespace db25::staged {

// Serialize the parser's AST (T2, untyped) to canonical s-expr: node type,
// source span, captured text / qualifier, and structural flags. Nothing is
// resolved - no types, no catalog ids.
[[nodiscard]] std::string ast_to_sexpr(const db25::ast::ASTNode* root);

// Serialize the resolved AST (T3): the SAME tree, with the analyzer's in-place
// annotations layered on - per-node resolved type + nullability and resolved
// table / column ids where the analyzer set them.
[[nodiscard]] std::string resolved_ast_to_sexpr(const db25::ast::ASTNode* root,
                                                const db25::semantic::Analyzer& analyzer);


// Serialize a logical (or optimized) plan tree to canonical s-expr. Multi-line,
// indented by plan depth; expressions render inline. Deterministic: the same
// plan always produces byte-identical output.
[[nodiscard]] std::string plan_to_sexpr(const db25::plan::LogicalNode* root);

// Serialize a single owned expression tree to canonical s-expr (inline form).
[[nodiscard]] std::string expr_to_sexpr(const db25::plan::Expr& e);

}  // namespace db25::staged
