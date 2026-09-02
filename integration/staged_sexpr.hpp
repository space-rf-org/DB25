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
#include <string_view>
#include <vector>

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

// Every ExprKind::Subquery embedded in a node's OWNED expressions, in a fixed
// traversal order.
//
// One function, used by BOTH the writer and the reader, and that is the whole
// point: the writer emits each subquery's inner plan as a `(subplan ...)` block
// in this order and the reader reattaches them in this order, so the two cannot
// disagree about which plan belongs to which subquery. Two collectors that had
// to be kept in step would eventually not be.
//
// It does NOT descend into a subquery's own inner plan - that plan is emitted
// separately, and its own subqueries with it, one level at a time.
void node_subqueries(const db25::plan::LogicalNode* n,
                     std::vector<db25::plan::Expr*>& out);

// Serialize the token stream (T1) to canonical s-expr: one atom per token,
// (kind "text" start end) with byte-offset spans. Trivia (whitespace / comment)
// is collapsed - kept only for lossless round-trip, never emitted as a token -
// per the whitespace/span policy in docs/layer-contracts.html. This is the
// span-authoritative layer.
[[nodiscard]] std::string tokens_to_sexpr(std::string_view sql);

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
