// DB25 end-to-end test harness - public contract (namespace db25::harness).
//
// This header is the contract the sibling test-authoring agents code against.
// It provides:
//   * a NULL-aware Value / Row / Table / Data model,
//   * a Stages driver over the full pipeline (parse -> analyze -> bind ->
//     optimize), owning the bound + optimized plans,
//   * a reference evaluator eval() with three-valued (NULL) logic and correlated
//     subquery support (nested-loop reference semantics),
//   * bag_equal() multiset equality (the differential oracle helper),
//   * a tiny test framework (test / check / run_all) plus the FALSIFIABILITY
//     GATE (run_gate) driven by a global mutant selector.
//
// The whole DB25 stack is built -fno-exceptions; this harness matches that.
// "Unsupported" is therefore a first-class, non-crashing outcome: eval() returns
// std::optional<Table> (nullopt == unsupported shape), so a differential check
// can be skipped rather than aborting.

#pragma once

#include "db25/plan/logical_plan.hpp"
#include "db25/plan/expr_ir.hpp"
#include "db25/semantic/catalog.hpp"
#include "db25/semantic/diagnostic.hpp"
#include "db25/ast/node_types.hpp"

#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

namespace db25::harness {

// ---------------------------------------------------------------------------
// NULL-aware value model.
//   std::monostate  == SQL NULL
//   arms: int64 / double / bool / string
// ---------------------------------------------------------------------------
using Value = std::variant<std::monostate, std::int64_t, double, bool, std::string>;
using Row = std::vector<Value>;
using Table = std::vector<Row>;
using Data = std::map<std::string, Table>;  // table_name -> rows (full catalog column order)

// NULL sentinel + constructors (thin, but they make test data readable).
inline Value null() { return Value{std::monostate{}}; }
inline Value vi(std::int64_t v) { return Value{v}; }
inline Value vd(double v) { return Value{v}; }
inline Value vb(bool v) { return Value{v}; }
inline Value vs(std::string v) { return Value{std::move(v)}; }

[[nodiscard]] bool is_null(const Value& v);
// NULL-aware equality where NULL == NULL is true (grouping / multiset / DISTINCT
// semantics, NOT SQL `=`). Used by bag_equal.
[[nodiscard]] bool value_identical(const Value& a, const Value& b);
[[nodiscard]] std::string value_to_string(const Value& v);

// ---------------------------------------------------------------------------
// Pipeline driver.
// ---------------------------------------------------------------------------
struct Stages {
    bool parsed = false;
    bool analyze_ok = false;
    bool bind_ok = false;
    bool optimize_ok = false;
    std::string bind_error;

    // Borrowed views into the owned plans below (null until the stage succeeds).
    db25::plan::LogicalNode* bound = nullptr;
    db25::plan::LogicalNode* optimized = nullptr;

    std::string bound_dump;
    std::string optimized_dump;

    // --- Stage-artifact fingerprints (for the conformance suite) ---
    // The parser's AST and the analyzer's diagnostics live only *inside* run()
    // (the AST is arena-owned by the Parser, which is a run()-local). To let a
    // conformance test assert what each pipeline STAGE produced - not just the
    // final plan - run() snapshots their salient contents here while they are
    // still alive. `ast_kinds` is every NodeType present in the parsed tree (the
    // parse-stage feature fingerprint); `diags` is a copy of the analyzer's
    // diagnostics (the analyze-stage effect). Plan-stage propagation is asserted
    // directly against `bound` / `optimized` with plan_contains(). Together they
    // let a test verify a feature seen at parse time actually reaches - and
    // transforms into - the expected downstream artifact.
    std::vector<db25::ast::NodeType> ast_kinds;
    std::vector<db25::semantic::Diagnostic> diags;

    // Owners - keep the plans alive for the lifetime of the Stages value. Held
    // by shared_ptr so Stages is copyable (the test suites pass Stages around by
    // value and store them in `const` locals); copies share the same plans. The
    // bound plan is a second, independent bind of the same statement so both the
    // pre- and post-optimization trees are simultaneously live.
    std::shared_ptr<db25::plan::LogicalNode> bound_owner;
    std::shared_ptr<db25::plan::LogicalNode> optimized_owner;
};

// Drive the full pipeline for `sql` against `cat`. Owns the Parser/Analyzer for
// the duration of binding; the returned plans are self-contained (owned Expr
// trees, copied strings) and remain valid after run() returns.
[[nodiscard]] Stages run(const db25::semantic::InMemoryCatalog& cat, std::string_view sql);

// ---------------------------------------------------------------------------
// Reference evaluator.
//   Returns nullopt when the plan uses an operator/expression the evaluator does
//   not implement (a first-class, non-crashing "unsupported" outcome).
// ---------------------------------------------------------------------------
[[nodiscard]] std::optional<Table> eval(const db25::plan::LogicalNode* plan, const Data& data);

// Multiset (order-independent) equality with NULL-aware element comparison.
[[nodiscard]] bool bag_equal(const Table& a, const Table& b);

// Sequence (order-SENSITIVE) equality: rows must match position-by-position.
// Use for ORDER BY tests - bag_equal cannot detect a dropped/reversed Sort.
[[nodiscard]] bool seq_equal(const Table& a, const Table& b);

// ---------------------------------------------------------------------------
// Structural plan predicates (for tests that assert an optimization happened).
// ---------------------------------------------------------------------------
[[nodiscard]] bool plan_contains(const db25::plan::LogicalNode* n, db25::plan::LogicalOp op);
[[nodiscard]] std::size_t count_subqueries(const db25::plan::LogicalNode* n);

// ---------------------------------------------------------------------------
// Stage-artifact predicates (for the conformance suite). These read the
// per-stage fingerprints captured on Stages so a test can assert that a SQL
// feature (a) was parsed, (b) produced / avoided a given analyzer diagnostic,
// and (c) propagated to the expected LogicalOp - i.e. its EFFECT survived into
// each subsequent artifact, not just that the query ran.
// ---------------------------------------------------------------------------
[[nodiscard]] bool ast_has(const Stages& s, db25::ast::NodeType kind);
[[nodiscard]] bool has_diagnostic(const Stages& s, db25::semantic::DiagnosticCode code);
[[nodiscard]] std::size_t error_count(const Stages& s);

// ---------------------------------------------------------------------------
// Shared default schema + data (rich enough for pessimistic tests).
// ---------------------------------------------------------------------------
[[nodiscard]] const db25::semantic::InMemoryCatalog& catalog();
[[nodiscard]] const Data& data();

// ---------------------------------------------------------------------------
// Test framework.
// ---------------------------------------------------------------------------
void test(std::string name, std::function<void()> body);
void check(bool cond, std::string_view what);

// Run every registered test once (clean unless DB25_MUTANT selects a mutant).
// Returns process exit code (0 == all passed).
int run_all();

// The falsifiability gate: clean run must pass; every mutant must be caught by
// >=1 test; any test that survives all mutants is reported NON-FALSIFIABLE
// (vacuous) and forces a non-zero exit. Returns process exit code.
int run_gate();

// ---------------------------------------------------------------------------
// Mutation control. `g_mutant` (0 == clean) is consulted globally by run()/eval;
// tests need no per-test wiring - a test is "falsifiable" iff some mutant makes
// it fail. See MUTANT CATALOG in harness.cpp.
// ---------------------------------------------------------------------------
extern int g_mutant;
[[nodiscard]] int mutant_count();
[[nodiscard]] const char* mutant_name(int id);

}  // namespace db25::harness
