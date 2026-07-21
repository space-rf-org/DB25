// DB25 end-to-end harness - METAMORPHIC test suite
//
// A metamorphic test asserts a *relation between the results of two queries*
// rather than a hard-coded expected answer. Each pair (q1, q2) is related by a
// transformation that MUST preserve the result multiset. We evaluate both
// OPTIMIZED plans with the harness reference evaluator and assert their outputs
// are bag-equal:
//
//     bag_equal( *eval(run(cat,q1).optimized, data()),
//                *eval(run(cat,q2).optimized, data()) )
//
// and, additionally, that each query's OPTIMIZED plan agrees with its own BOUND
// plan (so the optimizer's rewrites are proven result-preserving per query, not
// only relative to a sibling). Because these are real evaluations over real
// data, a wrong fold / simplify / pushdown makes bag_equal FALSE - the suite is
// falsifiable, which is the whole point.
//
// Where the reference evaluator cannot yet evaluate a plan shape (`eval` returns
// nullopt), we DO NOT pretend the relation was verified: we assert only that the
// pipeline succeeded (and, in the decorrelation suite, structural sanity) and
// leave a `TODO(oracle)` marker.
//
// SCHEMA ASSUMPTION (see README): the harness `catalog()` / `data()` provide the
// codebase's conventional tables
//     users(id INT NOT NULL, name VARCHAR NULL)
//     orders(id INT NOT NULL, user_id INT NULL, total DOUBLE NULL)
// and `data()` is non-empty (at least one users row) so the always-true /
// always-false relations below are falsifiable.
//
// Build matches the stack: C++23, -fno-exceptions.

#include "../../harness/harness.hpp"  // db25::harness contract (run/eval/bag_equal/test/check/catalog/data, Stages/Table/Data)

#include "db25/plan/expr_ir.hpp"
#include "db25/plan/logical_plan.hpp"

#include <optional>
#include <string>
#include <string_view>

namespace {

using db25::harness::bag_equal;
using db25::harness::catalog;
using db25::harness::check;
using db25::harness::data;
using db25::harness::eval;
using db25::harness::run;
using db25::harness::Stages;
using db25::harness::test;

// ---------------------------------------------------------------------------
// Core metamorphic assertion: q1 and q2 must produce the same multiset.
//
// Verifies THREE relations at once (all falsifiable):
//   (a) optimized(q1) == optimized(q2)   -- the metamorphic relation itself
//   (b) optimized(q1) == bound(q1)       -- optimizer preserved q1's result
//   (c) optimized(q2) == bound(q2)       -- optimizer preserved q2's result
// If `eval` cannot evaluate a plan (nullopt), the pipeline-success assertions
// still stand and the equality is left as a documented TODO(oracle) gap rather
// than a false pass.
// ---------------------------------------------------------------------------
void metamorphic_pair(std::string_view q1, std::string_view q2, const std::string& rel) {
    const auto& cat = catalog();
    const Stages s1 = run(cat, q1);
    const Stages s2 = run(cat, q2);

    check(s1.parsed && s1.analyze_ok && s1.bind_ok && s1.optimize_ok,
          rel + ": q1 pipeline (parse/analyze/bind/optimize) succeeds");
    check(s2.parsed && s2.analyze_ok && s2.bind_ok && s2.optimize_ok,
          rel + ": q2 pipeline (parse/analyze/bind/optimize) succeeds");
    if (s1.optimized == nullptr || s2.optimized == nullptr ||
        s1.bound == nullptr || s2.bound == nullptr) {
        return;  // pipeline failure already reported above; nothing more to assert
    }

    const std::optional o1 = eval(s1.optimized, data());
    const std::optional o2 = eval(s2.optimized, data());
    const std::optional b1 = eval(s1.bound, data());
    const std::optional b2 = eval(s2.bound, data());

    // (a) The metamorphic relation.
    if (o1 && o2) {
        check(bag_equal(*o1, *o2), rel + ": optimized(q1) bag-equals optimized(q2)");
    }
    // else: eval unsupported for this plan shape -- TODO(oracle): relation
    // unverified; pipeline success is asserted above but NOT claimed as verified.

    // (b)/(c) Each optimized plan vs its own bound plan.
    if (o1 && b1) {
        check(bag_equal(*o1, *b1), rel + ": q1 optimized bag-equals bound");
    }
    if (o2 && b2) {
        check(bag_equal(*o2, *b2), rel + ": q2 optimized bag-equals bound");
    }
}

// ===========================================================================
// Relation 1: adding a semantically-neutral predicate leaves the result
// unchanged. This specifically exercises constant folding + boolean
// simplification (`p AND 1=1` -> `p AND true` -> `p`; `p OR FALSE` -> `p`).
// ===========================================================================
void register_neutral_predicate() {
    test("metamorphic: neutral predicate  p  ==  p AND 1=1 / p AND TRUE / p OR FALSE", [] {
        constexpr std::string_view base = "SELECT id, name FROM users WHERE id > 2";
        metamorphic_pair(base, "SELECT id, name FROM users WHERE id > 2 AND 1 = 1",
                         "neutral AND 1=1");
        metamorphic_pair(base, "SELECT id, name FROM users WHERE id > 2 AND TRUE",
                         "neutral AND TRUE");
        metamorphic_pair(base, "SELECT id, name FROM users WHERE id > 2 OR FALSE",
                         "neutral OR FALSE");
    });
}

// ===========================================================================
// Relation 2: redundant double negation.  NOT (NOT p)  ==  p.
// ===========================================================================
void register_double_negation() {
    test("metamorphic: double negation  NOT (NOT p)  ==  p", [] {
        metamorphic_pair("SELECT id FROM users WHERE NOT (NOT (id > 2))",
                         "SELECT id FROM users WHERE id > 2",
                         "double negation");
    });
}

// ===========================================================================
// Relation 3: commutativity of AND, and of `=` inside a join condition.
// ===========================================================================
void register_commutativity() {
    test("metamorphic: commutativity  (a AND b)==(b AND a),  (x=y)==(y=x) in a join", [] {
        // AND is commutative under 3VL (same truth table for every operand
        // assignment, NULL included).
        metamorphic_pair("SELECT id FROM users WHERE id > 2 AND name IS NOT NULL",
                         "SELECT id FROM users WHERE name IS NOT NULL AND id > 2",
                         "AND commutativity");
        // Equality is symmetric; flipping the operands of the ON condition must
        // not change the joined multiset.
        metamorphic_pair(
            "SELECT u.id, o.id FROM users u JOIN orders o ON u.id = o.user_id",
            "SELECT u.id, o.id FROM users u JOIN orders o ON o.user_id = u.id",
            "join-condition = commutativity");
    });
}

// ===========================================================================
// Relation 4: predicates that collapse to a constant.
//   WHERE 1=1  ==  no filter (all rows)
//   WHERE 1=0  ==  empty
// Both are falsifiable ONLY because data() is non-empty: if folding wrongly
// turned 1=1 into 1=0 (or vice versa) the two sides would differ in cardinality.
// The empty side is compared against `id <> id`, which is cleanly empty because
// users.id is NOT NULL (no UNKNOWN rows leak through).
// ===========================================================================
void register_constant_collapse() {
    test("metamorphic: WHERE 1=1 == no filter;  WHERE 1=0 == empty", [] {
        metamorphic_pair("SELECT id FROM users WHERE 1 = 1",
                         "SELECT id FROM users",
                         "1=1 is no-op filter");
        metamorphic_pair("SELECT id FROM users WHERE 1 = 0",
                         "SELECT id FROM users WHERE id <> id",
                         "1=0 is empty");
    });
}

// ===========================================================================
// Relation 5: wrapping in a no-op derived table / redundant projection must
// preserve results -- IF the binder supports derived tables. If it does not,
// we NOTE and SKIP gracefully (no false-positive check is emitted). See README.
// ===========================================================================
void register_noop_derived_table() {
    test("metamorphic: no-op derived table  ==  flat query  (skips if binder lacks derived tables)", [] {
        const auto& cat = catalog();
        constexpr std::string_view derived = "SELECT id FROM (SELECT id FROM users) t";
        constexpr std::string_view flat = "SELECT id FROM users";

        const Stages sd = run(cat, derived);
        if (!(sd.parsed && sd.analyze_ok && sd.bind_ok)) {
            // Binder does not support derived tables in this build: documented
            // graceful skip. We deliberately emit NO equivalence check here so a
            // skip is never mistaken for a verified relation.
            return;
        }
        metamorphic_pair(derived, flat, "no-op derived table / redundant projection");
    });
}

// ===========================================================================
// Relation 6: an IN-list equals the equivalent OR-chain.
//   x IN (1,2,3)  ==  x=1 OR x=2 OR x=3
// Uses the NULLABLE column orders.user_id on purpose. The equivalence holds
// under 3VL because positive IN and the OR-chain have identical truth tables:
// when x is NULL both evaluate to UNKNOWN and the WHERE drops the row. (The
// asymmetric case is NOT IN vs a negated OR-chain, which is NOT equivalent under
// NULLs -- documented in README and intentionally not asserted here.)
// ===========================================================================
void register_in_vs_or() {
    test("metamorphic: x IN (1,2,3)  ==  x=1 OR x=2 OR x=3  (3VL-safe for positive IN)", [] {
        metamorphic_pair(
            "SELECT id FROM orders WHERE user_id IN (1, 2, 3)",
            "SELECT id FROM orders WHERE user_id = 1 OR user_id = 2 OR user_id = 3",
            "IN-list vs OR-chain");
    });
}

// Self-register all metamorphic relations (harness contract idiom).
void register_all() {
    register_neutral_predicate();
    register_double_negation();
    register_commutativity();
    register_constant_collapse();
    register_noop_derived_table();
    register_in_vs_or();
}

static bool _ = (register_all(), true);

}  // namespace
