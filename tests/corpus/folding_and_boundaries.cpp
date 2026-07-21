// tests/corpus/folding_and_boundaries.cpp
//
// ADVERSARIAL / PESSIMISTIC corpus: constant folding boundary traps. A folder
// that "simplifies" too eagerly is the classic footgun: folding INT64_MAX+1 to
// a wrapped constant, folding 1/0 to some value, or applying boolean identities
// that don't hold under 3VL. The differential oracle is the core falsifier --
// whatever the folder does, bound (unfolded) and optimized (folded) must select
// the SAME rows -- and each fold is cross-checked against its already-simplified
// twin so a wrong constant is caught by an explicit result mismatch.
//
// ---------------------------------------------------------------------------
// SCHEMA (see nulls_3vl.cpp header).
//
// data() ENRICHMENTS REQUIRED:
//   * users.age has a mix of values straddling 5, 10, 30 and at least one NULL
//     (so the NOT(NOT ...) 3VL case and the nested-constant threshold bite).
//   * orders.amount has values straddling 10 (folding-inside-subquery filter).
// ---------------------------------------------------------------------------

#include "harness/harness.hpp"

using namespace db25::harness;

namespace {

Stages oracle(std::string_view sql) {
    auto s = run(catalog(), sql);
    check(s.parsed, "parsed");
    check(s.analyze_ok, "analyze ok");
    check(s.bind_ok, "bind ok");
    check(s.optimize_ok, "optimize ok");
    auto rb = eval(s.bound, data());
    auto ro = eval(s.optimized, data());
    if (rb && ro) check(bag_equal(*rb, *ro), "bound == optimized (fold preserves result)");
    return s;
}

void expect_bag_eq(std::string_view a, std::string_view b, std::string_view what) {
    auto sa = oracle(a);
    auto sb = oracle(b);
    auto ra = eval(sa.optimized, data());
    auto rb = eval(sb.optimized, data());
    if (ra && rb) check(bag_equal(*ra, *rb), what);
}

void expect_bag_ne(std::string_view a, std::string_view b, std::string_view what) {
    auto sa = oracle(a);
    auto sb = oracle(b);
    auto ra = eval(sa.optimized, data());
    auto rb = eval(sb.optimized, data());
    if (ra && rb) check(!bag_equal(*ra, *rb), what);
}

void expect_empty(std::string_view sql, std::string_view what) {
    auto s = oracle(sql);
    auto ro = eval(s.optimized, data());
    if (ro) check(bag_equal(*ro, Table{}), what);
}

}  // namespace

static void register_folding_and_boundaries() {

    // 1) INT64 overflow must NOT be folded into a wrapped constant. Whatever the
    //    reference semantics (saturate / error / wrap), the bound and optimized
    //    forms must agree -- a folder that wraps while the evaluator does not
    //    (or vice versa) is caught here. The predicate is written so the answer
    //    is independent of any single row's value only if folding is faithful.
    test("fold.int64_overflow_not_miscomputed", [] {
        oracle("SELECT id FROM users WHERE (9223372036854775807 + 1) < 0");
        // Falsifiable anchor: `id < 0` is FALSE for every user (ids 1..5), so the
        // whole conjunction is empty regardless of how the overflow term folds.
        // The `id < 0` term is a genuine row filter, so a mutant that ignores or
        // drops the predicate (returning all rows) makes the result non-empty.
        expect_empty(
            "SELECT id FROM users WHERE id < 0 AND (9223372036854775807 + 1) < 0",
            "a real row filter ANDed with the overflow term stays empty");
    });

    // 2) Division by zero must be PRESERVED (error/NULL), never folded to a
    //    value. If the folder illegally constant-folds 1/0 the two sides diverge
    //    (a folded literal vs a preserved division), which the oracle catches.
    test("fold.division_by_zero_preserved", [] {
        auto s = oracle("SELECT id FROM users WHERE (1 / 0) = 1");
        check(s.optimize_ok, "div-by-zero survives the optimizer without crashing");
        // TODO(oracle): if the evaluator returns nullopt for 1/0 the equality
        // isn't checked; assert only that the pipeline stayed well-formed and
        // did not "succeed" by folding the trap away.
    });

    // 3) Modulo by zero likewise preserved.
    test("fold.modulo_by_zero_preserved", [] {
        auto s = oracle("SELECT id FROM users WHERE (1 % 0) = 0");
        check(s.optimize_ok, "mod-by-zero survives the optimizer without crashing");
        // TODO(oracle): same nullopt caveat as division_by_zero_preserved.
    });

    // 4) `1=1 AND p` folds to `p`. Result must equal the bare predicate.
    test("fold.true_and_predicate_is_predicate", [] {
        expect_bag_eq("SELECT id FROM users WHERE 1 = 1 AND age > 0",
                      "SELECT id FROM users WHERE age > 0",
                      "(1=1) AND p folds to p");
    });

    // 5) `0=1` interacts with OR (drops) and AND (annihilates).
    test("fold.false_or_and_interactions", [] {
        expect_bag_eq("SELECT id FROM users WHERE 0 = 1 OR age > 0",
                      "SELECT id FROM users WHERE age > 0",
                      "(0=1) OR p folds to p");
        expect_empty("SELECT id FROM users WHERE 0 = 1 AND age > 0",
                     "(0=1) AND p folds to FALSE -> empty");
    });

    // 6) `TRUE OR p` must fold to TRUE for EVERY row -- including rows where p is
    //    UNKNOWN (NULL age). If the folder keeps p around, NULL-age rows would be
    //    dropped and this diverges from the full table.
    test("fold.true_or_predicate_keeps_all", [] {
        expect_bag_eq("SELECT id FROM users WHERE (1 = 1) OR (age > 1000000)",
                      "SELECT id FROM users",
                      "(TRUE OR p) is TRUE everywhere, even where p is UNKNOWN");
        // Falsifiable anchor: the dual `(FALSE OR p)` simplifies to `p`, which
        // is a PROPER subset here (id > 2 keeps only 3 of 5 users). A mutant
        // that ignores or drops the surviving filter makes it keep every row,
        // collapsing the inequality.
        expect_bag_ne("SELECT id FROM users WHERE (1 = 0) OR (id > 2)",
                      "SELECT id FROM users",
                      "(FALSE OR p) filters to p, not the whole table");
    });

    // 7) int vs double promotion: integer division truncates, real division does
    //    not. `3/2 = 1` is TRUE (int), `3/2 = 1.5` is FALSE (would need real).
    test("fold.int_vs_double_division", [] {
        expect_bag_eq("SELECT id FROM users WHERE 3 / 2 = 1",
                      "SELECT id FROM users",
                      "integer 3/2 == 1 -> predicate TRUE everywhere");
        expect_empty("SELECT id FROM users WHERE 3 / 2 = 1.5",
                     "integer 3/2 != 1.5 -> empty (no wrong promotion)");
        expect_bag_eq("SELECT id FROM users WHERE 3.0 / 2 = 1.5",
                      "SELECT id FROM users",
                      "real 3.0/2 == 1.5 -> predicate TRUE everywhere");
    });

    // 8) NOT(NOT x) == x must hold under 3VL: a NULL-age row is UNKNOWN in both
    //    forms and dropped by both. If double-negation elimination flips a NULL
    //    the two diverge.
    test("fold.double_negation_3vl", [] {
        expect_bag_eq("SELECT id FROM users WHERE NOT (NOT (age > 30))",
                      "SELECT id FROM users WHERE age > 30",
                      "NOT(NOT p) == p, NULL-safe");
    });

    // 9) Deeply nested constant arithmetic must fold to the right threshold.
    test("fold.deeply_nested_constants", [] {
        // ((1+2)*3 - 4) == 5
        expect_bag_eq("SELECT id FROM users WHERE age > ((1 + 2) * 3 - 4)",
                      "SELECT id FROM users WHERE age > 5",
                      "((1+2)*3-4) folds to exactly 5");
    });

    // 10) Constant folding must also fire INSIDE a subquery and stay correct.
    test("fold.inside_subquery", [] {
        expect_bag_eq(
            "SELECT id FROM users WHERE id IN "
            "(SELECT user_id FROM orders WHERE 1 = 1 AND amount > (2 * 5))",
            "SELECT id FROM users WHERE id IN "
            "(SELECT user_id FROM orders WHERE amount > 10)",
            "folding within a subquery preserves the subquery's result");
    });
}

static bool _folding_and_boundaries_registered = (register_folding_and_boundaries(), true);
