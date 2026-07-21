// tests/corpus/nulls_3vl.cpp
//
// ADVERSARIAL / PESSIMISTIC corpus: three-valued-logic (NULL) traps.
//
// The optimizer's decorrelation + constant folding + boolean simplification are
// the passes most likely to quietly turn UNKNOWN into FALSE (or vice versa).
// Every test here is a falsifier: the differential oracle asserts that the
// *bound* plan and the *optimized* plan evaluate to the same rows, and where a
// crisp 3VL identity exists it is checked against a second, hand-reasoned query
// (both run through the same reference evaluator, so no C++ Value literals are
// needed -- the second query IS the expected result).
//
// ---------------------------------------------------------------------------
// SCHEMA ASSUMPTIONS (for the CORE integrator who owns catalog()/data()):
//   users(id INT NOT NULL, name TEXT, age INT NULL, city TEXT NULL,
//         manager_id INT NULL)
//   orders(id INT NOT NULL, user_id INT NULL, product_id INT NULL,
//          amount INT NULL, status TEXT NULL)
//   products(id INT NOT NULL, name TEXT, price INT NULL, category TEXT NULL)
//   emp(id INT NOT NULL, name TEXT, mgr_id INT NULL, dept_id INT NULL,
//       salary INT NULL)
//
// data() ENRICHMENTS REQUIRED to make these sharp (please confirm in data()):
//   * users.age has at least one NULL AND at least one non-NULL row.
//   * orders.user_id has at least one NULL row (so a NOT IN over it is UNKNOWN).
//   * orders is non-empty; there exists at least one users.id that is NOT
//     present among orders.user_id (an "unmatched" user) so anti-join / NOT IN
//     sets are non-trivial.
// ---------------------------------------------------------------------------

#include "harness/harness.hpp"

using namespace db25::harness;

namespace {

// Differential oracle: run the pipeline, assert every stage, and assert the
// bound plan and optimized plan agree row-for-row (multiset). Returns the
// Stages so a caller can add a further expected-result check.
Stages oracle(std::string_view sql) {
    auto s = run(catalog(), sql);
    check(s.parsed, "parsed");
    check(s.analyze_ok, "analyze ok");
    check(s.bind_ok, "bind ok");
    check(s.optimize_ok, "optimize ok");
    auto rb = eval(s.bound, data());
    auto ro = eval(s.optimized, data());
    if (rb && ro) {
        check(bag_equal(*rb, *ro), "bound == optimized (semantic equivalence)");
    }
    // If eval is unsupported for this shape, the four stage checks above are
    // still falsifiable, so the test is never vacuous.
    return s;
}

// Expected-result cross-check: two queries must produce the same multiset.
// Both go through oracle() first (so each also gets bound==optimized), then the
// optimized results are compared. Unverified only if eval can't run the shape.
void expect_bag_eq(std::string_view a, std::string_view b, std::string_view what) {
    auto sa = oracle(a);
    auto sb = oracle(b);
    auto ra = eval(sa.optimized, data());
    auto rb = eval(sb.optimized, data());
    if (ra && rb) check(bag_equal(*ra, *rb), what);
}

// Expected inequality: the two queries must NOT be equal (a NULL-sensitivity
// falsifier -- if the engine collapses UNKNOWN to a definite value the two
// converge and this fails).
void expect_bag_ne(std::string_view a, std::string_view b, std::string_view what) {
    auto sa = oracle(a);
    auto sb = oracle(b);
    auto ra = eval(sa.optimized, data());
    auto rb = eval(sb.optimized, data());
    if (ra && rb) check(!bag_equal(*ra, *rb), what);
}

// Result must be exactly empty.
void expect_empty(std::string_view sql, std::string_view what) {
    auto s = oracle(sql);
    auto ro = eval(s.optimized, data());
    if (ro) check(bag_equal(*ro, Table{}), what);
}

}  // namespace

static void register_nulls_3vl() {

    // 1) x NOT IN (subquery that contains a NULL) is UNKNOWN for *every* outer
    //    row -> the whole result must be empty. The single classic 3VL trap.
    test("nulls.not_in_with_null_yields_empty", [] {
        // orders.user_id contains a NULL, so `id NOT IN (SELECT user_id ...)`
        // can never be TRUE for any user.
        expect_empty(
            "SELECT id FROM users WHERE id NOT IN (SELECT user_id FROM orders)",
            "NOT IN over a NULL-bearing subquery drops all rows (UNKNOWN)");
    });

    // 2) NOT IN (nullable) must NOT be equal to an anti-join on equality
    //    (NOT EXISTS eq), because NULL in the IN-list poisons the former but not
    //    the latter. If the optimizer wrongly decorrelated NOT IN into an
    //    AntiJoin they would converge -> this fails. (The header confirms NOT IN
    //    only becomes an AntiJoin when neither side is nullable.)
    test("nulls.not_in_differs_from_antijoin_under_null", [] {
        expect_bag_ne(
            "SELECT id FROM users WHERE id NOT IN (SELECT user_id FROM orders)",
            "SELECT id FROM users u WHERE NOT EXISTS "
            "(SELECT 1 FROM orders o WHERE o.user_id = u.id)",
            "NOT IN(nullable) != NOT EXISTS(eq) when a NULL is present");
    });

    // 3) `x = NULL` is UNKNOWN, never TRUE -> empty. And it must differ from the
    //    IS NULL form, which is a definite predicate.
    test("nulls.equals_null_is_not_is_null", [] {
        expect_empty("SELECT id FROM users WHERE age = NULL",
                     "age = NULL is UNKNOWN for every row -> empty");
        expect_bag_ne("SELECT id FROM users WHERE age = NULL",
                      "SELECT id FROM users WHERE age IS NULL",
                      "= NULL (empty) differs from IS NULL (the NULL rows)");
    });

    // 4) Boolean simplification trap: `p AND FALSE` must fold to FALSE (empty)
    //    even when p is UNKNOWN, whereas `p AND TRUE` must keep p unchanged.
    test("nulls.and_false_vs_and_true", [] {
        expect_empty("SELECT id FROM users WHERE (age IS NULL) AND (1 = 0)",
                     "UNKNOWN/anything AND FALSE -> FALSE -> empty");
        expect_bag_eq("SELECT id FROM users WHERE (age IS NULL) AND (1 = 1)",
                      "SELECT id FROM users WHERE age IS NULL",
                      "p AND TRUE == p (identity preserved by simplification)");
    });

    // 5) COUNT(nullable) skips NULLs; COUNT(*) does not. With NULL-bearing age
    //    the two must differ. Falsifier if COUNT(age) wrongly counts NULLs.
    test("nulls.count_nullable_vs_count_star", [] {
        expect_bag_ne("SELECT COUNT(age) FROM users",
                      "SELECT COUNT(*) FROM users",
                      "COUNT(age) < COUNT(*) because age has NULLs");
    });

    // 6) SUM over an all-NULL input group == SUM over an empty input == NULL.
    //    Falsifier if SUM returns 0 for the all-NULL case.
    test("nulls.sum_over_all_null_is_null_like_empty", [] {
        expect_bag_eq("SELECT SUM(age) FROM users WHERE age IS NULL",
                      "SELECT SUM(age) FROM users WHERE 1 = 0",
                      "SUM over all-NULL == SUM over empty == NULL");
    });

    // 7) AVG over an empty relation is NULL (not 0, not a divide-by-zero).
    //    Cross-checked against a different empty AVG so the shared answer is NULL.
    test("nulls.avg_over_empty_is_null", [] {
        expect_bag_eq("SELECT AVG(age) FROM users WHERE 1 = 0",
                      "SELECT AVG(amount) FROM orders WHERE 1 = 0",
                      "AVG over empty == NULL (both sides)");
    });

    // 8) IS NULL and IS NOT NULL are total and complementary (no UNKNOWN):
    //    every row is in exactly one partition. Their UNION ALL must reconstruct
    //    the full column. Falsifier if a NULL leaks into the IS NOT NULL side or
    //    is dropped from both.
    test("nulls.is_null_partitions_are_complementary", [] {
        expect_bag_eq(
            "SELECT id FROM users WHERE age IS NULL "
            "UNION ALL SELECT id FROM users WHERE age IS NOT NULL",
            "SELECT id FROM users",
            "(IS NULL) + (IS NOT NULL) reconstructs all rows exactly once");
    });

    // 9) An ordinary comparison partitions into THREE buckets: the third
    //    (NULL age) is only recoverable via IS NULL, never via > or <=.
    //    Falsifier if NULL rows sneak into > or <= (2VL bug).
    test("nulls.comparison_leaves_null_bucket", [] {
        expect_bag_eq(
            "SELECT id FROM users WHERE age > 30 "
            "UNION ALL SELECT id FROM users WHERE age <= 30 "
            "UNION ALL SELECT id FROM users WHERE age IS NULL",
            "SELECT id FROM users",
            "> , <= , and IS NULL together cover every row exactly once");
    });

    // 10) CASE with a NULL-guard branch must equal the COALESCE it is
    //     semantically identical to. Falsifier if CASE mishandles the NULL test.
    test("nulls.case_null_branch_equals_coalesce", [] {
        expect_bag_eq(
            "SELECT CASE WHEN age IS NULL THEN -1 ELSE age END FROM users",
            "SELECT COALESCE(age, -1) FROM users",
            "CASE WHEN age IS NULL THEN -1 ELSE age END == COALESCE(age,-1)");
    });

    // 11) NOT IN over an *empty* subquery is TRUE for every row (even NULL outer
    //     rows), because the vacuous universal holds. Result == the whole table.
    //     Falsifier if an empty IN-list is mishandled as UNKNOWN.
    test("nulls.not_in_empty_subquery_keeps_all", [] {
        expect_bag_eq(
            "SELECT * FROM orders "
            "WHERE user_id NOT IN (SELECT id FROM users WHERE 1 = 0)",
            "SELECT * FROM orders",
            "NOT IN (empty set) is TRUE for every row, including NULL user_id");
    });
}

static bool _nulls_3vl_registered = (register_nulls_3vl(), true);
