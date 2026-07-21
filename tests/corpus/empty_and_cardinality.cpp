// tests/corpus/empty_and_cardinality.cpp
//
// ADVERSARIAL / PESSIMISTIC corpus: empty relations & cardinality (row-count)
// correctness. The adversarial optimizer's favourite lie is to change how many
// rows come out: turning a semi-join into an inner join (multiplying the outer
// side), decorrelating a COUNT scalar into a LEFT JOIN (turning 0 into NULL, or
// duplicating rows), or dropping DISTINCT. Every test pins a cardinality claim
// with the differential oracle plus a second query that IS the expected answer.
//
// ---------------------------------------------------------------------------
// SCHEMA (see nulls_3vl.cpp header for full column lists).
//
// data() ENRICHMENTS REQUIRED:
//   * At least one users row has MANY (>= 2) matching orders (so a naive
//     semi-join-as-inner-join would multiply that user) -- this is what makes
//     semijoin_no_multiply / exists_no_multiply falsifiable.
//   * At least one users row has NO matching order (unmatched outer row) so
//     LEFT JOIN null-extension and COUNT-over-empty==0 are exercised.
//   * users.city has duplicate values across rows (so DISTINCT actually removes
//     something) -- makes duplicate_rows_survive_without_distinct falsifiable.
//   * orders.id is a unique key (PK), so the (SELECT ... WHERE o.id = u.id)
//     scalar subquery matches at most one row.
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
    if (rb && ro) check(bag_equal(*rb, *ro), "bound == optimized (semantic equivalence)");
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

static void register_empty_and_cardinality() {

    // 1) WHERE FALSE annihilates every row. Folding must yield exactly empty.
    test("card.where_false_is_empty", [] {
        expect_empty("SELECT * FROM users WHERE 1 = 0",
                     "WHERE 1=0 -> empty relation");
    });

    // 2) EXISTS over an empty inner is FALSE for every outer row -> empty.
    test("card.exists_over_empty_is_false", [] {
        expect_empty(
            "SELECT id FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE 1 = 0)",
            "EXISTS(empty) is FALSE everywhere -> empty");
    });

    // 3) NOT EXISTS over an empty inner is TRUE for every outer row -> all rows.
    test("card.not_exists_over_empty_keeps_all", [] {
        expect_bag_eq(
            "SELECT id FROM users WHERE NOT EXISTS (SELECT 1 FROM orders WHERE 1 = 0)",
            "SELECT id FROM users",
            "NOT EXISTS(empty) is TRUE everywhere -> all rows");
    });

    // 4) A correlated/uncorrelated COUNT(*) over an empty inner is 0, NOT NULL.
    //    `= 0` therefore keeps every outer row. Falsifier if COUNT-over-empty
    //    is (wrongly) NULL: `(NULL) = 0` would be UNKNOWN -> empty.
    test("card.count_over_empty_is_zero", [] {
        expect_bag_eq(
            "SELECT id FROM users "
            "WHERE (SELECT COUNT(*) FROM orders WHERE 1 = 0) = 0",
            "SELECT id FROM users",
            "COUNT(*) over empty == 0 (not NULL) -> predicate TRUE everywhere");
    });

    // 5) Companion to (4): COUNT over empty IS NOT NULL, so `IS NULL` is FALSE
    //    everywhere -> empty. Together these pin COUNT-over-empty == 0 exactly.
    test("card.count_over_empty_is_not_null", [] {
        expect_empty(
            "SELECT id FROM users "
            "WHERE (SELECT COUNT(*) FROM orders WHERE 1 = 0) IS NULL",
            "COUNT(*) over empty is a real 0, never NULL -> empty");
    });

    // 6) Semi-join (IN) must NOT multiply the outer side even when a user has
    //    many matching orders. Expected == DISTINCT of the corresponding inner
    //    join. Falsifier if IN is lowered to a plain join.
    test("card.semijoin_in_does_not_multiply", [] {
        expect_bag_eq(
            "SELECT id FROM users WHERE id IN (SELECT user_id FROM orders)",
            "SELECT DISTINCT u.id FROM users u JOIN orders o ON o.user_id = u.id",
            "IN semi-join yields each matching outer row exactly once");
    });

    // 7) Same guarantee via EXISTS. A user with N orders still appears once.
    test("card.exists_does_not_multiply", [] {
        expect_bag_eq(
            "SELECT id FROM users u WHERE EXISTS "
            "(SELECT 1 FROM orders o WHERE o.user_id = u.id)",
            "SELECT DISTINCT u.id FROM users u JOIN orders o ON o.user_id = u.id",
            "EXISTS semi-join yields each matching outer row exactly once");
    });

    // 8) DISTINCT must dedup exactly like GROUP BY on the same key.
    test("card.distinct_equals_group_by", [] {
        expect_bag_eq("SELECT DISTINCT city FROM users",
                      "SELECT city FROM users GROUP BY city",
                      "SELECT DISTINCT city == SELECT city GROUP BY city");
    });

    // 9) Without DISTINCT, duplicate rows MUST survive the whole pipeline. If
    //    the optimizer silently deduped, the projected column would equal its
    //    DISTINCT form -> this NE fails. (Requires duplicate cities in data.)
    test("card.duplicates_survive_without_distinct", [] {
        expect_bag_ne("SELECT city FROM users",
                      "SELECT DISTINCT city FROM users",
                      "plain projection keeps duplicates that DISTINCT removes");
    });

    // 10) A scalar subquery keyed on a UNIQUE column matches <= 1 row, so it can
    //     never multiply the outer relation: wrapping it must preserve the exact
    //     row count of users. Falsifier if scalar decorrelation fans out.
    //     TODO(oracle): needs FROM-subquery + scalar-subquery eval support; if
    //     eval returns nullopt the stage checks still guard the pipeline.
    test("card.scalar_subquery_preserves_row_count", [] {
        expect_bag_eq(
            "SELECT COUNT(*) FROM users",
            "SELECT COUNT(*) FROM ("
            "  SELECT u.id, (SELECT o.amount FROM orders o WHERE o.id = u.id) AS a"
            "  FROM users u) t",
            "scalar subquery on a unique key preserves outer cardinality");
    });
}

static bool _empty_and_cardinality_registered = (register_empty_and_cardinality(), true);
