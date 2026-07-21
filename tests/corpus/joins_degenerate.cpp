// tests/corpus/joins_degenerate.cpp
//
// ADVERSARIAL / PESSIMISTIC corpus: degenerate & outer joins, and the single
// most dangerous optimizer bug -- pushing a predicate onto the NULL-supplying
// side of an outer join. The differential oracle (bound vs optimized) is the
// primary falsifier for every rewrite; outer-join semantics are additionally
// pinned against hand-reasoned equivalents (LEFT/RIGHT symmetry, FULL as a
// union of the one-sided joins).
//
// ---------------------------------------------------------------------------
// SCHEMA (see nulls_3vl.cpp header):
//   emp(id, name, mgr_id, dept_id, salary); mgr_id references emp.id (self-join,
//   at most one manager). users.id and orders.id both exist (for USING/NATURAL
//   over the common column name `id`).
//
// data() ENRICHMENTS REQUIRED:
//   * users and products both non-empty and small (cross join = |users|*|products|).
//   * some users with NO order and some orders whose user_id has NO user
//     (so LEFT / RIGHT / FULL null-extension all produce extended rows).
//   * orders.amount contains NULLs AND there exist unmatched users (so the
//     `WHERE o.amount IS NULL` after a LEFT JOIN has two distinct sources:
//     null-extended non-matches AND real NULL amounts) -- this is what makes the
//     no-pushdown test bite.
//   * emp has rows with non-NULL mgr_id pointing at existing emp.id, and some
//     with NULL mgr_id (top of hierarchy).
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
    if (rb && ro) check(bag_equal(*rb, *ro), "bound == optimized (join rewrite is faithful)");
    return s;
}

void expect_bag_eq(std::string_view a, std::string_view b, std::string_view what) {
    auto sa = oracle(a);
    auto sb = oracle(b);
    auto ra = eval(sa.optimized, data());
    auto rb = eval(sb.optimized, data());
    if (ra && rb) check(bag_equal(*ra, *rb), what);
}

}  // namespace

static void register_joins_degenerate() {

    // 1) CROSS JOIN cardinality == |A| * |B|, and it equals an INNER JOIN ON
    //    TRUE. Falsifier if cross join is mis-lowered or accidentally filtered.
    test("join.cross_equals_join_on_true", [] {
        expect_bag_eq(
            "SELECT u.id, p.id FROM users u CROSS JOIN products p",
            "SELECT u.id, p.id FROM users u JOIN products p ON 1 = 1",
            "CROSS JOIN == INNER JOIN ON TRUE (full cartesian product)");
    });

    // 2) Self-join on a unique key. Joining emp to emp on mgr_id = manager.id
    //    matches at most one manager row, so it equals the semi-join form
    //    (managed employees), with no multiplication.
    test("join.self_join_on_unique_key", [] {
        expect_bag_eq(
            "SELECT e.id FROM emp e JOIN emp m ON e.mgr_id = m.id",
            "SELECT e.id FROM emp e WHERE e.mgr_id IN (SELECT id FROM emp)",
            "self-join on unique manager id == semi-join, no multiply");
    });

    // 3) USING(id) merges the two `id` columns into ONE output column: the
    //    projected schema of `SELECT *` must be narrower than the same join
    //    written with ON (which keeps both id columns). Structural falsifier
    //    for frame compaction, plus a row-level equivalence on a shared column.
    test("join.using_compacts_frame", [] {
        auto su = oracle("SELECT * FROM users u JOIN orders o USING (id)");
        auto son = oracle("SELECT * FROM users u JOIN orders o ON u.id = o.id");
        if (su.optimize_ok && son.optimize_ok && su.optimized && son.optimized) {
            // USING drops the duplicate join column -> strictly fewer outputs.
            check(su.optimized->output.size() + 1 == son.optimized->output.size(),
                  "USING(id) yields exactly one fewer output column than ON u.id=o.id");
        }
        // Row-level: the surviving rows are the same on the shared key.
        expect_bag_eq(
            "SELECT u.id FROM users u JOIN orders o USING (id)",
            "SELECT u.id FROM users u JOIN orders o ON u.id = o.id",
            "USING(id) matches the same rows as ON u.id = o.id");
    });

    // 4) NATURAL JOIN over the sole common column `id` behaves like USING(id).
    test("join.natural_equals_using", [] {
        expect_bag_eq(
            "SELECT u.id FROM users u NATURAL JOIN orders o",
            "SELECT u.id FROM users u JOIN orders o USING (id)",
            "NATURAL JOIN (common col id) == USING(id)");
    });

    // 5) LEFT JOIN preserves EVERY left row (null-extending non-matches), so the
    //    DISTINCT set of left keys equals the full left table.
    test("join.left_join_preserves_all_left", [] {
        expect_bag_eq(
            "SELECT DISTINCT u.id FROM users u LEFT JOIN orders o ON o.user_id = u.id",
            "SELECT id FROM users",
            "LEFT JOIN keeps all left rows (null-extended when unmatched)");
    });

    // 6) RIGHT JOIN(A,B) == LEFT JOIN(B,A) with the projection swapped. A sharp
    //    outer-join symmetry law; falsifier if right-join null-extension is
    //    implemented asymmetrically.
    test("join.right_is_mirrored_left", [] {
        expect_bag_eq(
            "SELECT u.id, o.id FROM users u RIGHT JOIN orders o ON o.user_id = u.id",
            "SELECT u.id, o.id FROM orders o LEFT JOIN users u ON o.user_id = u.id",
            "A RIGHT JOIN B == B LEFT JOIN A (mirrored)");
    });

    // 7) FULL OUTER JOIN == (LEFT JOIN) UNION ALL (the RIGHT-only null-extended
    //    rows). This reconstructs the full-join multiset exactly.
    test("join.full_is_left_plus_right_only", [] {
        expect_bag_eq(
            "SELECT u.id, o.id FROM users u FULL JOIN orders o ON o.user_id = u.id",
            "SELECT u.id, o.id FROM users u LEFT JOIN orders o ON o.user_id = u.id "
            "UNION ALL "
            "SELECT u.id, o.id FROM users u RIGHT JOIN orders o ON o.user_id = u.id "
            "WHERE u.id IS NULL",
            "FULL JOIN == LEFT JOIN + (RIGHT-only null-extended rows)");
    });

    // 8) THE pushdown trap: a predicate on the NULL-SUPPLYING (right) side of a
    //    LEFT JOIN must NOT be pushed below the join. `WHERE o.amount IS NULL`
    //    legitimately selects BOTH null-extended non-matches AND real NULL
    //    amounts; pushing `o.amount IS NULL` into the orders scan pre-join would
    //    change which rows match and thus the whole result. The oracle asserts
    //    bound == optimized; any illegal push diverges. Additionally the result
    //    must be a strict SUPERSET of the pure "no match" rows.
    test("join.no_pushdown_into_null_supplying_side", [] {
        oracle(
            "SELECT u.id, o.amount FROM users u LEFT JOIN orders o "
            "ON o.user_id = u.id WHERE o.amount IS NULL");
        // The "no match at all" rows (o.id IS NULL) are a subset of the above;
        // if the optimizer wrongly pushed the amount filter it would lose the
        // real-NULL-amount matches, breaking this containment. We express it as:
        // (amount IS NULL) rows UNION ALL (matched rows with non-null amount)
        // reconstruct the plain LEFT JOIN, proving no row was dropped/pushed.
        expect_bag_eq(
            "SELECT u.id, o.amount FROM users u LEFT JOIN orders o "
            "ON o.user_id = u.id WHERE o.amount IS NULL "
            "UNION ALL "
            "SELECT u.id, o.amount FROM users u LEFT JOIN orders o "
            "ON o.user_id = u.id WHERE o.amount IS NOT NULL",
            "SELECT u.id, o.amount FROM users u LEFT JOIN orders o "
            "ON o.user_id = u.id",
            "amount IS NULL / IS NOT NULL partitions the LEFT JOIN with nothing "
            "pushed or dropped");
    });

    // 9) Contrast: on an INNER join a predicate on either side MAY be pushed and
    //    MUST stay equivalent whether written in WHERE or in the ON clause.
    test("join.inner_pushdown_is_equivalent", [] {
        expect_bag_eq(
            "SELECT u.id FROM users u JOIN orders o ON o.user_id = u.id "
            "WHERE o.amount > 100",
            "SELECT u.id FROM users u JOIN orders o "
            "ON o.user_id = u.id AND o.amount > 100",
            "INNER-join predicate is push-equivalent between WHERE and ON");
    });

    // 10) Multi-join with pushdown + column pruning. The oracle (bound vs
    //     optimized) is the real falsifier for the combined rewrite; the join
    //     chain is also pinned against a nested-IN equivalent.
    test("join.multi_join_pushdown_and_prune", [] {
        // DISTINCT on both sides so the equality is clean: the join form would
        // otherwise multiply u.id for users with several qualifying orders,
        // making a *correct* optimizer fail spuriously. The falsifier here is
        // the differential oracle inside expect_bag_eq (bound vs optimized for
        // the DISTINCT-join form) plus the nested-IN equivalent.
        expect_bag_eq(
            "SELECT DISTINCT u.id FROM users u "
            "JOIN orders o ON o.user_id = u.id "
            "JOIN products p ON p.id = o.product_id "
            "WHERE p.price > 50 AND u.age > 20",
            "SELECT u.id FROM users u WHERE u.age > 20 AND u.id IN ("
            "  SELECT o.user_id FROM orders o WHERE o.product_id IN ("
            "    SELECT p.id FROM products p WHERE p.price > 50))",
            "3-way join + pushdown/prune == the equivalent nested-IN filter");
    });
}

static bool _joins_degenerate_registered = (register_joins_degenerate(), true);
