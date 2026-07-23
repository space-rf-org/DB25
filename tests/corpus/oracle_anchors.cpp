// tests/corpus/oracle_anchors.cpp
//
// GROUND-TRUTH ORACLE ANCHORS.
//
// The bulk of the suite verifies the pipeline with the DIFFERENTIAL oracle:
// eval(bound) == eval(optimized). That is powerful for catching an optimizer
// that changes results, but it is near-self-referential about eval() itself --
// both sides run through the SAME reference evaluator, so a SYSTEMATIC eval
// defect (e.g. an outer join that never null-extends, an aggregate that is off
// by a constant, a 3VL filter that mishandles NULL) is invisible: bound and
// optimized agree on the wrong answer.
//
// These tests pin eval() to LITERAL hand-computed Tables over the data()
// fixture, so the evaluator is checked against external truth rather than
// against itself. Each anchor targets an area the differential oracle cannot
// self-check: outer-join null extension, three-valued filtering, and multi-
// aggregate group-by. Every anchor is falsifiable by at least one mutant
// (noted per test); the gate proves that.
//
// data() ground truth used below (see harness.cpp data()):
//   users   : (1,alice,30,NYC,-) (2,bob,NULL,LA,1) (3,carol,5,NYC,1)
//             (4,dave,40,LA,2)   (5,eve,25,NULL,2)
//   orders  : (100,u1,p10,50,paid) (101,u1,p11,5,paid) (102,u2,p12,30,pending)
//             (103,u3,p13,NULL,paid) (104,uNULL,p10,8,paid) (105,u4,p12,120,-)
//             (106,u9,p10,40,shipped)
//   emp     : (1,-,10,100) (2,1,10,200) (3,1,20,150) (4,2,20,120) (5,-,10,90)
//             [columns: id, mgr_id, dept_id, salary]

#include "harness/harness.hpp"

using namespace db25::harness;

namespace {

// Standard differential oracle plus stage assertions; returns the Stages so the
// caller can pin the optimized plan's eval to a literal Table.
Stages oracle(std::string_view sql) {
    auto s = run(catalog(), sql);
    check(s.parsed, "parsed");
    check(s.analyze_ok, "analyze ok");
    check(s.bind_ok, "bind ok");
    check(s.optimize_ok, "optimize ok");
    auto rb = eval(s.bound, data());
    auto ro = eval(s.optimized, data());
    if (rb && ro) check(bag_equal(*rb, *ro), "bound == optimized (differential oracle)");
    return s;
}

// Run `sql`, then assert eval(optimized) equals the literal expected multiset.
// `eval` returning nullopt (unsupported shape) is itself a failure here: an
// anchor whose eval is silently skipped would be vacuous, which defeats the
// point of the anchor.
void expect_rows(std::string_view sql, const Table& expected, std::string_view what) {
    auto s = oracle(sql);
    auto ro = eval(s.optimized, data());
    check(ro.has_value(), std::string("anchor eval supported: ") + std::string(what));
    if (ro) check(bag_equal(*ro, expected), what);
}

}  // namespace

static void register_oracle_anchors() {

    // ---- 1. THREE-VALUED FILTER. `age > 20` is UNKNOWN for bob (NULL age), so
    //   he is dropped, NOT kept. alice(30), dave(40), eve(25) pass; carol(5)
    //   fails the comparison. Pins the 3VL truth table for a WHERE predicate
    //   against a literal set -- the differential oracle cannot see a filter that
    //   systematically keeps NULL rows because both plans would keep them.
    //   Killed by M2 (Filter eval ignores the predicate -> all 5 users).
    test("anchor.three_valued_filter", [] {
        expect_rows("SELECT id FROM users WHERE age > 20",
                    { {vi(1)}, {vi(4)}, {vi(5)} },
                    "age > 20 keeps exactly {1,4,5} (NULL age dropped, not kept)");
    });

    // ---- 2. LEFT JOIN NULL EXTENSION. eve (user 5) has no orders; a LEFT JOIN
    //   must still emit one row for her with a NULL right side. Matched users
    //   pair with each of their orders. Orders 104 (user_id NULL) and 106
    //   (user_id 9) match no user and do NOT appear (left join is driven by the
    //   left side). Pinning the NULL-extended row is the only way to catch an
    //   eval that silently degrades LEFT to INNER.
    //   Killed by M16 (outer-join eval drops the (5, NULL) null-extended row).
    test("anchor.left_join_null_extension", [] {
        expect_rows(
            "SELECT u.id, o.id FROM users u LEFT JOIN orders o ON o.user_id = u.id",
            { {vi(1), vi(100)}, {vi(1), vi(101)}, {vi(2), vi(102)},
              {vi(3), vi(103)}, {vi(4), vi(105)}, {vi(5), null()} },
            "LEFT JOIN keeps eve as (5, NULL); orders 104/106 excluded");
    });

    // ---- 3. RIGHT JOIN NULL EXTENSION. Now the orders side is preserved:
    //   orders 104 (user_id NULL) and 106 (user_id 9) match no user and appear
    //   as (NULL, 104) / (NULL, 106). The matched orders pair with their user.
    //   Killed by M16 (the two (NULL, order) right-preserved rows go missing).
    test("anchor.right_join_null_extension", [] {
        expect_rows(
            "SELECT u.id, o.id FROM users u RIGHT JOIN orders o ON o.user_id = u.id",
            { {vi(1), vi(100)}, {vi(1), vi(101)}, {vi(2), vi(102)}, {vi(3), vi(103)},
              {vi(4), vi(105)}, {null(), vi(104)}, {null(), vi(106)} },
            "RIGHT JOIN keeps unmatched orders 104/106 as (NULL, id)");
    });

    // ---- 4. FULL JOIN NULL EXTENSION. Both sides preserved: eve's (5, NULL)
    //   AND the two unmatched orders (NULL, 104)/(NULL, 106), on top of every
    //   matched pair. This is the union of the LEFT and RIGHT anchors.
    //   Killed by M16 (all three null-extended rows drop, leaving only matches).
    test("anchor.full_join_null_extension", [] {
        expect_rows(
            "SELECT u.id, o.id FROM users u FULL JOIN orders o ON o.user_id = u.id",
            { {vi(1), vi(100)}, {vi(1), vi(101)}, {vi(2), vi(102)}, {vi(3), vi(103)},
              {vi(4), vi(105)}, {vi(5), null()}, {null(), vi(104)}, {null(), vi(106)} },
            "FULL JOIN keeps eve AND unmatched orders 104/106");
    });

    // ---- 5. MULTI-AGGREGATE GROUP BY. Two aggregates in one group, pinned to a
    //   literal Table (not query-vs-query), so an eval-side SUM/COUNT defect is
    //   caught rather than cancelling out. emp dept 10 = ids {1,2,5}
    //   (salaries 100+200+90 = 390, count 3); dept 20 = ids {3,4}
    //   (150+120 = 270, count 2).
    //   Killed by M4 (COUNT/SUM off by one -> {10:4:391, 20:3:271}).
    test("anchor.group_by_count_sum", [] {
        expect_rows(
            "SELECT dept_id, COUNT(*), SUM(salary) FROM emp GROUP BY dept_id",
            { {vi(10), vi(3), vi(390)}, {vi(20), vi(2), vi(270)} },
            "GROUP BY dept_id -> {10:3:390, 20:2:270}");
    });

    // ---- 6. COUNT(nullable) vs COUNT(*). order 103 has a NULL amount, so
    //   COUNT(amount) = 6 while COUNT(*) = 7. A single literal row pins the exact
    //   NULL-skipping semantics of COUNT(expr) against COUNT(*).
    //   Killed by M9 (COUNT(expr) counts the NULL row -> 7) and M4 (off by one).
    test("anchor.count_nullable_vs_star", [] {
        expect_rows("SELECT COUNT(amount), COUNT(*) FROM orders",
                    { {vi(6), vi(7)} },
                    "COUNT(amount)=6 (NULL skipped), COUNT(*)=7");
    });
}

static bool _oracle_anchors_registered = (register_oracle_anchors(), true);
