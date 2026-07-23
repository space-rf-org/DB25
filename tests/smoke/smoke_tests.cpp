// tests/smoke/smoke_tests.cpp
//
// Pessimistic SAMPLE tests that exercise the harness end-to-end AND prove the
// falsifiability machinery: every test here is designed to be killed by at least
// one mutant in the catalog (see harness.cpp). They use the differential oracle
// (bound vs optimized) on real adversarial queries plus explicit expected-value
// checks, so the gate can confirm the harness is not vacuously green.
//
// These are the CORE builder's own tests; they must pass clean and each must be
// falsifiable. The sibling suites (tests/corpus, tests/property, tests/metamorphic)
// are authored separately against the same db25::harness contract.

#include "harness/harness.hpp"

using namespace db25::harness;
using db25::plan::LogicalOp;

namespace {

// The standard differential oracle: drive the pipeline, assert every stage, and
// assert bound == optimized (multiset). Returns Stages for further checks.
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

// Two queries must yield the same multiset (via their optimized plans).
void expect_bag_eq(std::string_view a, std::string_view b, std::string_view what) {
    auto sa = oracle(a);
    auto sb = oracle(b);
    auto ra = eval(sa.optimized, data());
    auto rb = eval(sb.optimized, data());
    if (ra && rb) check(bag_equal(*ra, *rb), what);
}

void expect_empty(std::string_view sql, std::string_view what) {
    auto s = oracle(sql);
    auto ro = eval(s.optimized, data());
    if (ro) check(bag_equal(*ro, Table{}), what);
}

}  // namespace

static void register_smoke_tests() {

    // ---- 1. correlated EXISTS: differential oracle + structural decorrelation.
    //   Killed by M3 (SemiJoin-as-cross diverges bound vs optimized) and M1
    //   (structural: no SemiJoin because the optimizer was disabled).
    test("smoke.correlated_exists", [] {
        auto s = oracle(
            "SELECT u.id FROM users u WHERE EXISTS "
            "(SELECT 1 FROM orders o WHERE o.user_id = u.id)");
        // The optimizer must have decorrelated EXISTS into a SemiJoin.
        check(plan_contains(s.optimized, LogicalOp::SemiJoin),
              "optimized EXISTS is a SemiJoin");
        check(count_subqueries(s.optimized) == 0,
              "optimized EXISTS has no remaining Subquery");
        // Expected exact set: users 1..4 have orders; eve (5) does not.
        auto ro = eval(s.optimized, data());
        if (ro)
            expect_bag_eq(
                "SELECT u.id FROM users u WHERE EXISTS "
                "(SELECT 1 FROM orders o WHERE o.user_id = u.id)",
                "SELECT DISTINCT u.id FROM users u JOIN orders o ON o.user_id = u.id",
                "correlated EXISTS == DISTINCT equi-join");
    });

    // ---- 2. NOT IN with NULLs: the classic 3VL trap. orders.user_id has a NULL,
    //   so NOT IN is UNKNOWN for every user -> empty result.
    //   Killed by M2 (Filter ignored -> all users returned, not empty).
    test("smoke.not_in_with_nulls", [] {
        expect_empty(
            "SELECT id FROM users WHERE id NOT IN (SELECT user_id FROM orders)",
            "NOT IN over a NULL-bearing subquery -> empty (UNKNOWN everywhere)");
    });

    // ---- 3. correlated scalar SUM: decorrelation to LEFT JOIN + Aggregate.
    //   Differential oracle (nested-loop subquery vs decorrelated join) plus an
    //   explicit expected value for one user.
    //   Killed by M4 (SUM off by one breaks the expected-value check) and M2.
    test("smoke.correlated_scalar_sum", [] {
        oracle(
            "SELECT u.id, (SELECT SUM(o.amount) FROM orders o WHERE o.user_id = u.id) "
            "FROM users u");
        // User 1 has orders of amount 50 and 5 -> SUM = 55. Cross-check the
        // per-group aggregate against a direct aggregate.
        expect_bag_eq(
            "SELECT SUM(o.amount) FROM orders o WHERE o.user_id = 1",
            "SELECT SUM(amount) FROM orders WHERE user_id = 1",
            "correlated SUM for user 1 matches the direct aggregate");
    });

    // ---- 4. join + filter: predicate pushdown must be faithful, and the WHERE
    //   filter must actually remove rows.
    //   Killed by M2 (filter ignored) and M5 (optimizer drops the predicate).
    test("smoke.join_with_filter", [] {
        auto s = oracle(
            "SELECT u.id, o.amount FROM users u JOIN orders o ON o.user_id = u.id "
            "WHERE o.amount > 30");
        auto ro = eval(s.optimized, data());
        // Only orders with amount > 30 survive: order 100 (u1,50), 105 (u4,40).
        // (order 102 is amount 30 -> excluded; NULL amount excluded.)
        if (ro) check(ro->size() == 2, "join+filter keeps exactly the amount>30 rows");
    });

    // ---- 5. aggregate with GROUP BY: explicit expected values against a literal
    //   Table (NOT a query-vs-query check, so an eval-side aggregate defect is
    //   caught). emp.dept_id: 10 has 3 rows (ids 1,2,5); 20 has 2 (ids 3,4).
    //   Killed by M4 (COUNT off by one -> {10:4,20:3} != expected).
    test("smoke.group_by_counts", [] {
        auto s = oracle("SELECT dept_id, COUNT(*) FROM emp GROUP BY dept_id");
        auto ro = eval(s.optimized, data());
        if (ro) {
            Table expected = { {vi(10), vi(3)}, {vi(20), vi(2)} };
            check(bag_equal(*ro, expected),
                  "GROUP BY dept_id counts are exactly {10:3, 20:2}");
        }
    });

    // ---- 6. constant folding faithfulness: WHERE 1=1 AND p folds to p; the
    //   differential oracle guarantees the fold preserved the result.
    //   Killed by M2 (filter ignored) and M5 (predicate dropped).
    test("smoke.constant_fold_preserves", [] {
        expect_bag_eq("SELECT id FROM users WHERE 1 = 1 AND age > 25",
                      "SELECT id FROM users WHERE age > 25",
                      "(1=1) AND p == p after folding");
    });

    // ---- LIKE pattern matching against a literal oracle. The evaluator now
    //   implements LIKE (% = any run, _ = one char, NOT LIKE, NULL -> UNKNOWN);
    //   before this it returned nullopt and every LIKE query was silently skipped
    //   (the whole property-differential LIKE population - ~13% of queries - was
    //   unverified). users.name: alice, bob, carol, dave, eve.
    //   Killed by M2 (Filter eval ignores the predicate -> all rows returned).
    test("smoke.like_patterns", [] {
        struct C { const char* sql; Table want; };
        const std::vector<C> cases = {
            {"SELECT name FROM users WHERE name LIKE 'a%'",   {{vs("alice")}}},          // prefix
            {"SELECT name FROM users WHERE name LIKE '%e'",   {{vs("alice")},{vs("dave")},{vs("eve")}}},  // suffix
            {"SELECT name FROM users WHERE name LIKE '%a%'",  {{vs("alice")},{vs("carol")},{vs("dave")}}}, // contains
            {"SELECT name FROM users WHERE name LIKE '_o_'",  {{vs("bob")}}},            // single-char wildcard
            {"SELECT name FROM users WHERE name LIKE '____'", {{vs("dave")}}},           // exactly 4 chars
            {"SELECT name FROM users WHERE name LIKE 'alice'",{{vs("alice")}}},          // exact, no wildcard
            {"SELECT name FROM users WHERE name LIKE '%'",                               // matches everything
                {{vs("alice")},{vs("bob")},{vs("carol")},{vs("dave")},{vs("eve")}}},
            {"SELECT name FROM users WHERE name NOT LIKE 'a%'",
                {{vs("bob")},{vs("carol")},{vs("dave")},{vs("eve")}}},
        };
        for (const auto& c : cases) {
            auto s = oracle(c.sql);
            auto ro = eval(s.optimized, data());
            check(ro.has_value(), std::string("LIKE eval supported: ") + c.sql);
            if (ro) check(bag_equal(*ro, c.want),
                          std::string("LIKE result matches oracle: ") + c.sql);
        }
    });

    // ---- ORDER BY on a NON-selected column: the binder appends the sort key as
    //   a hidden trailing column of the Project so Sort can order by it; the Sort
    //   node's output schema is narrower, and the evaluator must project the
    //   hidden key back out. `SELECT id FROM users ORDER BY age` must therefore
    //   yield exactly one column (id), identical to the same query without the
    //   sort. Killed by M13 (Sort eval leaks the hidden key -> width 2, and the
    //   bag no longer matches the id-only set).
    test("smoke.order_by_hidden_key_not_leaked", [] {
        auto s = oracle("SELECT id FROM users ORDER BY age");
        auto ro = eval(s.optimized, data());
        check(ro.has_value(), "ORDER-BY-hidden-key eval supported");
        if (ro && !ro->empty()) {
            check(ro->front().size() == 1,
                  "ORDER BY a non-selected column outputs one column (hidden key dropped)");
        }
        expect_bag_eq("SELECT id FROM users ORDER BY age",
                      "SELECT id FROM users",
                      "ORDER BY a hidden column does not change the projected column set");
    });

    // ---- ORDER BY row SEQUENCE (order-SENSITIVE). Every other comparison in the
    //   suite is bag_equal (multiset), so a Sort that drops or reverses the order
    //   is completely invisible. Pin the actual row sequence against a literal
    //   ordered oracle - the only check that can detect a wrong sort. emp
    //   salaries: id5=90, id1=100, id4=120, id3=150, id2=200 (no NULLs).
    //   Killed by M14 (Sort eval reverses the order).
    test("smoke.order_by_sequence", [] {
        auto asc = oracle("SELECT id FROM emp ORDER BY salary");
        auto ra = eval(asc.optimized, data());
        check(ra.has_value(), "ORDER BY salary ASC eval supported");
        if (ra) {
            Table expected = { {vi(5)}, {vi(1)}, {vi(4)}, {vi(3)}, {vi(2)} };
            check(seq_equal(*ra, expected),
                  "ORDER BY salary ASC yields ids in exactly [5,1,4,3,2]");
        }
        auto desc = oracle("SELECT id FROM emp ORDER BY salary DESC");
        auto rd = eval(desc.optimized, data());
        check(rd.has_value(), "ORDER BY salary DESC eval supported");
        if (rd) {
            Table expected = { {vi(2)}, {vi(3)}, {vi(4)}, {vi(1)}, {vi(5)} };
            check(seq_equal(*rd, expected),
                  "ORDER BY salary DESC yields ids in exactly [2,3,4,1,5]");
        }
    });
}

static bool _smoke_registered = (register_smoke_tests(), true);
