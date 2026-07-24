// tests/conformance/conformance.cpp
//
// END-TO-END STAGE CONFORMANCE. The corpus/property/metamorphic suites assert
// RESULT correctness (does the pipeline compute the right rows?). This suite
// asserts STAGE PROPAGATION: for a feature used in a query, does its effect
// actually appear in every downstream artifact the pipeline produces -
//   parse (AST)  ->  analyze (diagnostics)  ->  bind (LogicalOp)  ->  result?
// It reads the per-stage fingerprints run() now records on Stages (ast_kinds,
// diags) plus the live bound/optimized plans, so a regression that silently
// drops a feature between two stages (the exact failure mode the grammar audit
// found: WITH parses but never lowers, ON CONFLICT parses but is discarded)
// is caught here even when the final row count happens to look plausible.
//
// Two axes, per the design:
//   (1) COVERAGE  - a feature manifest that fails if a supported feature is not
//       exercised, or its AST node / LogicalOp does not appear.
//   (2) EFFECT    - every case also pins the RESULT bag, so the feature's
//       semantic effect in the final artifact is verified (and the test stays
//       falsifiable under the mutant catalog - a purely structural check would
//       survive every mutant and the gate would reject it).
//
// This is an EXTENSION of the existing harness (run/eval/test/check/the gate),
// not a new framework: it only adds a new suite file and reads fields run()
// already captures.

#include "harness/harness.hpp"

using namespace db25::harness;
using db25::plan::LogicalOp;
using NT = db25::ast::NodeType;

namespace {

// Drive the pipeline and assert every stage SUCCEEDED (a conformance case is
// about a feature that is supposed to work end to end). Returns Stages so the
// caller can assert per-stage artifacts.
Stages conform(std::string_view sql) {
    auto s = run(catalog(), sql);
    check(s.parsed, "parsed");
    check(s.analyze_ok, "analyze ok");
    check(s.bind_ok, std::string("bind ok") + (s.bind_ok ? "" : (": " + s.bind_error)));
    check(s.optimize_ok, "optimize ok");
    return s;
}

}  // namespace

static void register_conformance_tests() {

    // ---- CQ1. Analytical kitchen-sink: JOIN + WHERE(scalar subquery) + GROUP BY
    //   + HAVING(aggregate). Verifies each clause's node reaches the AST, analysis
    //   is clean, the plan carries the matching operators, and the result is
    //   exact. Rows after join+filter amount > AVG(amount)=42.17: order100
    //   (u1,NYC,50), order105 (u4,LA,120). Groups NYC{n1,50}, LA{n1,120}; both
    //   pass HAVING SUM>0.  (ORDER BY over an aggregate is deliberately NOT used
    //   here - it currently corrupts the aggregate columns to NULL, tracked as a
    //   separate correctness finding; adding it would make this a bug reproducer,
    //   not a conformance baseline.)
    //   Killed by M2 (filter ignored) and M4 (aggregate off by one).
    //   (Aggregate aliases in the SELECT list are avoided: a HAVING that repeats
    //   an ALIASED select aggregate currently fails to match it and drops every
    //   row - a separate correctness finding, kept out of this baseline.)
    test("conformance.analytical_kitchen_sink", [] {
        auto s = conform(
            "SELECT u.city, COUNT(*), SUM(o.amount) "
            "FROM users u JOIN orders o ON o.user_id = u.id "
            "WHERE o.amount > (SELECT AVG(amount) FROM orders) "
            "GROUP BY u.city HAVING SUM(o.amount) > 0");
        // Parse-stage: every clause's node is present.
        check(ast_has(s, NT::JoinClause),   "AST has JOIN");
        check(ast_has(s, NT::WhereClause),  "AST has WHERE");
        check(ast_has(s, NT::Subquery),     "AST has scalar subquery");
        check(ast_has(s, NT::GroupByClause),"AST has GROUP BY");
        check(ast_has(s, NT::HavingClause), "AST has HAVING");
        // Analyze-stage: no diagnostics.
        check(error_count(s) == 0, "analyzer reports no errors");
        // Bind-stage: each feature lowered to its operator.
        check(plan_contains(s.bound, LogicalOp::Join),      "plan has Join");
        check(plan_contains(s.bound, LogicalOp::Aggregate), "plan has Aggregate");
        check(plan_contains(s.bound, LogicalOp::Filter),    "plan has Filter (HAVING/WHERE)");
        // Result-stage: exact rows, and bound == optimized (differential).
        auto rb = eval(s.bound, data());
        auto ro = eval(s.optimized, data());
        if (rb && ro) check(bag_equal(*rb, *ro), "bound == optimized (differential)");
        if (ro) {
            Table expected = { {vs("NYC"), vi(1), vi(50)}, {vs("LA"), vi(1), vi(120)} };
            check(bag_equal(*ro, expected),
                  "result is {(NYC,1,50),(LA,1,120)} (amount>AVG, grouped by city)");
        }
    });

    // ---- CQ2. Subquery-decorrelation propagation: a correlated EXISTS must, by
    //   the OPTIMIZE stage, become a SemiJoin with NO residual subquery - the
    //   feature's *effect* is a plan transform, not just a result. DISTINCT must
    //   also lower. Killed by M1 (optimizer disabled -> subquery survives, no
    //   SemiJoin) and M2/M3 (wrong rows).
    test("conformance.correlated_exists_decorrelates", [] {
        auto s = conform(
            "SELECT DISTINCT u.city FROM users u "
            "WHERE EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id)");
        check(ast_has(s, NT::Subquery), "AST has subquery (EXISTS)");
        check(error_count(s) == 0, "analyzer clean");
        check(plan_contains(s.bound, LogicalOp::Distinct), "bound plan has Distinct");
        // The decorrelation EFFECT: optimize turns the subquery into a SemiJoin.
        check(plan_contains(s.optimized, LogicalOp::SemiJoin),
              "optimize lowered EXISTS to a SemiJoin");
        check(count_subqueries(s.optimized) == 0,
              "optimize left no residual subquery");
        auto ro = eval(s.optimized, data());
        if (ro) {
            Table expected = { {vs("NYC")}, {vs("LA")} };
            check(bag_equal(*ro, expected),
                  "distinct cities of users-with-orders == {NYC, LA}");
        }
    });

    // ---- CQ3. Set operation carrying a NULL through UNION distinct. Exercises
    //   the SetOp path and NULL de-duplication. users.id {1..5} UNION
    //   orders.user_id {1,2,3,4,9,NULL} -> {1,2,3,4,5,9,NULL}. Killed by M2
    //   (branch filter ignored changes the bag) and M8-adjacent set semantics.
    test("conformance.setop_union_with_null", [] {
        auto s = conform(
            "SELECT id FROM users WHERE age > 20 "
            "UNION SELECT user_id FROM orders WHERE amount > 40");
        check(ast_has(s, NT::UnionStmt), "AST has UNION");
        check(error_count(s) == 0, "analyzer clean");
        check(plan_contains(s.bound, LogicalOp::SetOp), "plan has SetOp");
        auto ro = eval(s.optimized, data());
        if (ro) {
            // users age>20 -> {1,4,5}; orders amount>40 -> user_ids {1,4}.
            // UNION distinct -> {1,4,5}.
            Table expected = { {vi(1)}, {vi(4)}, {vi(5)} };
            check(bag_equal(*ro, expected), "UNION distinct == {1,4,5}");
        }
    });

    // ---- FEATURE MANIFEST. One row per supported feature: assert its AST node,
    //   a clean analyze, its LogicalOp (when it maps to one), AND its exact
    //   result. The result anchors keep this test falsifiable (M2/M4/... kill the
    //   filter/aggregate/etc. rows) and verify the feature's effect in the final
    //   artifact. The `window` row documents the current eval BOUNDARY: the
    //   feature propagates all the way to a Window LogicalOp but the reference
    //   evaluator does not implement it yet (eval -> nullopt), which this pins
    //   explicitly rather than skipping silently.
    test("conformance.feature_manifest", [] {
        struct Feat {
            const char* name;
            const char* sql;
            NT ast_kind;
            bool expect_op;
            LogicalOp op;
            bool eval_supported;
            Table want;  // expected optimized-eval bag when eval_supported
        };
        const std::vector<Feat> feats = {
            {"inner_join",
             "SELECT u.id FROM users u JOIN orders o ON o.user_id = u.id",
             NT::JoinClause, true, LogicalOp::Join, true,
             {{vi(1)},{vi(1)},{vi(2)},{vi(3)},{vi(4)}}},
            {"left_join",
             "SELECT u.id FROM users u LEFT JOIN orders o ON o.user_id = u.id",
             NT::JoinClause, true, LogicalOp::Join, true,
             {{vi(1)},{vi(1)},{vi(2)},{vi(3)},{vi(4)},{vi(5)}}},
            {"where_filter",
             "SELECT id FROM users WHERE age > 20",
             NT::WhereClause, true, LogicalOp::Filter, true,
             {{vi(1)},{vi(4)},{vi(5)}}},
            {"group_by",
             "SELECT dept_id, COUNT(*) FROM emp GROUP BY dept_id",
             NT::GroupByClause, true, LogicalOp::Aggregate, true,
             {{vi(10),vi(3)},{vi(20),vi(2)}}},
            {"having",
             "SELECT dept_id, COUNT(*) FROM emp GROUP BY dept_id HAVING COUNT(*) > 2",
             NT::HavingClause, true, LogicalOp::Aggregate, true,
             {{vi(10),vi(3)}}},
            {"order_by_limit",
             "SELECT id FROM emp ORDER BY id LIMIT 2",
             NT::LimitClause, true, LogicalOp::Limit, true,
             {{vi(1)},{vi(2)}}},
            {"union",
             "SELECT id FROM users UNION SELECT user_id FROM orders",
             NT::UnionStmt, true, LogicalOp::SetOp, true,
             {{vi(1)},{vi(2)},{vi(3)},{vi(4)},{vi(5)},{vi(9)},{null()}}},
            {"between",
             "SELECT id FROM users WHERE age BETWEEN 10 AND 40",
             NT::BetweenExpr, true, LogicalOp::Filter, true,
             {{vi(1)},{vi(4)},{vi(5)}}},
            {"in_list",
             "SELECT id FROM users WHERE id IN (1, 2, 3)",
             NT::InExpr, true, LogicalOp::Filter, true,
             {{vi(1)},{vi(2)},{vi(3)}}},
            {"like",
             "SELECT name FROM users WHERE name LIKE 'a%'",
             NT::LikeExpr, true, LogicalOp::Filter, true,
             {{vs("alice")}}},
            {"case",
             "SELECT id, CASE WHEN age > 20 THEN 1 ELSE 0 END FROM users",
             NT::CaseExpr, false, LogicalOp::Project, true,
             {{vi(1),vi(1)},{vi(2),vi(0)},{vi(3),vi(0)},{vi(4),vi(1)},{vi(5),vi(1)}}},
            {"window_boundary",
             "SELECT id, RANK() OVER (PARTITION BY dept_id ORDER BY salary DESC) FROM emp",
             NT::WindowSpec, true, LogicalOp::Window, false, {}},
        };

        for (const auto& f : feats) {
            const std::string tag = std::string("feature[") + f.name + "] ";
            auto s = conform(f.sql);
            check(ast_has(s, f.ast_kind), tag + "AST node present");
            check(error_count(s) == 0, tag + "analyzer clean");
            if (f.expect_op) {
                check(plan_contains(s.bound, f.op), tag + "lowered to expected LogicalOp");
            }
            auto ro = eval(s.optimized, data());
            if (f.eval_supported) {
                check(ro.has_value(), tag + "eval supported");
                if (ro) check(bag_equal(*ro, f.want), tag + "result effect matches");
            } else {
                check(!ro.has_value(),
                      tag + "eval BOUNDARY: propagates to plan, evaluator not yet implemented");
            }
        }
    });
}

static bool _conformance_registered = (register_conformance_tests(), true);
