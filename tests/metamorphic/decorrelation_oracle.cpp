// DB25 end-to-end harness - DECORRELATION ORACLE
//
// This is the direct, *evaluatable* proof that subquery decorrelation is
// semantics-preserving. For each correlated query we do BOTH of:
//
//   (i)  evaluate the OPTIMIZED plan -- the decorrelated Semi / Anti / Left-join
//        form the optimizer produces; and
//   (ii) evaluate the CORRELATED semantics INDEPENDENTLY -- by evaluating the
//        BOUND plan, which still contains the represented correlated Subquery,
//        so the reference evaluator's nested-loop / OuterRef machinery computes
//        the *true* correlated answer via a code path entirely disjoint from the
//        optimizer's rewrite.
//
// then assert  bag_equal( (i), (ii) ).  A wrong decorrelation makes this FALSE.
//
// INDEPENDENT ORACLE, per case:
//   * EXISTS / NOT EXISTS / IN / NOT IN : bound-plan eval (ii) is the primary
//     oracle, PLUS an "extra-independent" cross-check against a hand-written
//     equivalent SQL formulation (INNER JOIN + DISTINCT, or EXCEPT) that a human
//     can verify by inspection and that does NOT go through the decorrelation
//     pass at all.
//   * correlated scalar SUM / MIN / MAX / AVG, and the COUNT scalar : bound-plan
//     eval (ii) -- the represented-subquery nested loop -- is the oracle; there
//     is no decorrelation-independent SQL rewrite for a per-row scalar other
//     than the LEFT-JOIN-with-GROUP-BY form the optimizer itself emits, so the
//     bound plan is the right independent reference.
//
// Why not plain-C++ nested loops over data()?  The harness contract exposes
// `Table` and `Data` only opaquely (eval/bag_equal consume them; no row/column
// accessors are specified), so a hand-written C++ loop over data() cannot be
// written against the contract without guessing internal layout. Bound-plan eval
// is the contract-sanctioned independent oracle and is used in its place; the
// SQL cross-checks above add a second, decorrelation-free layer. If the harness
// later exposes Data/Table accessors, anchor-case C++ loops can be added.
// TODO(oracle): add plain-C++ anchor loops once Data/Table accessors exist.
//
// SCHEMA / DATA ASSUMPTIONS (see README) -- the harness catalog()/data() provide
//     users(id INT NOT NULL, name VARCHAR NULL)
//     orders(id INT NOT NULL, user_id INT NULL, total DOUBLE NULL)
//   and, to exercise the pessimistic paths, data() should contain:
//     - a user with NO matching orders     (EXISTS -> false; scalar -> NULL),
//     - an orders row with user_id = NULL   (NOT IN whole-result UNKNOWN),
//     - a user with >= 2 matching orders    (semi/anti join must NOT multiply
//                                            the outer row -- cardinality proof).
// These are data-content requirements; the assertions below are falsifiable
// exactly when such rows exist.
//
// Build matches the stack: C++23, -fno-exceptions.

#include "../../harness/harness.hpp"

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

using db25::plan::Expr;
using db25::plan::ExprKind;
using db25::plan::LogicalNode;
using db25::plan::LogicalOp;

// ---------------------------------------------------------------------------
// Tiny read-only plan/expr walkers for structural sanity checks.
// ---------------------------------------------------------------------------
const LogicalNode* find_op(const LogicalNode* n, LogicalOp op) {
    if (n == nullptr) {
        return nullptr;
    }
    if (n->op == op) {
        return n;
    }
    for (const auto& c : n->children) {
        if (const LogicalNode* r = find_op(c.get(), op)) {
            return r;
        }
    }
    return nullptr;
}

bool expr_has_subquery(const Expr* e) {
    if (e == nullptr) {
        return false;
    }
    if (e->kind == ExprKind::Subquery) {
        return true;
    }
    for (const auto& c : e->children) {
        if (expr_has_subquery(c.get())) {
            return true;
        }
    }
    return false;
}

// True if any Filter predicate or Project/aggregate expression in the tree still
// carries a represented Subquery (i.e. it was NOT decorrelated away).
bool plan_has_represented_subquery(const LogicalNode* n) {
    if (n == nullptr) {
        return false;
    }
    if (expr_has_subquery(n->predicate.get())) {
        return true;
    }
    for (const auto& e : n->exprs) {
        if (expr_has_subquery(e.get())) {
            return true;
        }
    }
    for (const auto& e : n->aggregates) {
        if (expr_has_subquery(e.get())) {
            return true;
        }
    }
    for (const auto& c : n->children) {
        if (plan_has_represented_subquery(c.get())) {
            return true;
        }
    }
    return false;
}

// ---------------------------------------------------------------------------
// Core oracle: optimized (decorrelated) result == bound (correlated) result.
// Returns the Stages so the caller can add case-specific structural checks.
// ---------------------------------------------------------------------------
Stages assert_decorrelation_preserves(std::string_view sql, const std::string& label) {
    const auto& cat = catalog();
    const Stages s = run(cat, sql);

    check(s.parsed && s.analyze_ok, label + ": parse+analyze succeed");
    check(s.bind_ok, label + ": bind succeeds (" + s.bind_error + ")");
    check(s.optimize_ok, label + ": optimize succeeds");
    if (s.bound == nullptr || s.optimized == nullptr) {
        return s;  // pipeline failure reported above
    }

    const std::optional opt = eval(s.optimized, data());   // (i) decorrelated form
    const std::optional bnd = eval(s.bound, data());        // (ii) true correlated semantics

    if (opt && bnd) {
        check(bag_equal(*opt, *bnd),
              label + ": decorrelated(optimized) bag-equals correlated(bound)");
    }
    // else: eval unsupported for a plan shape -- TODO(oracle): equality
    // unverified. Pipeline success is asserted above; structural checks added by
    // the caller still stand. We never claim the equality was verified here.
    return s;
}

// Extra-independent cross-check: two SQL formulations must agree. Used to pit the
// decorrelated result against a hand-written equivalent that never touches the
// decorrelation pass (INNER JOIN + DISTINCT / EXCEPT).
void assert_sql_equivalent(std::string_view a, std::string_view b, const std::string& label) {
    const auto& cat = catalog();
    const Stages sa = run(cat, a);
    const Stages sb = run(cat, b);
    check(sa.bind_ok && sa.optimize_ok, label + ": lhs pipeline succeeds");
    check(sb.bind_ok && sb.optimize_ok, label + ": rhs pipeline succeeds");
    if (sa.optimized == nullptr || sb.optimized == nullptr) {
        return;
    }
    const std::optional ea = eval(sa.optimized, data());
    const std::optional eb = eval(sb.optimized, data());
    if (ea && eb) {
        check(bag_equal(*ea, *eb), label + ": cross-check formulations agree");
    }
    // else: TODO(oracle) -- cross-check unverified (eval unsupported).
}

// ===========================================================================
// EXISTS (correlated)  ->  SemiJoin.
// Pessimistic content covered by data(): a user with no orders (EXISTS false),
// and a user with multiple orders (SemiJoin must emit the outer row exactly
// once -- the bound-plan EXISTS also emits once, so any multiplication fails
// bag_equal).
// ===========================================================================
void register_exists() {
    test("decorrelation: WHERE EXISTS (correlated)  ->  SemiJoin, result preserved", [] {
        constexpr std::string_view sql =
            "SELECT u.id FROM users u WHERE EXISTS "
            "(SELECT 1 FROM orders o WHERE o.user_id = u.id)";
        const Stages s = assert_decorrelation_preserves(sql, "EXISTS");
        if (s.optimized) {
            check(find_op(s.optimized, LogicalOp::SemiJoin) != nullptr,
                  "EXISTS: optimized plan contains a SemiJoin");
        }
        // Extra-independent: qualifying outer ids == distinct users that join an
        // order. (users.id is a key, so DISTINCT id reproduces the semi-join set;
        // this formulation never uses the decorrelation pass.)
        assert_sql_equivalent(
            sql, "SELECT DISTINCT u.id FROM users u JOIN orders o ON o.user_id = u.id",
            "EXISTS vs JOIN+DISTINCT");
    });
}

// ===========================================================================
// NOT EXISTS (correlated)  ->  AntiJoin.
// ===========================================================================
void register_not_exists() {
    test("decorrelation: WHERE NOT EXISTS (correlated)  ->  AntiJoin, result preserved", [] {
        constexpr std::string_view sql =
            "SELECT u.id FROM users u WHERE NOT EXISTS "
            "(SELECT 1 FROM orders o WHERE o.user_id = u.id)";
        const Stages s = assert_decorrelation_preserves(sql, "NOT EXISTS");
        if (s.optimized) {
            check(find_op(s.optimized, LogicalOp::AntiJoin) != nullptr,
                  "NOT EXISTS: optimized plan contains an AntiJoin");
        }
        // Extra-independent: users with no order == all users EXCEPT users that
        // join an order. Uses EXCEPT + JOIN, never the decorrelation pass.
        assert_sql_equivalent(
            sql,
            "SELECT id FROM users "
            "EXCEPT SELECT u.id FROM users u JOIN orders o ON o.user_id = u.id",
            "NOT EXISTS vs EXCEPT/JOIN");
    });
}

// ===========================================================================
// x IN (subquery)  ->  SemiJoin on the IN equality.
// Correlated IN additionally hoists the correlation predicate into the join.
// ===========================================================================
void register_in() {
    test("decorrelation: WHERE id IN (subquery)  ->  SemiJoin, result preserved", [] {
        // Uncorrelated IN.
        {
            constexpr std::string_view sql =
                "SELECT id FROM users WHERE id IN (SELECT user_id FROM orders)";
            const Stages s = assert_decorrelation_preserves(sql, "IN (uncorrelated)");
            if (s.optimized) {
                check(find_op(s.optimized, LogicalOp::SemiJoin) != nullptr,
                      "IN: optimized plan contains a SemiJoin");
            }
            // NULL caveat: orders.user_id may be NULL; positive IN drops the NULL
            // right rows AND any non-matching left rows identically in both forms.
            assert_sql_equivalent(
                sql, "SELECT DISTINCT u.id FROM users u JOIN orders o ON u.id = o.user_id",
                "IN vs JOIN+DISTINCT");
        }
        // Correlated IN (IN equality AND hoisted correlation).
        {
            constexpr std::string_view sql =
                "SELECT id FROM users u WHERE id IN "
                "(SELECT o.id FROM orders o WHERE o.user_id = u.id)";
            const Stages s = assert_decorrelation_preserves(sql, "IN (correlated)");
            if (s.optimized) {
                check(find_op(s.optimized, LogicalOp::SemiJoin) != nullptr,
                      "IN (correlated): optimized plan contains a SemiJoin");
            }
        }
    });
}

// ===========================================================================
// NOT IN -- the sharpest NULL trap.
//   * NOT IN over a NULLABLE projected column MUST stay a represented subquery
//     (an AntiJoin would be wrong under 3VL). And when the inner set contains a
//     NULL, the WHOLE result is empty (every probe is UNKNOWN). Here the
//     optimized == bound (no rewrite), and both must yield the empty set.
//   * NOT IN where both sides are NOT NULL is sound to make an AntiJoin.
// ===========================================================================
void register_not_in() {
    test("decorrelation: NOT IN nullable stays a Subquery (whole result UNKNOWN when inner has NULL)", [] {
        constexpr std::string_view sql =
            "SELECT id FROM users WHERE id NOT IN (SELECT user_id FROM orders)";
        const Stages s = assert_decorrelation_preserves(sql, "NOT IN (nullable)");
        if (s.optimized) {
            // Must NOT have been turned into an AntiJoin.
            check(find_op(s.optimized, LogicalOp::AntiJoin) == nullptr,
                  "NOT IN (nullable): NOT decorrelated to an AntiJoin");
            check(plan_has_represented_subquery(s.optimized),
                  "NOT IN (nullable): predicate stays a represented Subquery");
        }
        // Pessimistic emptiness: when orders.user_id contains a NULL, `id NOT IN
        // (...)` is UNKNOWN for every row, so the result is empty. Compare to a
        // provably-empty query (id <> id is empty because users.id is NOT NULL).
        // This is falsifiable only when data() actually contains a NULL user_id.
        assert_sql_equivalent(sql, "SELECT id FROM users WHERE id <> id",
                              "NOT IN (nullable, inner-NULL) yields empty");
    });

    test("decorrelation: NOT IN over NOT-NULL columns  ->  AntiJoin, result preserved", [] {
        constexpr std::string_view sql =
            "SELECT id FROM users WHERE id NOT IN (SELECT id FROM orders)";
        const Stages s = assert_decorrelation_preserves(sql, "NOT IN (not-null)");
        if (s.optimized) {
            check(find_op(s.optimized, LogicalOp::AntiJoin) != nullptr,
                  "NOT IN (not-null): optimized plan contains an AntiJoin");
        }
        // Extra-independent: users whose id is not an orders.id == users EXCEPT
        // (users whose id matches some orders.id).
        assert_sql_equivalent(
            sql,
            "SELECT id FROM users "
            "EXCEPT SELECT u.id FROM users u JOIN orders o ON u.id = o.id",
            "NOT IN (not-null) vs EXCEPT/JOIN");
    });
}

// ===========================================================================
// Correlated scalar aggregate subqueries (SUM / MIN / MAX / AVG) -> LEFT JOIN
// against the inner relation grouped by the correlation key.
// The pessimistic case is baked in: an outer row with no matching inner rows.
// Over the empty set SUM/MIN/MAX/AVG are NULL, and the LEFT-JOIN NULL for a
// non-matching outer row reproduces exactly that -- so bag_equal against the
// bound (nested-loop) plan is the proof. COUNT is deliberately excluded here
// (COUNT over empty is 0, not NULL) and tested separately below.
// ===========================================================================
void register_scalar_aggregates() {
    const auto one = [](const char* agg, const char* label) {
        std::string sql = std::string("SELECT u.id, (SELECT ") + agg +
                          "(o.amount) FROM orders o WHERE o.user_id = u.id) FROM users u";
        const Stages s = assert_decorrelation_preserves(sql, std::string("scalar ") + label);
        if (s.optimized) {
            const LogicalNode* j = find_op(s.optimized, LogicalOp::Join);
            check(j != nullptr && j->join_type == db25::ast::JoinType::Left,
                  std::string("scalar ") + label + ": optimized plan contains a LEFT Join");
            if (j != nullptr) {
                check(find_op(j, LogicalOp::Aggregate) != nullptr,
                      std::string("scalar ") + label + ": right input is a grouped Aggregate");
            }
        }
    };

    test("decorrelation: correlated scalar SUM/MIN/MAX/AVG  ->  LEFT JOIN, result preserved", [one] {
        one("SUM", "SUM");
        one("MIN", "MIN");
        one("MAX", "MAX");
        one("AVG", "AVG");
    });
}

// ===========================================================================
// COUNT scalar MUST NOT decorrelate the same way: COUNT over the empty set is 0,
// not NULL, so a plain LEFT JOIN (whose miss yields NULL) would be WRONG. The
// optimizer leaves it as a represented subquery. We assert (a) it was NOT turned
// into a LEFT-join+aggregate rewrite, and (b) optimized == bound (both compute
// the true per-row COUNT, including 0 for outer rows with no matching orders).
// ===========================================================================
void register_count_no_decorrelate() {
    test("decorrelation: correlated scalar COUNT must NOT decorrelate (0-vs-NULL trap)", [] {
        constexpr std::string_view sql =
            "SELECT u.id, (SELECT COUNT(*) FROM orders o WHERE o.user_id = u.id) FROM users u";
        const Stages s = assert_decorrelation_preserves(sql, "scalar COUNT");
        if (s.optimized) {
            check(plan_has_represented_subquery(s.optimized),
                  "scalar COUNT: subquery stays represented (not a LEFT-join rewrite)");
            // Belt-and-braces: no LEFT join was introduced for the COUNT.
            const LogicalNode* j = find_op(s.optimized, LogicalOp::Join);
            check(j == nullptr || j->join_type != db25::ast::JoinType::Left,
                  "scalar COUNT: no LEFT Join was introduced");
        }
    });
}

// Self-register (harness contract idiom).
void register_all() {
    register_exists();
    register_not_exists();
    register_in();
    register_not_in();
    register_scalar_aggregates();
    register_count_no_decorrelate();
}

static bool _ = (register_all(), true);

}  // namespace
