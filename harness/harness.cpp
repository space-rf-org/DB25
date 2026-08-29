// DB25 end-to-end test harness - implementation.
//
// MUTANT CATALOG (the falsifiability gate injects exactly one at a time via the
// global `g_mutant`; 0 == clean):
//
//   M1  optimizer disabled - run() returns the (un-optimized) bound plan as the
//       "optimized" plan. Caught by any test that asserts an optimization
//       actually happened (e.g. EXISTS -> SemiJoin decorrelation).
//   M2  Filter eval ignores its predicate (passes all child rows through).
//       Caught by any expected-value test over a filtered query.
//   M3  SemiJoin eval keeps every left row (no existence test) - i.e. it behaves
//       like a left-preserving cross. Caught by a decorrelated EXISTS/IN
//       differential test (bound uses the nested-loop subquery, optimized the
//       SemiJoin, so the two diverge).
//   M4  Aggregate eval returns a wrong value (COUNT/SUM perturbed by +1). Caught
//       by an expected-value aggregate test.
//   M5  optimize() output has a predicate dropped - the first Filter/Join
//       predicate in the optimized tree is nulled out. Caught by a filter
//       differential test (optimized loses rows relative to bound).
//   M6  optimize() corrupts the root output SCHEMA (appends a spurious column)
//       without touching the produced rows. Caught by the no-crash /
//       well-formedness and schema-preservation property tests.
//   M7  an optimizer pass, applied in isolation, drops a predicate (injected in
//       the property suite's apply_pass). Caught by the per-pass safety tests.
//   M8  Distinct eval does not de-duplicate. Caught by the DISTINCT==GROUP BY
//       and duplicates-survive-without-DISTINCT cardinality tests.
//   M9  COUNT(expr) counts NULL argument rows (so COUNT(nullable)==COUNT(*)).
//       Caught by the COUNT(nullable) < COUNT(*) NULL test.
//   M10 CASE eval ignores every WHEN and falls through to ELSE. Caught by the
//       CASE-vs-COALESCE equivalence test.
//   M11 membership in an EMPTY set evaluates to UNKNOWN, not FALSE (so
//       `x NOT IN (empty)` drops rows). Caught by the NOT-IN-over-empty test.
//   M12 a second optimize() pass changes the plan - a dropped predicate makes
//       optimize(optimize(p)) differ from optimize(p) (injected in the property
//       suite's idempotence check). Caught by the optimizer-idempotence property.
//   M13 Sort eval leaks hidden ORDER BY sort keys (skips the truncation to the
//       output width). Caught by the ORDER-BY-hidden-key width test.
//   M14 Sort eval reverses the row order (models a dropped/reversed ORDER BY
//       direction). Invisible to every bag_equal (multiset) check; caught ONLY
//       by the order-sensitive ORDER BY sequence test (seq_equal vs a literal
//       ordered oracle).
//   M15 COALESCE keeps only its first operand. Models a USING/NATURAL RIGHT/FULL
//       merge that keeps the left copy instead of COALESCE(left, right), so the
//       merged column is NULL for right-only rows. Caught by the RIGHT-JOIN-USING
//       merged-value test (literal oracle over an all-right-only join).
//
// A test is FALSIFIABLE iff some mutant makes it fail. The gate reports any test
// that survives every mutant as NON-FALSIFIABLE (vacuous) and exits non-zero.

#include "harness.hpp"

#include "db25/plan/binder.hpp"
#include "db25/plan/optimizer.hpp"
#include "db25/parser/parser.hpp"
#include "db25/semantic/analyzer.hpp"
#include "db25/ast/ast_node.hpp"

#include <algorithm>
#include <cctype>
#include <cmath>
#include <cstdio>
#include <cstdlib>

namespace db25::harness {

using db25::plan::Expr;
using db25::plan::ExprKind;
using db25::plan::BoolTest;
using db25::plan::LogicalNode;
using db25::plan::LogicalNodePtr;
using db25::plan::LogicalOp;
using db25::plan::SubqueryKind;
using db25::ast::BinaryOp;
using db25::ast::DataType;
using db25::ast::JoinType;
using db25::ast::UnaryOp;

int g_mutant = 0;

namespace {

// ===========================================================================
// Value helpers + three-valued logic.
// ===========================================================================

// A SQL boolean under 3VL: False / True / Unknown(NULL).
enum class Bool3 { False, True, Unknown };

Bool3 to_bool3(const Value& v) {
    if (std::holds_alternative<std::monostate>(v)) return Bool3::Unknown;
    if (const bool* b = std::get_if<bool>(&v)) return *b ? Bool3::True : Bool3::False;
    if (const std::int64_t* i = std::get_if<std::int64_t>(&v)) return *i != 0 ? Bool3::True : Bool3::False;
    if (const double* d = std::get_if<double>(&v)) return *d != 0.0 ? Bool3::True : Bool3::False;
    return Bool3::Unknown;
}

Value from_bool3(Bool3 b) {
    switch (b) {
        case Bool3::True: return Value{true};
        case Bool3::False: return Value{false};
        default: return Value{std::monostate{}};
    }
}

bool is_true(const Value& v) { return to_bool3(v) == Bool3::True; }

bool numeric_of(const Value& v, double& out) {
    if (const std::int64_t* i = std::get_if<std::int64_t>(&v)) { out = static_cast<double>(*i); return true; }
    if (const double* d = std::get_if<double>(&v)) { out = *d; return true; }
    if (const bool* b = std::get_if<bool>(&v)) { out = *b ? 1.0 : 0.0; return true; }
    return false;
}

bool both_int(const Value& a, const Value& b) {
    return std::holds_alternative<std::int64_t>(a) && std::holds_alternative<std::int64_t>(b);
}

// SQL `=` style comparison under 3VL. NULL operand -> Unknown. Returns -1/0/1 in
// `cmp` when both are comparable non-null; sets `unknown` when either is NULL or
// the pair is not comparable.
void compare3(const Value& a, const Value& b, int& cmp, bool& unknown) {
    unknown = false;
    if (std::holds_alternative<std::monostate>(a) || std::holds_alternative<std::monostate>(b)) {
        unknown = true;
        return;
    }
    if (const std::string* sa = std::get_if<std::string>(&a)) {
        if (const std::string* sb = std::get_if<std::string>(&b)) {
            cmp = sa->compare(*sb) < 0 ? -1 : (sa->compare(*sb) > 0 ? 1 : 0);
            return;
        }
        unknown = true;
        return;
    }
    double da, db;
    if (numeric_of(a, da) && numeric_of(b, db)) {
        cmp = da < db ? -1 : (da > db ? 1 : 0);
        return;
    }
    unknown = true;
}

}  // namespace

bool is_null(const Value& v) { return std::holds_alternative<std::monostate>(v); }

bool value_identical(const Value& a, const Value& b) {
    // NULL == NULL true; otherwise numeric-aware / string / bool equality.
    const bool na = is_null(a), nb = is_null(b);
    if (na || nb) return na && nb;
    if (std::holds_alternative<std::string>(a) || std::holds_alternative<std::string>(b)) {
        const std::string* sa = std::get_if<std::string>(&a);
        const std::string* sb = std::get_if<std::string>(&b);
        return sa && sb && *sa == *sb;
    }
    double da, db;
    if (numeric_of(a, da) && numeric_of(b, db)) return da == db;
    return false;
}

// Unquoted string form (used by string concatenation). Declared at namespace
// scope so the evaluator internals below can see it.
std::string value_to_string_unquoted(const Value& v);

std::string value_to_string(const Value& v) {
    if (is_null(v)) return "NULL";
    if (const std::int64_t* i = std::get_if<std::int64_t>(&v)) return std::to_string(*i);
    if (const double* d = std::get_if<double>(&v)) return std::to_string(*d);
    if (const bool* b = std::get_if<bool>(&v)) return *b ? "true" : "false";
    if (const std::string* s = std::get_if<std::string>(&v)) return "'" + *s + "'";
    return "?";
}

// ===========================================================================
// Evaluation context + forward decls.
// ===========================================================================
namespace {

struct EvalCtx {
    const Data* data;
    std::vector<const Row*> outers;  // correlated-subquery binding stack (back == depth 1)
    bool ok = true;                  // cleared on an unsupported shape
};

bool eval_node(const LogicalNode* n, EvalCtx& ctx, Table& out);
Value eval_expr(const Expr* e, const Row& row, EvalCtx& ctx);

// ---- arithmetic / comparison over BinaryOp ----
Value eval_arith(BinaryOp op, const Value& a, const Value& b, EvalCtx& ctx) {
    if (is_null(a) || is_null(b)) return null();
    if (op == BinaryOp::Concat) {
        return vs(value_to_string_unquoted(a) + value_to_string_unquoted(b));
    }
    double da, db;
    if (!numeric_of(a, da) || !numeric_of(b, db)) { ctx.ok = false; return null(); }
    const bool ints = both_int(a, b);
    // Integer arithmetic that would overflow int64 is not representable, so the
    // reference returns NULL - consistent with division by zero below, and (via
    // __builtin_*_overflow) free of the signed-overflow UB a plain `+`/`-`/`*`
    // would hit on out-of-range operands.
    switch (op) {
        case BinaryOp::Add:
            if (!ints) return vd(da + db);
            { std::int64_t r; return __builtin_add_overflow(std::get<std::int64_t>(a), std::get<std::int64_t>(b), &r) ? null() : vi(r); }
        case BinaryOp::Subtract:
            if (!ints) return vd(da - db);
            { std::int64_t r; return __builtin_sub_overflow(std::get<std::int64_t>(a), std::get<std::int64_t>(b), &r) ? null() : vi(r); }
        case BinaryOp::Multiply:
            if (!ints) return vd(da * db);
            { std::int64_t r; return __builtin_mul_overflow(std::get<std::int64_t>(a), std::get<std::int64_t>(b), &r) ? null() : vi(r); }
        case BinaryOp::Divide:
            if (db == 0.0) return null();  // reference: division by zero -> NULL
            return ints ? vi(std::get<std::int64_t>(a) / std::get<std::int64_t>(b)) : vd(da / db);
        case BinaryOp::Modulo:
            if (db == 0.0) return null();
            return ints ? vi(std::get<std::int64_t>(a) % std::get<std::int64_t>(b)) : vd(std::fmod(da, db));
        default: ctx.ok = false; return null();
    }
}

}  // namespace

std::string value_to_string_unquoted(const Value& v) {
    if (const std::string* s = std::get_if<std::string>(&v)) return *s;
    return value_to_string(v);
}

namespace {

Value eval_compare(BinaryOp op, const Value& a, const Value& b) {
    int cmp = 0;
    bool unknown = false;
    compare3(a, b, cmp, unknown);
    if (unknown) return null();
    bool r = false;
    switch (op) {
        case BinaryOp::Equal: r = (cmp == 0); break;
        case BinaryOp::NotEqual: r = (cmp != 0); break;
        case BinaryOp::LessThan: r = (cmp < 0); break;
        case BinaryOp::LessEqual: r = (cmp <= 0); break;
        case BinaryOp::GreaterThan: r = (cmp > 0); break;
        case BinaryOp::GreaterEqual: r = (cmp >= 0); break;
        default: break;
    }
    return vb(r);
}

bool is_row_comparison_op(BinaryOp op) {
    switch (op) {
        case BinaryOp::Equal:
        case BinaryOp::NotEqual:
        case BinaryOp::LessThan:
        case BinaryOp::LessEqual:
        case BinaryOp::GreaterThan:
        case BinaryOp::GreaterEqual:
            return true;
        default:
            return false;
    }
}

// Row comparison: (a0,a1,...) <op> (b0,b1,...) under SQL three-valued logic.
//   =  : TRUE iff every pair is equal; FALSE if any pair is definitely unequal;
//        UNKNOWN if a NULL pair is reached with no earlier definite inequality.
//   <> : the negation of =.
//   ordering (< <= > >=): lexicographic. The first position where the rows
//        differ decides; a NULL reached before any decisive difference makes the
//        whole comparison UNKNOWN.
Value eval_row_compare(BinaryOp op, const Expr* la, const Expr* lb,
                       const Row& row, EvalCtx& ctx) {
    if (la->kind != ExprKind::Row || lb->kind != ExprKind::Row ||
        la->children.size() != lb->children.size() || la->children.empty()) {
        ctx.ok = false;  // arity mismatch / non-row operand: unsupported shape
        return null();
    }
    // M18: compare only the first component, so (1,2) = (1,9) wrongly holds and
    // (1,2) < (1,9) wrongly fails. Models a row comparison that ignores all but
    // the leading column.
    const std::size_t n = (g_mutant == 18) ? 1 : la->children.size();
    const bool eq_family = (op == BinaryOp::Equal || op == BinaryOp::NotEqual);
    bool any_unknown = false;
    for (std::size_t i = 0; i < n; ++i) {
        const Value ai = eval_expr(la->children[i].get(), row, ctx);
        const Value bi = eval_expr(lb->children[i].get(), row, ctx);
        int cmp = 0;
        bool unk = false;
        compare3(ai, bi, cmp, unk);
        if (eq_family) {
            if (unk) { any_unknown = true; continue; }
            if (cmp != 0) return vb(op == BinaryOp::NotEqual);  // definitely unequal
            // equal at this position -> continue
        } else {
            if (unk) return null();      // cannot order past a NULL
            if (cmp != 0) {
                // First decisive position: strict inequality fixes the order, and
                // <= / >= agree with < / > here because the rows are not equal.
                const bool less = (op == BinaryOp::LessThan || op == BinaryOp::LessEqual);
                return vb(less ? (cmp < 0) : (cmp > 0));
            }
            // equal at this position -> continue
        }
    }
    // All compared positions were equal (eq-family may have seen NULLs).
    if (op == BinaryOp::Equal)    return any_unknown ? null() : vb(true);
    if (op == BinaryOp::NotEqual) return any_unknown ? null() : vb(false);
    // Ordering with all positions equal: < and > are false, <= and >= are true.
    return vb(op == BinaryOp::LessEqual || op == BinaryOp::GreaterEqual);
}

// 3VL membership: value IN {elems}. Returns True / False / Unknown.
Bool3 in_membership(const Value& v, const std::vector<Value>& elems) {
    // IN over an EMPTY set is definitely FALSE - even for a NULL probe - because
    // there is no element to compare against. Hence NOT IN (empty) is TRUE for
    // every row, including NULL rows. Handle this before the NULL-probe rule.
    if (elems.empty()) return Bool3::False;
    if (is_null(v)) return Bool3::Unknown;
    bool saw_null = false;
    for (const Value& e : elems) {
        if (is_null(e)) { saw_null = true; continue; }
        Value eq = eval_compare(BinaryOp::Equal, v, e);
        if (is_true(eq)) return Bool3::True;
    }
    return saw_null ? Bool3::Unknown : Bool3::False;
}

Value literal_value(const Expr* e) {
    const auto& lv = e->value.value;
    if (std::holds_alternative<std::int64_t>(lv)) return vi(std::get<std::int64_t>(lv));
    if (std::holds_alternative<double>(lv)) return vd(std::get<double>(lv));
    if (std::holds_alternative<bool>(lv)) return vb(std::get<bool>(lv));
    if (std::holds_alternative<std::string>(lv)) return vs(std::get<std::string>(lv));
    return null();  // monostate == NULL literal
}

Value eval_cast(const Value& v, DataType target) {
    if (is_null(v)) return null();
    switch (target) {
        case DataType::Boolean: {
            Bool3 b = to_bool3(v);
            return b == Bool3::Unknown ? null() : from_bool3(b);
        }
        case DataType::TinyInt:
        case DataType::SmallInt:
        case DataType::Integer:
        case DataType::BigInt: {
            double d;
            if (numeric_of(v, d)) return vi(static_cast<std::int64_t>(d));
            return null();
        }
        case DataType::Real:
        case DataType::Double:
        case DataType::Decimal: {
            double d;
            if (numeric_of(v, d)) return vd(d);
            return null();
        }
        case DataType::Char:
        case DataType::VarChar:
        case DataType::Text:
            return vs(value_to_string_unquoted(v));
        default:
            return v;  // identity for unmodelled target types
    }
}

// Evaluate a subquery Expr against the current row (which becomes the depth-1
// outer binding for the inner plan).
Value eval_subquery(const Expr* e, const Row& row, EvalCtx& ctx) {
    ctx.outers.push_back(&row);
    Table inner;
    const bool ok = eval_node(e->sub_plan.get(), ctx, inner);
    ctx.outers.pop_back();
    if (!ok) { ctx.ok = false; return null(); }

    switch (e->subquery_kind) {
        case SubqueryKind::Exists: {
            const bool exists = !inner.empty();
            const bool want = e->negated() ? !exists : exists;
            return vb(want);
        }
        case SubqueryKind::Scalar: {
            if (inner.empty()) return null();
            if (inner[0].empty()) return null();
            return inner[0][0];  // single-row single-column
        }
        case SubqueryKind::In: {
            // children[0] is the already-lowered left operand.
            Value left = e->children.empty() ? null() : eval_expr(e->children[0].get(), row, ctx);
            std::vector<Value> elems;
            elems.reserve(inner.size());
            for (const Row& r : inner)
                elems.push_back(r.empty() ? null() : r[0]);
            // M11: treat membership in an EMPTY set as UNKNOWN instead of FALSE,
            // so `x NOT IN (empty)` becomes UNKNOWN (drops the row) rather than
            // TRUE. Caught by the NOT-IN-over-empty-keeps-all test.
            Bool3 m = (g_mutant == 11 && elems.empty()) ? Bool3::Unknown
                                                        : in_membership(left, elems);
            if (e->negated()) {
                if (m == Bool3::True) m = Bool3::False;
                else if (m == Bool3::False) m = Bool3::True;
            }
            return from_bool3(m);
        }
    }
    ctx.ok = false;
    return null();
}

// SQL LIKE matcher: `%` matches any run (>= 0 chars), `_` matches exactly one.
// When `has_esc`, an `esc` char makes the next pattern char a literal (%, _, or
// esc). Recursive backtracking on `%`; patterns/strings here are short.
bool like_match(const char* s, const char* se, const char* p, const char* pe,
                char esc, bool has_esc) {
    while (p < pe) {
        char pc = *p;
        if (has_esc && pc == esc) {
            ++p;
            if (p >= pe) return false;          // dangling escape: no match
            if (s < se && *s == *p) { ++s; ++p; continue; }
            return false;
        }
        if (pc == '%') {
            ++p;
            for (const char* t = s;; ++t) {      // try every suffix, incl. empty
                if (like_match(t, se, p, pe, esc, has_esc)) return true;
                if (t >= se) return false;
            }
        }
        if (pc == '_') {
            if (s >= se) return false;
            ++s; ++p; continue;
        }
        if (s < se && *s == pc) { ++s; ++p; continue; }
        return false;
    }
    return s == se;
}

Value eval_expr(const Expr* e, const Row& row, EvalCtx& ctx) {
    if (e == nullptr) { ctx.ok = false; return null(); }
    switch (e->kind) {
        case ExprKind::Literal:
            return literal_value(e);
        case ExprKind::ColumnRef:
            if (e->input_index < row.size()) return row[e->input_index];
            ctx.ok = false;
            return null();
        case ExprKind::OuterRef: {
            const std::uint32_t d = e->outer_depth;
            if (d == 0 || d > ctx.outers.size()) { ctx.ok = false; return null(); }
            const Row* orow = ctx.outers[ctx.outers.size() - d];
            if (e->input_index < orow->size()) return (*orow)[e->input_index];
            ctx.ok = false;
            return null();
        }
        case ExprKind::BinaryOp: {
            // Row comparison: (a,...) <op> (b,...). Compare element-wise with
            // lexicographic 3VL before the scalar path evaluates the Row operands
            // (a Row has no scalar value).
            if (is_row_comparison_op(e->bin_op) &&
                (e->children[0]->kind == ExprKind::Row ||
                 e->children[1]->kind == ExprKind::Row)) {
                return eval_row_compare(e->bin_op, e->children[0].get(),
                                        e->children[1].get(), row, ctx);
            }
            const Value a = eval_expr(e->children[0].get(), row, ctx);
            // Short-circuit AND/OR under 3VL.
            if (e->bin_op == BinaryOp::And) {
                if (to_bool3(a) == Bool3::False) return vb(false);
                const Value b = eval_expr(e->children[1].get(), row, ctx);
                Bool3 ba = to_bool3(a), bb = to_bool3(b);
                if (ba == Bool3::False || bb == Bool3::False) return vb(false);
                if (ba == Bool3::Unknown || bb == Bool3::Unknown) return null();
                return vb(true);
            }
            if (e->bin_op == BinaryOp::Or) {
                if (to_bool3(a) == Bool3::True) return vb(true);
                const Value b = eval_expr(e->children[1].get(), row, ctx);
                Bool3 ba = to_bool3(a), bb = to_bool3(b);
                if (ba == Bool3::True || bb == Bool3::True) return vb(true);
                if (ba == Bool3::Unknown || bb == Bool3::Unknown) return null();
                return vb(false);
            }
            const Value b = eval_expr(e->children[1].get(), row, ctx);
            switch (e->bin_op) {
                case BinaryOp::Add:
                case BinaryOp::Subtract:
                case BinaryOp::Multiply:
                case BinaryOp::Divide:
                case BinaryOp::Modulo:
                case BinaryOp::Concat:
                    return eval_arith(e->bin_op, a, b, ctx);
                case BinaryOp::Equal:
                case BinaryOp::NotEqual:
                case BinaryOp::LessThan:
                case BinaryOp::LessEqual:
                case BinaryOp::GreaterThan:
                case BinaryOp::GreaterEqual:
                    return eval_compare(e->bin_op, a, b);
                default:
                    ctx.ok = false;
                    return null();
            }
        }
        case ExprKind::UnaryOp: {
            const Value a = eval_expr(e->children[0].get(), row, ctx);
            switch (e->un_op) {
                case UnaryOp::Not: {
                    Bool3 b = to_bool3(a);
                    if (b == Bool3::Unknown) return null();
                    return vb(b == Bool3::False);
                }
                case UnaryOp::Negate: {
                    if (is_null(a)) return null();
                    if (const std::int64_t* i = std::get_if<std::int64_t>(&a)) return vi(-*i);
                    double d;
                    if (numeric_of(a, d)) return vd(-d);
                    ctx.ok = false;
                    return null();
                }
                case UnaryOp::IsNull: return vb(is_null(a));
                case UnaryOp::IsNotNull: return vb(!is_null(a));
                default: ctx.ok = false; return null();
            }
        }
        case ExprKind::IsNull: {
            const Value a = eval_expr(e->children[0].get(), row, ctx);
            const bool isnull = is_null(a);
            return vb(e->negated() ? !isnull : isnull);  // negated == IS NOT NULL
        }
        case ExprKind::BooleanTest: {
            // IS [NOT] TRUE/FALSE/UNKNOWN collapses the operand's 3VL truth value
            // to a plain 2VL boolean (never NULL). This is exactly what makes
            // `x IS NOT TRUE` differ from `NOT x`: the former keeps NULL/UNKNOWN
            // rows, the latter propagates them as UNKNOWN and drops them.
            const Bool3 b = to_bool3(eval_expr(e->children[0].get(), row, ctx));
            bool result = false;
            switch (e->bool_test) {
                case BoolTest::True:    result = (b == Bool3::True);    break;
                case BoolTest::False:   result = (b == Bool3::False);   break;
                case BoolTest::Unknown: result = (b == Bool3::Unknown); break;
            }
            return vb(e->negated() ? !result : result);
        }
        case ExprKind::Row:
            // A row/tuple only has meaning inside a row comparison, which is
            // handled in the BinaryOp case before the operands are evaluated.
            // A standalone row value (e.g. SELECT (a, b)) has no scalar
            // representation in this evaluator - an explicit eval boundary.
            ctx.ok = false;
            return null();
        case ExprKind::InList: {
            const Value v = eval_expr(e->children[0].get(), row, ctx);
            std::vector<Value> elems;
            for (std::size_t i = 1; i < e->children.size(); ++i)
                elems.push_back(eval_expr(e->children[i].get(), row, ctx));
            Bool3 m = in_membership(v, elems);
            if (e->negated()) {
                if (m == Bool3::True) m = Bool3::False;
                else if (m == Bool3::False) m = Bool3::True;
            }
            return from_bool3(m);
        }
        case ExprKind::Between: {
            const Value v = eval_expr(e->children[0].get(), row, ctx);
            const Value lo = eval_expr(e->children[1].get(), row, ctx);
            const Value hi = eval_expr(e->children[2].get(), row, ctx);
            const Value ge = eval_compare(BinaryOp::GreaterEqual, v, lo);
            const Value le = eval_compare(BinaryOp::LessEqual, v, hi);
            Bool3 bge = to_bool3(ge), ble = to_bool3(le);
            Bool3 both;
            if (bge == Bool3::False || ble == Bool3::False) both = Bool3::False;
            else if (bge == Bool3::Unknown || ble == Bool3::Unknown) both = Bool3::Unknown;
            else both = Bool3::True;
            if (e->negated()) {
                if (both == Bool3::True) both = Bool3::False;
                else if (both == Bool3::False) both = Bool3::True;
            }
            return from_bool3(both);
        }
        case ExprKind::Like: {
            // {input, pattern[, escape]}. NULL input or pattern -> UNKNOWN (and
            // NOT LIKE of UNKNOWN is still UNKNOWN, i.e. NULL). Otherwise match
            // per SQL LIKE, then invert for NOT LIKE.
            const Value in = eval_expr(e->children[0].get(), row, ctx);
            const Value pat = eval_expr(e->children[1].get(), row, ctx);
            if (!ctx.ok) return null();
            if (is_null(in) || is_null(pat)) return null();
            const std::string* si = std::get_if<std::string>(&in);
            const std::string* pi = std::get_if<std::string>(&pat);
            if (si == nullptr || pi == nullptr) { ctx.ok = false; return null(); }
            char esc = '\0';
            bool has_esc = false;
            if (e->children.size() >= 3) {
                const Value ev = eval_expr(e->children[2].get(), row, ctx);
                if (!ctx.ok) return null();
                if (is_null(ev)) return null();
                const std::string* es = std::get_if<std::string>(&ev);
                if (es == nullptr || es->size() != 1) { ctx.ok = false; return null(); }
                esc = (*es)[0];
                has_esc = true;
            }
            bool m;
            if (e->case_insensitive()) {
                // ILIKE: ASCII-fold both input and pattern (and the escape char)
                // to lower case, then match. Folding both sides yields
                // case-insensitive semantics through the same matcher.
                auto fold = [](char c) {
                    return (c >= 'A' && c <= 'Z') ? static_cast<char>(c - 'A' + 'a') : c;
                };
                std::string ls(*si), lp(*pi);
                for (char& c : ls) c = fold(c);
                for (char& c : lp) c = fold(c);
                const char fesc = has_esc ? fold(esc) : esc;
                m = like_match(ls.data(), ls.data() + ls.size(),
                               lp.data(), lp.data() + lp.size(), fesc, has_esc);
            } else {
                m = like_match(si->data(), si->data() + si->size(),
                               pi->data(), pi->data() + pi->size(), esc, has_esc);
            }
            if (e->negated()) m = !m;
            return vb(m);
        }
        case ExprKind::Case: {
            // children: when0, then0, when1, then1, ..., [else]
            const std::size_t n = e->children.size();
            std::size_t i = 0;
            // M10: ignore every WHEN test and fall straight through to ELSE, so a
            // `CASE WHEN age IS NULL THEN -1 ELSE age END` no longer maps NULL to
            // -1. Caught by the CASE-vs-COALESCE equivalence test.
            if (g_mutant != 10) {
                for (; i + 1 < n; i += 2) {
                    const Value w = eval_expr(e->children[i].get(), row, ctx);
                    if (is_true(w)) return eval_expr(e->children[i + 1].get(), row, ctx);
                }
            } else {
                i = (n % 2 == 1) ? n - 1 : n;  // jump to the ELSE slot (if any)
            }
            if (i < n) return eval_expr(e->children[i].get(), row, ctx);  // else
            return null();
        }
        case ExprKind::Cast: {
            const Value a = eval_expr(e->children[0].get(), row, ctx);
            return eval_cast(a, e->target_type);
        }
        case ExprKind::ScalarFunction: {
            const std::string& fn = e->func_name;
            if (fn == "COALESCE" || fn == "IFNULL") {
                for (const auto& c : e->children) {
                    Value v = eval_expr(c.get(), row, ctx);
                    if (!ctx.ok) return null();
                    if (!is_null(v)) return v;
                    // M15: consider ONLY the first operand, dropping the rest. This
                    // models a USING/NATURAL RIGHT/FULL merge that keeps only the
                    // left copy instead of COALESCE(left, right): the merged column
                    // becomes NULL for every right-only row. Invisible unless a
                    // test pins the actual merged value.
                    if (g_mutant == 15) break;
                }
                return null();
            }
            if (fn == "ABS" && e->children.size() == 1) {
                Value v = eval_expr(e->children[0].get(), row, ctx);
                if (is_null(v)) return null();
                if (const std::int64_t* i = std::get_if<std::int64_t>(&v)) return vi(*i < 0 ? -*i : *i);
                double d; if (numeric_of(v, d)) return vd(d < 0 ? -d : d);
                ctx.ok = false; return null();
            }
            if ((fn == "LOWER" || fn == "UPPER") && e->children.size() == 1) {
                Value v = eval_expr(e->children[0].get(), row, ctx);
                if (is_null(v)) return null();
                if (const std::string* s = std::get_if<std::string>(&v)) {
                    std::string r = *s;
                    for (char& ch : r) ch = fn == "LOWER" ? static_cast<char>(std::tolower((unsigned char)ch))
                                                          : static_cast<char>(std::toupper((unsigned char)ch));
                    return vs(std::move(r));
                }
                ctx.ok = false; return null();
            }
            ctx.ok = false;  // unsupported scalar function
            return null();
        }
        case ExprKind::Subquery:
            return eval_subquery(e, row, ctx);
        case ExprKind::Aggregate:
            // An aggregate call is only meaningful inside Aggregate-node context,
            // which computes it directly. Reaching it here is unsupported.
            ctx.ok = false;
            return null();
        default:
            ctx.ok = false;
            return null();
    }
}

// ===========================================================================
// Aggregate computation.
// ===========================================================================

// Compute one aggregate call over `group_rows` (rows of the Aggregate's input).
Value compute_aggregate(const Expr* call, const std::vector<Row>& group_rows, EvalCtx& ctx) {
    const std::string& fn = call->func_name;
    const bool distinct = call->distinct;

    // Aggregate FILTER (WHERE p): only rows for which the predicate is TRUE
    // contribute to this aggregate. M17 ignores the filter, so a filtered
    // COUNT(*) wrongly equals the unfiltered COUNT(*).
    std::vector<Row> filtered;
    const std::vector<Row>* src = &group_rows;
    if (call->filter && g_mutant != 17) {
        for (const Row& r : group_rows)
            if (is_true(eval_expr(call->filter.get(), r, ctx))) filtered.push_back(r);
        src = &filtered;
    }
    const std::vector<Row>& rows = *src;

    if (fn == "COUNT") {
        std::int64_t n = 0;
        if (call->children.empty()) {
            n = static_cast<std::int64_t>(rows.size());  // COUNT(*)
        } else {
            std::vector<Value> seen;
            for (const Row& r : rows) {
                Value v = eval_expr(call->children[0].get(), r, ctx);
                // M9: count NULL argument rows too, so COUNT(age) wrongly equals
                // COUNT(*). Caught by the COUNT(nullable) < COUNT(*) test.
                if (is_null(v)) { if (g_mutant == 9) ++n; continue; }
                if (distinct) {
                    bool dup = false;
                    for (const Value& s : seen) if (value_identical(s, v)) { dup = true; break; }
                    if (dup) continue;
                    seen.push_back(v);
                }
                ++n;
            }
        }
        if (g_mutant == 4) n += 1;  // M4
        return vi(n);
    }

    // SUM / AVG / MIN / MAX collect non-null argument values.
    std::vector<Value> vals;
    for (const Row& r : rows) {
        Value v = eval_expr(call->children.empty() ? nullptr : call->children[0].get(), r, ctx);
        if (is_null(v)) continue;
        if (distinct) {
            bool dup = false;
            for (const Value& s : vals) if (value_identical(s, v)) { dup = true; break; }
            if (dup) continue;
        }
        vals.push_back(v);
    }

    if (fn == "SUM") {
        if (vals.empty()) return null();
        bool all_int = true;
        for (const Value& v : vals) if (!std::holds_alternative<std::int64_t>(v)) { all_int = false; break; }
        if (all_int) {
            std::int64_t s = 0;
            for (const Value& v : vals) s += std::get<std::int64_t>(v);
            if (g_mutant == 4) s += 1;  // M4
            return vi(s);
        }
        double s = 0;
        for (const Value& v : vals) { double d; numeric_of(v, d); s += d; }
        if (g_mutant == 4) s += 1;  // M4
        return vd(s);
    }
    if (fn == "AVG") {
        if (vals.empty()) return null();
        double s = 0;
        for (const Value& v : vals) { double d; numeric_of(v, d); s += d; }
        return vd(s / static_cast<double>(vals.size()));
    }
    if (fn == "MIN" || fn == "MAX") {
        if (vals.empty()) return null();
        Value best = vals[0];
        for (std::size_t i = 1; i < vals.size(); ++i) {
            int cmp = 0; bool unk = false;
            compare3(vals[i], best, cmp, unk);
            if (unk) continue;
            if ((fn == "MIN" && cmp < 0) || (fn == "MAX" && cmp > 0)) best = vals[i];
        }
        return best;
    }
    ctx.ok = false;
    return null();
}

// Map each Aggregate output column to its producer value (group key or aggregate)
// and build the group's output row. The output is select-list-shaped: each column
// is EITHER a group-key passthrough (carrying its source column's provenance /
// name) OR an aggregate result. We identify the group-key columns first, by
// provenance then by name, and treat every remaining column as the next
// aggregate value in select-list order.
//
// This deliberately does NOT identify an aggregate column by matching its name to
// the aggregate's func_name: an alias (`SUM(x) AS s`) renames the output column,
// so a func_name test silently fails to place the value and the column collapses
// to NULL - which corrupted every aliased aggregate (and, transitively, any
// HAVING / ORDER BY over one). Consuming the non-group-key columns positionally
// is alias-agnostic.
//
// Residual limitation: a group key that is a non-column EXPRESSION (e.g.
// `GROUP BY a + b`) carries no column provenance or source name, so it cannot be
// distinguished from an aggregate here and would be mis-consumed if interleaved
// with one. The corpus does not exercise expression group keys; revisit if it
// starts to.
Row build_aggregate_output_row(const LogicalNode* agg, const std::vector<Value>& gk_vals,
                               const std::vector<Value>& agg_vals) {
    const db25::plan::Schema& in = agg->child(0)->output;
    Row out;
    out.reserve(agg->output.size());
    std::size_t agg_cursor = 0;
    std::vector<bool> gk_used(agg->group_keys.size(), false);

    for (const auto& col : agg->output) {
        // (A) Unaliased aggregate: its output name is the next aggregate's
        //     func_name. This is the fast, unambiguous path and covers every
        //     query that does not alias its aggregates (incl. the decorrelated
        //     LEFT JOIN + Aggregate the optimizer builds).
        if (agg_cursor < agg->aggregates.size() &&
            col.name == agg->aggregates[agg_cursor]->func_name) {
            out.push_back(agg_cursor < agg_vals.size() ? agg_vals[agg_cursor] : null());
            ++agg_cursor;
            continue;
        }
        // (B) Group-key passthrough: match to a group_keys entry by source column
        //     id / table id, then by source column name.
        int chosen = -1;
        for (std::size_t i = 0; i < agg->group_keys.size(); ++i) {
            if (gk_used[i]) continue;
            const Expr* gk = agg->group_keys[i].get();
            if (gk->kind != ExprKind::ColumnRef) continue;
            if (gk->input_index >= in.size()) continue;
            if (col.column_id != 0 && in[gk->input_index].column_id == col.column_id &&
                in[gk->input_index].table_id == col.table_id) { chosen = static_cast<int>(i); break; }
        }
        if (chosen < 0) {
            for (std::size_t i = 0; i < agg->group_keys.size(); ++i) {
                if (gk_used[i]) continue;
                const Expr* gk = agg->group_keys[i].get();
                if (gk->kind == ExprKind::ColumnRef && gk->input_index < in.size() &&
                    !col.name.empty() && in[gk->input_index].name == col.name) { chosen = static_cast<int>(i); break; }
            }
        }
        if (chosen >= 0) {
            gk_used[static_cast<std::size_t>(chosen)] = true;
            out.push_back(gk_vals[static_cast<std::size_t>(chosen)]);
            continue;
        }
        // (C) A NON-EMPTY-named column that is neither a func_name match nor a
        //     group key is an ALIASED aggregate (`SUM(x) AS s` renamed the
        //     column). Consume the next aggregate value positionally - the fix for
        //     the alias -> NULL bug. The agg_cursor is shared with path (A), so
        //     mixed aliased/unaliased aggregates stay in select-list order.
        //     The name test is what separates this from (D): decorrelation emits a
        //     group-key passthrough with an EMPTY name and no provenance, which
        //     must NOT steal an aggregate value.
        if (!col.name.empty() && agg_cursor < agg_vals.size()) {
            out.push_back(agg_vals[agg_cursor++]);
            continue;
        }
        // (D) An empty-named / unmatched column is a group key whose provenance and
        //     name were rewritten (e.g. by decorrelation, which emits the grouping
        //     key with an empty name). Fall back to the first unused key, else NULL.
        for (std::size_t i = 0; i < agg->group_keys.size(); ++i)
            if (!gk_used[i]) { chosen = static_cast<int>(i); break; }
        if (chosen >= 0) {
            gk_used[static_cast<std::size_t>(chosen)] = true;
            out.push_back(gk_vals[static_cast<std::size_t>(chosen)]);
        } else {
            out.push_back(null());
        }
    }
    return out;
}

bool eval_aggregate(const LogicalNode* n, EvalCtx& ctx, Table& out) {
    Table child;
    if (!eval_node(n->child(0), ctx, child)) return false;

    const std::size_t nkeys = n->group_keys.size();

    // Group rows by the tuple of group-key values (NULL == NULL for grouping).
    std::vector<std::vector<Value>> group_keys_vals;  // distinct key tuples, first-appearance order
    std::vector<std::vector<Row>> group_rows;

    if (nkeys == 0) {
        // Implicit aggregation: exactly one group (even over an empty input).
        group_keys_vals.push_back({});
        group_rows.emplace_back(child.begin(), child.end());
    } else {
        for (const Row& r : child) {
            std::vector<Value> key;
            key.reserve(nkeys);
            for (const auto& gk : n->group_keys) key.push_back(eval_expr(gk.get(), r, ctx));
            if (!ctx.ok) return false;
            int found = -1;
            for (std::size_t g = 0; g < group_keys_vals.size(); ++g) {
                bool same = true;
                for (std::size_t k = 0; k < nkeys; ++k)
                    if (!value_identical(group_keys_vals[g][k], key[k])) { same = false; break; }
                if (same) { found = static_cast<int>(g); break; }
            }
            if (found < 0) {
                group_keys_vals.push_back(key);
                group_rows.emplace_back();
                group_rows.back().push_back(r);
            } else {
                group_rows[static_cast<std::size_t>(found)].push_back(r);
            }
        }
    }

    for (std::size_t g = 0; g < group_rows.size(); ++g) {
        std::vector<Value> agg_vals;
        agg_vals.reserve(n->aggregates.size());
        for (const auto& call : n->aggregates) {
            agg_vals.push_back(compute_aggregate(call.get(), group_rows[g], ctx));
            if (!ctx.ok) return false;
        }
        out.push_back(build_aggregate_output_row(n, group_keys_vals[g], agg_vals));
    }
    return true;
}

// ===========================================================================
// Sort helper.
// ===========================================================================
bool sort_less(const Row& a, const Row& b, const LogicalNode* n, EvalCtx& ctx) {
    for (const auto& key : n->sort_keys) {
        Value va = eval_expr(key.expr.get(), a, ctx);
        Value vb = eval_expr(key.expr.get(), b, ctx);
        const bool na = is_null(va), nb = is_null(vb);
        if (na || nb) {
            if (na && nb) continue;
            // NULLS ordering: explicit override, else default NULLS LAST for ASC,
            // NULLS FIRST for DESC (a common SQL default).
            bool nulls_first = key.nulls_order_explicit ? key.nulls_first : key.descending;
            if (na) return nulls_first;   // a is null -> a first iff nulls_first
            return !nulls_first;          // b is null -> a first iff !nulls_first
        }
        int cmp = 0; bool unk = false;
        compare3(va, vb, cmp, unk);
        if (unk || cmp == 0) continue;
        if (key.descending) cmp = -cmp;
        return cmp < 0;
    }
    return false;
}

// ===========================================================================
// Operator evaluation.
// ===========================================================================
bool eval_node(const LogicalNode* n, EvalCtx& ctx, Table& out) {
    if (n == nullptr) { ctx.ok = false; return false; }
    switch (n->op) {
        case LogicalOp::Scan: {
            auto it = ctx.data->find(n->table_name);
            const Table empty;
            const Table& src = (it == ctx.data->end()) ? empty : it->second;
            // Project full base rows down to this (possibly pruned) scan schema by
            // catalog column_id (1-based): base index = column_id - 1.
            for (const Row& base : src) {
                Row r;
                r.reserve(n->output.size());
                for (const auto& col : n->output) {
                    const std::size_t idx = col.column_id == 0 ? 0 : col.column_id - 1;
                    r.push_back(idx < base.size() ? base[idx] : null());
                }
                out.push_back(std::move(r));
            }
            return true;
        }
        case LogicalOp::Filter: {
            Table child;
            if (!eval_node(n->child(0), ctx, child)) return false;
            if (g_mutant == 2) { out = std::move(child); return true; }  // M2
            for (Row& r : child) {
                if (n->predicate == nullptr) { out.push_back(std::move(r)); continue; }
                Value v = eval_expr(n->predicate.get(), r, ctx);
                if (!ctx.ok) return false;
                if (is_true(v)) out.push_back(std::move(r));
            }
            return true;
        }
        case LogicalOp::Project: {
            Table child;
            if (!eval_node(n->child(0), ctx, child)) return false;
            for (const Row& r : child) {
                Row o;
                o.reserve(n->exprs.size());
                for (const auto& e : n->exprs) {
                    o.push_back(eval_expr(e.get(), r, ctx));
                    if (!ctx.ok) return false;
                }
                out.push_back(std::move(o));
            }
            return true;
        }
        case LogicalOp::Join: {
            Table left, right;
            if (!eval_node(n->child(0), ctx, left)) return false;
            if (!eval_node(n->child(1), ctx, right)) return false;
            const std::size_t lw = n->child(0)->output.size();
            const std::size_t rw = n->child(1)->output.size();
            const JoinType jt = n->join_type;
            const bool is_cross = (jt == JoinType::Cross);
            // LeftLateral is LEFT JOIN LATERAL: like a LEFT join, an unmatched
            // left row is kept with NULLs on the right. (The evaluator does not
            // model the per-left-row correlation itself; no corpus query is a
            // lateral join, so this only keeps the null-supplying side correct.)
            const bool keep_left =
                (jt == JoinType::Left || jt == JoinType::Full ||
                 jt == JoinType::LeftLateral);
            const bool keep_right = (jt == JoinType::Right || jt == JoinType::Full);
            // A JOIN ... USING / NATURAL join compacts its output: the predicate is
            // evaluated over the full left++right frame, but the emitted row drops
            // the duplicate right-hand columns (those whose name matches a left
            // column), so `output` is narrower than lw+rw. Mirror that here so the
            // Project above indexes the same (merged) frame the binder built.
            const bool full_concat = (n->output.size() == lw + rw);
            std::vector<bool> keep_col(lw + rw, true);
            if (!full_concat) {
                const auto& ls = n->child(0)->output;
                const auto& rs = n->child(1)->output;
                for (std::size_t j = 0; j < rw; ++j)
                    for (std::size_t i = 0; i < lw; ++i)
                        if (rs[j].name == ls[i].name) { keep_col[lw + j] = false; break; }
            }
            auto emit = [&](Row&& full) {
                if (full_concat) { out.push_back(std::move(full)); return; }
                Row o;
                o.reserve(n->output.size());
                for (std::size_t k = 0; k < full.size(); ++k)
                    if (keep_col[k]) o.push_back(std::move(full[k]));
                out.push_back(std::move(o));
            };
            std::vector<bool> right_matched(right.size(), false);
            for (const Row& lr : left) {
                bool matched = false;
                for (std::size_t ri = 0; ri < right.size(); ++ri) {
                    Row combined = lr;
                    combined.insert(combined.end(), right[ri].begin(), right[ri].end());
                    bool keep = is_cross || n->predicate == nullptr;
                    if (!keep) {
                        Value v = eval_expr(n->predicate.get(), combined, ctx);
                        if (!ctx.ok) return false;
                        keep = is_true(v);
                    }
                    if (keep) { emit(std::move(combined)); matched = true; right_matched[ri] = true; }
                }
                // M16: drop the null-extended row an outer join must emit for an
                // unmatched left row, degrading LEFT/FULL to an inner join. Caught
                // by the outer-join null-extension anchors (a NULL-padded row that
                // must appear goes missing).
                if (keep_left && !matched && g_mutant != 16) {
                    Row combined = lr;
                    for (std::size_t k = 0; k < rw; ++k) combined.push_back(null());
                    emit(std::move(combined));
                }
            }
            if (keep_right && g_mutant != 16) {  // M16: also drop right-side null-extension
                for (std::size_t ri = 0; ri < right.size(); ++ri) {
                    if (right_matched[ri]) continue;
                    Row combined;
                    combined.reserve(lw + rw);
                    for (std::size_t k = 0; k < lw; ++k) combined.push_back(null());
                    combined.insert(combined.end(), right[ri].begin(), right[ri].end());
                    emit(std::move(combined));
                }
            }
            return true;
        }
        case LogicalOp::SetOp: {
            Table a, b;
            if (!eval_node(n->child(0), ctx, a)) return false;
            if (!eval_node(n->child(1), ctx, b)) return false;
            auto row_in = [](const Row& r, const Table& t) {
                for (const Row& e : t) {
                    if (e.size() != r.size()) continue;
                    bool same = true;
                    for (std::size_t i = 0; i < r.size(); ++i)
                        if (!value_identical(e[i], r[i])) { same = false; break; }
                    if (same) return true;
                }
                return false;
            };
            auto push_distinct = [&](const Row& r) { if (!row_in(r, out)) out.push_back(r); };
            // Remove ONE row of `t` equal to `r` (multiset take); report success.
            // Used by the ALL variants to consume matched duplicates so the output
            // multiplicity is min(count_a, count_b) for INTERSECT ALL and
            // max(0, count_a - count_b) for EXCEPT ALL.
            auto take_one = [](const Row& r, Table& t) -> bool {
                for (std::size_t i = 0; i < t.size(); ++i) {
                    if (t[i].size() != r.size()) continue;
                    bool same = true;
                    for (std::size_t k = 0; k < r.size(); ++k)
                        if (!value_identical(t[i][k], r[k])) { same = false; break; }
                    if (same) { t.erase(t.begin() + static_cast<std::ptrdiff_t>(i)); return true; }
                }
                return false;
            };
            switch (n->set_op) {
                case db25::ast::SetOp::UnionAll:
                    out = std::move(a);
                    for (Row& r : b) out.push_back(std::move(r));
                    return true;
                case db25::ast::SetOp::Union:
                    for (const Row& r : a) push_distinct(r);
                    for (const Row& r : b) push_distinct(r);
                    return true;
                case db25::ast::SetOp::Intersect:
                    for (const Row& r : a) if (row_in(r, b)) push_distinct(r);
                    return true;
                case db25::ast::SetOp::IntersectAll: {
                    Table brem = b;  // consume matches so multiplicity is min(a,b)
                    for (const Row& r : a) if (take_one(r, brem)) out.push_back(r);
                    return true;
                }
                case db25::ast::SetOp::Except:
                    for (const Row& r : a) if (!row_in(r, b)) push_distinct(r);
                    return true;
                case db25::ast::SetOp::ExceptAll: {
                    Table brem = b;  // each b row cancels one a row: max(0, a - b)
                    for (const Row& r : a) if (!take_one(r, brem)) out.push_back(r);
                    return true;
                }
            }
            ctx.ok = false;
            return false;
        }
        case LogicalOp::SemiJoin:
        case LogicalOp::AntiJoin: {
            Table left, right;
            if (!eval_node(n->child(0), ctx, left)) return false;
            if (!eval_node(n->child(1), ctx, right)) return false;
            const bool anti = (n->op == LogicalOp::AntiJoin);
            for (const Row& lr : left) {
                if (n->op == LogicalOp::SemiJoin && g_mutant == 3) { out.push_back(lr); continue; }  // M3
                bool found = false;
                for (const Row& rr : right) {
                    Row combined = lr;
                    combined.insert(combined.end(), rr.begin(), rr.end());
                    bool keep = (n->predicate == nullptr);
                    if (!keep) {
                        Value v = eval_expr(n->predicate.get(), combined, ctx);
                        if (!ctx.ok) return false;
                        keep = is_true(v);
                    }
                    if (keep) { found = true; break; }
                }
                if (found != anti) out.push_back(lr);  // semi: keep if found; anti: keep if not
            }
            return true;
        }
        case LogicalOp::Aggregate:
            return eval_aggregate(n, ctx, out);
        case LogicalOp::Distinct: {
            Table child;
            if (!eval_node(n->child(0), ctx, child)) return false;
            // M8: skip de-duplication entirely (pass every child row through), so
            // DISTINCT no longer removes duplicates. Caught by the
            // DISTINCT==GROUP BY and duplicates-survive-without-DISTINCT tests.
            if (g_mutant == 8) { out = std::move(child); return true; }
            for (const Row& r : child) {
                bool dup = false;
                for (const Row& e : out) {
                    if (e.size() != r.size()) continue;
                    bool same = true;
                    for (std::size_t i = 0; i < r.size(); ++i)
                        if (!value_identical(e[i], r[i])) { same = false; break; }
                    if (same) { dup = true; break; }
                }
                if (!dup) out.push_back(r);
            }
            return true;
        }
        case LogicalOp::Sort: {
            Table child;
            if (!eval_node(n->child(0), ctx, child)) return false;
            std::stable_sort(child.begin(), child.end(),
                             [&](const Row& a, const Row& b) { return sort_less(a, b, n, ctx); });
            // M14: reverse the sorted order, modelling a Sort that produces the
            // wrong row order (a dropped or reversed ORDER BY direction). Caught
            // ONLY by the order-sensitive ORDER BY tests (seq_equal against a
            // literal ordered oracle); a bag_equal / multiset check cannot see it.
            if (g_mutant == 14) std::reverse(child.begin(), child.end());
            // The binder may append HIDDEN ORDER BY sort keys as trailing columns
            // of the child (so the Sort can order by a non-selected column). The
            // Sort node's own output schema is narrower in that case. Project each
            // row down to the declared output width, dropping any trailing hidden
            // key columns. Guard: only ever drop, never pad — if the child is
            // already at (or below) the declared width this is a no-op.
            // M13: skip the truncation, leaking trailing hidden sort keys into
            // the result (the pre-fix bug) so the width-preservation test is
            // falsifiable.
            const std::size_t w = n->output.size();
            for (Row& r : child) {
                if (g_mutant != 13 && r.size() > w) r.resize(w);
                out.push_back(std::move(r));
            }
            return true;
        }
        case LogicalOp::Limit: {
            Table child;
            if (!eval_node(n->child(0), ctx, child)) return false;
            std::size_t start = n->has_offset && n->offset > 0 ? static_cast<std::size_t>(n->offset) : 0;
            std::size_t count = child.size();
            if (n->has_limit && n->limit >= 0) count = static_cast<std::size_t>(n->limit);
            for (std::size_t i = start; i < child.size() && (i - start) < count; ++i)
                out.push_back(std::move(child[i]));
            return true;
        }
        case LogicalOp::Values: {
            const Row empty_row;
            for (const auto& vr : n->value_rows) {
                Row o;
                o.reserve(vr.size());
                for (const auto& e : vr) {
                    o.push_back(eval_expr(e.get(), empty_row, ctx));
                    if (!ctx.ok) return false;
                }
                out.push_back(std::move(o));
            }
            return true;
        }
        default:
            ctx.ok = false;  // SetOp, Window, DML, ... not supported
            return false;
    }
}

}  // namespace

std::optional<Table> eval(const LogicalNode* plan, const Data& data) {
    if (plan == nullptr) return std::nullopt;
    EvalCtx ctx;
    ctx.data = &data;
    Table out;
    if (!eval_node(plan, ctx, out) || !ctx.ok) return std::nullopt;
    return out;
}

// ===========================================================================
// bag_equal (multiset equality, NULL-aware).
// ===========================================================================
bool bag_equal(const Table& a, const Table& b) {
    if (a.size() != b.size()) return false;
    std::vector<bool> used(b.size(), false);
    for (const Row& ra : a) {
        bool matched = false;
        for (std::size_t j = 0; j < b.size(); ++j) {
            if (used[j]) continue;
            const Row& rb = b[j];
            if (rb.size() != ra.size()) continue;
            bool same = true;
            for (std::size_t i = 0; i < ra.size(); ++i)
                if (!value_identical(ra[i], rb[i])) { same = false; break; }
            if (same) { used[j] = true; matched = true; break; }
        }
        if (!matched) return false;
    }
    return true;
}

// ===========================================================================
// seq_equal (sequence equality, order-SENSITIVE, NULL-aware). Rows must match
// position-by-position - the only way to detect a dropped/reversed Sort, which
// bag_equal (multiset) is blind to.
// ===========================================================================
bool seq_equal(const Table& a, const Table& b) {
    if (a.size() != b.size()) return false;
    for (std::size_t i = 0; i < a.size(); ++i) {
        if (a[i].size() != b[i].size()) return false;
        for (std::size_t j = 0; j < a[i].size(); ++j)
            if (!value_identical(a[i][j], b[i][j])) return false;
    }
    return true;
}

// ===========================================================================
// Structural predicates.
// ===========================================================================
bool plan_contains(const LogicalNode* n, LogicalOp op) {
    if (n == nullptr) return false;
    if (n->op == op) return true;
    for (const auto& c : n->children)
        if (plan_contains(c.get(), op)) return true;
    // Descend into embedded subquery sub-plans as well.
    auto scan_exprs = [&](const std::vector<db25::plan::ExprPtr>& es) -> bool {
        for (const auto& e : es) {
            std::function<bool(const Expr*)> walk = [&](const Expr* x) -> bool {
                if (!x) return false;
                if (x->sub_plan && plan_contains(x->sub_plan.get(), op)) return true;
                for (const auto& ch : x->children) if (walk(ch.get())) return true;
                return false;
            };
            if (walk(e.get())) return true;
        }
        return false;
    };
    if (n->predicate) {
        std::function<bool(const Expr*)> walk = [&](const Expr* x) -> bool {
            if (!x) return false;
            if (x->sub_plan && plan_contains(x->sub_plan.get(), op)) return true;
            for (const auto& ch : x->children) if (walk(ch.get())) return true;
            return false;
        };
        if (walk(n->predicate.get())) return true;
    }
    if (scan_exprs(n->exprs)) return true;
    return false;
}

std::size_t count_subqueries(const LogicalNode* n) {
    if (n == nullptr) return 0;
    std::size_t total = 0;
    std::function<void(const Expr*)> walk = [&](const Expr* x) {
        if (!x) return;
        if (x->kind == ExprKind::Subquery) ++total;
        for (const auto& ch : x->children) walk(ch.get());
    };
    if (n->predicate) walk(n->predicate.get());
    for (const auto& e : n->exprs) walk(e.get());
    for (const auto& e : n->aggregates) walk(e.get());
    for (const auto& e : n->group_keys) walk(e.get());
    for (const auto& c : n->children) total += count_subqueries(c.get());
    return total;
}

// --- Stage-artifact predicates over the fingerprints captured by run() ---
bool ast_has(const Stages& s, db25::ast::NodeType kind) {
    for (const auto k : s.ast_kinds) {
        if (k == kind) return true;
    }
    return false;
}

bool has_diagnostic(const Stages& s, db25::semantic::DiagnosticCode code) {
    for (const auto& d : s.diags) {
        if (d.code == code) return true;
    }
    return false;
}

std::size_t error_count(const Stages& s) {
    std::size_t n = 0;
    for (const auto& d : s.diags) {
        if (d.severity == db25::semantic::Severity::Error) ++n;
    }
    return n;
}

// ===========================================================================
// Pipeline driver.
// ===========================================================================
namespace {
// Recursively null out the first Filter/Join predicate found (mutant M5).
bool drop_first_predicate(LogicalNode* n) {
    if (n == nullptr) return false;
    if ((n->op == LogicalOp::Filter || n->op == LogicalOp::Join ||
         n->op == LogicalOp::SemiJoin || n->op == LogicalOp::AntiJoin) && n->predicate) {
        n->predicate.reset();
        return true;
    }
    for (auto& c : n->children)
        if (drop_first_predicate(c.get())) return true;
    return false;
}
}  // namespace

// Depth-first walk of the parsed AST, recording every NodeType encountered.
// This is the parse-stage "feature fingerprint" a conformance test asserts
// against (e.g. a query that uses a window function must yield a WindowFunction
// node here). The AST is arena-owned by the Parser, so this runs while the
// Parser is still alive inside run() and copies only the small kind vector out.
static void collect_ast_kinds(const db25::ast::ASTNode* n,
                              std::vector<db25::ast::NodeType>& out) {
    if (n == nullptr) return;
    out.push_back(n->node_type);
    for (const db25::ast::ASTNode* c = n->first_child; c != nullptr; c = c->next_sibling) {
        collect_ast_kinds(c, out);
    }
}

Stages run(const db25::semantic::InMemoryCatalog& cat, std::string_view sql) {
    Stages st;
    db25::parser::Parser parser;
    auto parsed = parser.parse(sql);
    st.parsed = parsed.has_value();
    if (!st.parsed) return st;

    // Parse-stage artifact: fingerprint the AST before it is discarded.
    collect_ast_kinds(parsed.value(), st.ast_kinds);

    db25::semantic::Analyzer analyzer(cat);
    analyzer.analyze(parsed.value());
    st.analyze_ok = !analyzer.has_errors();
    // Analyze-stage artifact: snapshot the diagnostics (the analyzer is a
    // run()-local and will not outlive this call).
    st.diags = analyzer.diagnostics();

    // Bind twice so the pre- and post-optimization plans are both live.
    db25::plan::Binder binder_a(analyzer, cat);
    db25::plan::BindResult bound = binder_a.bind(parsed.value());
    st.bind_ok = bound.ok;
    st.bind_error = bound.error;
    if (!bound.ok) return st;

    db25::plan::Binder binder_b(analyzer, cat);
    db25::plan::BindResult second = binder_b.bind(parsed.value());
    if (!second.ok) return st;  // should not happen if the first succeeded

    st.bound_owner = std::shared_ptr<LogicalNode>(std::move(bound.root));
    st.bound = st.bound_owner.get();
    st.bound_dump = db25::plan::dump_plan(st.bound);

    if (g_mutant == 1) {
        // M1: optimizer disabled - the "optimized" plan is the un-optimized bind.
        st.optimized_owner = std::shared_ptr<LogicalNode>(std::move(second.root));
    } else {
        st.optimized_owner = std::shared_ptr<LogicalNode>(db25::plan::optimize(std::move(second.root)));
        if (g_mutant == 5) drop_first_predicate(st.optimized_owner.get());  // M5
        if (g_mutant == 6 && st.optimized_owner && !st.optimized_owner->output.empty()) {
            // M6: corrupt the optimized plan's output SCHEMA - append a spurious
            // column at the root. This breaks the output-width IR invariant and
            // changes the root result schema, but leaves the produced ROWS
            // untouched (eval builds rows from exprs/children, not this vector),
            // so the differential oracle is unaffected. Caught by the no-crash /
            // well-formedness and schema-preservation property tests.
            st.optimized_owner->output.push_back(st.optimized_owner->output.back());
        }
    }
    st.optimized = st.optimized_owner.get();
    st.optimized_dump = db25::plan::dump_plan(st.optimized);
    st.optimize_ok = st.optimized != nullptr;
    return st;
}

// ===========================================================================
// Mutant registry.
// ===========================================================================
namespace {
struct MutantInfo { int id; const char* name; };
const MutantInfo kMutants[] = {
    {1, "M1 optimizer-disabled (bound returned as optimized)"},
    {2, "M2 Filter eval ignores predicate"},
    {3, "M3 SemiJoin eval keeps all left rows (as cross)"},
    {4, "M4 Aggregate COUNT/SUM off by one"},
    {5, "M5 optimize() drops first predicate"},
    {6, "M6 optimize() corrupts the root output schema (spurious column)"},
    {7, "M7 an isolated optimizer pass drops a predicate"},
    {8, "M8 Distinct eval does not de-duplicate"},
    {9, "M9 COUNT(expr) counts NULL argument rows"},
    {10, "M10 CASE eval ignores every WHEN (falls through to ELSE)"},
    {11, "M11 membership in an empty set is UNKNOWN, not FALSE"},
    {12, "M12 second optimize() pass changes the plan (non-idempotent)"},
    {13, "M13 Sort eval leaks hidden ORDER BY sort keys (no truncation to output width)"},
    {14, "M14 Sort eval reverses the row order (wrong ORDER BY direction)"},
    {15, "M15 COALESCE keeps only its first operand (USING/NATURAL merge loses the right copy)"},
    {16, "M16 outer-join eval drops null-extended rows (LEFT/RIGHT/FULL degrade to inner)"},
    {17, "M17 aggregate FILTER ignored (filtered aggregate degrades to unfiltered)"},
    {18, "M18 row comparison uses only the first component (ignores later columns)"},
};
}  // namespace

int mutant_count() { return static_cast<int>(sizeof(kMutants) / sizeof(kMutants[0])); }
const char* mutant_name(int id) {
    for (const auto& m : kMutants) if (m.id == id) return m.name;
    return "?";
}

// ===========================================================================
// Test framework.
// ===========================================================================
namespace {
struct TestCase {
    std::string name;
    std::function<void()> body;
};
std::vector<TestCase>& registry() {
    static std::vector<TestCase> r;
    return r;
}
bool g_current_failed = false;
bool g_verbose = true;
}  // namespace

void test(std::string name, std::function<void()> body) {
    registry().push_back({std::move(name), std::move(body)});
}

void check(bool cond, std::string_view what) {
    if (!cond) {
        g_current_failed = true;
        if (g_verbose) std::printf("      check FAILED: %.*s\n", static_cast<int>(what.size()), what.data());
    }
}

namespace {
bool run_one(const TestCase& t) {
    g_current_failed = false;
    t.body();
    return !g_current_failed;
}

// Optional test selection: DB25_TEST_FILTER is a substring; only tests whose
// name contains it are run. Empty / unset -> all tests. Lets the gate be
// demonstrated over a chosen subset (e.g. DB25_TEST_FILTER=smoke) while the full
// GLOB'd suite stays available.
std::vector<const TestCase*> selected_tests() {
    std::vector<const TestCase*> out;
    const char* f = std::getenv("DB25_TEST_FILTER");
    const std::string filter = f ? f : "";
    for (const auto& t : registry())
        if (filter.empty() || t.name.find(filter) != std::string::npos)
            out.push_back(&t);
    return out;
}
}  // namespace

int run_all() {
    if (const char* m = std::getenv("DB25_MUTANT")) g_mutant = std::atoi(m);
    if (g_mutant != 0)
        std::printf("== running suite under MUTANT %d: %s ==\n", g_mutant, mutant_name(g_mutant));
    else
        std::printf("== running suite (clean) ==\n");

    auto tests = selected_tests();
    int passed = 0, failed = 0;
    for (const TestCase* t : tests) {
        const bool ok = run_one(*t);
        std::printf("  [%s] %s\n", ok ? "PASS" : "FAIL", t->name.c_str());
        ok ? ++passed : ++failed;
    }
    std::printf("== %d passed, %d failed, %d total ==\n", passed, failed,
                static_cast<int>(tests.size()));
    return failed == 0 ? 0 : 1;
}

int run_gate() {
    g_verbose = false;  // suppress per-check noise during gate sweeps
    auto tests = selected_tests();
    const std::size_t nt = tests.size();
    std::printf("========================================================\n");
    std::printf(" DB25 FALSIFIABILITY GATE  (%zu tests, %d mutants)\n", nt, mutant_count());
    std::printf("========================================================\n");

    bool gate_ok = true;

    // (i) clean run must fully pass.
    g_mutant = 0;
    std::printf("\n[clean] all tests must PASS:\n");
    std::vector<bool> clean_pass(nt);
    int clean_failures = 0;
    for (std::size_t i = 0; i < nt; ++i) {
        clean_pass[i] = run_one(*tests[i]);
        if (!clean_pass[i]) { std::printf("  CLEAN FAIL: %s\n", tests[i]->name.c_str()); ++clean_failures; }
    }
    if (clean_failures == 0) std::printf("  OK: %zu/%zu passed clean\n", nt, nt);
    else { std::printf("  GATE FAIL: %d test(s) fail even clean\n", clean_failures); gate_ok = false; }

    // (ii) each mutant must be caught by >=1 test; track which tests ever failed.
    std::vector<bool> ever_killed(nt, false);
    std::printf("\n[mutants] each must be CAUGHT by >=1 test:\n");
    for (const auto& m : kMutants) {
        g_mutant = m.id;
        int caught = 0;
        for (std::size_t i = 0; i < nt; ++i) {
            const bool ok = run_one(*tests[i]);
            if (!ok) { ever_killed[i] = true; ++caught; }
        }
        if (caught > 0) std::printf("  OK: %s  (caught by %d test(s))\n", m.name, caught);
        else { std::printf("  GATE FAIL: SURVIVED -> %s\n", m.name); gate_ok = false; }
    }
    g_mutant = 0;

    // (iii) any test never killed by any mutant is NON-FALSIFIABLE (vacuous).
    std::printf("\n[falsifiability] every test must be killed by >=1 mutant:\n");
    int vacuous = 0;
    for (std::size_t i = 0; i < nt; ++i) {
        if (!ever_killed[i]) {
            std::printf("  NON-FALSIFIABLE (vacuous): %s\n", tests[i]->name.c_str());
            ++vacuous;
        }
    }
    if (vacuous == 0) std::printf("  OK: all %zu tests are falsifiable\n", nt);
    else { std::printf("  GATE FAIL: %d vacuous test(s)\n", vacuous); gate_ok = false; }

    std::printf("\n========================================================\n");
    std::printf(" GATE %s\n", gate_ok ? "PASSED" : "FAILED");
    std::printf("========================================================\n");
    return gate_ok ? 0 : 1;
}

// ===========================================================================
// Shared default catalog + data.
// ===========================================================================
// The shared default schema. Column lists and names match the contract the
// sibling suites documented in their headers:
//   users(id INT NOT NULL, name TEXT, age INT NULL, city TEXT NULL, manager_id INT NULL)
//   orders(id INT NOT NULL, user_id INT NULL, product_id INT NULL, amount INT NULL, status TEXT NULL)
//   products(id INT NOT NULL, name TEXT, price INT NULL, category TEXT NULL)
//   emp(id INT NOT NULL, name TEXT, mgr_id INT NULL, dept_id INT NULL, salary INT NULL)
const db25::semantic::InMemoryCatalog& catalog() {
    static db25::semantic::InMemoryCatalog cat = [] {
        db25::semantic::InMemoryCatalog c;
        using db25::semantic::DataType;
        c.add_table("users", {
            {"id", DataType::Integer, false},
            {"name", DataType::Text, true},
            {"age", DataType::Integer, true},
            {"city", DataType::Text, true},
            {"manager_id", DataType::Integer, true},
        });
        c.add_table("orders", {
            {"id", DataType::Integer, false},
            {"user_id", DataType::Integer, true},
            {"product_id", DataType::Integer, true},
            {"amount", DataType::Integer, true},
            {"status", DataType::Text, true},
        });
        c.add_table("products", {
            {"id", DataType::Integer, false},
            {"name", DataType::Text, true},
            {"price", DataType::Integer, true},
            {"category", DataType::Text, true},
        });
        c.add_table("emp", {
            {"id", DataType::Integer, false},
            {"name", DataType::Text, true},
            {"mgr_id", DataType::Integer, true},
            {"dept_id", DataType::Integer, true},
            {"salary", DataType::Integer, true},
        });
        return c;
    }();
    return cat;
}

// Populated tables (rows in catalog column order). Engineered to satisfy every
// enrichment the sibling suites request: NULLs in age/city/user_id/amount/
// price/status/mgr_id; duplicate cities and categories; a user with >=2 orders
// (id 1) and a user with none (id 5, also absent from orders.user_id); an order
// whose user_id (9) has no user (RIGHT/FULL null-extension); amounts straddling
// 10 and 100; prices straddling 50 and 100; unique orders.id (scalar-subquery
// keys); emp.mgr_id self-referencing emp.id with NULLs at the top.
const Data& data() {
    static Data d = [] {
        Data m;
        m["users"] = {
            //  id      name          age      city      manager_id
            {vi(1), vs("alice"), vi(30),  vs("NYC"), null()},
            {vi(2), vs("bob"),   null(),  vs("LA"),  vi(1)},   // NULL age
            {vi(3), vs("carol"), vi(5),   vs("NYC"), vi(1)},   // city NYC dup
            {vi(4), vs("dave"),  vi(40),  vs("LA"),  vi(2)},   // city LA dup
            {vi(5), vs("eve"),   vi(25),  null(),    vi(2)},   // NULL city; no orders
        };
        m["orders"] = {
            //  id        user_id   product_id amount    status
            {vi(100), vi(1),    vi(10), vi(50),  vs("paid")},
            {vi(101), vi(1),    vi(11), vi(5),   vs("paid")},    // user 1 has >=2 orders
            {vi(102), vi(2),    vi(12), vi(30),  vs("pending")},
            {vi(103), vi(3),    vi(13), null(),  vs("paid")},    // NULL amount
            {vi(104), null(),   vi(10), vi(8),   vs("paid")},    // NULL user_id (NOT IN trap)
            {vi(105), vi(4),    vi(12), vi(120), null()},        // NULL status; amount > 100
            {vi(106), vi(9),    vi(10), vi(40),  vs("shipped")}, // user_id 9: no such user
        };
        m["products"] = {
            //  id       name             price     category
            {vi(10), vs("widget"),    vi(9),    vs("tools")},
            {vi(11), vs("gadget"),    null(),   vs("tools")},    // NULL price; category dup
            {vi(12), vs("gizmo"),     vi(75),   vs("toys")},     // price > 50, <= 100
            {vi(13), vs("doohickey"), vi(150),  vs("toys")},     // price > 100
        };
        m["emp"] = {
            //  id      name       mgr_id    dept_id   salary
            {vi(1), vs("emp1"), null(),  vi(10), vi(100)},  // top of hierarchy
            {vi(2), vs("emp2"), vi(1),   vi(10), vi(200)},
            {vi(3), vs("emp3"), vi(1),   vi(20), vi(150)},
            {vi(4), vs("emp4"), vi(2),   vi(20), vi(120)},
            {vi(5), vs("emp5"), null(),  vi(10), vi(90)},   // another NULL mgr_id
        };
        return m;
    }();
    return d;
}

}  // namespace db25::harness

// ===========================================================================
// Entry point. db25_harness runs the suite; db25_gate is the same binary built
// with -DDB25_GATE_MAIN (default gate mode). --gate / --suite override.
// (Define DB25_HARNESS_NO_MAIN to reuse the harness TU as a library without its
// main - e.g. for a standalone diagnostic driver.)
// ===========================================================================
#ifndef DB25_HARNESS_NO_MAIN
int main(int argc, char** argv) {
    bool gate =
#ifdef DB25_GATE_MAIN
        true;
#else
        false;
#endif
    for (int i = 1; i < argc; ++i) {
        std::string a = argv[i];
        if (a == "--gate") gate = true;
        else if (a == "--suite") gate = false;
    }
    return gate ? db25::harness::run_gate() : db25::harness::run_all();
}
#endif  // DB25_HARNESS_NO_MAIN
