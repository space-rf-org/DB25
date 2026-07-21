// DB25 property-based + fuzzing suite - properties asserted over thousands of
// deterministically generated SQL queries.
//
// Every property is a registered harness `test()` that loops seeds 0..N,
// generates a query with the seed-driven generator (sql_generator.hpp), drives
// it through the real parse -> analyze -> bind -> optimize stack, and asserts an
// invariant with harness `check()`. On ANY failure the check message carries
// the exact seed and SQL, so the failure replays deterministically:
//
//     DB25_PROP_SEED=<seed> <test-binary>
//
// regenerates the identical query (see README.md).
//
// DETERMINISM: the only entropy is the integer seed. No std::random_device, no
// clock. N is tunable via DB25_PROP_SEEDS (default 3000, fast).
//
// FALSIFIABILITY: these are genuine assertions, not tautologies. If the
// optimizer changed a result multiset, a schema, an IR width, or was not
// idempotent, the corresponding check() fails and prints the seed.
//
// Two drivers are used, both over the SAME harness catalog()/data():
//   * the harness `run()` (bound + optimized plans, with dumps) for the
//     no-crash, schema-preservation, and differential-equivalence properties;
//   * a local owned parse/analyze/bind pipeline for the properties that must
//     *call* optimize()/individual passes on a plan they own (idempotence,
//     per-pass safety) - `run()` returns borrowed pointers it still owns.

#include "harness.hpp"          // db25::harness contract (run/eval/bag_equal/test/check/catalog/data)
#include "sql_generator.hpp"    // db25::proptest::generate_query

#include "db25/plan/logical_plan.hpp"
#include "db25/plan/expr_ir.hpp"
#include "db25/plan/optimizer.hpp"
#include "db25/plan/binder.hpp"

#include "db25/semantic/analyzer.hpp"
#include "db25/semantic/catalog.hpp"
#include "db25/parser/parser.hpp"

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <memory>
#include <string>
#include <vector>

namespace {

using db25::plan::LogicalNode;
using db25::plan::LogicalNodePtr;
using db25::plan::LogicalOp;
using db25::plan::Schema;
using db25::plan::Expr;
using db25::plan::ExprKind;

// ---------------------------------------------------------------------------
// Tunables (deterministic; only affect how many seeds are swept).
// ---------------------------------------------------------------------------
int seed_count() {
    if (const char* e = std::getenv("DB25_PROP_SEEDS")) {
        const int n = std::atoi(e);
        if (n > 0) return n;
    }
    return 3000;  // a few thousand seeds, fast
}

// If DB25_PROP_SEED is set, the suite sweeps ONLY that seed (single-case
// replay). Returns -1 when not set.
long single_seed() {
    if (const char* e = std::getenv("DB25_PROP_SEED")) return std::atol(e);
    return -1;
}

// Invoke `body(seed)` for each seed in the active range.
template <typename F>
void for_each_seed(F&& body) {
    const long only = single_seed();
    if (only >= 0) {
        body(static_cast<std::uint64_t>(only));
        return;
    }
    const int n = seed_count();
    for (int s = 0; s < n; ++s) body(static_cast<std::uint64_t>(s));
}

std::string seed_tag(std::uint64_t seed, const std::string& sql) {
    return "seed=" + std::to_string(seed) + " sql=[" + sql + "]";
}

// ---------------------------------------------------------------------------
// Owned parse -> analyze -> bind pipeline. Keeps the Parser + Analyzer alive
// alongside the produced plan (the plan carries diagnostic back-pointers into
// the parser arena), so the whole struct must outlive any use of `root`.
// ---------------------------------------------------------------------------
struct BoundPlan {
    std::unique_ptr<db25::parser::Parser> parser;
    std::unique_ptr<db25::semantic::Analyzer> analyzer;
    LogicalNodePtr root;
    bool ok = false;
};

BoundPlan bind_owned(const db25::semantic::InMemoryCatalog& cat, std::string_view sql) {
    BoundPlan bp;
    bp.parser = std::make_unique<db25::parser::Parser>();
    auto parsed = bp.parser->parse(sql);
    if (!parsed || *parsed == nullptr) return bp;

    bp.analyzer = std::make_unique<db25::semantic::Analyzer>(cat);
    bp.analyzer->analyze(*parsed);
    if (bp.analyzer->has_errors()) return bp;  // only cleanly-analyzing queries

    db25::plan::Binder binder(*bp.analyzer, cat);
    auto br = binder.bind(*parsed);
    if (!br.ok || !br.root) return bp;

    bp.root = std::move(br.root);
    bp.ok = true;
    return bp;
}

// ---------------------------------------------------------------------------
// IR-invariant walker: verifies structural well-formedness of an optimized
// plan. Returns the first problem found (empty string == plan is well-formed).
// ---------------------------------------------------------------------------
struct IrChecker {
    std::string problem;

    void fail(std::string m) {
        if (problem.empty()) problem = std::move(m);
    }

    // The flat input frame a node's ColumnRefs index into: the concatenation of
    // all its children's output schemas (Join frame == child0 ++ child1).
    static Schema input_frame(const LogicalNode* n) {
        Schema s;
        for (const auto& c : n->children)
            for (const auto& col : c->output) s.push_back(col);
        return s;
    }

    template <typename F>
    static void each_root(const LogicalNode* n, F f) {
        if (n->predicate) f(n->predicate.get());
        for (const auto& e : n->exprs) if (e) f(e.get());
        for (const auto& e : n->group_keys) if (e) f(e.get());
        for (const auto& e : n->aggregates) if (e) f(e.get());
        for (const auto& e : n->window_functions) if (e) f(e.get());
        for (const auto& k : n->sort_keys) if (k.expr) f(k.expr.get());
        for (const auto& row : n->value_rows)
            for (const auto& e : row) if (e) f(e.get());
        for (const auto& a : n->assignments) if (a.value) f(a.value.get());
    }

    void walk_expr(const Expr* e, const Schema& input,
                   std::vector<const Schema*>& enclosing) {
        if (!e) return;
        switch (e->kind) {
            case ExprKind::ColumnRef:
                if (e->input_index >= input.size())
                    fail("ColumnRef.input_index " + std::to_string(e->input_index) +
                         " out of range for input width " + std::to_string(input.size()));
                break;
            case ExprKind::OuterRef: {
                const std::uint32_t d = e->outer_depth;
                if (d < 1 || d > enclosing.size()) {
                    fail("dangling OuterRef: outer_depth " + std::to_string(d) +
                         " with " + std::to_string(enclosing.size()) +
                         " enclosing scope(s)");
                } else {
                    const Schema* frame = enclosing[enclosing.size() - d];
                    if (e->input_index >= frame->size())
                        fail("OuterRef.input_index " + std::to_string(e->input_index) +
                             " out of range for enclosing width " +
                             std::to_string(frame->size()));
                }
                break;
            }
            case ExprKind::Subquery: {
                // The IN/probe operand(s) resolve against the current input.
                for (const auto& ch : e->children) walk_expr(ch.get(), input, enclosing);
                // The subquery body is a nested scope: push this operator's
                // input as the immediately-enclosing frame (OuterRef depth 1).
                enclosing.push_back(&input);
                walk_plan(e->sub_plan.get(), enclosing);
                enclosing.pop_back();
                return;  // children already walked
            }
            default:
                for (const auto& ch : e->children) walk_expr(ch.get(), input, enclosing);
                break;
        }
        // Window frame expressions share this operator's input frame.
        if (e->kind == ExprKind::WindowFunction) {
            for (const auto& p : e->window.partition_by)
                walk_expr(p.get(), input, enclosing);
            for (const auto& k : e->window.order_by)
                walk_expr(k.expr.get(), input, enclosing);
        }
    }

    void check_output_width(const LogicalNode* n) {
        const std::size_t c0 = n->children.size() > 0 ? n->children[0]->output.size() : 0;
        const std::size_t c1 = n->children.size() > 1 ? n->children[1]->output.size() : 0;
        const std::size_t out = n->output.size();
        auto want = [&](bool cond, const char* what) {
            if (!cond) fail(std::string("output-width invariant failed for ") +
                            db25::plan::logical_op_to_string(n->op) + ": " + what);
        };
        switch (n->op) {
            case LogicalOp::Filter:
            case LogicalOp::Sort:
            case LogicalOp::Limit:
            case LogicalOp::Distinct:
                want(out == c0, "output must equal child width");
                break;
            case LogicalOp::Project:
                want(out == n->exprs.size(), "output must equal expr count");
                break;
            case LogicalOp::Returning:
                if (!n->exprs.empty()) want(out == n->exprs.size(), "output vs expr count");
                break;
            case LogicalOp::Join:
                if (n->predicate)
                    want(out == c0 + c1, "ON-join output must equal left++right");
                else
                    want(out <= c0 + c1, "join output must not exceed left++right");
                break;
            case LogicalOp::SemiJoin:
            case LogicalOp::AntiJoin:
                want(out == c0, "semi/anti-join output must equal left width");
                break;
            case LogicalOp::Aggregate:
                want(out == n->group_keys.size() + n->aggregates.size(),
                     "aggregate output must equal group keys + aggregates");
                break;
            case LogicalOp::Window:
                want(out == c0 + n->window_functions.size(),
                     "window output must equal child + window functions");
                break;
            case LogicalOp::SetOp:
                want(out == c0 && (n->children.size() < 2 || out == c1),
                     "set-op branches must share width");
                break;
            case LogicalOp::Values:
                for (const auto& row : n->value_rows)
                    want(row.size() == out, "every VALUES row must match output width");
                break;
            default:
                break;  // Scan / DML: no strict width invariant checked here
        }
    }

    void walk_plan(const LogicalNode* n, std::vector<const Schema*>& enclosing) {
        if (!n) return;
        check_output_width(n);
        const Schema input = input_frame(n);
        each_root(n, [&](const Expr* e) { walk_expr(e, input, enclosing); });
        for (const auto& c : n->children) walk_plan(c.get(), enclosing);
    }
};

std::string ir_problem(const LogicalNode* root) {
    IrChecker ic;
    std::vector<const Schema*> enclosing;
    ic.walk_plan(root, enclosing);
    return ic.problem;
}

bool same_schema(const Schema& a, const Schema& b, std::string& why) {
    if (a.size() != b.size()) {
        why = "column count " + std::to_string(a.size()) + " != " + std::to_string(b.size());
        return false;
    }
    for (std::size_t i = 0; i < a.size(); ++i) {
        if (a[i].type != b[i].type) {
            why = "column " + std::to_string(i) + " type changed";
            return false;
        }
        if (a[i].nullable != b[i].nullable) {
            why = "column " + std::to_string(i) + " nullability changed";
            return false;
        }
    }
    return true;
}

// ===========================================================================
// Property 1: no-crash / well-formedness of the optimized plan.
// ===========================================================================
void prop_no_crash_wellformed() {
    const auto& cat = db25::harness::catalog();
    long bound = 0, checked = 0;
    for_each_seed([&](std::uint64_t seed) {
        const std::string sql = db25::proptest::generate_query(cat, seed);
        auto st = db25::harness::run(cat, sql);
        if (!st.parsed || !st.analyze_ok || !st.bind_ok || !st.optimize_ok) return;
        if (!st.optimized) return;
        ++bound;
        const std::string prob = ir_problem(st.optimized);
        db25::harness::check(prob.empty(), seed_tag(seed, sql) + " : IR invariant: " + prob);
        ++checked;
    });
    std::printf("[prop] no-crash/well-formedness: %ld optimized plans checked\n", checked);
}

// ===========================================================================
// Property 2: type / nullability preservation - optimize() never changes the
// root output schema (result shape).
// ===========================================================================
void prop_schema_preservation() {
    const auto& cat = db25::harness::catalog();
    long checked = 0;
    for_each_seed([&](std::uint64_t seed) {
        const std::string sql = db25::proptest::generate_query(cat, seed);
        auto st = db25::harness::run(cat, sql);
        if (!st.parsed || !st.analyze_ok || !st.bind_ok || !st.optimize_ok) return;
        if (!st.bound || !st.optimized) return;
        std::string why;
        const bool ok = same_schema(st.bound->output, st.optimized->output, why);
        db25::harness::check(ok, seed_tag(seed, sql) + " : root schema changed: " + why);
        ++checked;
    });
    std::printf("[prop] schema preservation: %ld plans checked\n", checked);
}

// ===========================================================================
// Property 3: optimizer idempotence - a second optimize pass changes nothing
// (structural equality via dump_plan).
// ===========================================================================
void prop_optimizer_idempotence() {
    const auto& cat = db25::harness::catalog();
    long checked = 0;
    for_each_seed([&](std::uint64_t seed) {
        const std::string sql = db25::proptest::generate_query(cat, seed);
        BoundPlan bp = bind_owned(cat, sql);
        if (!bp.ok) return;
        LogicalNodePtr once = db25::plan::optimize(std::move(bp.root));
        if (!once) return;
        const std::string dump_once = db25::plan::dump_plan(once.get());
        LogicalNodePtr twice = db25::plan::optimize(std::move(once));
        const std::string dump_twice = db25::plan::dump_plan(twice.get());
        db25::harness::check(dump_once == dump_twice,
                             seed_tag(seed, sql) + " : optimize() not idempotent");
        ++checked;
    });
    std::printf("[prop] optimizer idempotence: %ld plans checked\n", checked);
}

// ===========================================================================
// Property 4 (centerpiece): differential equivalence - the optimizer preserves
// the result multiset. eval() is the 3VL reference evaluator; where it supports
// both the bound and optimized plans, their bags must be equal.
// ===========================================================================
void prop_differential_equivalence() {
    const auto& cat = db25::harness::catalog();
    long applied = 0, skipped = 0;
    for_each_seed([&](std::uint64_t seed) {
        const std::string sql = db25::proptest::generate_query(cat, seed);
        auto st = db25::harness::run(cat, sql);
        if (!st.parsed || !st.analyze_ok || !st.bind_ok || !st.optimize_ok) return;
        if (!st.bound || !st.optimized) return;

        auto ev_bound = db25::harness::eval(st.bound, db25::harness::data());
        auto ev_opt = db25::harness::eval(st.optimized, db25::harness::data());
        if (!ev_bound.has_value() || !ev_opt.has_value()) {
            ++skipped;  // unsupported shape: a skip is NEVER a pass
            return;
        }
        db25::harness::check(db25::harness::bag_equal(*ev_bound, *ev_opt),
                             seed_tag(seed, sql) +
                                 " : optimizer changed the result multiset");
        ++applied;
    });
    std::printf("[prop] differential equivalence: oracle applied %ld, skipped %ld "
                "(%.1f%% covered)\n",
                applied, skipped,
                (applied + skipped) ? 100.0 * applied / (applied + skipped) : 0.0);
}

// ===========================================================================
// Property 5: individual-pass safety - each optimizer pass, applied in
// isolation, preserves eval-equivalence where checkable.
// ===========================================================================
enum class PassId { Fold, SimplifyBool, PushFilters, PruneColumns, Decorrelate };

const char* pass_name(PassId p) {
    switch (p) {
        case PassId::Fold: return "fold_constants";
        case PassId::SimplifyBool: return "simplify_booleans";
        case PassId::PushFilters: return "push_down_filters";
        case PassId::PruneColumns: return "prune_columns";
        case PassId::Decorrelate: return "decorrelate_exists";
    }
    return "?";
}

// M7: after the real pass runs, null out the first Filter/Join predicate, so the
// isolated pass appears to have silently dropped a condition. This makes the
// per-pass safety property falsifiable - a pass that changes the result multiset
// must be caught. (A nulled Join predicate becomes a cross product and a nulled
// Filter drops the row test, both of which the differential eval detects.)
bool drop_first_predicate_local(LogicalNode* n) {
    if (n == nullptr) return false;
    if ((n->op == LogicalOp::Filter || n->op == LogicalOp::Join ||
         n->op == LogicalOp::SemiJoin || n->op == LogicalOp::AntiJoin) &&
        n->predicate) {
        n->predicate.reset();
        return true;
    }
    for (auto& c : n->children)
        if (drop_first_predicate_local(c.get())) return true;
    return false;
}

void apply_pass(PassId p, LogicalNodePtr& root) {
    switch (p) {
        case PassId::Fold: db25::plan::fold_constants(root.get()); break;
        case PassId::SimplifyBool: db25::plan::simplify_booleans(root.get()); break;
        case PassId::PushFilters: db25::plan::push_down_filters(root); break;
        case PassId::PruneColumns: db25::plan::prune_columns(root); break;
        case PassId::Decorrelate: db25::plan::decorrelate_exists(root); break;
    }
    if (db25::harness::g_mutant == 7) drop_first_predicate_local(root.get());
}

void run_single_pass_property(PassId pass) {
    const auto& cat = db25::harness::catalog();
    long applied = 0, skipped = 0;
    for_each_seed([&](std::uint64_t seed) {
        const std::string sql = db25::proptest::generate_query(cat, seed);

        // Reference: eval the un-optimized bound plan.
        BoundPlan ref = bind_owned(cat, sql);
        if (!ref.ok) return;
        auto ev_ref = db25::harness::eval(ref.root.get(), db25::harness::data());
        if (!ev_ref.has_value()) { ++skipped; return; }

        // Apply just this pass on an independently-bound plan.
        BoundPlan tp = bind_owned(cat, sql);
        if (!tp.ok) { ++skipped; return; }
        apply_pass(pass, tp.root);
        auto ev_after = db25::harness::eval(tp.root.get(), db25::harness::data());
        if (!ev_after.has_value()) { ++skipped; return; }

        db25::harness::check(db25::harness::bag_equal(*ev_ref, *ev_after),
                             seed_tag(seed, sql) + " : pass '" + pass_name(pass) +
                                 "' changed the result multiset");
        ++applied;
    });
    std::printf("[prop] pass-safety[%s]: applied %ld, skipped %ld\n", pass_name(pass),
                applied, skipped);
}

// ---------------------------------------------------------------------------
// Registration.
// ---------------------------------------------------------------------------
void register_all() {
    db25::harness::test("property/no-crash-and-ir-well-formedness",
                        prop_no_crash_wellformed);
    db25::harness::test("property/type-nullability-preservation",
                        prop_schema_preservation);
    db25::harness::test("property/optimizer-idempotence", prop_optimizer_idempotence);
    db25::harness::test("property/differential-equivalence",
                        prop_differential_equivalence);
    db25::harness::test("property/pass-safety/fold_constants",
                        [] { run_single_pass_property(PassId::Fold); });
    db25::harness::test("property/pass-safety/simplify_booleans",
                        [] { run_single_pass_property(PassId::SimplifyBool); });
    db25::harness::test("property/pass-safety/push_down_filters",
                        [] { run_single_pass_property(PassId::PushFilters); });
    db25::harness::test("property/pass-safety/prune_columns",
                        [] { run_single_pass_property(PassId::PruneColumns); });
    db25::harness::test("property/pass-safety/decorrelate_exists",
                        [] { run_single_pass_property(PassId::Decorrelate); });
}

const bool _registered = (register_all(), true);

}  // namespace
