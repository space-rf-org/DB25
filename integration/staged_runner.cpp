// DB25 staged-artifact test runner (Phase A: plan layers).
//
// For each fixture, drive the frontend pipeline (parse -> analyze -> bind ->
// optimize) and pin the s-expr of each stage artifact against a committed
// golden. This is the END-TO-END mode + PER-STAGE comparison from the layer
// contracts: on a mismatch the runner reports the FIRST diverging stage, so a
// regression is localized to the module that produced it rather than only
// showing up as a wrong final result.
//
// Phase A pins all five stage artifacts: tokens, ast, resolved, logical,
// optimized. A fixture may pin any subset - a section that is absent is simply
// not compared, so the format stays forward-compatible.
//
// Fixture file format (one statement per file, sections in pipeline order):
//   -- sql
//   SELECT ...
//   -- tokens
//   (tokens (kw "SELECT" 0 6) ...)
//   -- ast
//   (SelectStmt ...)
//   -- resolved
//   (SelectStmt ... :type ... :null ...)
//   -- logical
//   (Project ...)
//   -- optimized
//   (Project ...)
//
// Usage:
//   staged_runner <dir>            verify every *.fixture in <dir> (default)
//   staged_runner --update <dir>   (re)generate the golden sections in place
#include "staged_sexpr.hpp"
#include "staged_sexpr_read.hpp"

#include "db25/parser/parser.hpp"
#include "db25/plan/binder.hpp"
#include "db25/plan/optimizer.hpp"
#include "db25/semantic/analyzer.hpp"
#include "db25/semantic/catalog.hpp"

#include "db25/physical/lowering.hpp"
#include "db25/physical/sexpr.hpp"
#include "db25/physical/spec.hpp"

#include <cstdio>
#include <filesystem>
#include <fstream>
#include <functional>
#include <map>
#include <sstream>
#include <string>
#include <vector>
#include <algorithm>

using namespace db25;
namespace fs = std::filesystem;

namespace {

// The fixed fixture catalog. Mirrors the layer-contracts worked example plus a
// grouping table, so fixtures resolve against a stable, documented schema.
semantic::InMemoryCatalog build_catalog() {
    semantic::InMemoryCatalog cat;
    cat.add_table("users", {
        semantic::ColumnInfo{"id", ast::DataType::Integer, /*nullable=*/false},
        semantic::ColumnInfo{"name", ast::DataType::VarChar, /*nullable=*/true},
    });
    cat.add_table("orders", {
        semantic::ColumnInfo{"id", ast::DataType::Integer, /*nullable=*/false},
        semantic::ColumnInfo{"user_id", ast::DataType::Integer, /*nullable=*/true},
        semantic::ColumnInfo{"total", ast::DataType::Double, /*nullable=*/true},
    });
    cat.add_table("emp", {
        semantic::ColumnInfo{"id", ast::DataType::Integer, /*nullable=*/false},
        semantic::ColumnInfo{"dept", ast::DataType::Text, /*nullable=*/true},
        semantic::ColumnInfo{"region", ast::DataType::Text, /*nullable=*/true},
        semantic::ColumnInfo{"salary", ast::DataType::Double, /*nullable=*/true},
    });
    return cat;
}

// A parsed fixture file: ordered section list (name -> body) preserving order.
struct Fixture {
    std::vector<std::pair<std::string, std::string>> sections;

    std::string* find(const std::string& name) {
        for (auto& [k, v] : sections) if (k == name) return &v;
        return nullptr;
    }
    const std::string* find(const std::string& name) const {
        for (const auto& [k, v] : sections) if (k == name) return &v;
        return nullptr;
    }
};

std::string trim(const std::string& s) {
    std::size_t a = s.find_first_not_of(" \t\r\n");
    if (a == std::string::npos) return "";
    std::size_t b = s.find_last_not_of(" \t\r\n");
    return s.substr(a, b - a + 1);
}

Fixture parse_fixture(const std::string& text) {
    Fixture f;
    std::istringstream in(text);
    std::string line;
    std::string cur_name;
    std::string cur_body;
    auto flush = [&]() {
        if (!cur_name.empty()) f.sections.emplace_back(cur_name, trim(cur_body));
        cur_body.clear();
    };
    while (std::getline(in, line)) {
        const std::string t = trim(line);
        if (t.rfind("-- ", 0) == 0) {           // section header "-- <name>"
            flush();
            cur_name = trim(t.substr(3));
        } else if (!cur_name.empty()) {
            cur_body += line;
            cur_body.push_back('\n');
        }
    }
    flush();
    return f;
}

std::string serialize_fixture(const Fixture& f) {
    std::string out;
    for (const auto& [name, body] : f.sections) {
        out += "-- " + name + "\n";
        out += trim(body);
        out += "\n\n";
    }
    return out;
}

// Produce the plan-stage s-expr artifacts for one SQL statement. Binds twice so
// the pre- and post-optimization trees are independent (optimize() consumes the
// plan it is given).
struct StageArtifacts {
    std::string tokens;
    std::string ast;
    std::string resolved;
    std::string logical;
    std::string optimized;
    std::string physical;
};

#ifndef DB25_PHYSICAL_SPEC_DIR
#define DB25_PHYSICAL_SPEC_DIR "."
#endif

// The shipped physical spec, loaded once. Lowering goes through it rather than
// the built-in single-candidate fallback, so the physical goldens pin what the
// REAL configuration produces - a spec that stopped loading, or stopped
// conforming, must fail loudly here rather than silently reverting the harness to
// a planner nobody runs.
const physical::PhysicalSpec& shipped_spec() {
    static const physical::PhysicalSpec spec = [] {
        std::string error;
        auto loaded = physical::load_spec(
            std::string(DB25_PHYSICAL_SPEC_DIR) + "/physical.spec.sexpr", error);
        if (!loaded) {
            std::printf("staged_runner: cannot load physical spec: %s\n", error.c_str());
            std::exit(2);
        }
        return *loaded;
    }();
    return spec;
}

StageArtifacts run_stages(const semantic::InMemoryCatalog& cat, const std::string& sql) {
    // T1: the token stream (independent of parse success).
    const std::string tokens = staged::tokens_to_sexpr(sql);

    parser::Parser p;
    auto res = p.parse(sql);
    if (!res.has_value()) {
        return {tokens,          "(parse-error)", "(parse-error)",
                "(parse-error)", "(parse-error)", "(parse-error)"};
    }

    // T2: untyped AST, straight off the parser.
    const std::string ast = staged::ast_to_sexpr(res.value());

    // T3: the SAME tree, annotated in place by the analyzer.
    semantic::Analyzer analyzer(cat);
    analyzer.analyze(res.value());
    const std::string resolved = staged::resolved_ast_to_sexpr(res.value(), analyzer);

    plan::Binder binder_a(analyzer, cat);
    plan::BindResult bound = binder_a.bind(res.value());
    // A bind failure is a first-class, pinned outcome (e.g. a construct the
    // analyzer accepts but the binder does not yet lower): record the reason so
    // the golden documents exactly where in the pipeline the statement stops.
    const std::string logical =
        bound.ok ? staged::plan_to_sexpr(bound.root.get())
                 : "(bind-error \"" + bound.error + "\")";

    plan::Binder binder_b(analyzer, cat);
    plan::BindResult bound2 = binder_b.bind(res.value());
    std::string optimized;
    std::string physical;
    if (bound2.ok) {
        plan::LogicalNodePtr opt = plan::optimize(std::move(bound2.root));
        optimized = staged::plan_to_sexpr(opt.get());
        // T6: lower the optimized plan to a physical plan. Increment 0 lowers
        // scan / filter / project / one join; a statement using anything else is
        // a first-class PINNED outcome (`(lower-error ...)`), exactly as a bind
        // failure is pinned above - the golden then documents precisely how far
        // down the pipeline that statement currently gets.
        if (opt) {
            physical::LoweringContext pctx;
            pctx.spec = &shipped_spec();
            physical::LoweringResult lowered = physical::lower(*opt, pctx);
            physical = lowered.ok ? physical::physical_to_sexpr(*lowered.plan)
                                  : "(lower-error \"" + lowered.error + "\")";
        } else {
            physical = "(lower-error \"no optimized plan\")";
        }
    } else {
        optimized = "(bind-error \"" + bound2.error + "\")";
        physical = "(bind-error \"" + bound2.error + "\")";
    }
    return {trim(tokens),     trim(ast),       trim(resolved),
            trim(logical),    trim(optimized), trim(physical)};
}

// --- Falsifiability gate (plan layers) --------------------------------------
//
// A structural golden is only worth committing if a corruption of the artifact
// it pins would actually change it - otherwise the check is vacuous. The gate
// applies a small catalog of plan mutations (mirroring the harness's own plan
// mutants - a dropped predicate, a corrupted output schema) to the produced
// plan and asserts the golden no longer matches. A real-plan golden that no
// applicable mutation can budge is vacuous and fails the gate.

// M-drop-predicate: null the first predicate in the tree (cf. harness M5/M7).
bool drop_first_predicate(plan::LogicalNode* n) {
    if (n == nullptr) return false;
    if (n->predicate) { n->predicate.reset(); return true; }
    for (auto& c : n->children) {
        if (drop_first_predicate(c.get())) return true;
    }
    return false;
}

// M-corrupt-schema: append a spurious column to the root output (cf. harness M6).
void corrupt_root_schema(plan::LogicalNode* n) {
    if (n == nullptr) return;
    plan::ColumnSchema c;
    c.name = "__mutant__";
    c.type = ast::DataType::Integer;
    c.nullable = true;
    n->output.push_back(std::move(c));
}

struct GateResult {
    bool gateable = false;  // the golden pins a real plan (not a bind/parse error)
    bool stale = false;     // produced plan no longer matches the committed golden
    int applied = 0;        // mutations that were applicable to this plan
    int caught = 0;         // applicable mutations that changed the s-expr
};

bool is_real_plan(const std::string& golden) {
    return !golden.empty() && golden.front() == '(' &&
           golden.rfind("(bind-error", 0) != 0 && golden.rfind("(parse-error", 0) != 0 &&
           golden.rfind("(lower-error", 0) != 0;
}

// `make` rebinds a FRESH owned plan each call (mutations are destructive).
GateResult gate_plan(const std::string& golden,
                     const std::function<plan::LogicalNodePtr()>& make) {
    GateResult r;
    if (!is_real_plan(golden)) return r;
    r.gateable = true;

    if (auto base = make(); base && trim(staged::plan_to_sexpr(base.get())) != golden) {
        r.stale = true;  // fixture is out of date; regenerate with --update
    }
    if (auto p = make(); p && drop_first_predicate(p.get())) {
        ++r.applied;
        if (trim(staged::plan_to_sexpr(p.get())) != golden) ++r.caught;
    }
    if (auto p = make(); p) {
        corrupt_root_schema(p.get());
        ++r.applied;
        if (trim(staged::plan_to_sexpr(p.get())) != golden) ++r.caught;
    }
    return r;
}

// The PHYSICAL golden is gated by the same logical mutations: the physical plan
// is lowered FROM the logical one, so a dropped predicate or a corrupted output
// schema must show up in the lowered plan too. A physical golden no logical
// mutation can budge is vacuous, exactly as for the plan goldens above.
GateResult gate_physical(const std::string& golden,
                         const std::function<plan::LogicalNodePtr()>& make) {
    GateResult r;
    if (!is_real_plan(golden)) return r;  // (lower-error ...) is not gateable
    r.gateable = true;

    const auto render = [](plan::LogicalNode* p) -> std::string {
        if (p == nullptr) return {};
        physical::LoweringContext pctx;
        pctx.spec = &shipped_spec();
        physical::LoweringResult lowered = physical::lower(*p, pctx);
        return lowered.ok ? trim(physical::physical_to_sexpr(*lowered.plan)) : std::string{};
    };

    if (auto base = make(); base && render(base.get()) != golden) {
        r.stale = true;  // fixture is out of date; regenerate with --update
    }
    if (auto p = make(); p && drop_first_predicate(p.get())) {
        ++r.applied;
        if (render(p.get()) != golden) ++r.caught;
    }
    if (auto p = make(); p) {
        corrupt_root_schema(p.get());
        ++r.applied;
        if (render(p.get()) != golden) ++r.caught;
    }
    return r;
}

}  // namespace

// ---------------------------------------------------------------------------
// --fields: the LOGICAL WRITER's distinguishability sweep.
//
// A golden is a test only to the extent that two different plans render
// differently, and --roundtrip does NOT establish that. It proves
// write(read(golden)) == golden, which is losslessness "w.r.t. the rendered
// fields" - a field the writer never renders is outside the guarantee, and a
// reader that also drops it is a perfectly consistent fixed point.
//
// That hole has now produced two defects. The whole DML payload rendered as a
// bare `(Update :out ())`, so two different UPDATEs shared a golden. And
// SemiJoin / AntiJoin fell through to `default:` and rendered no match
// condition, so
//   EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id)
// and
//   EXISTS (SELECT 1 FROM orders WHERE orders.user_id > users.id)
// produced BYTE-IDENTICAL logical and optimized goldens, while their physical
// plans are a hash semi join on a key and a nested loop with a residual.
//
// So: build a node per logical operator carrying a value in every payload field
// it uses, render it, change ONE field, and require the rendering to change.
// The operator list is taken from logical_op_to_string itself - the same
// authority the writer switches on - so an operator added to the IR is swept
// here without anyone remembering to add it.
namespace fieldsweep {

// Owned expressions the built nodes borrow.
struct Fixtures {
    plan::ExprPtr col0, col1, lit1, lit2, agg, win;
};
Fixtures& fx() {
    static Fixtures f = [] {
        Fixtures v;
        const auto col = [](std::uint32_t i) {
            auto e = std::make_unique<plan::Expr>(plan::ExprKind::ColumnRef);
            e->input_index = i;
            e->type = ast::DataType::Integer;
            return e;
        };
        const auto lit = [](std::int64_t n) {
            auto e = std::make_unique<plan::Expr>(plan::ExprKind::Literal);
            e->type = ast::DataType::Integer;
            e->value.value = n;
            return e;
        };
        v.col0 = col(0);
        v.col1 = col(1);
        v.lit1 = lit(1);
        v.lit2 = lit(2);
        v.agg = std::make_unique<plan::Expr>(plan::ExprKind::Aggregate);
        v.agg->func_name = "SUM";
        v.win = std::make_unique<plan::Expr>(plan::ExprKind::WindowFunction);
        v.win->func_name = "RANK";
        return v;
    }();
    return f;
}

plan::ExprPtr col(std::uint32_t i) {
    auto e = std::make_unique<plan::Expr>(plan::ExprKind::ColumnRef);
    e->input_index = i;
    e->type = ast::DataType::Integer;
    return e;
}
plan::ExprPtr lit(std::int64_t n) {
    auto e = std::make_unique<plan::Expr>(plan::ExprKind::Literal);
    e->type = ast::DataType::Integer;
    e->value.value = n;
    return e;
}
plan::ExprPtr agg_call(const char* name) {
    auto e = std::make_unique<plan::Expr>(plan::ExprKind::Aggregate);
    e->func_name = name;
    return e;
}
plan::ExprPtr win_call(const char* name) {
    auto e = std::make_unique<plan::Expr>(plan::ExprKind::WindowFunction);
    e->func_name = name;
    return e;
}

// A node with a representative value in every payload field its operator uses.
plan::LogicalNodePtr base(plan::LogicalOp op) {
    auto n = std::make_unique<plan::LogicalNode>(op);
    n->output = {{"a", ast::DataType::Integer, false}, {"b", ast::DataType::Integer, true}};
    n->table_name = "t";
    n->alias = "x";
    n->predicate = col(0);
    n->exprs.push_back(col(0));
    n->join_type = ast::JoinType::Inner;
    n->group_keys.push_back(col(0));
    n->aggregates.push_back(agg_call("SUM"));
    n->grouping_sets = {{0}};
    n->window_functions.push_back(win_call("RANK"));
    n->sort_keys.push_back(plan::SortKeyIR{col(0), false, false, false});
    n->value_rows.push_back({});
    n->value_rows.back().push_back(lit(1));
    n->has_limit = true;
    n->limit = 10;
    n->has_offset = true;
    n->offset = 5;
    n->set_op = ast::SetOp::Union;
    n->target_columns = {"id"};
    n->assignments.push_back(plan::Assignment{2, lit(1)});
    n->conflict_action = plan::ConflictAction::DoUpdate;
    n->conflict_columns = {"id"};
    return n;
}

struct FieldCase {
    plan::LogicalOp op;
    const char* field;
    void (*mutate)(plan::LogicalNode&);
};

void m_table(plan::LogicalNode& n) { n.table_name = "other"; }
void m_alias(plan::LogicalNode& n) { n.alias = "y"; }
void m_output(plan::LogicalNode& n) { n.output.pop_back(); }
void m_pred(plan::LogicalNode& n) { n.predicate = col(1); }
void m_exprs(plan::LogicalNode& n) { n.exprs.clear(); n.exprs.push_back(col(1)); }
void m_join_type(plan::LogicalNode& n) { n.join_type = ast::JoinType::Left; }
void m_group_keys(plan::LogicalNode& n) { n.group_keys.clear(); n.group_keys.push_back(col(1)); }
void m_aggs(plan::LogicalNode& n) { n.aggregates.clear(); n.aggregates.push_back(agg_call("MIN")); }
void m_gsets(plan::LogicalNode& n) { n.grouping_sets = {{0}, {}}; }
void m_windows(plan::LogicalNode& n) {
    n.window_functions.clear();
    n.window_functions.push_back(win_call("DENSE_RANK"));
}
void m_sort_col(plan::LogicalNode& n) {
    n.sort_keys.clear();
    n.sort_keys.push_back(plan::SortKeyIR{col(1), false, false, false});
}
void m_sort_dir(plan::LogicalNode& n) {
    n.sort_keys.clear();
    n.sort_keys.push_back(plan::SortKeyIR{col(0), true, false, false});
}
void m_values(plan::LogicalNode& n) {
    n.value_rows.clear();
    n.value_rows.push_back({});
    n.value_rows.back().push_back(lit(2));
}
void m_limit(plan::LogicalNode& n) { n.limit = 11; }
void m_offset(plan::LogicalNode& n) { n.offset = 6; }
void m_setop(plan::LogicalNode& n) { n.set_op = ast::SetOp::Except; }
void m_target_cols(plan::LogicalNode& n) { n.target_columns = {"other"}; }
void m_assignments(plan::LogicalNode& n) {
    n.assignments.clear();
    n.assignments.push_back(plan::Assignment{2, lit(2)});
}
void m_conflict_action(plan::LogicalNode& n) { n.conflict_action = plan::ConflictAction::DoNothing; }
void m_conflict_cols(plan::LogicalNode& n) { n.conflict_columns = {"other"}; }

const std::vector<FieldCase>& cases() {
    using LO = plan::LogicalOp;
    static const std::vector<FieldCase> v{
        {LO::Scan, "table_name", m_table},
        {LO::Scan, "alias", m_alias},
        {LO::Scan, "output", m_output},
        {LO::Filter, "predicate", m_pred},
        {LO::Project, "exprs", m_exprs},
        {LO::Join, "join_type", m_join_type},
        {LO::Join, "predicate", m_pred},
        // The two that rendered nothing at all until this sweep was written.
        {LO::SemiJoin, "predicate", m_pred},
        {LO::AntiJoin, "predicate", m_pred},
        {LO::Aggregate, "group_keys", m_group_keys},
        {LO::Aggregate, "aggregates", m_aggs},
        {LO::Aggregate, "grouping_sets", m_gsets},
        {LO::Window, "window_functions", m_windows},
        // DISTINCT carries no payload - it is over every output column - so what
        // distinguishes two of its plans is the schema and the child. Asserting
        // the schema is rendered is the honest statement of that, and a writer
        // that dropped `:out` fails it.
        {LO::Distinct, "output", m_output},
        {LO::Sort, "sort_keys.expr", m_sort_col},
        {LO::Sort, "sort_keys.direction", m_sort_dir},
        {LO::Limit, "limit", m_limit},
        {LO::Limit, "offset", m_offset},
        {LO::SetOp, "set_op", m_setop},
        {LO::Values, "value_rows", m_values},
        {LO::Insert, "table_name", m_table},
        {LO::Insert, "target_columns", m_target_cols},
        {LO::Insert, "conflict_action", m_conflict_action},
        {LO::Insert, "conflict_columns", m_conflict_cols},
        {LO::Insert, "assignments", m_assignments},
        {LO::Update, "table_name", m_table},
        {LO::Update, "assignments", m_assignments},
        {LO::Delete, "table_name", m_table},
        {LO::Returning, "exprs", m_exprs},
        {LO::RecursiveCTE, "table_name", m_table},
        {LO::RecursiveCTE, "set_op", m_setop},
        {LO::WorkingTableScan, "table_name", m_table},
        {LO::CreateTableAs, "table_name", m_table},
    };
    return v;
}

}  // namespace fieldsweep

// Every logical operator the IR names, taken from its own string table - the
// same authority the writer switches on - so a new operator is swept here
// without anyone remembering to add it.
static std::vector<plan::LogicalOp> all_logical_ops() {
    std::vector<plan::LogicalOp> ops;
    for (int i = 0; i < 256; ++i) {
        const auto op = static_cast<plan::LogicalOp>(i);
        if (std::string(plan::logical_op_to_string(op)) != "?") ops.push_back(op);
    }
    return ops;
}

static int run_field_sweep() {
    long checked = 0, invisible = 0, uncovered = 0;
    for (const fieldsweep::FieldCase& c : fieldsweep::cases()) {
        auto before = fieldsweep::base(c.op);
        auto after = fieldsweep::base(c.op);
        c.mutate(*after);
        const std::string sb = staged::plan_to_sexpr(before.get());
        const std::string sa = staged::plan_to_sexpr(after.get());
        ++checked;
        if (sb == sa) {
            ++invisible;
            std::printf("  INVISIBLE %s.%s does not reach the rendered plan:\n    %s\n",
                        plan::logical_op_to_string(c.op), c.field, sb.c_str());
        }
    }
    for (const plan::LogicalOp op : all_logical_ops()) {
        bool covered = false;
        for (const fieldsweep::FieldCase& c : fieldsweep::cases()) {
            covered = covered || (c.op == op);
        }
        if (!covered) {
            ++uncovered;
            std::printf("  UNCOVERED %s has no field case - what distinguishes two "
                        "of its plans?\n", plan::logical_op_to_string(op));
        }
    }
    std::printf("staged_runner --fields: %ld field(s) checked, %ld invisible, "
                "%ld operator(s) uncovered\n", checked, invisible, uncovered);
    return (invisible == 0 && uncovered == 0) ? 0 : 1;
}

int main(int argc, char** argv) {
    bool update = false;
    bool gate = false;
    bool roundtrip = false;
    bool inject = false;
    bool fields = false;
    std::string dir;
    for (int i = 1; i < argc; ++i) {
        const std::string a = argv[i];
        if (a == "--update") update = true;
        else if (a == "--gate") gate = true;
        else if (a == "--roundtrip") roundtrip = true;
        else if (a == "--inject") inject = true;
        else if (a == "--fields") fields = true;
        else dir = a;
    }
    // --fields needs no fixture directory: it renders nodes it builds itself.
    if (fields) return run_field_sweep();
    if (dir.empty()) {
        std::printf("staged_runner: usage: staged_runner "
                    "[--update|--gate|--roundtrip|--inject|--fields] <fixture-dir>\n");
        return 2;
    }

    const semantic::InMemoryCatalog cat = build_catalog();

    std::vector<fs::path> files;
    for (const auto& e : fs::directory_iterator(dir)) {
        if (e.is_regular_file() && e.path().extension() == ".fixture") files.push_back(e.path());
    }
    std::sort(files.begin(), files.end());

    // --gate: falsifiability check. Every real-plan golden must be caught
    // (changed) by at least one plan mutation; a golden nothing can budge is
    // vacuous. Also flags stale goldens (produced plan != committed golden).
    if (gate) {
        long gated = 0, vacuous = 0, stale = 0;
        for (const auto& path : files) {
            std::ifstream is(path);
            std::stringstream ss;
            ss << is.rdbuf();
            Fixture f = parse_fixture(ss.str());
            const std::string* sql = f.find("sql");
            if (sql == nullptr) continue;

            parser::Parser p;
            auto res = p.parse(*sql);
            if (!res.has_value()) continue;  // no plan to gate
            semantic::Analyzer an(cat);
            an.analyze(res.value());
            const auto bind_once = [&]() -> plan::LogicalNodePtr {
                plan::Binder b(an, cat);
                plan::BindResult br = b.bind(res.value());
                return br.ok ? std::move(br.root) : plan::LogicalNodePtr{};
            };
            const auto opt_once = [&]() -> plan::LogicalNodePtr {
                plan::LogicalNodePtr r = bind_once();
                return r ? plan::optimize(std::move(r)) : plan::LogicalNodePtr{};
            };

            const std::string name = path.filename().string();
            const std::pair<const char*, GateResult> checks[] = {
                {"logical", gate_plan(f.find("logical") ? *f.find("logical") : "", bind_once)},
                {"optimized", gate_plan(f.find("optimized") ? *f.find("optimized") : "", opt_once)},
                {"physical", gate_physical(f.find("physical") ? *f.find("physical") : "", opt_once)},
            };
            for (const auto& [label, gr] : checks) {
                if (!gr.gateable) continue;
                ++gated;
                if (gr.stale) {
                    ++stale;
                    std::printf("  STALE %s [%s]: produced plan != golden (run --update)\n",
                                name.c_str(), label);
                }
                if (gr.caught == 0) {
                    ++vacuous;
                    std::printf("  VACUOUS %s [%s]: no plan mutation changes this golden\n",
                                name.c_str(), label);
                } else {
                    std::printf("  ok %s [%s]: caught by %d/%d mutation(s)\n",
                                name.c_str(), label, gr.caught, gr.applied);
                }
            }
        }
        std::printf("staged_runner --gate: %ld plan goldens, %ld vacuous, %ld stale\n",
                    gated, vacuous, stale);
        return (vacuous == 0 && stale == 0) ? 0 : 1;
    }

    // --roundtrip: Phase B losslessness check. Read each real-plan golden back
    // into a LogicalNode and re-serialize; write(read(golden)) must equal the
    // golden, proving the plan s-expr is a lossless encoding of the rendered
    // fields (and exercising the reader that per-module injection builds on).
    if (roundtrip) {
        long checked = 0, notlossless = 0, reader_errs = 0, deferred = 0;
        for (const auto& path : files) {
            std::ifstream is(path);
            std::stringstream ss;
            ss << is.rdbuf();
            Fixture f = parse_fixture(ss.str());
            const std::string name = path.filename().string();
            // A fixture whose plan uses a construct the Phase-B s-expr reader does
            // not cover yet carries a `-- phaseb` marker; its round-trip is a
            // KNOWN, enumerated deferral, not a failure. (Extending the reader to
            // these constructs removes the marker - see the gap register.)
            if (f.find("phaseb")) { ++deferred; continue; }
            for (const char* label : {"logical", "optimized"}) {
                const std::string* golden = f.find(label);
                if (golden == nullptr || !is_real_plan(*golden)) continue;
                std::string err;
                plan::LogicalNodePtr p = staged::plan_from_sexpr(*golden, err);
                if (!p) {
                    ++reader_errs;
                    std::printf("  READER-ERROR %s [%s]: %s\n", name.c_str(), label, err.c_str());
                    continue;
                }
                ++checked;
                const std::string rt = trim(staged::plan_to_sexpr(p.get()));
                if (rt != *golden) {
                    ++notlossless;
                    std::printf("  NOT-LOSSLESS %s [%s]:\n    --- golden ---\n%s\n    --- read->write ---\n%s\n",
                                name.c_str(), label, golden->c_str(), rt.c_str());
                }
            }
        }
        std::printf("staged_runner --roundtrip: %ld plan goldens, %ld not-lossless, %ld reader-errors, %ld deferred (phaseb)\n",
                    checked, notlossless, reader_errs, deferred);
        return (notlossless == 0 && reader_errs == 0) ? 0 : 1;
    }

    // --inject: per-module optimizer isolation. Read the LOGICAL golden back
    // into a plan (no binder involved), optimize it, and pin the result against
    // the OPTIMIZED golden - testing the optimizer alone on a committed input.
    if (inject) {
        long checked = 0, mism = 0, errs = 0, deferred = 0;
        for (const auto& path : files) {
            std::ifstream is(path);
            std::stringstream ss;
            ss << is.rdbuf();
            Fixture f = parse_fixture(ss.str());
            const std::string name = path.filename().string();
            // See the roundtrip note: a `-- phaseb` fixture is a known Phase-B
            // reader deferral, skipped and counted rather than failed.
            if (f.find("phaseb")) { ++deferred; continue; }
            const std::string* log = f.find("logical");
            const std::string* opt = f.find("optimized");
            if (log == nullptr || opt == nullptr || !is_real_plan(*log) || !is_real_plan(*opt)) {
                continue;
            }
            std::string err;
            plan::LogicalNodePtr p = staged::plan_from_sexpr(*log, err);
            if (!p) { ++errs; std::printf("  READER-ERROR %s: %s\n", name.c_str(), err.c_str()); continue; }
            ++checked;
            plan::LogicalNodePtr optimized = plan::optimize(std::move(p));
            const std::string got = trim(staged::plan_to_sexpr(optimized.get()));
            if (got != *opt) {
                ++mism;
                std::printf("  INJECT-MISMATCH %s:\n    --- optimized golden ---\n%s\n    --- optimize(read(logical)) ---\n%s\n",
                            name.c_str(), opt->c_str(), got.c_str());
            } else {
                std::printf("  ok %s: optimize(read(logical)) == optimized golden\n", name.c_str());
            }
        }
        std::printf("staged_runner --inject: %ld goldens, %ld mismatches, %ld reader-errors, %ld deferred (phaseb)\n",
                    checked, mism, errs, deferred);
        return (mism == 0 && errs == 0) ? 0 : 1;
    }

    long total = 0, mismatches = 0, updated = 0;
    // The stages compared, in pipeline order - so a mismatch reports the FIRST
    // stage that diverged.
    const std::vector<std::string> ordered_stages = {"tokens",  "ast",       "resolved",
                                                     "logical", "optimized", "physical"};

    for (const auto& path : files) {
        std::ifstream is(path);
        std::stringstream ss;
        ss << is.rdbuf();
        Fixture f = parse_fixture(ss.str());

        const std::string* sql = f.find("sql");
        if (sql == nullptr) {
            std::printf("  SKIP %s: no -- sql section\n", path.filename().string().c_str());
            continue;
        }
        ++total;
        const StageArtifacts got = run_stages(cat, *sql);
        std::map<std::string, std::string> produced = {
            {"tokens", got.tokens},       {"ast", got.ast},
            {"resolved", got.resolved},   {"logical", got.logical},
            {"optimized", got.optimized}, {"physical", got.physical}};

        if (update) {
            for (const auto& stage : ordered_stages) {
                if (std::string* sec = f.find(stage)) *sec = produced[stage];
                else f.sections.emplace_back(stage, produced[stage]);
            }
            std::ofstream os(path);
            os << serialize_fixture(f);
            ++updated;
            continue;
        }

        bool first_div = true;
        for (const auto& stage : ordered_stages) {
            const std::string* want = f.find(stage);
            if (want == nullptr) continue;  // stage not pinned in this fixture
            if (trim(*want) != produced[stage]) {
                if (first_div) {
                    ++mismatches;
                    first_div = false;
                    std::printf("  MISMATCH %s: first diverging stage = '%s'\n",
                                path.filename().string().c_str(), stage.c_str());
                    std::printf("    --- want ---\n%s\n    --- got ---\n%s\n",
                                trim(*want).c_str(), produced[stage].c_str());
                }
            }
        }
    }

    if (update) {
        std::printf("staged_runner: updated %ld fixture(s)\n", updated);
        return 0;
    }
    std::printf("staged_runner: %ld fixtures, %ld mismatches\n", total, mismatches);
    return mismatches == 0 ? 0 : 1;
}
