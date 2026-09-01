// stage_timer: one query through every DB25 frontend stage, with each stage's
// artifact dumped and its time measured in isolation. This is the tool behind
// the "bytes -> a plan" numbers, including the physical stage (T6).
//
// THE REFERENCE QUERY. bench/reference_query.sql is the query DB25 is measured
// against - the position paper's comparison query (CTE + aggregate + window +
// LATERAL + ORDER BY over a window output + LIMIT). It is read from that file at
// runtime so there is exactly one copy of it in the tree. It is deliberately a
// query the frontend does not yet fully serve: the physical planner's operator
// set has no Aggregate, Window or Limit, so T6 reports LOWER FAILED. That is the
// honest state of the project and the tool prints it rather than hiding it. When
// those operators land, this number starts moving on its own.
//
// The simple join query is kept alongside it as the physically-lowerable case,
// which is the only query from which a real T6 figure can be read today. It is
// labelled as such so nobody mistakes it for the headline.
//
// A lab tool, not a gate: it is built (so it cannot rot) but not run in ctest.
#include "db25/parser/parser.hpp"
#include "db25/plan/binder.hpp"
#include "db25/plan/logical_plan.hpp"
#include "db25/plan/optimizer.hpp"
#include "db25/semantic/analyzer.hpp"
#include "db25/semantic/catalog.hpp"

#include "db25/physical/lowering.hpp"
#include "db25/physical/spec.hpp"
#include "db25/physical/sexpr.hpp"

#include "staged_sexpr.hpp"

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>

using namespace db25;
using clk = std::chrono::steady_clock;
static double us(clk::duration d) {
    return std::chrono::duration<double, std::micro>(d).count();
}

// One catalog serving both queries: a/b for the simple join, emp/orders for the
// reference query. Column types match bench/reference_query.sql's declared
// schema exactly, so the DB25 measurement and a PostgreSQL run see the same one.
static semantic::InMemoryCatalog make_catalog() {
    semantic::InMemoryCatalog c;
    c.add_table("a", {semantic::ColumnInfo{"id", ast::DataType::Integer, false},
                      semantic::ColumnInfo{"x", ast::DataType::Integer, true}});
    c.add_table("b", {semantic::ColumnInfo{"id", ast::DataType::Integer, false},
                      semantic::ColumnInfo{"y", ast::DataType::VarChar, true}});
    c.add_table("emp", {semantic::ColumnInfo{"id", ast::DataType::Integer, false},
                        semantic::ColumnInfo{"name", ast::DataType::VarChar, true},
                        semantic::ColumnInfo{"dept_id", ast::DataType::Integer, true},
                        semantic::ColumnInfo{"salary", ast::DataType::Double, true}});
    c.add_table("orders", {semantic::ColumnInfo{"id", ast::DataType::Integer, false},
                           semantic::ColumnInfo{"user_id", ast::DataType::Integer, true},
                           semantic::ColumnInfo{"total", ast::DataType::Double, true}});
    return c;
}

static double median(std::vector<double>& v) {
    std::sort(v.begin(), v.end());
    return v.empty() ? 0.0 : v[v.size() / 2];
}

#ifndef DB25_PHYSICAL_SPEC_DIR
#define DB25_PHYSICAL_SPEC_DIR "."
#endif
#ifndef DB25_REFERENCE_QUERY
#define DB25_REFERENCE_QUERY "bench/reference_query.sql"
#endif

// T6 must time the planner the project actually ships, which means lowering
// through the SHIPPED SPEC. Timing a default LoweringContext instead measured the
// built-in fallback mapping - fewer candidates, no spec-driven implementation
// rules - so the headline planner number described a configuration nobody runs.
//
// Unit 1.5 fixed exactly this in the staged harness. The same defect survived
// here, and survived longer, because a timing tool has no golden to disagree
// with: a number that is quietly measuring the wrong thing still looks like a
// number. It was caught only when an optimization improved this tool by 3% and
// the planner's own benchmark by 16%, and the gap had to be explained.
static const db25::physical::PhysicalSpec& shipped_spec() {
    static const db25::physical::PhysicalSpec spec = [] {
        std::string error;
        auto loaded = db25::physical::load_spec(
            std::string(DB25_PHYSICAL_SPEC_DIR) + "/physical.spec.sexpr", error);
        if (!loaded) {
            std::printf("stage_timer: cannot load physical spec: %s\n", error.c_str());
            std::exit(2);
        }
        return *loaded;
    }();
    return spec;
}

static db25::physical::LoweringContext timing_context() {
    db25::physical::LoweringContext ctx;
    ctx.spec = &shipped_spec();
    return ctx;
}

// Read the reference query from its file, stripping `--` comment lines and
// collapsing the remainder onto one line. Reading it (rather than embedding a
// copy) is what keeps the measured query and the documented query the same
// query: an edit to the .sql file cannot silently fail to reach this tool.
static std::string load_reference_query() {
    std::ifstream in(DB25_REFERENCE_QUERY);
    if (!in) {
        std::printf("stage_timer: cannot open reference query %s\n", DB25_REFERENCE_QUERY);
        std::exit(2);
    }
    std::string sql, line;
    while (std::getline(in, line)) {
        if (line.compare(0, 2, "--") == 0) continue;
        const auto first = line.find_first_not_of(" \t\r");
        if (first == std::string::npos) continue;
        if (!sql.empty()) sql += ' ';
        sql.append(line, first, line.find_last_not_of(" \t\r") - first + 1);
    }
    if (!sql.empty() && sql.back() == ';') sql.pop_back();
    return sql;
}

struct Stages {
    double parse = 0, analyze = 0, bind = 0, optimize = 0, lower = 0;
    bool lowered = false;   // T6 produced a physical plan
    double total() const { return parse + analyze + bind + optimize + lower; }
};

// Dump every stage artifact for one cold run. A failure to lower is reported and
// returned, not fatal: the reference query is expected to fail T6 until the
// physical operator set grows, and the tool must still print T1-T5 for it.
static bool dump_artifacts(const std::string& sql, const semantic::InMemoryCatalog& cat,
                           const db25::physical::LoweringContext& pctx) {
    std::printf("SQL: %s\n\n", sql.c_str());
    std::printf("---- T1 TOKENS ----\n%s\n\n", staged::tokens_to_sexpr(sql).c_str());

    parser::Parser parser;
    auto pr = parser.parse(sql);
    if (!pr.has_value()) { std::printf("PARSE FAILED\n\n"); return false; }
    ast::ASTNode* root = pr.value();
    std::printf("---- T2 AST ----\n%s\n\n", staged::ast_to_sexpr(root).c_str());

    semantic::Analyzer analyzer(cat);
    analyzer.analyze(root);
    std::printf("---- T3 RESOLVED ----\n%s\n\n",
                staged::resolved_ast_to_sexpr(root, analyzer).c_str());

    plan::Binder binder(analyzer, cat);
    auto bound = binder.bind(root);
    if (!bound.ok) { std::printf("BIND FAILED: %s\n\n", bound.error.c_str()); return false; }
    std::printf("---- T4 LOGICAL ----\n%s\n\n", staged::plan_to_sexpr(bound.root.get()).c_str());

    auto optimized = plan::optimize(std::move(bound.root));
    std::printf("---- T5 OPTIMIZED ----\n%s\n\n", staged::plan_to_sexpr(optimized.get()).c_str());

    auto lowered = physical::lower(*optimized, pctx);
    if (!lowered.ok) {
        std::printf("---- T6 PHYSICAL ----\nLOWER FAILED: %s\n\n", lowered.error.c_str());
        return false;
    }
    std::printf("---- T6 PHYSICAL ----\n%s\n\n",
                physical::physical_to_sexpr(*lowered.plan).c_str());
    return true;
}

static Stages time_stages(const std::string& sql, const semantic::InMemoryCatalog& cat,
                          const db25::physical::LoweringContext& pctx, int N) {
    std::vector<double> par, ana, bin, opt, low;
    par.reserve(N); ana.reserve(N); bin.reserve(N); opt.reserve(N); low.reserve(N);
    bool lowered = true;

    for (int i = 0; i < N; ++i) {
        parser::Parser p;
        auto t0 = clk::now();
        auto r = p.parse(sql);
        auto t1 = clk::now();
        par.push_back(us(t1 - t0));
        ast::ASTNode* rt = r.value();

        semantic::Analyzer an(cat);
        auto t2 = clk::now();
        an.analyze(rt);
        auto t3 = clk::now();
        ana.push_back(us(t3 - t2));

        plan::Binder bd(an, cat);
        auto t4 = clk::now();
        auto bo = bd.bind(rt);
        auto t5 = clk::now();
        bin.push_back(us(t5 - t4));

        auto t6 = clk::now();
        auto op = plan::optimize(std::move(bo.root));
        auto t7 = clk::now();
        opt.push_back(us(t7 - t6));

        auto t8 = clk::now();
        auto lo = physical::lower(*op, pctx);
        auto t9 = clk::now();
        low.push_back(us(t9 - t8));
        lowered = lowered && lo.ok;
    }

    Stages s;
    s.parse = median(par); s.analyze = median(ana); s.bind = median(bin);
    s.optimize = median(opt); s.lower = median(low); s.lowered = lowered;
    return s;
}

// A T6 that failed measures how long the planner takes to give up, which is not
// a planning time and must never be added into a headline total. Print it as
// such, and exclude it from the total.
static void report(const char* label, const Stages& s) {
    std::printf("  %s\n", label);
    std::printf("    %-26s %10.2f\n", "T2 parse (incl tokenize)", s.parse);
    std::printf("    %-26s %10.2f\n", "T3 analyze", s.analyze);
    std::printf("    %-26s %10.2f\n", "T4 bind", s.bind);
    std::printf("    %-26s %10.2f\n", "T5 optimize", s.optimize);
    if (s.lowered) {
        std::printf("    %-26s %10.2f\n", "T6 physical (lower)", s.lower);
        std::printf("    %-26s %10s\n", "--------------------------", "----------");
        std::printf("    %-26s %10.2f\n", "OVERALL (parse->physical)", s.total());
    } else {
        std::printf("    %-26s %10s   (%.2f us to reject; not planning time)\n",
                    "T6 physical (lower)", "FAILED", s.lower);
        std::printf("    %-26s %10s\n", "--------------------------", "----------");
        std::printf("    %-26s %10.2f\n", "OVERALL (parse->logical)",
                    s.parse + s.analyze + s.bind + s.optimize);
    }
    std::printf("\n");
}

int main() {
    const std::string reference = load_reference_query();
    const std::string join_query = "SELECT a.x, b.y FROM a JOIN b ON a.id = b.id WHERE a.x > 10";
    const auto cat = make_catalog();
    const auto pctx = timing_context();

    std::printf("========== REFERENCE QUERY ARTIFACTS (single cold run) ==========\n\n");
    dump_artifacts(reference, cat, pctx);

    std::printf("========== SIMPLE JOIN ARTIFACTS (single cold run) ==========\n\n");
    dump_artifacts(join_query, cat, pctx);

    const int N = 2000;
    const Stages ref = time_stages(reference, cat, pctx, N);
    const Stages join = time_stages(join_query, cat, pctx, N);

    std::printf("========== TIMING  (median over %d runs, microseconds) ==========\n\n", N);
    report("REFERENCE QUERY  (bench/reference_query.sql - the headline number)", ref);
    report("SIMPLE JOIN      (the only query with a real T6 today)", join);
    if (!ref.lowered) {
        std::printf("  The reference query does not lower: the physical operator set is\n"
                    "  Scan/Filter/Project/HashJoin/MergeJoin/NestedLoopJoin/Sort/FormatConvert,\n"
                    "  with no Aggregate, Window or Limit. Until those land, the reference\n"
                    "  query's headline is a parse->logical number and T6 is unmeasured on it.\n\n");
    }
    return 0;
}
