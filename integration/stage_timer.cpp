// stage_timer: one query through every DB25 frontend stage, with each stage's
// artifact dumped and its time measured in isolation. This is the tool behind the
// "bytes -> a plan" numbers - now extended with the physical stage (T6).
//
// Increment 0's physical lowering handles scan / filter / project / one join, so
// this tool uses the simple join query it fully lowers. The reference HTAP query
// (CTE + window + LATERAL) still measures through T5 (logical) elsewhere; its
// physical lowering arrives as later increments add those operators. A lab tool,
// not a gate: it is built (so it cannot rot) but not run in ctest.
#include "db25/parser/parser.hpp"
#include "db25/plan/binder.hpp"
#include "db25/plan/logical_plan.hpp"
#include "db25/plan/optimizer.hpp"
#include "db25/semantic/analyzer.hpp"
#include "db25/semantic/catalog.hpp"

#include "db25/physical/lowering.hpp"
#include "db25/physical/sexpr.hpp"

#include "staged_sexpr.hpp"

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <string>
#include <vector>

using namespace db25;
using clk = std::chrono::steady_clock;
static double us(clk::duration d) {
    return std::chrono::duration<double, std::micro>(d).count();
}

static semantic::InMemoryCatalog make_catalog() {
    semantic::InMemoryCatalog c;
    c.add_table("a", {semantic::ColumnInfo{"id", ast::DataType::Integer, false},
                      semantic::ColumnInfo{"x", ast::DataType::Integer, true}});
    c.add_table("b", {semantic::ColumnInfo{"id", ast::DataType::Integer, false},
                      semantic::ColumnInfo{"y", ast::DataType::VarChar, true}});
    return c;
}

static double median(std::vector<double>& v) {
    std::sort(v.begin(), v.end());
    return v.empty() ? 0.0 : v[v.size() / 2];
}

int main() {
    const std::string sql = "SELECT a.x, b.y FROM a JOIN b ON a.id = b.id WHERE a.x > 10";
    const auto cat = make_catalog();

    std::printf("========== STAGE ARTIFACTS (single cold run) ==========\n\n");
    std::printf("SQL: %s\n\n", sql.c_str());
    std::printf("---- T1 TOKENS ----\n%s\n\n", staged::tokens_to_sexpr(sql).c_str());

    parser::Parser parser;
    auto pr = parser.parse(sql);
    if (!pr.has_value()) { std::printf("PARSE FAILED\n"); return 1; }
    ast::ASTNode* root = pr.value();
    std::printf("---- T2 AST ----\n%s\n\n", staged::ast_to_sexpr(root).c_str());

    semantic::Analyzer analyzer(cat);
    analyzer.analyze(root);
    std::printf("---- T3 RESOLVED ----\n%s\n\n",
                staged::resolved_ast_to_sexpr(root, analyzer).c_str());

    plan::Binder binder(analyzer, cat);
    auto bound = binder.bind(root);
    if (!bound.ok) { std::printf("BIND FAILED: %s\n", bound.error.c_str()); return 1; }
    std::printf("---- T4 LOGICAL ----\n%s\n\n", staged::plan_to_sexpr(bound.root.get()).c_str());

    auto optimized = plan::optimize(std::move(bound.root));
    std::printf("---- T5 OPTIMIZED ----\n%s\n\n", staged::plan_to_sexpr(optimized.get()).c_str());

    auto lowered = physical::lower(*optimized);
    if (!lowered.ok) { std::printf("LOWER FAILED: %s\n", lowered.error.c_str()); return 1; }
    std::printf("---- T6 PHYSICAL ----\n%s\n\n",
                physical::physical_to_sexpr(*lowered.plan).c_str());

    const int N = 2000;
    std::vector<double> par, ana, bin, opt, low, e2e;
    par.reserve(N); ana.reserve(N); bin.reserve(N); opt.reserve(N); low.reserve(N); e2e.reserve(N);

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
        auto lo = physical::lower(*op);
        auto t9 = clk::now();
        low.push_back(us(t9 - t8));

        e2e.push_back(us(t1 - t0) + us(t3 - t2) + us(t5 - t4) + us(t7 - t6) + us(t9 - t8));
    }

    std::printf("========== TIMING  (median over %d runs, microseconds) ==========\n\n", N);
    std::printf("  %-28s %10.2f\n", "T2 parse (incl tokenize)", median(par));
    std::printf("  %-28s %10.2f\n", "T3 analyze", median(ana));
    std::printf("  %-28s %10.2f\n", "T4 bind", median(bin));
    std::printf("  %-28s %10.2f\n", "T5 optimize", median(opt));
    std::printf("  %-28s %10.2f\n", "T6 physical (lower)", median(low));
    std::printf("  %-28s %10s\n", "----------------------------", "----------");
    std::printf("  %-28s %10.2f\n", "OVERALL (parse->physical)", median(e2e));
    return 0;
}
