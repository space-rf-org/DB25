// physical_integration: a CI-gated end-to-end check that the physical planner is
// pinned into the umbrella and lowers a full-pipeline plan. Drives
// parse -> analyze -> bind -> optimize (db25-logical-plan) -> lower
// (db25-physical-plan) on the Increment-0 query and asserts a physical plan with
// the expected shape comes out. This is the cross-repo seam the pin exists for;
// the physical planner's own unit tests live in its repo.
#include "db25/parser/parser.hpp"
#include "db25/plan/binder.hpp"
#include "db25/plan/logical_plan.hpp"
#include "db25/plan/optimizer.hpp"
#include "db25/semantic/analyzer.hpp"
#include "db25/semantic/catalog.hpp"

#include "db25/physical/lowering.hpp"
#include "db25/physical/sexpr.hpp"
#include "db25/physical/spec.hpp"

#include <cstdio>
#include <string>

#ifndef DB25_PHYSICAL_SPEC_DIR
#define DB25_PHYSICAL_SPEC_DIR "."
#endif

using namespace db25;

static int g_failures = 0;
#define CHECK(cond)                                                        \
    do {                                                                   \
        if (!(cond)) {                                                     \
            std::printf("  FAIL: %s (%s:%d)\n", #cond, __FILE__, __LINE__); \
            ++g_failures;                                                  \
        }                                                                  \
    } while (0)

static bool contains(const std::string& hay, const char* needle) {
    return hay.find(needle) != std::string::npos;
}

int main() {
    semantic::InMemoryCatalog cat;
    cat.add_table("a", {semantic::ColumnInfo{"id", ast::DataType::Integer, false},
                        semantic::ColumnInfo{"x", ast::DataType::Integer, true}});
    cat.add_table("b", {semantic::ColumnInfo{"id", ast::DataType::Integer, false},
                        semantic::ColumnInfo{"y", ast::DataType::VarChar, true}});

    const std::string sql = "SELECT a.x, b.y FROM a JOIN b ON a.id = b.id WHERE a.x > 10";

    parser::Parser parser;
    auto pr = parser.parse(sql);
    CHECK(pr.has_value());
    if (!pr) { std::printf("physical_integration: parse failed\n"); return 1; }

    semantic::Analyzer analyzer(cat);
    analyzer.analyze(pr.value());
    CHECK(!analyzer.has_errors());

    plan::Binder binder(analyzer, cat);
    auto bound = binder.bind(pr.value());
    CHECK(bound.ok);
    if (!bound.ok) { std::printf("physical_integration: bind failed: %s\n", bound.error.c_str()); return 1; }

    auto optimized = plan::optimize(std::move(bound.root));

    // Lower through the SHIPPED spec (the real configuration), not the built-in
    // fallback - so this gates the spec the project actually ships.
    std::string spec_error;
    auto spec = physical::load_spec(std::string(DB25_PHYSICAL_SPEC_DIR) + "/physical.spec.sexpr",
                                    spec_error);
    CHECK(spec.has_value());
    if (!spec) {
        std::printf("physical_integration: cannot load spec: %s\n", spec_error.c_str());
        return 1;
    }
    physical::LoweringContext pctx;
    pctx.spec = &*spec;

    // The cross-repo seam: lower the optimized logical plan to a physical plan.
    auto lowered = physical::lower(*optimized, pctx);
    CHECK(lowered.ok);
    if (!lowered.ok) {
        std::printf("physical_integration: lower failed: %s\n", lowered.error.c_str());
        return 1;
    }
    CHECK(lowered.plan != nullptr);

    const std::string s = physical::physical_to_sexpr(*lowered.plan);
    CHECK(contains(s, "HashJoin"));   // the join lowered to a hash join
    CHECK(contains(s, "SeqScan"));    // over base scans
    CHECK(contains(s, "Project"));    // the projection survived to the top

    if (g_failures == 0) {
        std::printf("physical_integration: ok\n%s\n", s.c_str());
        return 0;
    }
    std::printf("physical_integration: %d failure(s)\n", g_failures);
    return 1;
}
