// DB25 side of the three-way comparison: SQL -> parse -> analyze -> bind ->
// optimize -> physical lowering, and stop. Same boundary the other two are held
// to (EXPLAIN, no execution). Reports the median over N runs, in microseconds.
#include "db25/parser/parser.hpp"
#include "db25/plan/binder.hpp"
#include "db25/plan/optimizer.hpp"
#include "db25/semantic/analyzer.hpp"
#include "db25/semantic/catalog.hpp"
#include "db25/physical/lowering.hpp"

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>

using namespace db25;
using Clock = std::chrono::steady_clock;

static void seed_catalog(semantic::InMemoryCatalog& c) {
    const auto I = ast::DataType::Integer;
    const auto V = ast::DataType::VarChar;
    c.add_table("emp",    {{"id", I, true}, {"name", V, true}, {"dept_id", I, true}, {"salary", I, true}});
    c.add_table("dept",   {{"id", I, true}, {"name", V, true}});
    c.add_table("orders", {{"id", I, true}, {"user_id", I, true}, {"total", I, true}});
    c.add_table("cust",   {{"id", I, true}, {"name", V, true}, {"city", V, true}});
    c.add_table("prod",   {{"id", I, true}, {"name", V, true}, {"price", I, true}});
    c.add_table("items",  {{"id", I, true}, {"order_id", I, true}, {"prod_id", I, true}, {"qty", I, true}});
}
static void seed_cards(physical::CardinalityModel& m) {
    m.base_rows["emp"] = 20000;   m.base_rows["dept"] = 50;
    m.base_rows["orders"] = 50000; m.base_rows["cust"] = 20000;
    m.base_rows["prod"] = 2000;   m.base_rows["items"] = 100000;
}

struct Result { bool ok = false; double us = 0; std::string note; };

static Result run_once(const std::string& sql, const physical::PhysicalSpec* spec,
                       semantic::InMemoryCatalog& cat, const physical::CardinalityModel& card,
                       const physical::CalibrationProfile& cal,
                       bool time_it, double* out_us) {
    const auto t0 = Clock::now();
    parser::Parser p;
    auto pr = p.parse(sql);
    if (!pr) return {false, 0, "parse"};
    semantic::Analyzer an(cat);
    an.analyze(pr.value());
    if (an.has_errors()) return {false, 0, "analyze"};
    plan::Binder b(an, cat);
    auto bound = b.bind(pr.value());
    if (!bound.ok || !bound.root) return {false, 0, "bind"};
    auto opt = plan::optimize(std::move(bound.root));
    physical::LoweringContext ctx;
    ctx.spec = spec; ctx.calibration = &cal; ctx.cardinality = &card;
    auto low = physical::lower(*opt, ctx);
    const auto t1 = Clock::now();
    if (time_it && out_us)
        *out_us = std::chrono::duration<double, std::micro>(t1 - t0).count();
    if (!low.ok || !low.plan) return {false, 0, "lower"};
    return {true, 0, ""};
}

int main(int argc, char** argv) {
    const std::string qfile = argv[1];
    const int runs = argc > 2 ? std::atoi(argv[2]) : 200;
    std::string spec_err;
    auto spec = physical::load_spec(std::string(DB25_PHYSICAL_SPEC_DIR) + "/physical.spec.sexpr", spec_err);

    semantic::InMemoryCatalog cat; seed_catalog(cat);
    physical::CardinalityModel card; seed_cards(card);
    const physical::CalibrationProfile cal = physical::default_calibration();

    std::ifstream in(qfile);
    std::string line;
    while (std::getline(in, line)) {
        if (line.empty()) continue;
        const auto p1 = line.find('|'), p2 = line.find('|', p1 + 1);
        const std::string id = line.substr(0, p1);
        const std::string sql = line.substr(p2 + 1);

        const Result probe = run_once(sql, spec ? &*spec : nullptr, cat, card, cal, false, nullptr);
        if (!probe.ok) { std::printf("%s\tFAIL:%s\n", id.c_str(), probe.note.c_str()); continue; }
        std::vector<double> t;
        t.reserve(static_cast<std::size_t>(runs));
        for (int i = 0; i < runs; ++i) {
            double us = 0;
            run_once(sql, spec ? &*spec : nullptr, cat, card, cal, true, &us);
            t.push_back(us);
        }
        std::sort(t.begin(), t.end());
        std::printf("%s\t%.2f\n", id.c_str(), t[t.size() / 2]);
    }
    return 0;
}
