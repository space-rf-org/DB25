// Where DB25's microseconds actually go: the same pipeline as db25_bench, with a
// clock between each stage. Answers the question the single number cannot -
// how much of the front end is the PLANNER, which is the only part PostgreSQL's
// own Planning Time is measuring.
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
#include <string>
#include <vector>

using namespace db25;
using Clock = std::chrono::steady_clock;
static double us(Clock::time_point a, Clock::time_point b) {
    return std::chrono::duration<double, std::micro>(b - a).count();
}
static void seed_catalog(semantic::InMemoryCatalog& c) {
    const auto I = ast::DataType::Integer; const auto V = ast::DataType::VarChar;
    c.add_table("emp",{{"id",I,true},{"name",V,true},{"dept_id",I,true},{"salary",I,true}});
    c.add_table("dept",{{"id",I,true},{"name",V,true}});
    c.add_table("orders",{{"id",I,true},{"user_id",I,true},{"total",I,true}});
    c.add_table("cust",{{"id",I,true},{"name",V,true},{"city",V,true}});
    c.add_table("prod",{{"id",I,true},{"name",V,true},{"price",I,true}});
    c.add_table("items",{{"id",I,true},{"order_id",I,true},{"prod_id",I,true},{"qty",I,true}});
}
int main(int argc, char** argv) {
    const int runs = argc > 2 ? std::atoi(argv[2]) : 500;
    std::string e;
    auto spec = physical::load_spec(std::string(DB25_PHYSICAL_SPEC_DIR)+"/physical.spec.sexpr", e);
    semantic::InMemoryCatalog cat; seed_catalog(cat);
    physical::CardinalityModel card;
    card.base_rows["emp"]=20000; card.base_rows["dept"]=50; card.base_rows["orders"]=50000;
    card.base_rows["cust"]=20000; card.base_rows["prod"]=2000; card.base_rows["items"]=100000;
    const auto cal = physical::default_calibration();

    std::ifstream in(argv[1]); std::string line;
    std::printf("id\tparse\tanalyze+bind\tlogical\tphysical\ttotal\n");
    while (std::getline(in, line)) {
        if (line.empty()) continue;
        const auto p1=line.find('|'), p2=line.find('|',p1+1);
        const std::string id=line.substr(0,p1), sql=line.substr(p2+1);
        std::vector<double> a,b,c,d,t;
        for (int i=0;i<runs;++i) {
            const auto t0=Clock::now();
            parser::Parser p; auto pr=p.parse(sql);
            const auto t1=Clock::now();
            if(!pr) break;
            semantic::Analyzer an(cat); an.analyze(pr.value());
            plan::Binder bd(an,cat); auto bound=bd.bind(pr.value());
            const auto t2=Clock::now();
            if(!bound.ok||!bound.root) break;
            auto opt=plan::optimize(std::move(bound.root));
            const auto t3=Clock::now();
            physical::LoweringContext ctx;
            ctx.spec=spec?&*spec:nullptr; ctx.calibration=&cal; ctx.cardinality=&card;
            auto low=physical::lower(*opt,ctx);
            const auto t4=Clock::now();
            if(!low.ok) break;
            a.push_back(us(t0,t1)); b.push_back(us(t1,t2));
            c.push_back(us(t2,t3)); d.push_back(us(t3,t4)); t.push_back(us(t0,t4));
        }
        if (a.empty()) { std::printf("%s\tFAIL\n", id.c_str()); continue; }
        auto med=[](std::vector<double>& v){ std::sort(v.begin(),v.end()); return v[v.size()/2]; };
        std::printf("%s\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\n",
                    id.c_str(), med(a), med(b), med(c), med(d), med(t));
    }
}
