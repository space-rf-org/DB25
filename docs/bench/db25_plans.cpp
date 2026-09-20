#include "db25/parser/parser.hpp"
#include "db25/plan/binder.hpp"
#include "db25/plan/optimizer.hpp"
#include "db25/semantic/analyzer.hpp"
#include "db25/semantic/catalog.hpp"
#include "db25/physical/lowering.hpp"
#include "db25/physical/sexpr.hpp"
#include <cstdio>
#include <fstream>
#include <regex>
#include <string>
using namespace db25;
int main(int argc, char** argv) {
    semantic::InMemoryCatalog c;
    const auto I = ast::DataType::Integer; const auto V = ast::DataType::VarChar;
    c.add_table("emp",{{"id",I,true},{"name",V,true},{"dept_id",I,true},{"salary",I,true}});
    c.add_table("dept",{{"id",I,true},{"name",V,true}});
    c.add_table("orders",{{"id",I,true},{"user_id",I,true},{"total",I,true}});
    c.add_table("cust",{{"id",I,true},{"name",V,true},{"city",V,true}});
    c.add_table("prod",{{"id",I,true},{"name",V,true},{"price",I,true}});
    c.add_table("items",{{"id",I,true},{"order_id",I,true},{"prod_id",I,true},{"qty",I,true}});
    physical::CardinalityModel card;
    card.base_rows["emp"]=20000; card.base_rows["dept"]=50; card.base_rows["orders"]=50000;
    card.base_rows["cust"]=20000; card.base_rows["prod"]=2000; card.base_rows["items"]=100000;
    auto cal = physical::default_calibration();
    std::string err; auto spec = physical::load_spec(std::string(DB25_PHYSICAL_SPEC_DIR)+"/physical.spec.sexpr", err);
    std::ifstream in(argv[1]); std::string line;
    while (std::getline(in,line)) {
        if (line.empty()) continue;
        auto p1=line.find('|'), p2=line.find('|',p1+1);
        const std::string id=line.substr(0,p1), sql=line.substr(p2+1);
        parser::Parser p; auto pr=p.parse(sql);
        if(!pr){std::printf("%s\tPARSE-FAIL\n",id.c_str());continue;}
        semantic::Analyzer an(c); an.analyze(pr.value());
        plan::Binder b(an,c); auto bd=b.bind(pr.value());
        if(!bd.ok){std::printf("%s\tBIND-FAIL\n",id.c_str());continue;}
        auto opt=plan::optimize(std::move(bd.root));
        physical::LoweringContext ctx; ctx.spec=spec?&*spec:nullptr; ctx.calibration=&cal; ctx.cardinality=&card;
        auto low=physical::lower(*opt,ctx);
        if(!low.ok||!low.plan){std::printf("%s\tLOWER-FAIL\n",id.c_str());continue;}
        const std::string s = physical::physical_to_sexpr(*low.plan);
        static const std::regex kJoin("(HashJoin|MergeJoin|NestedLoopJoin|HashSemiJoin|"
                                      "HashAntiJoin|NestedLoopSemiJoin|NestedLoopAntiJoin)");
        std::string joins;
        for (std::sregex_iterator it(s.begin(), s.end(), kJoin), e; it != e; ++it)
            joins += (joins.empty() ? "" : "|") + it->str();
        std::printf("%s\t%s\tcand=%zu\trows=%.0f\tgroups=%zu\tgoals=%zu\tpruned=%zu\n", id.c_str(), joins.empty()?"-":joins.c_str(), low.candidates_considered, low.estimated_rows, low.memo_groups, low.optimization_goals, low.candidates_pruned);
    }
}
