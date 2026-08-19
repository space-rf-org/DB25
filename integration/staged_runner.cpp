// DB25 staged-artifact test runner (Phase A: plan layers).
//
// For each fixture, drive the frontend pipeline (parse -> analyze -> bind ->
// optimize) and pin the s-expr of each stage artifact against a committed
// golden. This is the END-TO-END mode + PER-STAGE comparison from the layer
// contracts: on a mismatch the runner reports the FIRST diverging stage, so a
// regression is localized to the module that produced it rather than only
// showing up as a wrong final result.
//
// Phase A covers the plan layers (logical, optimized). The token / AST /
// resolved-AST sections follow once their writers land; unknown sections in a
// fixture are compared verbatim if present and ignored otherwise, so the file
// format is forward-compatible.
//
// Fixture file format (one statement per file, sections in pipeline order):
//   -- sql
//   SELECT ...
//   -- logical
//   (project ...)
//   -- optimized
//   (project ...)
//
// Usage:
//   staged_runner <dir>            verify every *.fixture in <dir> (default)
//   staged_runner --update <dir>   (re)generate the golden sections in place
#include "staged_sexpr.hpp"

#include "db25/parser/parser.hpp"
#include "db25/plan/binder.hpp"
#include "db25/plan/optimizer.hpp"
#include "db25/semantic/analyzer.hpp"
#include "db25/semantic/catalog.hpp"

#include <cstdio>
#include <filesystem>
#include <fstream>
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
struct PlanArtifacts {
    std::string logical;
    std::string optimized;
};

PlanArtifacts run_plan_stages(const semantic::InMemoryCatalog& cat, const std::string& sql) {
    parser::Parser p;
    auto res = p.parse(sql);
    if (!res.has_value()) return {"(parse-error)", "(parse-error)"};

    semantic::Analyzer analyzer(cat);
    analyzer.analyze(res.value());

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
    if (bound2.ok) {
        plan::LogicalNodePtr opt = plan::optimize(std::move(bound2.root));
        optimized = staged::plan_to_sexpr(opt.get());
    } else {
        optimized = "(bind-error \"" + bound2.error + "\")";
    }
    return {trim(logical), trim(optimized)};
}

}  // namespace

int main(int argc, char** argv) {
    bool update = false;
    std::string dir;
    for (int i = 1; i < argc; ++i) {
        const std::string a = argv[i];
        if (a == "--update") update = true;
        else dir = a;
    }
    if (dir.empty()) {
        std::printf("staged_runner: usage: staged_runner [--update] <fixture-dir>\n");
        return 2;
    }

    const semantic::InMemoryCatalog cat = build_catalog();

    std::vector<fs::path> files;
    for (const auto& e : fs::directory_iterator(dir)) {
        if (e.is_regular_file() && e.path().extension() == ".fixture") files.push_back(e.path());
    }
    std::sort(files.begin(), files.end());

    long total = 0, mismatches = 0, updated = 0;
    // The stages compared, in pipeline order - so a mismatch reports the FIRST
    // stage that diverged.
    const std::vector<std::string> ordered_stages = {"logical", "optimized"};

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
        const PlanArtifacts got = run_plan_stages(cat, *sql);
        std::map<std::string, std::string> produced = {
            {"logical", got.logical}, {"optimized", got.optimized}};

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
