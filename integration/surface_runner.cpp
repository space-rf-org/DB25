// THE DECLARED SQL SURFACE, CHECKED.
//
// docs/sql-surface.md says what the whole stack does with each construct. It is
// not a description of the code, it is a CLAIM about the code - and until now
// nothing checked it, so it drifted: it called `JOIN ... USING`, standalone
// `VALUES` and CTEs unsupported long after they began lowering end to end, and
// it called `NATURAL JOIN` semantically broken after that was fixed. Three
// understatements and an overstatement, in the one document a reader consults to
// learn what DB25 does.
//
// So the document IS the spec, and this runs it. Each row names a construct,
// runnable SQL, and the stage the stack is DECLARED to reach. This drives the
// real pipeline and compares. It fails in BOTH directions, which is the point:
//
//   * declared further than reality  -> the document promises what we do not do
//   * declared short of reality      -> something started working and nobody
//                                       updated the claim, which is how the
//                                       document rotted the first time
//
// There is no --update mode, deliberately. A spec that rewrites itself to match
// the code is not a spec, it is a mirror; the whole value is that a human wrote
// the claim down and has to revisit it when it breaks.
#include "db25/parser/parser.hpp"
#include "db25/plan/binder.hpp"
#include "db25/plan/optimizer.hpp"
#include "db25/semantic/analyzer.hpp"
#include "db25/semantic/catalog.hpp"
#include "db25/physical/lowering.hpp"

#include <cstdio>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>

using namespace db25;

// How far the stack got. The last stage that SUCCEEDED.
enum class Stage { Reject, Parse, Analyze, Optimize, Full };

static const char* stage_name(Stage s) {
    switch (s) {
        case Stage::Reject:   return "reject";
        case Stage::Parse:    return "parse";
        case Stage::Analyze:  return "analyze";
        case Stage::Optimize: return "optimize";
        case Stage::Full:     return "full";
    }
    return "?";
}
static bool stage_from(const std::string& s, Stage& out) {
    if (s == "reject")   { out = Stage::Reject;   return true; }
    if (s == "parse")    { out = Stage::Parse;    return true; }
    if (s == "analyze")  { out = Stage::Analyze;  return true; }
    if (s == "optimize") { out = Stage::Optimize; return true; }
    if (s == "full")     { out = Stage::Full;     return true; }
    return false;
}

// The fixed catalog the document names. Kept here rather than in the document so
// the SQL in each row stays readable.
static void seed(semantic::InMemoryCatalog& cat) {
    cat.add_table("users", {semantic::ColumnInfo{"id", ast::DataType::Integer, false},
                            semantic::ColumnInfo{"name", ast::DataType::VarChar, true},
                            semantic::ColumnInfo{"age", ast::DataType::Integer, true},
                            semantic::ColumnInfo{"city", ast::DataType::VarChar, true}});
    cat.add_table("orders", {semantic::ColumnInfo{"id", ast::DataType::Integer, false},
                             semantic::ColumnInfo{"user_id", ast::DataType::Integer, true},
                             semantic::ColumnInfo{"amount", ast::DataType::Integer, true}});
    cat.add_table("emp", {semantic::ColumnInfo{"id", ast::DataType::Integer, false},
                          semantic::ColumnInfo{"mgr_id", ast::DataType::Integer, true},
                          semantic::ColumnInfo{"dept_id", ast::DataType::Integer, true},
                          semantic::ColumnInfo{"salary", ast::DataType::Integer, true}});
}

static Stage reached(const std::string& sql) {
    semantic::InMemoryCatalog cat;
    seed(cat);

    parser::Parser p;
    auto pr = p.parse(sql);
    if (!pr) return Stage::Reject;

    semantic::Analyzer an(cat);
    an.analyze(pr.value());
    if (an.has_errors()) return Stage::Parse;

    plan::Binder b(an, cat);
    auto bound = b.bind(pr.value());
    if (!bound.ok || !bound.root) return Stage::Analyze;

    auto opt = plan::optimize(std::move(bound.root));
    if (!opt) return Stage::Analyze;

    physical::LoweringContext ctx;
    const auto low = physical::lower(*opt, ctx);
    return low.ok && low.plan ? Stage::Full : Stage::Optimize;
}

struct Row {
    std::string construct, sql, declared, cause;
    int line = 0;
};

// Reads the markdown table out of the document. The document is the spec, so it
// is parsed rather than duplicated - two copies could disagree, and a spec that
// can disagree with itself is worse than no spec.
static std::vector<Row> read_surface(const std::string& path, std::string& err) {
    std::ifstream in(path);
    if (!in) { err = "cannot open " + path; return {}; }
    std::vector<Row> rows;
    std::string line;
    int n = 0;
    bool in_table = false;
    while (std::getline(in, line)) {
        ++n;
        if (line.rfind("| Construct ", 0) == 0) { in_table = true; continue; }
        if (in_table && line.rfind("|---", 0) == 0) continue;
        if (in_table && (line.empty() || line[0] != '|')) { in_table = false; continue; }
        if (!in_table) continue;

        std::vector<std::string> cells;
        std::string cur;
        bool in_code = false;
        for (std::size_t i = 1; i < line.size(); ++i) {
            const char c = line[i];
            if (c == '`') in_code = !in_code;
            if (c == '|' && !in_code) { cells.push_back(cur); cur.clear(); continue; }
            cur += c;
        }
        if (!cur.empty()) cells.push_back(cur);
        if (cells.size() < 4) continue;

        const auto trim = [](std::string s) {
            const auto b = s.find_first_not_of(" \t");
            if (b == std::string::npos) return std::string{};
            const auto e = s.find_last_not_of(" \t");
            s = s.substr(b, e - b + 1);
            if (s.size() >= 2 && s.front() == '`' && s.back() == '`') s = s.substr(1, s.size() - 2);
            return s;
        };
        Row r;
        r.construct = trim(cells[0]);
        r.sql = trim(cells[1]);
        r.declared = trim(cells[2]);
        r.cause = trim(cells[3]);
        r.line = n;
        if (!r.construct.empty() && !r.sql.empty()) rows.push_back(std::move(r));
    }
    return rows;
}

int main(int argc, char** argv) {
    const std::string doc = argc > 1 ? argv[1] : "docs/sql-surface.md";
    std::string err;
    const std::vector<Row> rows = read_surface(doc, err);
    if (!err.empty()) { std::printf("sql_surface: %s\n", err.c_str()); return 2; }
    if (rows.empty()) {
        std::printf("sql_surface: no rows parsed from %s - the table format changed and\n"
                    "this check silently stopped checking anything.\n", doc.c_str());
        return 2;
    }

    int mismatches = 0, missing_cause = 0;
    for (const Row& r : rows) {
        Stage want{};
        if (!stage_from(r.declared, want)) {
            std::printf("  line %d: %s declares stage '%s', not one of "
                        "reject/parse/analyze/optimize/full\n",
                        r.line, r.construct.c_str(), r.declared.c_str());
            ++mismatches;
            continue;
        }
        // Anything short of `full` is a limit, and a limit without a stated cause
        // is the thing this document exists to prevent.
        if (want != Stage::Full && r.cause.empty()) {
            std::printf("  line %d: %s stops at '%s' with no cause given\n",
                        r.line, r.construct.c_str(), r.declared.c_str());
            ++missing_cause;
        }
        const Stage got = reached(r.sql);
        if (got != want) {
            std::printf("  line %d: %s\n      declared '%s' but the stack reaches '%s'\n"
                        "      sql: %s\n      %s\n",
                        r.line, r.construct.c_str(), stage_name(want), stage_name(got),
                        r.sql.c_str(),
                        got > want ? "-> it started working; update the claim and drop the cause"
                                   : "-> the document promises more than the stack does");
            ++mismatches;
        }
    }
    std::printf("sql_surface: %zu construct(s) checked, %d mismatch(es), "
                "%d missing cause(s)\n", rows.size(), mismatches, missing_cause);
    return (mismatches == 0 && missing_cause == 0) ? 0 : 1;
}
