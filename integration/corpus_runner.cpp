// DB25 frontend SQL-corpus runner.
//
// Replays a committed corpus of real SQL statements (harvested from
// sqllogictest, dialect-corrected to DB25's canon) through the frontend and
// pins the artifact each stage produces. Two tiers:
//   Tier R (robustness): every statement runs parse -> analyze without crashing
//     or tripping UB. Under ASan/UBSan in CI this is the safety net.
//   Tier E (expectation): each statement's committed db25_parse / db25_analyze
//     columns must match what the frontend produces now. A change that alters
//     parsing or analysis of any statement fails the test until the golden is
//     regenerated (--update) and reviewed - so behaviour cannot drift silently.
//
// Statements replay per SESSION in order: DDL builds a live catalog that later
// DML/queries resolve against (cascade-aware). Sessions get a fresh catalog.
#include "db25/ast/node_types.hpp"
#include "db25/parser/parser.hpp"
#include "db25/semantic/analyzer.hpp"
#include "db25/semantic/catalog_manager.hpp"
#include "db25/semantic/ddl.hpp"
#include <cstdio>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>
using namespace db25;
using namespace db25::semantic;
using db25::ast::NodeType;

namespace {
struct Row { std::string session, tag, cat, parse, analyze, sql; };

std::vector<Row> load(const std::string& path) {
    std::vector<Row> rows; std::ifstream in(path); std::string line;
    while (std::getline(in, line)) {
        if (line.empty() || line[0] == '#') continue;
        std::vector<std::string> f; std::string cur; std::istringstream ss(line);
        while (std::getline(ss, cur, '\t')) f.push_back(cur);
        if (f.size() < 6) continue;
        rows.push_back({f[0], f[1], f[2], f[3], f[4], f[5]});
    }
    return rows;
}

bool is_ddl(NodeType t) {
    return t == NodeType::CreateTableStmt || t == NodeType::CreateIndexStmt ||
           t == NodeType::AlterTableStmt || t == NodeType::DropStmt ||
           t == NodeType::CreateViewStmt || t == NodeType::TruncateStmt;
}
}  // namespace

int main(int argc, char** argv) {
    // Usage:
    //   corpus_runner <corpus.tsv>            verify the golden (default)
    //   corpus_runner --update <corpus.tsv>   regenerate the golden columns
    bool update = false;
    std::string path = "corpus.tsv";
    for (int i = 1; i < argc; ++i) {
        const std::string a = argv[i];
        if (a == "--update") update = true;
        else path = a;
    }
    std::vector<Row> rows = load(path);
    if (rows.empty()) {
        std::printf("corpus_runner: no rows loaded from '%s'\n", path.c_str());
        return 1;
    }

    std::string err, session; CatalogManager* mgr = nullptr;
    const std::string cat_path = "/tmp/db25_corpus.db25cat";
    parser::Parser p;
    long tot = 0, accept = 0, mismatch = 0, src_divergent = 0;

    for (Row& r : rows) {
        if (r.session != session) {          // new session -> fresh catalog
            delete mgr; std::remove(cat_path.c_str());
            mgr = new CatalogManager(cat_path, err); session = r.session;
        }
        ++tot;
        std::string parse_v, analyze_v = "na";
        auto pr = p.parse(r.sql);
        if (pr.has_value()) {
            ++accept; parse_v = "accept"; auto* root = pr.value();
            if (is_ddl(root->node_type)) {
                analyze_v = execute_ddl(root, *mgr).ok ? "exec_ok" : "exec_err";
            } else {
                Analyzer a(mgr->catalog()); a.analyze(root);
                analyze_v = a.has_errors() ? "diag" : "clean";
            }
        } else {
            parse_v = "reject";
        }
        if (r.tag == "ok" && parse_v == "reject") ++src_divergent;
        if (update) { r.parse = parse_v; r.analyze = analyze_v; }
        else if (r.parse != parse_v || r.analyze != analyze_v) {
            ++mismatch;
            std::printf("  MISMATCH [%s] want(%s,%s) got(%s,%s): %s\n",
                        r.session.c_str(), r.parse.c_str(), r.analyze.c_str(),
                        parse_v.c_str(), analyze_v.c_str(), r.sql.substr(0, 70).c_str());
        }
    }
    delete mgr; std::remove(cat_path.c_str());

    if (update) {
        std::ofstream out(path);
        out << "# session\tsource_tag\tcategory\tdb25_parse\tdb25_analyze\tsql\n";
        for (const Row& r : rows)
            out << r.session << '\t' << r.tag << '\t' << r.cat << '\t'
                << r.parse << '\t' << r.analyze << '\t' << r.sql << '\n';
        std::printf("corpus_runner: wrote golden for %ld statements\n", tot);
        return 0;
    }
    std::printf("corpus_runner: %ld statements, %ld parsed (%.1f%%), "
                "%ld source-tag divergences (documented in golden), %ld golden mismatches\n",
                tot, accept, 100.0 * accept / tot, src_divergent, mismatch);
    return mismatch == 0 ? 0 : 1;
}
