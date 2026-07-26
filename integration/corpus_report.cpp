// DB25 frontend artifact report generator.
//
// Replays a session of SQL statements through the frontend and emits a
// human-readable, verifiable Markdown report of the ARTIFACT PRODUCED AT EACH
// STAGE for every statement:
//
//   parse    -> the AST (node tree: type, captured text, alias)
//   analyze  -> diagnostics (code + message), or "clean"
//   DDL      -> the resulting catalog object (columns, constraints)
//   bind     -> the logical plan tree (dump_plan)
//   optimize -> the optimized logical plan tree
//
// Statements replay in order against one catalog, so DDL sets up the schema the
// queries below it resolve and plan against. The output is meant to be read and
// checked by a human: each stage's artifact is shown verbatim next to the SQL
// that produced it.
//
// Usage: corpus_report <statements.sql> [> REPORT.md]

#include "db25/ast/node_types.hpp"
#include "db25/parser/parser.hpp"
#include "db25/plan/binder.hpp"
#include "db25/plan/logical_plan.hpp"
#include "db25/plan/optimizer.hpp"
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
using db25::ast::ASTNode;
using db25::ast::NodeType;

namespace {

bool is_ddl(NodeType t) {
    return t == NodeType::CreateTableStmt || t == NodeType::CreateIndexStmt ||
           t == NodeType::AlterTableStmt || t == NodeType::DropStmt ||
           t == NodeType::CreateViewStmt || t == NodeType::TruncateStmt;
}

// Split a .sql file into statements on top-level ';'. Full-line "--" comments
// are captured as the caption for the statement that follows.
struct Stmt { std::string caption, sql; };
std::vector<Stmt> load(const std::string& path) {
    std::ifstream in(path);
    std::string all((std::istreambuf_iterator<char>(in)),
                    std::istreambuf_iterator<char>());
    std::vector<Stmt> out;
    std::string cur, caption;
    std::istringstream ls(all);
    std::string line;
    while (std::getline(ls, line)) {
        std::string t = line;
        // trim leading spaces for the comment check
        std::size_t s = t.find_first_not_of(" \t");
        if (s != std::string::npos && t.compare(s, 2, "--") == 0) {
            caption = t.substr(s + 2);
            if (!caption.empty() && caption.front() == ' ') caption.erase(0, 1);
            continue;
        }
        cur += line;
        cur += ' ';
        if (cur.find(';') != std::string::npos) {
            std::string sql = cur.substr(0, cur.find(';'));
            // trim
            std::size_t a = sql.find_first_not_of(" \t");
            std::size_t b = sql.find_last_not_of(" \t");
            if (a != std::string::npos) {
                out.push_back({caption, sql.substr(a, b - a + 1)});
            }
            cur.clear();
            caption.clear();
        }
    }
    return out;
}

void dump_ast(std::string& md, const ASTNode* n, int depth) {
    if (n == nullptr) return;
    for (int i = 0; i < depth; ++i) md += "  ";
    md += ast::node_type_to_string(n->node_type);
    if (!n->primary_text.empty()) {
        md += " '";
        md.append(n->primary_text.data(), n->primary_text.size());
        md += "'";
    }
    if (!n->schema_name.empty()) {
        md += " [";
        md.append(n->schema_name.data(), n->schema_name.size());
        md += "]";
    }
    md += "\n";
    for (const ASTNode* c = n->first_child; c != nullptr; c = c->next_sibling) {
        dump_ast(md, c, depth + 1);
    }
}

void catalog_effect(std::string& md, const Catalog& cat, const ASTNode* root) {
    // Report the table the DDL just touched (best-effort: the first TableRef /
    // primary_text naming a table now present in the catalog).
    const ASTNode* tref = nullptr;
    for (const ASTNode* c = root->first_child; c != nullptr; c = c->next_sibling) {
        if (c->node_type == NodeType::TableRef || c->node_type == NodeType::Identifier) {
            tref = c;
            break;
        }
    }
    std::string name = tref != nullptr ? std::string(tref->primary_text)
                                       : std::string(root->primary_text);
    const TableInfo* t = name.empty() ? nullptr : cat.find_table(name);
    if (t == nullptr) {
        md += "_(no catalog table named `" + name + "` after this statement)_\n";
        return;
    }
    md += "table `" + t->name + "`:\n";
    for (const ColumnInfo& c : t->columns) {
        md += "- `" + c.name + "` " + ast::data_type_to_string(c.type);
        if (!c.nullable) md += " NOT NULL";
        if (c.has_default) md += " DEFAULT " + c.default_expr;
        md += "\n";
    }
    for (const Constraint& c : t->constraints) {
        const char* k = c.kind == Constraint::Kind::PrimaryKey  ? "PRIMARY KEY"
                        : c.kind == Constraint::Kind::Unique     ? "UNIQUE"
                        : c.kind == Constraint::Kind::ForeignKey ? "FOREIGN KEY"
                                                                 : "CHECK";
        md += std::string("- constraint ") + k;
        if (!c.name.empty()) md += " `" + c.name + "`";
        if (!c.expr.empty()) md += " (" + c.expr + ")";
        md += "\n";
    }
}

}  // namespace

int main(int argc, char** argv) {
    if (argc < 2) {
        std::fprintf(stderr, "usage: corpus_report <statements.sql>\n");
        return 2;
    }
    std::vector<Stmt> stmts = load(argv[1]);

    std::string err;
    const std::string path = "/tmp/db25_report.db25cat";
    std::remove(path.c_str());
    CatalogManager mgr(path, err);
    parser::Parser p;

    std::string md;
    md += "# DB25 frontend — per-stage artifact report\n\n";
    md += "Generated by `corpus_report` from `" + std::string(argv[1]) +
          "`. Every statement below is replayed through the frontend in order "
          "against one catalog; each stage's artifact is shown verbatim so it "
          "can be read and checked against the SQL that produced it.\n\n";
    md += "Stages: **parse** → AST · **analyze** → diagnostics · DDL → **catalog "
          "effect** · query/DML → **logical plan** → **optimized plan**.\n";

    int n = 0;
    for (const Stmt& s : stmts) {
        ++n;
        md += "\n---\n\n## " + std::to_string(n) + ". ";
        md += s.caption.empty() ? std::string("statement") : s.caption;
        md += "\n\n```sql\n" + s.sql + "\n```\n\n";

        auto pr = p.parse(s.sql);
        if (!pr.has_value()) {
            md += "**parse** → ❌ rejected (no AST produced)\n";
            continue;
        }
        ASTNode* root = pr.value();

        md += "**parse → AST**\n\n```\n";
        dump_ast(md, root, 0);
        md += "```\n\n";

        if (is_ddl(root->node_type)) {
            const auto r = execute_ddl(root, mgr);
            md += "**catalog effect** (execute_ddl: ";
            md += r.ok ? "ok" : ("rejected — " + r.error);
            md += ")\n\n";
            if (r.ok) catalog_effect(md, mgr.catalog(), root);
            continue;
        }

        Analyzer analyzer(mgr.catalog());
        analyzer.analyze(root);
        md += "**analyze → diagnostics**\n\n";
        if (analyzer.diagnostics().empty()) {
            md += "_clean (no diagnostics)_\n\n";
        } else {
            for (const Diagnostic& d : analyzer.diagnostics()) {
                md += std::string("- ") +
                      (d.severity == Severity::Error ? "error" : "warning") + ": " +
                      d.message + "\n";
            }
            md += "\n";
        }

        plan::Binder binder(analyzer, mgr.catalog());
        plan::BindResult bound = binder.bind(root);
        if (!bound.ok || bound.root == nullptr) {
            md += "**logical plan** → not produced";
            if (!bound.error.empty()) md += " (" + bound.error + ")";
            md += "\n";
            continue;
        }
        md += "**bind → logical plan**\n\n```\n" + plan::dump_plan(bound.root.get()) +
              "```\n\n";

        plan::LogicalNodePtr opt = plan::optimize(std::move(bound.root));
        md += "**optimize → logical plan**\n\n```\n" + plan::dump_plan(opt.get()) +
              "```\n";
    }

    std::remove(path.c_str());
    std::fputs(md.c_str(), stdout);
    return 0;
}
