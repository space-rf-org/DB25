// DB25 frontend artifact report generator.
//
// Replays a session of SQL statements through the frontend and emits a
// human-readable, verifiable report of the ARTIFACT PRODUCED AT EACH STAGE for
// every statement:
//
//   parse    -> the AST (node tree: type, captured text, alias)
//   analyze  -> diagnostics (severity + message), or "clean"
//   DDL      -> the resulting catalog object (columns, constraints)
//   bind     -> the logical plan tree (dump_plan)
//   optimize -> the optimized logical plan tree
//
// Statements replay in order against one catalog, so DDL sets up the schema the
// queries below it resolve and plan against. The output is meant to be read and
// checked by a human: each stage's artifact is shown verbatim next to the SQL
// that produced it.
//
// Output formats:
//   corpus_report <statements.sql>            Markdown to stdout (default)
//   corpus_report --html <statements.sql>     self-contained HTML to stdout
//                                             (AST rendered as a tree diagram)

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

// ---- statement splitting (top-level ';', comments stripped) -----------------
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
        std::size_t s = line.find_first_not_of(" \t");
        if (s != std::string::npos && line.compare(s, 2, "--") == 0) {
            caption = line.substr(s + 2);
            if (!caption.empty() && caption.front() == ' ') caption.erase(0, 1);
            continue;
        }
        cur += line;
        cur += ' ';
        std::size_t sc;
        while ((sc = cur.find(';')) != std::string::npos) {
            std::string sql = cur.substr(0, sc);
            std::size_t a = sql.find_first_not_of(" \t");
            std::size_t b = sql.find_last_not_of(" \t");
            if (a != std::string::npos) out.push_back({caption, sql.substr(a, b - a + 1)});
            cur.erase(0, sc + 1);
            caption.clear();
        }
    }
    return out;
}

std::string esc(std::string_view v) {  // HTML-escape
    std::string o;
    for (char c : v) {
        switch (c) {
            case '&': o += "&amp;"; break;
            case '<': o += "&lt;"; break;
            case '>': o += "&gt;"; break;
            case '"': o += "&quot;"; break;
            default: o += c;
        }
    }
    return o;
}

// ---- AST rendering ----------------------------------------------------------
void ast_indent(std::string& out, const ASTNode* n, int depth) {
    if (n == nullptr) return;
    for (int i = 0; i < depth; ++i) out += "  ";
    out += ast::node_type_to_string(n->node_type);
    if (!n->primary_text.empty()) {
        out += " '"; out.append(n->primary_text.data(), n->primary_text.size()); out += "'";
    }
    if (!n->schema_name.empty()) {
        out += " ["; out.append(n->schema_name.data(), n->schema_name.size()); out += "]";
    }
    out += "\n";
    for (const ASTNode* c = n->first_child; c != nullptr; c = c->next_sibling)
        ast_indent(out, c, depth + 1);
}

void ast_html(std::string& out, const ASTNode* n) {
    if (n == nullptr) return;
    out += "<li><span class=\"node\"><span class=\"nt\">";
    out += esc(ast::node_type_to_string(n->node_type));
    out += "</span>";
    if (!n->primary_text.empty())
        out += "<span class=\"ntext\">" + esc(n->primary_text) + "</span>";
    if (!n->schema_name.empty())
        out += "<span class=\"nalias\">" + esc(n->schema_name) + "</span>";
    out += "</span>";
    if (n->first_child != nullptr) {
        out += "<ul>";
        for (const ASTNode* c = n->first_child; c != nullptr; c = c->next_sibling)
            ast_html(out, c);
        out += "</ul>";
    }
    out += "</li>";
}

// ---- captured per-statement artifacts (rendered while the AST is alive) -----
struct Section {
    std::string caption, sql;
    bool parse_ok = false;
    std::string ast_indent_s, ast_html_s;
    bool is_ddl = false;
    std::string ddl_status;                              // "ok" / "rejected — ..."
    std::vector<std::string> catalog_lines;
    std::vector<std::pair<bool, std::string>> diags;     // (is_error, message)
    bool analyzed = false;
    std::string plan, opt_plan, plan_note;
};

void catalog_lines(std::vector<std::string>& lines, const Catalog& cat,
                   const ASTNode* root) {
    const ASTNode* tref = nullptr;
    for (const ASTNode* c = root->first_child; c != nullptr; c = c->next_sibling)
        if (c->node_type == NodeType::TableRef || c->node_type == NodeType::Identifier) {
            tref = c; break;
        }
    std::string name = tref ? std::string(tref->primary_text)
                            : std::string(root->primary_text);
    const TableInfo* t = name.empty() ? nullptr : cat.find_table(name);
    if (t == nullptr) { lines.push_back("(no catalog table named `" + name + "`)"); return; }
    lines.push_back("table `" + t->name + "`:");
    for (const ColumnInfo& c : t->columns) {
        std::string s = "  " + c.name + " " + ast::data_type_to_string(c.type);
        if (!c.nullable) s += " NOT NULL";
        if (c.has_default) s += " DEFAULT " + c.default_expr;
        lines.push_back(s);
    }
    for (const Constraint& c : t->constraints) {
        const char* k = c.kind == Constraint::Kind::PrimaryKey  ? "PRIMARY KEY"
                        : c.kind == Constraint::Kind::Unique     ? "UNIQUE"
                        : c.kind == Constraint::Kind::ForeignKey ? "FOREIGN KEY"
                                                                 : "CHECK";
        std::string s = std::string("  constraint ") + k;
        if (!c.name.empty()) s += " `" + c.name + "`";
        if (!c.expr.empty()) s += " (" + c.expr + ")";
        lines.push_back(s);
    }
}

// ---- document emitters ------------------------------------------------------
std::string md_document(const std::vector<Section>& secs, const std::string& src) {
    std::string md = "# DB25 frontend — per-stage artifact report\n\n";
    md += "Generated by `corpus_report` from `" + src + "`. Every statement is "
          "replayed through the frontend in order against one catalog; each "
          "stage's artifact is shown verbatim.\n\n";
    md += "Stages: **parse** → AST · **analyze** → diagnostics · DDL → **catalog "
          "effect** · query/DML → **logical plan** → **optimized plan**.\n";
    int n = 0;
    for (const Section& s : secs) {
        ++n;
        md += "\n---\n\n## " + std::to_string(n) + ". " +
              (s.caption.empty() ? "statement" : s.caption) + "\n\n```sql\n" +
              s.sql + "\n```\n\n";
        if (!s.parse_ok) { md += "**parse** → ❌ rejected (no AST produced)\n"; continue; }
        md += "**parse → AST**\n\n```\n" + s.ast_indent_s + "```\n\n";
        if (s.is_ddl) {
            md += "**catalog effect** (execute_ddl: " + s.ddl_status + ")\n\n";
            for (const std::string& l : s.catalog_lines) md += l + "\n";
            continue;
        }
        md += "**analyze → diagnostics**\n\n";
        if (s.diags.empty()) md += "_clean (no diagnostics)_\n\n";
        else {
            for (const auto& d : s.diags)
                md += std::string("- ") + (d.first ? "error" : "warning") + ": " +
                      d.second + "\n";
            md += "\n";
        }
        if (!s.plan.empty())
            md += "**bind → logical plan**\n\n```\n" + s.plan + "```\n\n"
                  "**optimize → logical plan**\n\n```\n" + s.opt_plan + "```\n";
        else
            md += "**logical plan** → " + s.plan_note + "\n";
    }
    return md;
}

std::string html_document(const std::vector<Section>& secs, const std::string& src) {
    std::string h =
        "<!doctype html>\n<html lang=\"en\"><head><meta charset=\"utf-8\">\n"
        "<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">\n"
        "<title>DB25 frontend — per-stage artifact report</title>\n<style>\n"
        ":root{--bg:#ffffff;--fg:#1f2328;--muted:#57606a;--line:#d0d7de;"
        "--card:#ffffff;--head:#f6f8fa;--code:#f6f8fa;--sqlbg:#0d1117;"
        "--sqlfg:#e6edf3;--accent:#0969da;--err:#cf222e;--warn:#9a6700;--ok:#1a7f37;"
        "--badge:#ddf4ff;--badgefg:#0550ae;--text:#0a3069;--alias:#8250df}\n"
        "@media (prefers-color-scheme:dark){:root{--bg:#0d1117;--fg:#e6edf3;"
        "--muted:#8b949e;--line:#30363d;--card:#161b22;--head:#161b22;--code:#161b22;"
        "--sqlbg:#010409;--sqlfg:#e6edf3;--accent:#2f81f7;--err:#ff7b72;--warn:#d29922;"
        "--ok:#3fb950;--badge:#121d2f;--badgefg:#79c0ff;--text:#7ee787;--alias:#d2a8ff}}\n"
        "*{box-sizing:border-box}body{margin:0 auto;max-width:1040px;padding:2rem 1rem;"
        "font:15px/1.55 system-ui,-apple-system,Segoe UI,Roboto,sans-serif;"
        "color:var(--fg);background:var(--bg)}\n"
        "h1{font-size:1.5rem;margin:0 0 .3rem}p.lead{color:var(--muted);margin:.2rem 0 1.5rem}\n"
        "code,pre{font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace}\n"
        ".stmt{border:1px solid var(--line);border-radius:10px;margin:1.5rem 0;"
        "background:var(--card);overflow:hidden}\n"
        ".stmt>h2{font-size:.95rem;margin:0;padding:.6rem .9rem;background:var(--head);"
        "border-bottom:1px solid var(--line)}\n"
        ".stmt>h2 .num{color:var(--muted);font-weight:600;margin-right:.4rem}\n"
        "pre.sql{margin:0;padding:.8rem .9rem;background:var(--sqlbg);color:var(--sqlfg);"
        "overflow-x:auto;font-size:.85rem;white-space:pre-wrap;word-break:break-word}\n"
        ".body{padding:.4rem .9rem .9rem}\n"
        ".stage{font-size:.68rem;text-transform:uppercase;letter-spacing:.06em;"
        "font-weight:700;color:var(--muted);margin:1rem 0 .35rem}\n"
        "pre.art{margin:0;padding:.6rem .8rem;background:var(--code);border:1px solid var(--line);"
        "border-radius:6px;overflow-x:auto;font-size:.82rem;white-space:pre}\n"
        "ul.diag{margin:.2rem 0;padding-left:1.1rem}ul.diag li{margin:.15rem 0}\n"
        ".err{color:var(--err)}.warn{color:var(--warn)}.ok{color:var(--ok);font-weight:600}\n"
        ".note{color:var(--muted)}\n"
        // AST tree (file-explorer guide lines)
        "ul.ast,ul.ast ul{list-style:none;margin:0;padding:0}\n"
        "ul.ast{padding:.4rem .2rem;background:var(--code);border:1px solid var(--line);"
        "border-radius:6px;overflow-x:auto;font-size:.82rem}\n"
        "ul.ast ul{margin-left:.55rem;padding-left:.7rem;border-left:1px solid var(--line)}\n"
        "ul.ast li{position:relative;padding:1px 0 1px .7rem}\n"
        "ul.ast li::before{content:'';position:absolute;left:-.02rem;top:.72em;width:.55rem;"
        "border-top:1px solid var(--line)}\n"
        "ul.ast>li{padding-left:.2rem}ul.ast>li::before{display:none}\n"
        ".node{white-space:nowrap}\n"
        ".nt{display:inline-block;background:var(--badge);color:var(--badgefg);"
        "border-radius:4px;padding:0 .35rem;font-family:ui-monospace,monospace;font-size:.78rem}\n"
        ".ntext{color:var(--text);font-family:ui-monospace,monospace;margin-left:.4rem}\n"
        ".ntext::before{content:\"'\"}.ntext::after{content:\"'\"}\n"
        ".nalias{color:var(--alias);font-family:ui-monospace,monospace;margin-left:.4rem}\n"
        ".nalias::before{content:'['}.nalias::after{content:']'}\n"
        "</style></head><body>\n";
    h += "<h1>DB25 frontend — per-stage artifact report</h1>\n";
    h += "<p class=\"lead\">Generated by <code>corpus_report</code> from <code>" +
         esc(src) + "</code>. Each SQL statement is replayed through the frontend in "
         "order against one catalog; the artifact each stage produces is shown so it "
         "can be read and checked against the SQL. Stages: <b>parse</b> → AST · "
         "<b>analyze</b> → diagnostics · DDL → <b>catalog effect</b> · query/DML → "
         "<b>logical plan</b> → <b>optimized plan</b>.</p>\n";
    int n = 0;
    for (const Section& s : secs) {
        ++n;
        h += "<section class=\"stmt\"><h2><span class=\"num\">" + std::to_string(n) +
             "</span>" + esc(s.caption.empty() ? "statement" : s.caption) + "</h2>\n";
        h += "<pre class=\"sql\">" + esc(s.sql) + "</pre>\n<div class=\"body\">\n";
        if (!s.parse_ok) {
            h += "<div class=\"stage\">parse → AST</div><p class=\"err\">rejected "
                 "(no AST produced)</p>\n</div></section>\n";
            continue;
        }
        h += "<div class=\"stage\">parse → AST</div>\n<ul class=\"ast\">" +
             s.ast_html_s + "</ul>\n";
        if (s.is_ddl) {
            h += "<div class=\"stage\">catalog effect &nbsp;<span class=\"note\">"
                 "(execute_ddl: " + esc(s.ddl_status) + ")</span></div>\n<pre class=\"art\">";
            for (const std::string& l : s.catalog_lines) h += esc(l) + "\n";
            h += "</pre>\n</div></section>\n";
            continue;
        }
        h += "<div class=\"stage\">analyze → diagnostics</div>\n";
        if (s.diags.empty()) h += "<p class=\"ok\">clean (no diagnostics)</p>\n";
        else {
            h += "<ul class=\"diag\">";
            for (const auto& d : s.diags)
                h += "<li class=\"" + std::string(d.first ? "err" : "warn") + "\">" +
                     (d.first ? "error: " : "warning: ") + esc(d.second) + "</li>";
            h += "</ul>\n";
        }
        if (!s.plan.empty()) {
            h += "<div class=\"stage\">bind → logical plan</div>\n<pre class=\"art\">" +
                 esc(s.plan) + "</pre>\n";
            h += "<div class=\"stage\">optimize → logical plan</div>\n<pre class=\"art\">" +
                 esc(s.opt_plan) + "</pre>\n";
        } else {
            h += "<div class=\"stage\">logical plan</div>\n<p class=\"note\">" +
                 esc(s.plan_note) + "</p>\n";
        }
        h += "</div></section>\n";
    }
    h += "</body></html>\n";
    return h;
}

}  // namespace

int main(int argc, char** argv) {
    bool html = false;
    std::string path;
    for (int i = 1; i < argc; ++i) {
        std::string a = argv[i];
        if (a == "--html") html = true;
        else path = a;
    }
    if (path.empty()) {
        std::fprintf(stderr, "usage: corpus_report [--html] <statements.sql>\n");
        return 2;
    }
    std::vector<Stmt> stmts = load(path);

    std::string err;
    const std::string cat_path = "/tmp/db25_report.db25cat";
    std::remove(cat_path.c_str());
    CatalogManager mgr(cat_path, err);
    parser::Parser p;

    std::vector<Section> secs;
    for (const Stmt& st : stmts) {
        Section s;
        s.caption = st.caption;
        s.sql = st.sql;
        auto pr = p.parse(st.sql);
        if (!pr.has_value()) { secs.push_back(std::move(s)); continue; }
        s.parse_ok = true;
        ASTNode* root = pr.value();
        ast_indent(s.ast_indent_s, root, 0);
        ast_html(s.ast_html_s, root);

        if (is_ddl(root->node_type)) {
            s.is_ddl = true;
            const auto r = execute_ddl(root, mgr);
            s.ddl_status = r.ok ? "ok" : ("rejected — " + r.error);
            if (r.ok) catalog_lines(s.catalog_lines, mgr.catalog(), root);
            secs.push_back(std::move(s));
            continue;
        }

        Analyzer analyzer(mgr.catalog());
        analyzer.analyze(root);
        s.analyzed = true;
        for (const Diagnostic& d : analyzer.diagnostics())
            s.diags.emplace_back(d.severity == Severity::Error, d.message);

        plan::Binder binder(analyzer, mgr.catalog());
        plan::BindResult bound = binder.bind(root);
        if (!bound.ok || bound.root == nullptr) {
            s.plan_note = "not produced" +
                          (bound.error.empty() ? std::string() : " (" + bound.error + ")");
            secs.push_back(std::move(s));
            continue;
        }
        s.plan = plan::dump_plan(bound.root.get());
        plan::LogicalNodePtr opt = plan::optimize(std::move(bound.root));
        s.opt_plan = plan::dump_plan(opt.get());
        secs.push_back(std::move(s));
    }
    std::remove(cat_path.c_str());

    const std::string doc = html ? html_document(secs, path) : md_document(secs, path);
    std::fputs(doc.c_str(), stdout);
    return 0;
}
