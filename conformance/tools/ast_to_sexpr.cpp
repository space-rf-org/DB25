/*
 * DB25 conformance — reference canonical-AST serializer (bootstrap).
 *
 * Emits the parser-agnostic canonical S-expression defined in
 * ../README.md for a single SQL statement. Used to (a) freeze positive-corpus
 * goldens and (b) demonstrate the format. It links the existing DB25 parser as
 * a REFERENCE producer; the canonical format itself is independent of any one
 * parser (Track A or a Track-B challenge entry must emit byte-identical text).
 *
 * Usage:   ast_to_sexpr "<sql>"        (or SQL on stdin)
 * Output:  ACCEPT line = the canonical S-expr;   REJECT <message> on error.
 *          A second stderr line reports trailing_tokens (silent-truncation
 *          signal) — informational, not part of the golden.
 *
 * Build (against a built db25-sql-parser tree; see README "Running"):
 *   g++ -std=c++23 -O2 -fno-exceptions \
 *       -I $P/include -I $P/external/tokenizer/include \
 *       ast_to_sexpr.cpp $P/build/libdb25parser.a $P/build/libdb25arena.a -o ast_to_sexpr
 *
 * TODO(canonicalize): map DB25 NodeType spellings to a formalism-neutral
 * vocabulary (e.g. BinaryExpr "=" -> (= ...)). For bootstrap we emit the
 * DB25 node-type names verbatim so the format is real and testable today.
 */
#include "db25/parser/parser.hpp"
#include "db25/ast/ast_node.hpp"
#include "db25/ast/node_types.hpp"
#include <cstdio>
#include <string>
#include <string_view>

using namespace db25;
using namespace db25::ast;

static void emit_escaped(const std::string_view s) {
    std::fputc('"', stdout);
    for (const char c : s) {
        if (c == '"' || c == '\\') std::fputc('\\', stdout);
        std::fputc(c, stdout);
    }
    std::fputc('"', stdout);
}

static void emit(const ASTNode* n) {
    std::fputc('(', stdout);
    std::fputs(node_type_to_string(n->node_type), stdout);
    if (!n->primary_text.empty()) { std::fputc(' ', stdout); emit_escaped(n->primary_text); }
    if (!n->schema_name.empty())  { std::fputs(" :schema ", stdout);  emit_escaped(n->schema_name); }
    if (!n->catalog_name.empty()) { std::fputs(" :catalog ", stdout); emit_escaped(n->catalog_name); }
    for (auto* c = n->first_child; c; c = c->next_sibling) { std::fputc(' ', stdout); emit(c); }
    std::fputc(')', stdout);
}

int main(int argc, char** argv) {
    std::string sql;
    if (argc > 1) {
        sql = argv[1];
    } else {
        char buf[65536]; size_t r;
        while ((r = std::fread(buf, 1, sizeof buf, stdin)) > 0) sql.append(buf, r);
    }
    parser::Parser p;
    auto res = p.parse(sql);
    if (!res) {
        const auto& e = res.error();
        std::printf("REJECT %s (L%u:C%u)\n", e.message.c_str(), e.line, e.column);
        return 1;
    }
    emit(res.value());
    std::fputc('\n', stdout);
    std::fprintf(stderr, "trailing_tokens=%zu\n", p.trailing_token_count());
    return 0;
}
