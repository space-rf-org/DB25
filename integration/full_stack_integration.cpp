// DB25 full-stack integration test.
//
// The falsifiability harness (db25_harness / db25_gate) exercises the SELECT
// pipeline (parse -> analyze -> bind -> optimize) and pins result correctness.
// It does NOT cover DDL or DML: those are utility statements (Option C) that
// never become logical plans. This integration test closes that gap and, for
// every statement kind - DDL, DML and queries - asserts the ARTIFACT each stage
// produces against explicit expectations:
//
//   * parse    -> the AST (node structure / captured text / action flags)
//   * analyze  -> for DDL: the catalog state; for DML/queries: diagnostics
//   * persist  -> the durable snapshot (deterministic, byte-stable round-trip)
//   * session  -> the transaction state machine
//
// It is a standalone executable (its own main), independent of the mutation
// gate, so broad structural assertions are welcome here.

#include "db25/ast/node_types.hpp"
#include "db25/parser/parser.hpp"
#include "db25/semantic/analyzer.hpp"
#include "db25/semantic/catalog_manager.hpp"
#include "db25/semantic/catalog_snapshot.hpp"
#include "db25/semantic/check_eval.hpp"
#include "db25/semantic/ddl.hpp"
#include "db25/semantic/transaction.hpp"

#include <cstdio>
#include <cstring>
#include <fstream>
#include <sstream>
#include <string>

using namespace db25;
using namespace db25::semantic;

// ----------------------------------------------------------------------------
// Tiny assertion harness.
// ----------------------------------------------------------------------------
namespace {
int g_pass = 0, g_fail = 0;
const char* g_section = "";
void expect(bool ok, const std::string& what) {
    if (ok) { ++g_pass; return; }
    ++g_fail;
    std::printf("  FAIL [%s]: %s\n", g_section, what.c_str());
}
#define EXPECT(cond, what) expect((cond), (what))

// ---- AST helpers: match by the string node_type_to_string emits ----
int count_type(const ast::ASTNode* n, const char* name) {
    if (n == nullptr) return 0;
    int c = std::strcmp(ast::node_type_to_string(n->node_type), name) == 0 ? 1 : 0;
    for (const auto* k = n->first_child; k != nullptr; k = k->next_sibling)
        c += count_type(k, name);
    return c;
}
const ast::ASTNode* first_type(const ast::ASTNode* n, const char* name) {
    if (n == nullptr) return nullptr;
    if (std::strcmp(ast::node_type_to_string(n->node_type), name) == 0) return n;
    for (const auto* k = n->first_child; k != nullptr; k = k->next_sibling)
        if (const auto* h = first_type(k, name)) return h;
    return nullptr;
}
std::string txt(const ast::ASTNode* n) { return n ? std::string(n->primary_text) : ""; }
std::string nm(const ast::ASTNode* n) { return n ? std::string(n->schema_name) : ""; }
bool root_is(const ast::ASTNode* n, const char* name) {
    return n && std::strcmp(ast::node_type_to_string(n->node_type), name) == 0;
}

std::string read_file(const std::string& p) {
    std::ifstream in(p, std::ios::binary);
    std::ostringstream ss; ss << in.rdbuf(); return ss.str();
}

// ============================================================================
// SECTION 1: parser AST artifact - every statement KIND parses to the right
// top-level node and carries the expected structure.
// ============================================================================
void section_parse_artifacts(parser::Parser& p) {
    g_section = "parse";
    struct Case { const char* sql; const char* root; };
    const Case roots[] = {
        {"CREATE TABLE t (a INT)", "CreateTableStmt"},
        {"CREATE INDEX i ON t (a)", "CreateIndexStmt"},
        {"ALTER TABLE t ADD COLUMN b INT", "AlterTableStmt"},
        {"DROP TABLE t", "DropStmt"},
        {"INSERT INTO t (a) VALUES (1)", "InsertStmt"},
        {"UPDATE t SET a = 1 WHERE a = 0", "UpdateStmt"},
        {"DELETE FROM t WHERE a = 1", "DeleteStmt"},
        {"SELECT a FROM t", "SelectStmt"},
        {"SELECT a FROM t1 UNION SELECT b FROM t2", "UnionStmt"},
        {"BEGIN", "BeginStmt"},
        {"COMMIT", "CommitStmt"},
        {"SAVEPOINT s1", "SavepointStmt"},
    };
    for (const Case& c : roots) {
        auto r = p.parse(c.sql);
        EXPECT(r.has_value() && root_is(r.value(), c.root),
               std::string("root is ") + c.root + " for: " + c.sql);
    }

    // Rich CREATE TABLE: every constraint kind, named + composite, captured text.
    {
        auto r = p.parse(
            "CREATE TABLE t (a INT, b INT, c TEXT NOT NULL DEFAULT 'z', "
            "CONSTRAINT pk PRIMARY KEY (a, b), CONSTRAINT uq UNIQUE (c), "
            "CONSTRAINT fk FOREIGN KEY (a) REFERENCES par (id), "
            "CONSTRAINT ck CHECK (a > 0 AND b > 0))");
        const auto* n = r.value();
        EXPECT(count_type(n, "ColumnDefinition") == 3, "3 columns");
        EXPECT(count_type(n, "PrimaryKeyConstraint") == 1 &&
               count_type(first_type(n, "PrimaryKeyConstraint"), "Identifier") == 2,
               "composite PK, 2 cols");
        EXPECT(nm(first_type(n, "PrimaryKeyConstraint")) == "pk", "PK named");
        EXPECT(nm(first_type(n, "UniqueConstraint")) == "uq", "UNIQUE named");
        EXPECT(nm(first_type(n, "ForeignKeyConstraint")) == "fk" &&
               first_type(n, "ReferencesClause") != nullptr, "FK named + references");
        EXPECT(nm(first_type(n, "CheckConstraint")) == "ck" &&
               txt(first_type(n, "CheckConstraint")) == "a > 0 AND b > 0",
               "CHECK named + text captured");
        EXPECT(txt(first_type(n, "DefaultClause")) == "'z'", "DEFAULT text captured");
    }

    // Complex analytical query: each clause node present exactly once.
    {
        auto r = p.parse(
            "SELECT u.id, COUNT(o.oid) AS n FROM users u JOIN orders o ON u.id = o.uid "
            "WHERE u.age > (SELECT AVG(age) FROM users) GROUP BY u.id "
            "HAVING COUNT(o.oid) > 1 ORDER BY n DESC LIMIT 10");
        const auto* n = r.value();
        for (const char* k : {"JoinClause", "Subquery", "GroupByClause", "HavingClause",
                              "OrderByClause", "LimitClause", "WhereClause"})
            EXPECT(count_type(n, k) == 1, std::string("one ") + k);
        EXPECT(count_type(n, "FunctionCall") == 3, "three aggregate calls");
    }

    // CTE + window, and the rich expression grammar.
    {
        auto r = p.parse(
            "WITH r AS (SELECT id, ROW_NUMBER() OVER (PARTITION BY d ORDER BY s DESC) AS rn "
            "FROM emp) SELECT * FROM r WHERE rn = 1");
        const auto* n = r.value();
        EXPECT(count_type(n, "CTEDefinition") == 1, "CTE present");
        EXPECT(count_type(n, "WindowSpec") == 1 && count_type(n, "PartitionByClause") == 1,
               "window + partition");
        EXPECT(count_type(n, "Star") == 1, "SELECT *");
    }
    {
        auto r = p.parse(
            "SELECT CASE WHEN a > 0 THEN 'p' ELSE 'z' END, b IN (1, 2, 3), "
            "CAST(c AS INTEGER), f LIKE 'x%', g BETWEEN 1 AND 9, h IS NOT NULL FROM t");
        const auto* n = r.value();
        for (const char* k : {"CaseExpr", "InExpr", "CastExpr", "LikeExpr", "BetweenExpr",
                              "IsNullExpr"})
            EXPECT(count_type(n, k) == 1, std::string("expr ") + k);
    }

    // ALTER action flag encodings.
    {
        auto a = p.parse("ALTER TABLE t DROP CONSTRAINT uq CASCADE");
        EXPECT((first_type(a.value(), "AlterTableAction")->semantic_flags & 0x21) == 0x21,
               "DROP CONSTRAINT|CASCADE flags");
        auto b = p.parse("ALTER TABLE t ALTER COLUMN c SET NOT NULL");
        EXPECT((first_type(b.value(), "AlterTableAction")->semantic_flags & 0x08) != 0,
               "SET NOT NULL flag");
    }
}

// ============================================================================
// SECTION 2: DDL -> catalog artifact + durable snapshot.
// ============================================================================
void section_ddl_catalog(parser::Parser& p) {
    g_section = "ddl/catalog";
    const std::string path = "/tmp/db25_integ_ddl.db25cat";
    std::remove(path.c_str());
    std::string err;
    CatalogManager mgr(path, err);
    auto ddl_ok = [&](const std::string& s) {
        auto a = p.parse(s); return a.has_value() && execute_ddl(a.value(), mgr).ok;
    };
    auto ddl_bad = [&](const std::string& s) {
        auto a = p.parse(s); return a.has_value() && !execute_ddl(a.value(), mgr).ok;
    };

    EXPECT(ddl_ok("CREATE TABLE par (id INTEGER PRIMARY KEY, code TEXT, "
                  "CONSTRAINT uq_code UNIQUE (code))"), "create par");
    EXPECT(ddl_ok("CREATE TABLE t (a INTEGER NOT NULL, b INTEGER, c TEXT DEFAULT 'x', "
                  "CONSTRAINT ck_a CHECK (a >= 0), "
                  "CONSTRAINT fk_p FOREIGN KEY (c) REFERENCES par (code))"), "create t");
    EXPECT(ddl_ok("CREATE INDEX ix_b ON t (b)"), "create index");

    // ALTER surface.
    EXPECT(ddl_ok("ALTER TABLE t ADD COLUMN d DOUBLE DEFAULT 1.5"), "add column");
    EXPECT(ddl_ok("ALTER TABLE t ALTER COLUMN b SET NOT NULL"), "set not null");
    EXPECT(ddl_ok("ALTER TABLE t ALTER COLUMN c SET DEFAULT 'y'"), "set default");
    EXPECT(ddl_ok("ALTER TABLE t ADD CONSTRAINT uq_ab UNIQUE (a, b)"), "add unique");
    EXPECT(ddl_ok("ALTER TABLE t ADD CONSTRAINT ck_d CHECK (d > 0)"), "add check");

    // Catalog artifact matches expectations.
    const TableInfo* t = mgr.catalog().find_table("t");
    EXPECT(t != nullptr, "t exists");
    if (t != nullptr) {
        EXPECT(t->columns.size() == 4, "4 columns");
        EXPECT(!t->find_column("a")->nullable && !t->find_column("b")->nullable,
               "a,b NOT NULL");
        EXPECT(t->find_column("c")->default_expr == "'y'", "c default updated to 'y'");
        EXPECT(t->find_column("d")->default_expr == "1.5", "d default 1.5");
        int uq = 0, ck = 0, fk = 0;
        for (const Constraint& c : t->constraints) {
            uq += c.kind == Constraint::Kind::Unique;
            ck += c.kind == Constraint::Kind::Check;
            fk += c.kind == Constraint::Kind::ForeignKey;
        }
        EXPECT(uq == 1 && ck == 2 && fk == 1, "constraint census uq1 ck2 fk1");
    }

    // Integrity rejections and the RESTRICT/CASCADE dependency rule.
    EXPECT(ddl_bad("ALTER TABLE t ADD CONSTRAINT uq_ab UNIQUE (b)"), "reject dup name");
    EXPECT(ddl_bad("ALTER TABLE t ADD CHECK (a)"), "reject non-boolean CHECK");
    // par.id is a PRIMARY KEY column -> DROP NOT NULL on it is refused.
    EXPECT(ddl_bad("ALTER TABLE par ALTER COLUMN id DROP NOT NULL"),
           "reject DROP NOT NULL on PK column");
    // t.a is NOT NULL but not a key column -> dropping NOT NULL is allowed.
    EXPECT(ddl_ok("ALTER TABLE t ALTER COLUMN a DROP NOT NULL"), "drop not null on non-PK ok");
    EXPECT(ddl_bad("ALTER TABLE par DROP CONSTRAINT uq_code"),
           "reject drop UNIQUE depended on by FK (RESTRICT)");
    EXPECT(ddl_ok("ALTER TABLE par DROP CONSTRAINT uq_code CASCADE"),
           "CASCADE drops UNIQUE + detaches FK");

    // Snapshot determinism + durable reload.
    const std::string on_disk = read_file(path);
    {
        std::string e;
        CatalogManager mgr2(path, e);
        EXPECT(serialize_catalog(mgr2.catalog()) == on_disk,
               "snapshot deterministic (reload+reserialize == on-disk)");
        EXPECT(mgr2.catalog().find_table("t") &&
               mgr2.catalog().find_table("t")->find_column("d"),
               "durable reload recovers schema");
    }
    std::remove(path.c_str());
}

// ============================================================================
// SECTION 3: DML + queries -> analyzer diagnostics artifact.
// ============================================================================
void section_dml_diagnostics(parser::Parser& p) {
    g_section = "dml/diagnostics";
    const std::string path = "/tmp/db25_integ_dml.db25cat";
    std::remove(path.c_str());
    std::string err;
    CatalogManager mgr(path, err);
    auto ddl = [&](const std::string& s) {
        auto a = p.parse(s); return a.has_value() && execute_ddl(a.value(), mgr).ok;
    };
    ddl("CREATE TABLE users (id INTEGER NOT NULL, name TEXT, age INTEGER CHECK (age >= 0))");
    ddl("CREATE TABLE orders (oid INTEGER NOT NULL, uid INTEGER, total DOUBLE)");
    const Catalog& cat = mgr.catalog();

    auto n_code = [&](const std::string& sql, DiagnosticCode code) {
        auto a = p.parse(sql);
        if (!a.has_value()) return -1;
        Analyzer an(cat); an.analyze(a.value());
        int c = 0; for (const auto& d : an.diagnostics()) if (d.code == code) ++c;
        return c;
    };
    auto errs = [&](const std::string& sql) {
        auto a = p.parse(sql);
        if (!a.has_value()) return -1;
        Analyzer an(cat); an.analyze(a.value());
        int c = 0; for (const auto& d : an.diagnostics())
            if (d.severity == Severity::Error) ++c;
        return c;
    };

    // Clean statements -> no errors.
    EXPECT(errs("SELECT id, name FROM users WHERE age > 18") == 0, "clean SELECT");
    EXPECT(errs("SELECT u.id, o.total FROM users u JOIN orders o ON u.id = o.uid") == 0,
           "clean JOIN");
    EXPECT(errs("SELECT u.id FROM users u WHERE u.age > (SELECT AVG(age) FROM users)") == 0,
           "clean scalar subquery");
    EXPECT(errs("INSERT INTO users (id, name, age) VALUES (1, 'a', 20)") == 0, "clean INSERT");
    EXPECT(errs("UPDATE users SET name = 'x' WHERE id = 1") == 0, "clean UPDATE");
    EXPECT(errs("DELETE FROM users WHERE id = 1") == 0, "clean DELETE");

    // Each diagnostic fires exactly where expected.
    struct DCase { const char* sql; DiagnosticCode code; int n; const char* what; };
    const DCase cases[] = {
        {"SELECT * FROM ghost", DiagnosticCode::UnresolvedTable, 1, "unresolved table"},
        {"SELECT nope FROM users", DiagnosticCode::UnresolvedColumn, 1, "unresolved column"},
        {"INSERT INTO users (id, name) VALUES (1)", DiagnosticCode::InsertArityMismatch, 1,
         "arity mismatch"},
        {"INSERT INTO users (id, id) VALUES (1, 2)", DiagnosticCode::DuplicateColumn, 1,
         "duplicate target column"},
        {"INSERT INTO users (name) VALUES ('a')", DiagnosticCode::NotNullViolation, 1,
         "NOT NULL omitted"},
        {"INSERT INTO users (id, name) VALUES (NULL, 'a')", DiagnosticCode::NotNullViolation,
         1, "NOT NULL explicit NULL"},
        {"INSERT INTO users DEFAULT VALUES", DiagnosticCode::NotNullViolation, 1,
         "NOT NULL DEFAULT VALUES"},
        {"UPDATE users SET id = NULL", DiagnosticCode::NotNullViolation, 1, "NOT NULL update"},
        {"INSERT INTO users (id, age) VALUES (1, -5)", DiagnosticCode::CheckViolation, 1,
         "CHECK violation"},
        {"INSERT INTO users (id, age) VALUES (1, 5)", DiagnosticCode::CheckViolation, 0,
         "CHECK satisfied"},
        {"SELECT id FROM users WHERE COUNT(*) > 1", DiagnosticCode::AggregateInWhere, 1,
         "aggregate in WHERE"},
        {"SELECT id FROM users LIMIT -1", DiagnosticCode::InvalidLimit, 1, "invalid LIMIT"},
        {"SELECT id FROM users u, orders u", DiagnosticCode::DuplicateRelation, 1,
         "duplicate relation"},
    };
    for (const DCase& c : cases)
        EXPECT(n_code(c.sql, c.code) == c.n, c.what);
    std::remove(path.c_str());
}

// ============================================================================
// SECTION 4: constant CHECK evaluation artifact (the reference folder directly).
// ============================================================================
void section_check_eval() {
    g_section = "check-eval";
    auto bind = [](const char* name, const ast::ASTNode* lit) {
        CheckBindings b; b.emplace(name, lit); return b;
    };
    // Build literal nodes via the parser so we use real AST literals.
    parser::Parser p;
    auto lit = [&](const char* v) -> const ast::ASTNode* {
        auto r = p.parse(std::string("SELECT ") + v);
        return first_type(r.value(), "SelectList")->first_child;
    };
    EXPECT(evaluate_check("age >= 0", bind("age", lit("-5"))) == CheckResult::False,
           "-5 fails age>=0");
    EXPECT(evaluate_check("age >= 0", bind("age", lit("5"))) == CheckResult::True,
           "5 passes age>=0");
    EXPECT(evaluate_check("age >= 0", bind("age", lit("NULL"))) == CheckResult::Unknown,
           "NULL -> unknown");
    EXPECT(evaluate_check("a > 0 AND a < 10", bind("a", lit("5"))) == CheckResult::True,
           "5 in (0,10)");
    EXPECT(evaluate_check("a > 0 AND a < 10", bind("a", lit("50"))) == CheckResult::False,
           "50 not in (0,10)");
    EXPECT(evaluate_check("x BETWEEN 1 AND 9", bind("x", lit("9"))) == CheckResult::True,
           "9 in [1,9]");
    EXPECT(evaluate_check("x BETWEEN 1 AND 9", bind("x", lit("10"))) == CheckResult::False,
           "10 not in [1,9]");
}

// ============================================================================
// SECTION 5: transaction state-machine artifact.
// ============================================================================
void section_transactions(parser::Parser& p) {
    g_section = "transactions";
    auto apply = [&](TransactionManager& m, const char* sql) {
        auto a = p.parse(sql);
        return a.has_value() ? m.apply(a.value()).status : TxnResult::Status::Error;
    };
    TransactionManager m;
    EXPECT(apply(m, "BEGIN") == TxnResult::Status::Ok && m.in_transaction(), "begin");
    EXPECT(apply(m, "SAVEPOINT s1") == TxnResult::Status::Ok, "savepoint s1");
    EXPECT(apply(m, "SAVEPOINT s2") == TxnResult::Status::Ok && m.savepoint_depth() == 2,
           "savepoint s2");
    EXPECT(apply(m, "ROLLBACK TO SAVEPOINT s1") == TxnResult::Status::Ok &&
           m.savepoint_depth() == 1 && !m.has_savepoint("s2"),
           "rollback to s1 truncates");
    EXPECT(apply(m, "COMMIT") == TxnResult::Status::Ok && !m.in_transaction() &&
           m.savepoint_depth() == 0, "commit clears");
    EXPECT(apply(m, "COMMIT") == TxnResult::Status::Warning, "redundant commit is warning");
    EXPECT(apply(m, "SELECT 1") == TxnResult::Status::Error, "non-txn stmt is error");
}

}  // namespace

int main() {
    parser::Parser p;
    section_parse_artifacts(p);
    section_ddl_catalog(p);
    section_dml_diagnostics(p);
    section_check_eval();
    section_transactions(p);
    std::printf("db25_integration: %d passed, %d failed\n", g_pass, g_fail);
    return g_fail == 0 ? 0 : 1;
}
