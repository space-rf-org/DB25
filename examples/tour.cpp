// DB25 guided tour - drive the whole pipeline for five queries of increasing
// difficulty and print what each stage produces.
//
// The umbrella entry point is db25::harness::run(catalog, sql): it runs
// parse -> analyze -> bind -> optimize and hands back a Stages value that owns
// both the bound and optimized logical plans. db25::harness::eval() then runs a
// plan over concrete in-memory rows with three-valued (NULL) logic, so we can
// print an actual result table - the engine has no executor of its own yet, and
// the reference evaluator stands in for one here.
//
// Build:  see examples/README.md  (target: db25_tour)
// Run:    ./build/examples/db25_tour

#include "harness/harness.hpp"

#include <cstdio>
#include <string>
#include <string_view>
#include <vector>

namespace h = db25::harness;

namespace {

// Render an evaluated table as aligned rows. NULL prints as the SQL keyword.
void print_table(const h::Table& t) {
    if (t.empty()) {
        std::printf("    (0 rows)\n");
        return;
    }
    for (const auto& row : t) {
        std::printf("    ");
        for (std::size_t i = 0; i < row.size(); ++i) {
            std::printf("%s%s", i ? " | " : "", h::value_to_string(row[i]).c_str());
        }
        std::printf("\n");
    }
    std::printf("    (%zu row%s)\n", t.size(), t.size() == 1 ? "" : "s");
}

// Indent a multi-line plan dump under a heading.
void print_block(const char* heading, const std::string& body) {
    std::printf("  %s\n", heading);
    std::string line;
    for (char c : body) {
        if (c == '\n') { std::printf("    %s\n", line.c_str()); line.clear(); }
        else line.push_back(c);
    }
    if (!line.empty()) std::printf("    %s\n", line.c_str());
}

void walk(int level, std::string_view title, std::string_view sql) {
    std::printf("\n============================================================\n");
    std::printf("Level %d - %.*s\n", level, static_cast<int>(title.size()), title.data());
    std::printf("============================================================\n");
    std::printf("  SQL: %.*s\n\n", static_cast<int>(sql.size()), sql.data());

    // Stage 1-4: tokenize -> parse -> analyze -> bind, then Stage 5: optimize.
    h::Stages s = h::run(h::catalog(), sql);
    std::printf("  pipeline: parsed=%d  analyzed=%d  bound=%d  optimized=%d\n",
                s.parsed, s.analyze_ok, s.bind_ok, s.optimize_ok);
    if (!s.bind_ok) {
        std::printf("  bind error: %s\n", s.bind_error.c_str());
        return;
    }

    print_block("Bound logical plan (parse->analyze->bind):", s.bound_dump);
    print_block("Optimized logical plan (after optimize()):", s.optimized_dump);

    // Stage 6: evaluate the optimized plan over the shipped sample rows.
    std::printf("  Result over sample data:\n");
    auto out = h::eval(s.optimized, h::data());
    if (!out) {
        std::printf("    (reference evaluator does not implement this shape)\n");
    } else {
        print_table(*out);
    }
}

}  // namespace

int main() {
    std::printf("DB25 guided tour - five queries, whole pipeline\n");
    std::printf("Catalog: users(id,name,age,city,manager_id), "
                "orders(id,user_id,product_id,amount,status),\n"
                "         products(id,name,price,category), "
                "emp(id,name,mgr_id,dept_id,salary)\n");

    // Level 1 - the simplest query: a projection over a base table scan.
    walk(1, "Scan + Project",
         "SELECT id, name FROM users");

    // Level 2 - a WHERE clause becomes a Filter; NULL age is excluded (3VL).
    walk(2, "Filter (WHERE) with three-valued logic",
         "SELECT name, age FROM users WHERE age >= 18");

    // Level 3 - GROUP BY becomes an Aggregate (group keys ++ aggregates).
    walk(3, "Aggregate (GROUP BY / COUNT)",
         "SELECT city, COUNT(*) AS n FROM users GROUP BY city");

    // Level 4 - a join, and a hex literal (0x0A == 10) in the filter. The
    // optimizer pushes the single-table predicate below the join.
    walk(4, "Join + predicate pushdown (with a hex literal)",
         "SELECT u.name, o.amount "
         "FROM users u JOIN orders o ON u.id = o.user_id "
         "WHERE o.amount >= 0x0A");

    // Level 5 - a correlated scalar subquery (per-user order count) with a hex
    // literal (0x12 == 18) in the outer filter. The optimizer decorrelates the
    // subquery into a join+aggregate.
    walk(5, "Correlated scalar subquery + decorrelation",
         "SELECT u.name, "
         "       (SELECT COUNT(*) FROM orders o WHERE o.user_id = u.id) AS orders "
         "FROM users u "
         "WHERE u.age >= 0x12");

    std::printf("\nDone.\n");
    return 0;
}
