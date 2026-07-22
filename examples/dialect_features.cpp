// DB25 dialect features - show the six SQL-dialect features the stack accepts
// today, each driven through parse -> analyze -> bind, then all six combined in
// a single statement.
//
//   1. hex / binary integer literals + leading-dot floats  (0xFF, 0b1010, .5)
//   2. delimited (double-quoted) identifiers                ("id", "name")
//   3. single-quote '' string escape                        ('O''Brien')
//   4. CAST type modifiers                                  (DECIMAL(10,2))
//   5. ARRAY[] constructor                                  (ARRAY[1,2,3])
//   6. COLLATE annotation                                   (x COLLATE "C")
//
// Build:  see examples/README.md  (target: db25_dialect_features)

#include "harness/harness.hpp"

#include <cstdio>
#include <string_view>

namespace h = db25::harness;

namespace {

void feature(std::string_view label, std::string_view sql) {
    h::Stages s = h::run(h::catalog(), sql);
    std::printf("  [%s] %-24.*s  %.*s\n",
                (s.parsed && s.analyze_ok && s.bind_ok) ? "OK  " : "FAIL",
                static_cast<int>(label.size()), label.data(),
                static_cast<int>(sql.size()), sql.data());
    if (!s.bind_ok) std::printf("        bind error: %s\n", s.bind_error.c_str());
}

}  // namespace

int main() {
    std::printf("DB25 dialect features (each: parse -> analyze -> bind)\n\n");

    feature("hex / binary / .5",    "SELECT id FROM users WHERE id = 0xFF OR id = 0b1010");
    feature("delimited identifier", "SELECT \"id\", \"name\" FROM users");
    feature("string '' escape",     "SELECT name FROM users WHERE name = 'O''Brien'");
    feature("CAST modifiers",       "SELECT CAST(age AS DECIMAL(10,2)) FROM users");
    feature("ARRAY[] constructor",  "SELECT ARRAY[1, 2, 3] FROM users");
    feature("COLLATE annotation",   "SELECT name COLLATE \"C\" FROM users");

    // All six at once. The bound plan shows: delimited ids resolved as columns,
    // hex/binary/.5 as typed literals, the CAST carrying its (precision, scale),
    // ARRAY and COLLATE lowered as ScalarFunctions, and the '' escape collapsed.
    const char* combined =
        "SELECT CAST(\"id\" AS DECIMAL(10,2)), "
        "       ARRAY[0xFF, 0b1010, .5], "
        "       \"name\" COLLATE \"C\" "
        "FROM users "
        "WHERE \"name\" COLLATE \"C\" = 'O''Brien' AND \"id\" = 0xFF";

    std::printf("\n  All six combined in one statement:\n    %s\n\n", combined);
    h::Stages s = h::run(h::catalog(), combined);
    std::printf("  pipeline: parsed=%d analyzed=%d bound=%d optimized=%d\n",
                s.parsed, s.analyze_ok, s.bind_ok, s.optimize_ok);
    if (s.bind_ok) {
        std::printf("  Bound plan:\n");
        std::string line;
        for (char c : s.bound_dump) {
            if (c == '\n') { std::printf("    %s\n", line.c_str()); line.clear(); }
            else line.push_back(c);
        }
        if (!line.empty()) std::printf("    %s\n", line.c_str());
    } else {
        std::printf("  bind error: %s\n", s.bind_error.c_str());
    }
    return s.bind_ok ? 0 : 1;
}
