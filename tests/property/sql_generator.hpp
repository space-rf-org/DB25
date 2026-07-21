// DB25 property-based test suite - deterministic, seed-driven SQL generator.
//
// Produces RANDOM-BUT-VALID SQL strings over the harness catalog (the tables
// users / orders / products / emp). The generator is a *pure function of an
// integer seed*: same seed => byte-identical SQL, on any machine, forever
// (FoundationDB style). This is what makes a failing property replayable - the
// property prints the seed, and re-running with that seed regenerates exactly
// the SQL that broke.
//
// Determinism rules honored here:
//   * the PRNG is a hand-written splitmix64 seeded ONLY by the caller's integer
//     seed - no std::random_device, no clock, no global RNG, no hashing of
//     addresses. Every bit of entropy is a pure function of `seed`.
//   * the generator reads the *actual* column list of each catalog table at
//     construction (types + nullability), so it stays correct if the harness
//     catalog changes its columns; it never hard-codes a schema.
//
// Grammar coverage (biased toward the interesting / pessimistic - lots of NULLs,
// subqueries, and decorrelatable shapes, because those are where an optimizer
// rewrite is most likely to be wrong):
//   * SELECT lists: qualified column refs, arithmetic, CASE, scalar aggregates,
//     and correlated scalar aggregate subqueries (the LEFT-JOIN decorrelation
//     shape).
//   * FROM with 0..2 joins (INNER / LEFT) on real integer key columns
//     (foreign-key-like pairs inferred from the catalog, e.g. users.id =
//     orders.user_id).
//   * optional WHERE: AND/OR trees of comparisons, IS [NOT] NULL, LIKE,
//     BETWEEN, `x [NOT] IN (subquery)`, and [NOT] EXISTS (correlated) subqueries.
//   * optional GROUP BY + a simple HAVING over an aggregate.
//   * optional DISTINCT / ORDER BY / LIMIT / OFFSET.
//   * nested correlated subqueries to a caller-controlled depth.
//
// The generator is header-only and -fno-exceptions clean (no throwing STL
// paths: only operator[] guarded by modulo, std::to_string, string append).

#pragma once

#include "db25/ast/node_types.hpp"        // db25::ast::DataType
#include "db25/semantic/catalog.hpp"      // InMemoryCatalog / TableInfo / ColumnInfo

#include <cstdint>
#include <string>
#include <vector>

namespace db25::proptest {

using db25::ast::DataType;

// ---------------------------------------------------------------------------
// splitmix64 - a tiny, fully deterministic PRNG. Seeded solely by the integer
// `seed`; identical output for identical seed on every platform.
// ---------------------------------------------------------------------------
class Rng {
public:
    explicit Rng(std::uint64_t seed) : state_(seed) {}

    std::uint64_t next() {
        std::uint64_t z = (state_ += 0x9E3779B97F4A7C15ULL);
        z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ULL;
        z = (z ^ (z >> 27)) * 0x94D049BB133111EBULL;
        return z ^ (z >> 31);
    }

    // Uniform integer in [lo, hi] (inclusive). lo >= hi returns lo.
    int between(int lo, int hi) {
        if (hi <= lo) return lo;
        const std::uint64_t span = static_cast<std::uint64_t>(hi - lo) + 1;
        return lo + static_cast<int>(next() % span);
    }

    // True with probability pct/100.
    bool chance(int pct) { return static_cast<int>(next() % 100) < pct; }

    // Index in [0, n) (n must be > 0).
    std::size_t index(std::size_t n) { return static_cast<std::size_t>(next() % n); }

private:
    std::uint64_t state_;
};

// ---------------------------------------------------------------------------
// Catalog model captured up front (name / type / nullability per column).
// ---------------------------------------------------------------------------
struct GenColumn {
    std::string name;
    DataType type = DataType::Unknown;
    bool nullable = true;
};
struct GenTable {
    std::string name;
    std::vector<GenColumn> cols;
};

enum class Cat { Numeric, Integerish, String, Other };

inline Cat categorize(DataType t) {
    switch (t) {
        case DataType::TinyInt:
        case DataType::SmallInt:
        case DataType::Integer:
        case DataType::BigInt:
            return Cat::Integerish;
        case DataType::Decimal:
        case DataType::Real:
        case DataType::Double:
            return Cat::Numeric;  // numeric but not an integer key
        case DataType::Char:
        case DataType::VarChar:
        case DataType::Text:
            return Cat::String;
        default:
            return Cat::Other;
    }
}
inline bool is_numeric(DataType t) {
    const Cat c = categorize(t);
    return c == Cat::Numeric || c == Cat::Integerish;
}
inline bool is_integerish(DataType t) { return categorize(t) == Cat::Integerish; }
inline bool is_string(DataType t) { return categorize(t) == Cat::String; }

// ---------------------------------------------------------------------------
// The generator.
// ---------------------------------------------------------------------------
class SqlGenerator {
public:
    // `complexity` (0..3+) scales join count, predicate breadth, subquery
    // frequency, and correlation depth.
    SqlGenerator(const db25::semantic::InMemoryCatalog& cat, std::uint64_t seed,
                 int complexity)
        : rng_(seed ^ 0xD1B54A32D192ED03ULL), complexity_(complexity) {
        load_tables(cat);
    }

    // Emit one SELECT statement. Pure function of the constructor seed.
    std::string generate() {
        alias_counter_ = 0;
        from_.clear();
        if (tables_.empty()) return "SELECT 1";  // degenerate catalog

        std::string sql = build_select(/*outer=*/{}, /*depth=*/0);
        return sql;
    }

private:
    // One in-scope relation: an alias bound to a catalog table.
    struct Rel {
        std::string alias;
        const GenTable* tbl;
    };

    // ----- catalog capture ------------------------------------------------
    void load_tables(const db25::semantic::InMemoryCatalog& cat) {
        static const char* kNames[] = {"users", "orders", "products", "emp"};
        for (const char* nm : kNames) {
            const db25::semantic::TableInfo* ti = cat.find_table(nm);
            if (!ti) continue;
            GenTable gt;
            gt.name = ti->name;
            for (const auto& c : ti->columns) {
                gt.cols.push_back(GenColumn{c.name, c.type, c.nullable});
            }
            if (!gt.cols.empty()) tables_.push_back(std::move(gt));
        }
    }

    const GenTable& pick_table() { return tables_[rng_.index(tables_.size())]; }

    std::string fresh_alias() { return "t" + std::to_string(alias_counter_++); }

    // A column of a relation matching a category. Cat::Other matches anything
    // (and always returns a column for a non-empty table); a specific category
    // returns nullptr when the table has no such column, so callers can fall
    // back rather than emit a type-invalid expression (e.g. SUM(<string>)).
    const GenColumn* pick_col(const GenTable* t, Cat want, bool prefer_nullable) {
        std::vector<const GenColumn*> match, nullable_match;
        for (const auto& c : t->cols) {
            const bool ok = (want == Cat::Other) ? true : (categorize(c.type) == want);
            if (ok) {
                match.push_back(&c);
                if (c.nullable) nullable_match.push_back(&c);
            }
        }
        if (match.empty()) return nullptr;
        if (prefer_nullable && !nullable_match.empty() && rng_.chance(70))
            return nullable_match[rng_.index(nullable_match.size())];
        return match[rng_.index(match.size())];
    }

    static std::string singular(const std::string& s) {
        if (s.size() > 1 && s.back() == 's') return s.substr(0, s.size() - 1);
        return s;
    }
    static const GenColumn* find_named(const GenTable* t, const std::string& n) {
        for (const auto& c : t->cols)
            if (c.name == n) return &c;
        return nullptr;
    }
    static const GenColumn* first_int(const GenTable* t) {
        const GenColumn* any = nullptr;
        for (const auto& c : t->cols) {
            if (is_integerish(c.type)) {
                if (c.name == "id") return &c;
                if (!any) any = &c;
            }
        }
        return any;
    }

    // Infer a type-correct equi-join predicate between two relations. Prefers a
    // foreign-key-like pair (users.id = orders.user_id), falls back to any two
    // integer columns, and finally to the first columns (rare; may not analyze,
    // in which case the whole query is simply skipped by the properties).
    std::string join_condition(const Rel& l, const Rel& r) {
        // FK: r has "<singular(l)>_id", l has "id".
        {
            const GenColumn* rfk = find_named(r.tbl, singular(l.tbl->name) + "_id");
            const GenColumn* lid = find_named(l.tbl, "id");
            if (rfk && lid && is_integerish(rfk->type) && is_integerish(lid->type))
                return l.alias + ".id = " + r.alias + "." + rfk->name;
        }
        {
            const GenColumn* lfk = find_named(l.tbl, singular(r.tbl->name) + "_id");
            const GenColumn* rid = find_named(r.tbl, "id");
            if (lfk && rid && is_integerish(lfk->type) && is_integerish(rid->type))
                return l.alias + "." + lfk->name + " = " + r.alias + ".id";
        }
        const GenColumn* li = first_int(l.tbl);
        const GenColumn* ri = first_int(r.tbl);
        if (li && ri) return l.alias + "." + li->name + " = " + r.alias + "." + ri->name;
        return l.alias + "." + l.tbl->cols[0].name + " = " + r.alias + "." +
               r.tbl->cols[0].name;
    }

    // ----- literals -------------------------------------------------------
    std::string num_literal() {
        if (rng_.chance(30)) {
            // a small decimal
            return std::to_string(rng_.between(0, 99)) + "." +
                   std::to_string(rng_.between(0, 9));
        }
        int v = rng_.between(-5, 500);
        return std::to_string(v);
    }
    std::string str_literal() {
        static const char* pool[] = {"'a'", "'abc'", "'x%'", "'foo'", "''", "'bar'"};
        return pool[rng_.index(6)];
    }
    std::string literal_for(DataType t) {
        if (is_string(t)) return str_literal();
        return num_literal();
    }

    // ----- expression fragments ------------------------------------------
    std::string colref(const Rel& r, const GenColumn* c) {
        return r.alias + "." + c->name;
    }

    // A scalar expression over the given scope (used in SELECT items and CASE
    // branches). May recurse into a correlated scalar subquery.
    std::string scalar_expr(const std::vector<Rel>& scope, int depth) {
        const int k = rng_.between(0, 9);
        const Rel& r = scope[rng_.index(scope.size())];

        // Correlated scalar aggregate subquery (decorrelation target). Only when
        // budget remains and there is an integer key to correlate on.
        if (k <= 1 && depth < max_depth() && has_int_key(r)) {
            std::string sub = correlated_scalar_subquery(scope, depth + 1);
            if (!sub.empty()) return sub;
        }

        if (k <= 4) {  // plain column ref (bias toward nullable columns)
            const GenColumn* c = pick_col(r.tbl, Cat::Other, /*prefer_nullable=*/true);
            return colref(r, c);
        }
        if (k <= 6) {  // arithmetic over a numeric column
            const GenColumn* c = pick_col(r.tbl, Cat::Numeric, false);
            if (!c) c = pick_col(r.tbl, Cat::Integerish, false);
            if (!c) return colref(r, pick_col(r.tbl, Cat::Other, true));
            static const char* ops[] = {"+", "-", "*"};
            const char* op = ops[rng_.index(3)];
            if (rng_.chance(50)) {
                const GenColumn* c2 = pick_col(r.tbl, Cat::Numeric, false);
                if (!c2) c2 = pick_col(r.tbl, Cat::Integerish, false);
                if (c2) return "(" + colref(r, c) + " " + op + " " + colref(r, c2) + ")";
            }
            return "(" + colref(r, c) + " " + op + " " + num_literal() + ")";
        }
        if (k <= 8) {  // CASE
            return "CASE WHEN " + predicate(scope, depth) + " THEN " + num_literal() +
                   " ELSE " + num_literal() + " END";
        }
        // bare aggregate is not valid in a scalar (non-grouped) list; use a col
        const GenColumn* c = pick_col(r.tbl, Cat::Other, true);
        return colref(r, c);
    }

    bool has_int_key(const Rel& r) { return first_int(r.tbl) != nullptr; }

    // (SELECT AGG(inner.num) FROM inner WHERE inner.k = outer.k) - a correlated
    // scalar aggregate subquery, the LEFT-JOIN decorrelation shape. AGG is one
    // of SUM/MIN/MAX/AVG (all NULL over the empty set, so the decorrelation is
    // value-preserving).
    std::string correlated_scalar_subquery(const std::vector<Rel>& outer, int depth) {
        (void)depth;  // this shape does not nest further; kept for call symmetry
        const Rel& o = outer[rng_.index(outer.size())];
        const GenColumn* okey = first_int(o.tbl);
        if (!okey) return {};

        // Choose an inner table that has an integer key AND a numeric column.
        const GenTable* inner = nullptr;
        const GenColumn* inner_key = nullptr;
        const GenColumn* inner_num = nullptr;
        for (int tries = 0; tries < static_cast<int>(tables_.size()); ++tries) {
            const GenTable* cand = &tables_[rng_.index(tables_.size())];
            const GenColumn* ik = first_int(cand);
            const GenColumn* in = nullptr;
            for (const auto& c : cand->cols)
                if (is_numeric(c.type)) { in = &c; break; }
            if (ik && in) { inner = cand; inner_key = ik; inner_num = in; break; }
        }
        if (!inner) return {};

        std::string ia = fresh_alias();
        static const char* aggs[] = {"SUM", "MIN", "MAX", "AVG"};
        const char* agg = aggs[rng_.index(4)];
        std::string s = "(SELECT " + std::string(agg) + "(" + ia + "." + inner_num->name +
                        ") FROM " + inner->name + " " + ia + " WHERE " + ia + "." +
                        inner_key->name + " = " + o.alias + "." + okey->name;
        // occasionally add a local filter inside the subquery
        if (rng_.chance(30)) {
            const GenColumn* c = pick_col(inner, Cat::Other, true);
            s += " AND " + ia + "." + c->name + " IS NOT NULL";
        }
        s += ")";
        return s;
    }

    // A boolean predicate over `scope`. Recurses into IN / EXISTS subqueries.
    std::string predicate(const std::vector<Rel>& scope, int depth) {
        const int roll = rng_.between(0, 99);

        // EXISTS / NOT EXISTS correlated subquery.
        if (roll < 18 && depth < max_depth()) {
            std::string e = exists_subquery(scope, depth + 1);
            if (!e.empty()) return e;
        }
        // x [NOT] IN (subquery)
        if (roll < 30 && depth < max_depth()) {
            std::string e = in_subquery(scope, depth + 1);
            if (!e.empty()) return e;
        }

        const Rel& r = scope[rng_.index(scope.size())];

        if (roll < 55) {  // IS [NOT] NULL - bias toward nullable columns for NULLs
            const GenColumn* c = pick_col(r.tbl, Cat::Other, /*prefer_nullable=*/true);
            return colref(r, c) + (rng_.chance(55) ? " IS NULL" : " IS NOT NULL");
        }
        if (roll < 70) {  // BETWEEN over a numeric column
            const GenColumn* c = pick_col(r.tbl, Cat::Numeric, false);
            if (!c) c = pick_col(r.tbl, Cat::Integerish, false);
            if (c) {
                int lo = rng_.between(0, 50), hi = lo + rng_.between(1, 100);
                return colref(r, c) + " BETWEEN " + std::to_string(lo) + " AND " +
                       std::to_string(hi);
            }
        }
        if (roll < 80) {  // LIKE over a string column
            const GenColumn* c = pick_col(r.tbl, Cat::String, true);
            if (c) return colref(r, c) + (rng_.chance(50) ? " LIKE " : " NOT LIKE ") +
                          "'%a%'";
        }
        // comparison
        const GenColumn* c = pick_col(r.tbl, Cat::Other, true);
        static const char* nops[] = {"=", "<>", "<", "<=", ">", ">="};
        if (is_string(c->type)) {
            const char* op = rng_.chance(50) ? "=" : "<>";
            return colref(r, c) + " " + op + " " + str_literal();
        }
        const char* op = nops[rng_.index(6)];
        // column-to-column sometimes (same relation, numeric)
        if (rng_.chance(30)) {
            const GenColumn* c2 = pick_col(r.tbl, categorize(c->type), false);
            if (c2 && is_numeric(c->type) && is_numeric(c2->type))
                return colref(r, c) + " " + op + " " + colref(r, c2);
        }
        return colref(r, c) + " " + op + " " + literal_for(c->type);
    }

    // Combine 1..N predicates with AND / OR.
    std::string predicate_tree(const std::vector<Rel>& scope, int depth) {
        int n = rng_.between(1, 1 + complexity_);
        std::string s = predicate(scope, depth);
        for (int i = 1; i < n; ++i) {
            const char* conj = rng_.chance(65) ? " AND " : " OR ";
            s += conj + predicate(scope, depth);
        }
        return s;
    }

    // [NOT] EXISTS (SELECT 1 FROM inner WHERE inner.k = outer.k [AND local...])
    std::string exists_subquery(const std::vector<Rel>& outer, int depth) {
        const Rel& o = outer[rng_.index(outer.size())];
        const GenColumn* okey = first_int(o.tbl);
        if (!okey) return {};
        const GenTable* inner = nullptr;
        const GenColumn* ikey = nullptr;
        for (int t = 0; t < static_cast<int>(tables_.size()); ++t) {
            const GenTable* cand = &tables_[rng_.index(tables_.size())];
            const GenColumn* ik = first_int(cand);
            if (ik) { inner = cand; ikey = ik; break; }
        }
        if (!inner) return {};

        std::string ia = fresh_alias();
        std::vector<Rel> inner_scope = outer;   // correlation: outer stays visible
        inner_scope.push_back(Rel{ia, inner});

        std::string s = (rng_.chance(35) ? "NOT EXISTS (" : "EXISTS (");
        s += "SELECT 1 FROM " + inner->name + " " + ia + " WHERE " + ia + "." +
             ikey->name + " = " + o.alias + "." + okey->name;
        // optionally nest a deeper correlated predicate
        if (depth < max_depth() && rng_.chance(35)) {
            s += " AND " + predicate(inner_scope, depth + 1);
        } else if (rng_.chance(40)) {
            const GenColumn* c = pick_col(inner, Cat::Other, true);
            s += " AND " + ia + "." + c->name + " IS NOT NULL";
        }
        s += ")";
        return s;
    }

    // outer.col [NOT] IN (SELECT inner.col2 FROM inner [WHERE ...])
    std::string in_subquery(const std::vector<Rel>& outer, int depth) {
        const Rel& o = outer[rng_.index(outer.size())];
        const GenColumn* probe = pick_col(o.tbl, Cat::Integerish, false);
        if (!probe) return {};
        const GenTable* inner = nullptr;
        const GenColumn* iproj = nullptr;
        for (int t = 0; t < static_cast<int>(tables_.size()); ++t) {
            const GenTable* cand = &tables_[rng_.index(tables_.size())];
            const GenColumn* ip = first_int(cand);
            if (ip) { inner = cand; iproj = ip; break; }
        }
        if (!inner) return {};

        std::string ia = fresh_alias();
        std::string s = colref(o, probe) + (rng_.chance(30) ? " NOT IN (" : " IN (");
        s += "SELECT " + ia + "." + iproj->name + " FROM " + inner->name + " " + ia;
        if (rng_.chance(45)) {
            std::vector<Rel> inner_scope{Rel{ia, inner}};
            s += " WHERE " + predicate(inner_scope, depth + 1);
        }
        s += ")";
        return s;
    }

    // ----- FROM / full query ----------------------------------------------
    // Build a FROM clause into `from_` (the query's relation scope) and return
    // its SQL text.
    std::string build_from() {
        const GenTable& base = pick_table();
        Rel b{fresh_alias(), &base};
        from_.push_back(b);
        std::string sql = base.name + " " + b.alias;

        int max_joins = complexity_ >= 1 ? 2 : 0;
        if (tables_.size() < 2) max_joins = 0;
        int njoins = 0;
        for (int i = 0; i < max_joins; ++i) {
            if (!rng_.chance(complexity_ >= 2 ? 70 : 45)) break;
            ++njoins;
        }
        for (int j = 0; j < njoins; ++j) {
            const GenTable& rt = pick_table();
            Rel r{fresh_alias(), &rt};
            const Rel left = from_[rng_.index(from_.size())];
            const char* jt = rng_.chance(55) ? "INNER JOIN" : "LEFT JOIN";
            sql += " " + std::string(jt) + " " + rt.name + " " + r.alias + " ON " +
                   join_condition(left, r);
            from_.push_back(r);
        }
        return sql;
    }

    // The core SELECT builder. `outer` is any enclosing scope (empty at top);
    // `depth` gates subquery nesting.
    std::string build_select(const std::vector<Rel>& outer, int depth) {
        (void)outer;
        std::vector<Rel> saved = from_;
        from_.clear();
        std::string from_sql = build_from();
        std::vector<Rel> scope = from_;

        // Decide the query shape.
        const bool grouped = complexity_ >= 1 && rng_.chance(30) && has_numeric(scope);
        const bool pure_agg =
            !grouped && rng_.chance(12) && has_numeric(scope);  // whole-table aggregate
        const bool distinct = !grouped && !pure_agg && rng_.chance(20);

        std::string sql = "SELECT ";
        if (distinct) sql += "DISTINCT ";

        int out_cols = 0;
        std::vector<std::string> group_keys;

        if (grouped) {
            // group keys (1..2) then aggregates (1..2); SELECT list is exactly
            // those, keeping the query analyzer-legal.
            int nkeys = rng_.between(1, 2);
            for (int i = 0; i < nkeys; ++i) {
                const Rel& r = scope[rng_.index(scope.size())];
                const GenColumn* c = pick_col(r.tbl, Cat::Other, true);
                std::string ref = colref(r, c);
                group_keys.push_back(ref);
                if (out_cols) sql += ", ";
                sql += ref + " AS c" + std::to_string(out_cols++);
            }
            int naggs = rng_.between(1, 2);
            for (int i = 0; i < naggs; ++i) {
                sql += ", " + aggregate_expr(scope) + " AS c" + std::to_string(out_cols++);
            }
        } else if (pure_agg) {
            int naggs = rng_.between(1, 3);
            for (int i = 0; i < naggs; ++i) {
                if (out_cols) sql += ", ";
                sql += aggregate_expr(scope) + " AS c" + std::to_string(out_cols++);
            }
        } else {
            int nitems = rng_.between(1, 2 + complexity_);
            for (int i = 0; i < nitems; ++i) {
                if (out_cols) sql += ", ";
                sql += scalar_expr(scope, depth) + " AS c" + std::to_string(out_cols++);
            }
        }

        sql += " FROM " + from_sql;

        // WHERE
        if (rng_.chance(grouped ? 45 : 70)) {
            sql += " WHERE " + predicate_tree(scope, depth);
        }

        // GROUP BY + HAVING
        if (grouped) {
            sql += " GROUP BY ";
            for (std::size_t i = 0; i < group_keys.size(); ++i) {
                if (i) sql += ", ";
                sql += group_keys[i];
            }
            if (rng_.chance(45)) {
                static const char* cops[] = {">", ">=", "<", "<=", "="};
                sql += " HAVING " + aggregate_expr(scope) + " " + cops[rng_.index(5)] +
                       " " + std::to_string(rng_.between(0, 100));
            }
        }

        // ORDER BY (by output ordinal - always legal)
        if (out_cols > 0 && rng_.chance(35)) {
            sql += " ORDER BY " + std::to_string(rng_.between(1, out_cols));
            if (rng_.chance(50)) sql += rng_.chance(50) ? " DESC" : " ASC";
        }

        // LIMIT / OFFSET
        if (rng_.chance(30)) {
            sql += " LIMIT " + std::to_string(rng_.between(0, 100));
            if (rng_.chance(30)) sql += " OFFSET " + std::to_string(rng_.between(0, 20));
        }

        from_ = saved;
        return sql;
    }

    std::string aggregate_expr(const std::vector<Rel>& scope) {
        const int k = rng_.between(0, 5);
        if (k == 0) return "COUNT(*)";
        const Rel& r = scope[rng_.index(scope.size())];
        if (k == 1) {
            const GenColumn* c = pick_col(r.tbl, Cat::Other, true);
            return std::string(rng_.chance(40) ? "COUNT(DISTINCT " : "COUNT(") +
                   colref(r, c) + ")";
        }
        const GenColumn* c = pick_col(r.tbl, Cat::Numeric, false);
        if (!c) c = pick_col(r.tbl, Cat::Integerish, false);
        if (!c) return "COUNT(*)";
        static const char* aggs[] = {"SUM", "AVG", "MIN", "MAX"};
        return std::string(aggs[rng_.index(4)]) + "(" + colref(r, c) + ")";
    }

    bool has_numeric(const std::vector<Rel>& scope) {
        for (const auto& r : scope)
            for (const auto& c : r.tbl->cols)
                if (is_numeric(c.type)) return true;
        return false;
    }

    int max_depth() const { return complexity_ >= 3 ? 3 : (complexity_ >= 2 ? 2 : 1); }

    Rng rng_;
    int complexity_;
    std::vector<GenTable> tables_;
    std::vector<Rel> from_;
    int alias_counter_ = 0;
};

// Convenience: derive a query purely from a seed. The complexity is itself a
// deterministic function of the seed, so seed alone reproduces the query.
inline std::string generate_query(const db25::semantic::InMemoryCatalog& cat,
                                  std::uint64_t seed) {
    const int complexity = static_cast<int>(seed % 4);  // 0..3, deterministic
    SqlGenerator gen(cat, seed, complexity);
    return gen.generate();
}

}  // namespace db25::proptest
