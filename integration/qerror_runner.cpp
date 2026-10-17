// qerror_runner: how wrong is the cardinality estimate, measured against the
// rows the query actually produces.
//
// WHY THIS EXISTS. The planner's estimates were compared against PostgreSQL and
// DuckDB once, by hand, for a paper. That comparison needs two external engines
// and produces a number with no pass/fail, so it stays a lab exercise. But half
// of it needs no external engine at all: DB25 estimates a row count, and the
// reference evaluator in this repo produces the actual rows. The difference
// between those two is the planner's own accuracy, it is computable here, and
// NOTHING was watching it - a cost-model change could halve the quality of
// every estimate and the whole suite would stay green, because every test in it
// asserts on RESULTS, which estimates do not affect.
//
// THE METRIC. q-error, as in Leis et al., "How Good Are Query Optimizers,
// Really?": for one query, max(est/act, act/est) - a symmetric factor-of-wrong
// where 1.0 is exact, and 2.0 means "out by 2x" whichever way it erred. The
// headline is the GEOMETRIC mean over the query set, because these are ratios:
// an arithmetic mean lets one wild overestimate drown out everything else,
// while the geometric mean treats "4x too high" and "4x too low" as equally bad.
//
// WHAT IS DELIBERATELY EXCLUDED. A query that returns no rows. q-error is a
// RATIO, and with act = 0 it is either undefined or infinite depending on how
// you fudge it. This project has already been bitten by a measurement that
// silently counted empty results as agreement (see scripts/vacuity.sh), so
// empty-result queries are counted and reported separately rather than folded
// in with a convenient epsilon.
#include "harness/harness.hpp"

#include "db25/physical/lowering.hpp"
#include "db25/physical/cost.hpp"
#include "db25/physical/spec.hpp"
#include "db25/physical/statistics.hpp"

#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <cstdint>
#include <string>
#include <variant>
#include <vector>

namespace {

using db25::harness::Table;

// The queries. Chosen to span the shapes whose estimates come from DIFFERENT
// parts of the model - a scan, a filter, each join flavour, an aggregate, a
// semi-join - so that a regression in one rule cannot hide behind the others.
// WHAT THE FIRST RUN FOUND, and what came of it. Two cases - filter_eq and
// join_filtered, both equality against a STRING literal - estimated ~0 rows
// where the answers are 2 and 3, for an 80x and an 86x error. The cause was in
// db25-physical-plan's selectivity_of(): it read the column statistics and then
// discarded them, because one guard
//
//     if (!literal_number(*other, v)) return equality ? defaults.equality : ...
//
// covered two different requirements. The RANGE arithmetic needs a number, to
// interpolate against a numeric min/max; ndv-based EQUALITY does not. So every
// text equality was priced at the 0.005 Selinger default however good the
// statistics were.
//
// Fixed in db25-physical-plan#54, which narrowed the guard to the case it was
// written for - a comparison between two COLUMNS - and left literal_number
// gating only the range branch. The geometric mean went 3.233x -> 1.267x, and
// filter_eq is now exact.
//
// The point for anyone reading this later: NOTHING ELSE SAW IT. Eighteen tests
// in the physical planner, a hundred and three here, five falsifiability gates,
// all green before and after - because estimates do not change answers. This
// file is the only thing in the project that looks at the number the planner
// exists to produce.
struct Case { const char* name; const char* sql; };
const Case kCases[] = {
    {"scan",            "SELECT id FROM users"},
    {"filter_eq",       "SELECT id FROM users WHERE city = 'NYC'"},
    {"filter_range",    "SELECT id FROM users WHERE age > 20"},
    {"join_fk",         "SELECT u.id, o.id FROM users u JOIN orders o ON o.user_id = u.id"},
    {"join_filtered",   "SELECT u.id FROM users u JOIN orders o ON o.user_id = u.id "
                        "WHERE o.status = 'paid'"},
    {"left_join",       "SELECT u.id, o.id FROM users u LEFT JOIN orders o ON o.user_id = u.id"},
    {"group_by",        "SELECT city, COUNT(*) FROM users GROUP BY city"},
    {"semi_join",       "SELECT id FROM users u WHERE EXISTS "
                        "(SELECT 1 FROM orders o WHERE o.user_id = u.id)"},
    {"three_way",       "SELECT u.id FROM users u JOIN orders o ON o.user_id = u.id "
                        "JOIN products p ON p.id = o.product_id"},
};

// Base cardinalities taken from the fixture itself rather than typed in. A
// hand-written row count is a second source of truth that goes stale the first
// time someone adds a row, and then the q-error moves for a reason that has
// nothing to do with the planner.
db25::physical::CardinalityModel cardinality_from_fixture() {
    db25::physical::CardinalityModel card;
    for (const auto& [table, rows] : db25::harness::data()) {
        card.base_rows[table] = static_cast<double>(rows.size());
    }
    return card;
}

// Statistics, also derived from the fixture.
//
// WITHOUT THIS the measurement reports the planner's FALLBACK path - every
// predicate priced at the flat default - and calling that "the planner's
// accuracy" would understate it by a wide margin. The first run of this tool
// did exactly that: four queries estimated 0.5 rows, which is 5 x the default
// 0.1 selectivity, and the geometric mean came out at 2.441x. The statistics
// the planner is built to use were simply not supplied.
//
// So the numbers here are computed the way a catalog would compute them -
// distinct count, null fraction, numeric range, per column - and the result
// measures the estimator that actually ships rather than the one it degrades to.
db25::physical::StatisticsCatalog statistics_from_fixture() {
    db25::physical::StatisticsCatalog stats;
    const auto& cat = db25::harness::catalog();

    for (const auto& [table_name, rows] : db25::harness::data()) {
        const auto* info = cat.find_table(table_name);
        if (info == nullptr) continue;

        db25::physical::TableStats ts;
        ts.rows = static_cast<double>(rows.size());

        for (std::size_t col = 0; col < info->columns.size(); ++col) {
            std::vector<std::string> seen;   // distinct VALUES, by their identity
            std::size_t nulls = 0;
            bool has_num = false;
            double lo = 0.0, hi = 0.0;

            for (const auto& row : rows) {
                if (col >= row.size()) continue;
                const auto& v = row[col];
                if (db25::harness::is_null(v)) { ++nulls; continue; }
                const std::string key = db25::harness::value_to_string(v);
                bool dup = false;
                for (const auto& k : seen) if (k == key) { dup = true; break; }
                if (!dup) seen.push_back(key);
                if (const auto* i = std::get_if<std::int64_t>(&v)) {
                    const double d = static_cast<double>(*i);
                    if (!has_num) { lo = hi = d; has_num = true; }
                    else { if (d < lo) lo = d; if (d > hi) hi = d; }
                }
            }

            db25::physical::ColumnStats cs;
            cs.ndv = static_cast<double>(seen.size());
            cs.null_fraction = rows.empty() ? 0.0
                             : static_cast<double>(nulls) / static_cast<double>(rows.size());
            cs.has_range = has_num;
            cs.min_value = lo;
            cs.max_value = hi;
            // UNIQUE is a guarantee, not a measurement: assert it only where the
            // catalog says so, never because this fixture happens to have no
            // duplicates. Inferring it from five rows would hand the estimator a
            // promise the data cannot keep.
            cs.unique = false;
            ts.columns[info->columns[col].column_id] = cs;
        }
        stats.add_table(info->table_id, std::move(ts));
    }
    return stats;
}

double q_error(double est, double act) {
    if (est <= 0.0) est = 1.0;  // an estimate of zero rows is still a guess of "about one"
    return est > act ? est / act : act / est;
}

}  // namespace

int main(int argc, char** argv) {
    double baseline = -1.0;
    for (int i = 1; i < argc; ++i) {
        const std::string a = argv[i];
        if (a.rfind("--baseline=", 0) == 0) baseline = std::atof(a.c_str() + 11);
    }

    std::string spec_error;
    auto spec = db25::physical::load_spec(
        std::string(DB25_PHYSICAL_SPEC_DIR) + "/physical.spec.sexpr", spec_error);
    if (!spec) {
        std::printf("qerror: cannot load the physical spec: %s\n", spec_error.c_str());
        return 1;
    }

    auto card = cardinality_from_fixture();
    const auto stats = statistics_from_fixture();
    card.stats = &stats;
    db25::physical::CalibrationProfile cal{};

    std::printf("== cardinality estimate vs actual rows ==\n\n");
    std::printf("  %-16s %10s %10s %9s\n", "query", "estimated", "actual", "q-error");

    double log_sum = 0.0;
    int measured = 0, empty = 0, skipped = 0;
    double worst = 1.0;
    std::string worst_name;

    for (const Case& c : kCases) {
        auto st = db25::harness::run(db25::harness::catalog(), c.sql);
        if (!st.optimize_ok || st.optimized == nullptr) {
            std::printf("  %-16s %10s %10s %9s  (pipeline)\n", c.name, "-", "-", "skip");
            ++skipped;
            continue;
        }
        const auto actual_rows = db25::harness::eval(st.optimized, db25::harness::data());
        if (!actual_rows.has_value()) {
            std::printf("  %-16s %10s %10s %9s  (eval)\n", c.name, "-", "-", "skip");
            ++skipped;
            continue;
        }

        db25::physical::LoweringContext pctx;
        pctx.spec = &*spec;
        pctx.calibration = &cal;
        pctx.cardinality = &card;
        auto lowered = db25::physical::lower(*st.optimized, pctx);
        if (!lowered.ok || lowered.plan == nullptr) {
            std::printf("  %-16s %10s %10s %9s  (lower)\n", c.name, "-", "-", "skip");
            ++skipped;
            continue;
        }

        const double est = lowered.estimated_rows;
        const double act = static_cast<double>(actual_rows->size());

        if (act == 0.0) {
            // Not a measurement. See the header: a ratio against zero is not a
            // number, and pretending otherwise is how a metric starts lying.
            std::printf("  %-16s %10.1f %10.0f %9s  (empty result)\n", c.name, est, act, "-");
            ++empty;
            continue;
        }

        const double q = q_error(est, act);
        std::printf("  %-16s %10.1f %10.0f %9.2fx\n", c.name, est, act, q);
        log_sum += std::log(q);
        ++measured;
        if (q > worst) { worst = q; worst_name = c.name; }
    }

    if (measured == 0) {
        std::printf("\nQERROR MEASUREMENT FAILED: no query produced a comparable estimate.\n");
        std::printf("  Every case was skipped or returned no rows. This is a fault in the\n");
        std::printf("  measurement, not a statement about the planner.\n");
        return 1;
    }

    const double geo = std::exp(log_sum / measured);
    std::printf("\n  measured %d, empty %d, skipped %d\n", measured, empty, skipped);
    std::printf("  GEOMETRIC MEAN q-error: %.3fx   (worst: %s at %.2fx)\n",
                geo, worst_name.c_str(), worst);

    if (baseline < 0.0) {
        std::printf("  (no baseline given; reporting only)\n");
        return 0;
    }
    std::printf("  BASELINE: %.3fx\n", baseline);
    // Higher is worse: this is a factor-of-wrong, not a score.
    if (geo > baseline + 1e-9) {
        std::printf("\nQERROR REGRESSION: %.3fx is worse than the recorded %.3fx.\n", geo, baseline);
        std::printf("  The planner's estimates got further from the truth. Results are\n");
        std::printf("  unaffected - every other test here still passes - which is exactly\n");
        std::printf("  why this is measured separately.\n");
        return 1;
    }
    if (geo < baseline - 1e-9) {
        // Round UP to the precision this tool prints. The obvious version
        // suggested "%.3f of the measured value" - and recording that number
        // then FAILED the next run, because 1.2674 displays as 1.267 while
        // being greater than it. A tool whose advice does not pass its own
        // check is worse than one that gives none, so the suggestion is a true
        // upper bound on what was measured rather than a prettier rendering of
        // it.
        const double suggest = std::ceil(geo * 1000.0) / 1000.0;
        std::printf("  IMPROVED: lower the baseline to %.3f to lock it in.\n", suggest);
    }
    std::printf("  OK: no worse than baseline.\n");
    return 0;
}
