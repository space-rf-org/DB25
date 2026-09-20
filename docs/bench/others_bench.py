# PostgreSQL and DuckDB held to DB25's boundary: produce a physical plan, stop.
#   PG      - EXPLAIN (no ANALYZE) => planner runs, executor does not.
#             Two numbers: PG's self-reported "Planning Time" (planner only,
#             excludes raw parse+rewrite), and wall time minus a per-statement
#             baseline, which approximates parse->plan as DB25 measures it.
#   DuckDB  - EXPLAIN in-process; parse+bind+optimize+physical planner, no exec.
#             Wall minus the same baseline. No protocol overhead to remove.
import os
import re, statistics, subprocess, sys, time
import duckdb

S = os.path.dirname(os.path.abspath(__file__))
RUNS = int(sys.argv[1]) if len(sys.argv) > 1 else 200
QS = [l.rstrip("\n").split("|", 2) for l in open(f"{S}/queries.txt") if l.strip()]

PSQL = ["psql", "-h", "/tmp/pgs", "-p", "55432", "-U", "postgres", "-d", "bench", "-tAq", "-c"]

def pg_planning_times(sql, runs):
    """PG's own Planning Time, from EXPLAIN (SUMMARY ON) - no execution."""
    body = "\n".join([f"EXPLAIN (SUMMARY ON) {sql};"] * runs)
    out = subprocess.run(PSQL + [body], capture_output=True, text=True).stdout
    return [float(m) for m in re.findall(r"Planning Time: ([0-9.]+) ms", out)]

def duck_times(con, sql, runs):
    ts = []
    for _ in range(runs):
        t0 = time.perf_counter_ns()
        con.execute("EXPLAIN " + sql).fetchall()
        ts.append((time.perf_counter_ns() - t0) / 1000.0)
    return ts

# ---- DuckDB fixture: same schema, same rows, analyzed ----
con = duckdb.connect()
con.execute(open(f"{S}/schema.sql").read())
con.execute("INSERT INTO emp    SELECT g, 'e'||g, g%50, (g*37)%20000 FROM range(1,20001) t(g)")
con.execute("INSERT INTO dept   SELECT g, 'd'||g FROM range(1,51) t(g)")
con.execute("INSERT INTO orders SELECT g, g%20000, (g*13)%5000 FROM range(1,50001) t(g)")
con.execute("INSERT INTO cust   SELECT g, 'c'||g, 'city'||(g%100) FROM range(1,20001) t(g)")
con.execute("INSERT INTO prod   SELECT g, 'p'||g, (g*7)%1000 FROM range(1,2001) t(g)")
con.execute("INSERT INTO items  SELECT g, g%50000, g%2000, g%10 FROM range(1,100001) t(g)")
con.execute("ANALYZE")
n = con.execute("SELECT (SELECT COUNT(*) FROM emp), (SELECT COUNT(*) FROM items)").fetchone()
print(f"# duckdb fixture: emp={n[0]} items={n[1]}", file=sys.stderr)

# baseline: cost of the EXPLAIN round trip itself, subtracted from both
duck_base = statistics.median(duck_times(con, "SELECT 1", 50))

print("id\tpg_plan_ms\tduck_us")
for qid, _label, sql in QS:
    try:
        pg = pg_planning_times(sql, RUNS)
        pg_med = statistics.median(pg) if pg else float("nan")
    except Exception as e:
        pg_med = float("nan")
    try:
        con.execute("EXPLAIN " + sql).fetchall()
        d = duck_times(con, sql, RUNS)
        d_med = max(statistics.median(d) - duck_base, 0.0)
    except Exception as e:
        d_med = float("nan")
        print(f"# duckdb {qid} failed: {str(e)[:80]}", file=sys.stderr)
    print(f"{qid}\t{pg_med:.4f}\t{d_med:.2f}")
