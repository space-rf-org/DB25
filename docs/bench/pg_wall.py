# PG's PLANNING TIME excludes raw parse and rewrite; DB25's number includes parse.
# So measure PG the other way too: wall time for N EXPLAINs in ONE warm session,
# minus the same N of a trivial statement, divided by N. That nets out process
# start and protocol and leaves per-statement parse -> rewrite -> plan, which is
# the boundary DB25 is measured at.
import os
import re, statistics, subprocess, sys, time
S = os.path.dirname(os.path.abspath(__file__))
N = int(sys.argv[1]) if len(sys.argv) > 1 else 200
QS = [l.rstrip("\n").split("|", 2) for l in open(f"{S}/queries.txt") if l.strip()]
PSQL = ["psql","-h","/tmp/pgs","-p","55432","-U","postgres","-d","bench","-tAq","-c"]

def wall_us(sql, n, reps=5):
    body = "\n".join([f"EXPLAIN {sql};"] * n)
    ts = []
    for _ in range(reps):
        t0 = time.perf_counter_ns()
        subprocess.run(PSQL + [body], capture_output=True, text=True)
        ts.append((time.perf_counter_ns() - t0) / 1000.0)
    return statistics.median(ts)

def planner_ms(sql, n):
    body = "\n".join([f"EXPLAIN (SUMMARY ON) {sql};"] * n)
    out = subprocess.run(PSQL + [body], capture_output=True, text=True).stdout
    v = [float(m) for m in re.findall(r"Planning Time: ([0-9.]+) ms", out)]
    return statistics.median(v) if v else float("nan")

base = wall_us("SELECT 1", N)
print(f"# baseline: {N} x EXPLAIN SELECT 1 = {base:.0f}us total", file=sys.stderr)
print("id\tpg_parse_plan_us\tpg_planner_us")
for qid, _l, sql in QS:
    w = (wall_us(sql, N) - base) / N
    print(f"{qid}\t{max(w,0):.2f}\t{planner_ms(sql, N)*1000:.2f}")
