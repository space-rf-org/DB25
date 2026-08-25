#!/usr/bin/env python3
"""Generate the DB25 SQL coverage catalog from the test harness.

Single source of truth: the SQL already lives in the harness. This scans it and
renders a catalog (JSON census + HTML) so "what SQL is supported and tested" is
DERIVED, never re-typed, and cannot drift from the tests.

Sources
  - corpus/corpus.tsv        curated, CI-pinned statements (parse+analyze status)
  - corpus/staged/*.fixture  end-to-end s-expr pins (parse->analyze->bind->optimize)
  - unit-test files          breadth pointer (indexed by file, not re-extracted)

Outputs
  - docs/coverage/catalog.json   machine-readable census (the coverage matrix)
  - docs/coverage-catalog.html   published, browsable catalog

Run from the umbrella repo root:  python3 tools/gen_coverage_catalog.py
"""
from __future__ import annotations
import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

# Unit-test directories to index as a breadth pointer (relative to sibling repos
# / submodules). Kept as a pointer, not re-extracted, so the catalog can't drift.
TEST_DIRS = [
    ("parser", ROOT.parent / "db25-sql-parser" / "tests"),
    ("analyzer", Path("/home/user/DB25-Semantic-Analyzer/tests")),
    ("logical-plan", ROOT / "external" / "db25-logical-plan" / "tests"),
]

# --- Feature detection ------------------------------------------------------
# (tag, regex) — order-independent; a statement carries every tag it matches.
FEATURES = [
    ("cte-recursive", r"\bWITH\s+RECURSIVE\b"),
    ("cte", r"\bWITH\b"),
    ("values", r"\bVALUES\b"),
    ("join-inner", r"\bINNER\s+JOIN\b|\b(?<!LEFT )(?<!RIGHT )(?<!FULL )(?<!CROSS )JOIN\b"),
    ("join-left", r"\bLEFT\s+(OUTER\s+)?JOIN\b"),
    ("join-right", r"\bRIGHT\s+(OUTER\s+)?JOIN\b"),
    ("join-full", r"\bFULL\s+(OUTER\s+)?JOIN\b"),
    ("join-cross", r"\bCROSS\s+JOIN\b"),
    ("join-using", r"\bUSING\s*\("),
    ("join-natural", r"\bNATURAL\b"),
    ("where", r"\bWHERE\b"),
    ("group-by", r"\bGROUP\s+BY\b"),
    ("rollup", r"\bROLLUP\b"),
    ("cube", r"\bCUBE\b"),
    ("grouping-sets", r"\bGROUPING\s+SETS\b"),
    ("having", r"\bHAVING\b"),
    ("order-by", r"\bORDER\s+BY\b"),
    ("limit-offset", r"\bLIMIT\b|\bOFFSET\b"),
    ("distinct", r"\bDISTINCT\b"),
    ("window", r"\bOVER\s*\("),
    ("window-filter", r"\bFILTER\s*\(\s*WHERE\b"),
    ("set-union", r"\bUNION\b"),
    ("set-intersect", r"\bINTERSECT\b"),
    ("set-except", r"\bEXCEPT\b"),
    ("subquery-exists", r"\bEXISTS\s*\("),
    ("subquery-in", r"\bIN\s*\(\s*SELECT\b"),
    ("quantified-cmp", r"[<>=!]\s*(ALL|ANY|SOME)\s*\("),
    ("subquery-scalar", r"\(\s*SELECT\b"),
    ("case", r"\bCASE\b"),
    ("cast", r"\bCAST\s*\(|::"),
    ("collate", r"\bCOLLATE\b"),
    ("like", r"\b(I?LIKE)\b"),
    ("between", r"\bBETWEEN\b"),
    ("in-list", r"\bIN\s*\("),
    ("array", r"\bARRAY\s*\[|\[\s*\]"),
    ("interval", r"\bINTERVAL\b"),
    ("extract", r"\bEXTRACT\s*\("),
    ("aggregate", r"\b(COUNT|SUM|AVG|MIN|MAX)\s*\("),
    ("coalesce-nullif", r"\b(COALESCE|NULLIF|GREATEST|LEAST)\s*\("),
    # DDL constraints
    ("c-not-null", r"\bNOT\s+NULL\b"),
    ("c-primary-key", r"\bPRIMARY\s+KEY\b"),
    ("c-foreign-key", r"\bFOREIGN\s+KEY\b|\bREFERENCES\b"),
    ("c-check", r"\bCHECK\s*\("),
    ("c-default", r"\bDEFAULT\b"),
    ("c-unique", r"\bUNIQUE\b"),
]

STMT_KIND = [
    ("DDL", "CREATE TABLE", r"^\s*CREATE\s+TABLE\b"),
    ("DDL", "CREATE INDEX", r"^\s*CREATE\s+(UNIQUE\s+)?INDEX\b"),
    ("DDL", "CREATE VIEW", r"^\s*CREATE\s+(OR\s+REPLACE\s+)?VIEW\b"),
    ("DDL", "ALTER TABLE", r"^\s*ALTER\s+TABLE\b"),
    ("DDL", "DROP", r"^\s*DROP\b"),
    ("DDL", "TRUNCATE", r"^\s*TRUNCATE\b"),
    ("DML", "INSERT", r"^\s*INSERT\b"),
    ("DML", "UPDATE", r"^\s*UPDATE\b"),
    ("DML", "DELETE", r"^\s*DELETE\b"),
    ("DQL", "WITH", r"^\s*WITH\b"),
    ("DQL", "SELECT", r"^\s*(\(\s*)?SELECT\b"),
    ("DQL", "VALUES", r"^\s*VALUES\b"),
]


def classify(sql: str):
    kind, stmt = "OTHER", "?"
    for k, s, rx in STMT_KIND:
        if re.search(rx, sql, re.IGNORECASE):
            kind, stmt = k, s
            break
    feats = [tag for tag, rx in FEATURES if re.search(rx, sql, re.IGNORECASE)]
    return kind, stmt, feats


def load_corpus():
    entries = []
    path = ROOT / "corpus" / "corpus.tsv"
    for line in path.read_text().splitlines():
        if not line or line.startswith("#"):
            continue
        f = line.split("\t")
        if len(f) < 6:
            continue
        session, tag, cat, parse_v, analyze_v, sql = f[0], f[1], f[2], f[3], f[4], f[5]
        kind, stmt, feats = classify(sql)
        if parse_v == "reject":
            status = "rejected (by design)"
        elif analyze_v == "exec_ok":
            status = "DDL applied"
        elif analyze_v == "clean":
            status = "analyzed clean"
        elif analyze_v == "diag":
            status = "diagnostic (negative test)"
        else:
            status = f"{parse_v}/{analyze_v}"
        entries.append(dict(sql=sql, kind=kind, stmt=stmt, features=feats,
                            status=status, source=f"corpus.tsv · {session}",
                            tier="corpus"))
    return entries


def load_fixtures():
    entries = []
    fdir = ROOT / "corpus" / "staged"
    for fx in sorted(fdir.glob("*.fixture")):
        text = fx.read_text()
        # sectioned: -- sql / -- tokens / -- ast / -- resolved / -- logical / -- optimized
        secs = {}
        cur = None
        for line in text.splitlines():
            m = re.match(r"^--\s*(\w+)\s*$", line)
            if m:
                cur = m.group(1).lower()
                secs[cur] = []
            elif cur is not None:
                secs[cur].append(line)
        sql = "\n".join(secs.get("sql", [])).strip()
        if not sql:
            continue
        logical = "\n".join(secs.get("logical", []))
        pins = [s for s in ("tokens", "ast", "resolved", "logical", "optimized") if s in secs]
        if "bind-error" in logical:
            status = "e2e pinned; bind deferred (known gap)"
        else:
            status = "full pipeline pinned (s-expr)"
        kind, stmt, feats = classify(sql)
        entries.append(dict(sql=sql, kind=kind, stmt=stmt, features=feats,
                            status=status, source=f"staged/{fx.name}",
                            tier="staged", pins=pins))
    return entries


def index_tests():
    idx = []
    for repo, d in TEST_DIRS:
        if not d.exists():
            continue
        for f in sorted(d.rglob("*.cpp")):
            n = len(re.findall(r"\.parse\s*\(", f.read_text(errors="ignore")))
            if n:
                idx.append(dict(repo=repo, file=f.name, sites=n))
    return idx


# A construct's coverage location tells us where a regression WOULD be caught.
# CI-pinned (corpus) is the strongest net; e2e-only (staged fixture) still runs
# in CI; not-in-corpus means it lives only in unit tests, so the corpus golden
# would not catch a semantic drift there (the #2 work-list).
def coverage_matrix(entries):
    rows = []
    for tag, _rx in FEATURES:
        corpus = sum(1 for e in entries if tag in e["features"] and e["tier"] == "corpus")
        staged = sum(1 for e in entries if tag in e["features"] and e["tier"] == "staged")
        if corpus > 0:
            state = "ci-pinned"
        elif staged > 0:
            state = "e2e-only"
        else:
            state = "not-in-corpus"
        rows.append(dict(feature=tag, corpus=corpus, staged=staged, state=state))
    return rows


def main():
    corpus = load_corpus()
    fixtures = load_fixtures()
    tests = index_tests()
    entries = corpus + fixtures
    matrix = coverage_matrix(entries)

    census = dict(
        entries=entries,
        test_index=tests,
        matrix=matrix,
        summary=summarize(entries, tests),
    )
    out_json = ROOT / "docs" / "coverage" / "catalog.json"
    out_json.parent.mkdir(parents=True, exist_ok=True)
    out_json.write_text(json.dumps(census, indent=2))
    print(f"wrote {out_json.relative_to(ROOT)}  ({len(entries)} entries)")

    tpl = (ROOT / "tools" / "catalog_template.html").read_text()
    html = tpl.replace('"__CENSUS_DATA__"', json.dumps(census))
    out_html = ROOT / "docs" / "coverage-catalog.html"
    out_html.write_text(html)
    print(f"wrote {out_html.relative_to(ROOT)}")

    print_summary(census["summary"])
    gaps = [m["feature"] for m in matrix if m["state"] == "not-in-corpus"]
    print(f"  CI-gap constructs ({len(gaps)}):", ", ".join(gaps))
    return census


def summarize(entries, tests):
    by_kind, by_stmt, by_feat, by_status = {}, {}, {}, {}
    for e in entries:
        by_kind[e["kind"]] = by_kind.get(e["kind"], 0) + 1
        by_stmt[e["stmt"]] = by_stmt.get(e["stmt"], 0) + 1
        by_status[e["status"]] = by_status.get(e["status"], 0) + 1
        for t in e["features"]:
            by_feat[t] = by_feat.get(t, 0) + 1
    return dict(total=len(entries), by_kind=by_kind, by_stmt=by_stmt,
                by_feature=dict(sorted(by_feat.items(), key=lambda kv: -kv[1])),
                by_status=by_status,
                test_sites=sum(t["sites"] for t in tests),
                test_files=len(tests))


def print_summary(s):
    print("  kind:   ", s["by_kind"])
    print("  stmt:   ", s["by_stmt"])
    print("  status: ", s["by_status"])
    print("  test index:", s["test_files"], "files,", s["test_sites"], "parse() sites")
    print("  top features:", dict(list(s["by_feature"].items())[:12]))


if __name__ == "__main__":
    main()
