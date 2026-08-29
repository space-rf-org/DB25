#!/usr/bin/env python3
# Build a committed SQL corpus for the DB25 frontend from public test suites.
# Two source kinds are supported:
#   * "slt" - sqllogictest records (self-labeling: statement ok/error, query, and
#             onlyif/skipif <engine> dialect gates).
#   * "pg"  - PostgreSQL regression .sql files (plain SQL; statements split on
#             top-level ';', psql meta-commands and non-core statement kinds
#             classified out of scope).
# Each source becomes an independent SESSION (its own catalog). Every statement is
# classified in-scope (standard SQL, DB25's Postgres-leaning canon) or out-of-scope
# (dialect / engine-specific / non-core) - and NO record is dropped silently: every
# exclusion is counted against a named reason in COVERAGE.md.
#
# Emits corpus.tsv: session <tab> source_tag <tab> category <tab> db25_parse
# <tab> db25_analyze <tab> sql  (the db25 columns are filled by corpus_runner --update).
import re
from collections import Counter

# (file, kind, limit, session). Order is significant: sessions replay in order.
SOURCES = [
    ("sample_select1.test.slt", "slt", 150, "select1"),
    ("sample_in1.test.slt",     "slt", 400, "in1"),
    ("pg/case.sql",             "pg",  400, "pg_case"),
    # DB25-authored curated session (committed under corpus/pg/), not harvested:
    # exercises every accepted LATERAL join shape end to end.
    ("pg/lateral.sql",          "pg",  50,  "lateral"),
]
CANON_ENGINES = {"postgresql", "postgres"}
DIALECT = [
    (re.compile(r"\bx'[0-9A-Fa-f]*'"), "dialect:sqlite-blob-literal"),
    (re.compile(r"\bAUTOINCREMENT\b", re.I), "dialect:sqlite-autoincrement"),
    (re.compile(r"\bWITHOUT\s+ROWID\b", re.I), "dialect:sqlite-without-rowid"),
    (re.compile(r"\bPRAGMA\b", re.I), "dialect:pragma"),
    (re.compile(r"\bGLOB\b", re.I), "dialect:sqlite-glob"),
]
# Non-core statement kinds (session / utility / server-specific) excluded from a
# frontend corpus, keyed by leading keyword.
PG_SKIP = {"SET","RESET","SHOW","BEGIN","COMMIT","ROLLBACK","SAVEPOINT","RELEASE",
           "EXPLAIN","COPY","VACUUM","ANALYZE","GRANT","REVOKE","COMMENT","DO",
           "DISCARD","CLUSTER","REINDEX","PREPARE","EXECUTE","DEALLOCATE","LOCK",
           "FETCH","DECLARE","CLOSE","LISTEN","NOTIFY","CALL","REFRESH","SECURITY",
           "START","END","ABORT","CHECKPOINT"}
# CREATE / DROP object kinds DB25 models; anything else (FUNCTION, DOMAIN, TYPE,
# OPERATOR, CAST, AGGREGATE, EXTENSION, SEQUENCE, TRIGGER, ...) is out of scope.
CORE_OBJECTS = {"TABLE","INDEX","VIEW"}

def leading(sql):
    return sql.lstrip().split(None, 1)[0].upper() if sql.strip() else ""

def category(sql):
    h = leading(sql)
    if h in ("CREATE","ALTER","DROP","TRUNCATE"): return "ddl"
    if h in ("INSERT","UPDATE","DELETE"): return "dml"
    if h in ("SELECT","WITH","VALUES","TABLE"): return "query"
    return "other"

def classify_out_of_scope(sql):
    """Return an exclusion reason, or None if in-scope."""
    h = leading(sql)
    if h in PG_SKIP:
        return f"out-of-scope:{h.lower()}"
    if h in ("CREATE","DROP"):
        parts = sql.split()
        obj = parts[1].upper().rstrip("(;") if len(parts) > 1 else ""
        # peek past CREATE [OR REPLACE] / [GLOBAL|TEMP|UNLOGGED|MATERIALIZED] ...
        if obj in ("OR","GLOBAL","LOCAL","TEMP","TEMPORARY","UNLOGGED","MATERIALIZED"):
            obj = next((p.upper() for p in parts[2:] if p.upper() not in
                        ("OR","REPLACE","GLOBAL","LOCAL","TEMP","TEMPORARY","UNLOGGED")), "")
        if obj not in CORE_OBJECTS:
            return f"out-of-scope:{h.lower()}-{obj.lower() or 'unknown'}"
    if category(sql) == "other":
        return "unclassified-leading-keyword"
    return next((r for rx, r in DIALECT if rx.search(sql)), None)

# ---- sqllogictest extraction --------------------------------------------------
def extract_slt(path):
    lines = open(path, encoding="utf-8", errors="replace").read().split("\n")
    i = 0; pend = None
    while i < len(lines):
        toks = lines[i].split()
        if not toks: i += 1; continue
        if toks[0] in ("onlyif","skipif") and len(toks) >= 2:
            pend = (toks[0], toks[1].lower()); i += 1; continue
        if toks[0] == "statement" and len(toks) >= 2 and toks[1] in ("ok","error"):
            tag = toks[1]; i += 1; sql = []
            while i < len(lines) and lines[i].strip() != "": sql.append(lines[i]); i += 1
            yield (tag, pend, " ".join(s.strip() for s in sql)); pend = None; continue
        if toks[0] == "query":
            i += 1; sql = []
            while i < len(lines) and lines[i].strip() not in ("----",""): sql.append(lines[i]); i += 1
            while i < len(lines) and lines[i].strip() != "": i += 1
            yield ("ok", pend, " ".join(s.strip() for s in sql)); pend = None; continue
        pend = None; i += 1

# ---- PostgreSQL .sql extraction (comment- + quote- + dollar-quote-aware split) -
def extract_pg(path):
    txt = open(path, encoding="utf-8", errors="replace").read()
    txt = re.sub(r"/\*.*?\*/", "", txt, flags=re.S)   # block comments
    txt = re.sub(r"--[^\n]*", "", txt)                # line comments
    cur = ""; i = 0; n = len(txt); inq = False; dollar = None
    while i < n:
        ch = txt[i]
        if dollar:                                     # inside $tag$ ... $tag$
            if txt.startswith(dollar, i): cur += dollar; i += len(dollar); dollar = None; continue
            cur += ch; i += 1; continue
        if inq:                                        # inside '...'
            cur += ch; inq = (ch != "'"); i += 1; continue
        if ch == "'": inq = True; cur += ch; i += 1; continue
        m = re.match(r"\$[A-Za-z0-9_]*\$", txt[i:])
        if m: dollar = m.group(0); cur += dollar; i += len(dollar); continue
        if ch == ";":
            s = " ".join(cur.split())
            if s: yield ("ok", None, s)
            cur = ""; i += 1; continue
        cur += ch; i += 1

# ---- drive ---------------------------------------------------------------------
# Guarded so the module can be imported (to reuse extract_pg / extract_slt /
# category) without re-harvesting - re-harvest only runs as `python3 generate.py`.
def main():
    allrows = []; excl = Counter(); total = 0; kept_by_sess = Counter()
    for path, kind, limit, sess in SOURCES:
        extract = extract_slt if kind == "slt" else extract_pg
        n = 0
        for tag, pend, sql in extract(path):
            if not sql.strip(): continue
            total += 1
            if pend:                                       # slt engine gates
                k, eng = pend
                if k == "onlyif" and eng not in CANON_ENGINES: excl[f"engine-only:{eng}"] += 1; continue
                if k == "skipif" and eng in CANON_ENGINES: excl["engine-skip:postgresql"] += 1; continue
            reason = classify_out_of_scope(sql)
            if reason: excl[reason] += 1; continue
            allrows.append((sess, tag, category(sql), sql)); n += 1; kept_by_sess[sess] += 1
            if n >= limit: break

    with open("corpus.tsv", "w") as f:
        f.write("# session\tsource_tag\tcategory\tdb25_parse\tdb25_analyze\tsql\n")
        for sess, tag, cat, sql in allrows:
            f.write(f"{sess}\t{tag}\t{cat}\t?\t?\t{sql}\n")
    with open("COVERAGE.md", "w") as f:
        f.write("# Corpus coverage\n\nSources (see SOURCES.md for provenance + licensing):\n\n")
        for p, k, l, s in SOURCES: f.write(f"- `{p}` ({k}) -> session `{s}` (<= {l} in-scope)\n")
        f.write(f"\n- records scanned: {total}\n- in-scope (kept): {len(allrows)}\n- excluded: {sum(excl.values())}\n")
        f.write("\n## Exclusions by reason (no silent drops)\n\n")
        for r, n in excl.most_common(): f.write(f"- `{r}`: {n}\n")
        f.write("\n## In-scope by session\n\n")
        for _, _, _, s in SOURCES: f.write(f"- {s}: {kept_by_sess[s]}\n")
        cat = Counter(c for _, _, c, _ in allrows)
        f.write("\n## In-scope by category\n\n")
        for c, n in cat.most_common(): f.write(f"- {c}: {n}\n")
    print(f"scanned={total} kept={len(allrows)} excluded={sum(excl.values())} sessions={dict(kept_by_sess)}")
    print("exclusions:", dict(excl))


if __name__ == "__main__":
    main()
