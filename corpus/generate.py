#!/usr/bin/env python3
# Build a committed SQL corpus for the DB25 frontend from sqllogictest sources.
# Each source becomes an independent SESSION (its own catalog). Every statement
# is classified in-scope (standard SQL, DB25's Postgres-leaning canon) or
# out-of-scope (dialect / engine-specific) - and NO record is dropped silently:
# every exclusion is counted against a named reason in COVERAGE.md.
#
# Emits corpus.tsv: session <tab> source_tag <tab> category <tab> sql
# (the db25 behaviour columns are filled by corpus_runner --update).
import re
from collections import Counter

# (file, limit, session-name). Order is significant: sessions replay in order.
SOURCES = [
    ("sample_select1.test.slt", 150, "select1"),
    ("sample_in1.test.slt",     400, "in1"),
]
CANON_ENGINES = {"postgresql", "postgres"}
DIALECT = [
    (re.compile(r"\bx'[0-9A-Fa-f]*'"), "dialect:sqlite-blob-literal"),
    (re.compile(r"\bAUTOINCREMENT\b", re.I), "dialect:sqlite-autoincrement"),
    (re.compile(r"\bWITHOUT\s+ROWID\b", re.I), "dialect:sqlite-without-rowid"),
    (re.compile(r"\bPRAGMA\b", re.I), "dialect:pragma"),
    (re.compile(r"\bGLOB\b", re.I), "dialect:sqlite-glob"),
]
def category(sql):
    h = sql.lstrip().split(None,1)[0].upper() if sql.strip() else ""
    if h in ("CREATE","ALTER","DROP","TRUNCATE"): return "ddl"
    if h in ("INSERT","UPDATE","DELETE"): return "dml"
    if h in ("SELECT","WITH","VALUES","TABLE"): return "query"
    return "other"
def extract(path):
    lines=open(path,encoding="utf-8",errors="replace").read().split("\n")
    i=0; pend=None
    while i<len(lines):
        toks=lines[i].split()
        if not toks: i+=1; continue
        if toks[0] in ("onlyif","skipif") and len(toks)>=2:
            pend=(toks[0],toks[1].lower()); i+=1; continue
        if toks[0]=="statement" and len(toks)>=2 and toks[1] in ("ok","error"):
            tag=toks[1]; i+=1; sql=[]
            while i<len(lines) and lines[i].strip()!="": sql.append(lines[i]); i+=1
            yield(tag,pend," ".join(s.strip() for s in sql)); pend=None; continue
        if toks[0]=="query":
            i+=1; sql=[]
            while i<len(lines) and lines[i].strip() not in ("----",""): sql.append(lines[i]); i+=1
            while i<len(lines) and lines[i].strip()!="": i+=1
            yield("ok",pend," ".join(s.strip() for s in sql)); pend=None; continue
        pend=None; i+=1

allrows=[]; excl=Counter(); total=0; kept_by_sess=Counter()
for path,limit,sess in SOURCES:
    n=0
    for tag,pend,sql in extract(path):
        if not sql.strip(): continue
        total+=1
        if pend:
            kind,eng=pend
            if kind=="onlyif" and eng not in CANON_ENGINES: excl[f"engine-only:{eng}"]+=1; continue
            if kind=="skipif" and eng in CANON_ENGINES: excl["engine-skip:postgresql"]+=1; continue
        hit=next((r for rx,r in DIALECT if rx.search(sql)), None)
        if hit: excl[hit]+=1; continue
        if category(sql)=="other": excl["unclassified-leading-keyword"]+=1; continue
        allrows.append((sess,tag,category(sql),sql)); n+=1; kept_by_sess[sess]+=1
        if n>=limit: break

with open("corpus.tsv","w") as f:
    f.write("# session\tsource_tag\tcategory\tdb25_parse\tdb25_analyze\tsql\n")
    for sess,tag,cat,sql in allrows:
        f.write(f"{sess}\t{tag}\t{cat}\t?\t?\t{sql}\n")
with open("COVERAGE.md","w") as f:
    f.write("# Corpus coverage\n\nSources (sqllogictest; SQLite = public domain, sqllogictest = MIT):\n\n")
    for p,l,s in SOURCES: f.write(f"- `{p}` -> session `{s}` (<= {l} in-scope)\n")
    f.write(f"\n- records scanned: {total}\n- in-scope (kept): {len(allrows)}\n- excluded: {sum(excl.values())}\n")
    f.write("\n## Exclusions by reason (no silent drops)\n\n")
    for r,n in excl.most_common(): f.write(f"- `{r}`: {n}\n")
    f.write("\n## In-scope by session / category\n\n")
    for s in [x[2] for x in SOURCES]: f.write(f"- {s}: {kept_by_sess[s]}\n")
    cat=Counter(c for _,_,c,_ in allrows)
    f.write("\n")
    for c,n in cat.most_common(): f.write(f"- {c}: {n}\n")
print(f"scanned={total} kept={len(allrows)} excluded={sum(excl.values())} sessions={dict(kept_by_sess)}")
