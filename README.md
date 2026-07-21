# DB25

Umbrella / meta repository for the **DB25** SQL engine — the home for cross-cutting
architecture, methodology, and research documents that span more than one stage of the
engine.

DB25 is a from-scratch SQL engine built one repo per stage:

| Stage | Repo |
|-------|------|
| Tokenizer (SIMD lexer) | `DB25-sql-tokenizer` |
| Parser (arena AST) | `db25-sql-parser` |
| Semantic analyzer | `DB25-Semantic-Analyzer` |
| Binder + logical optimizer | `db25-logical-plan` |
| Physical planner / executor / storage | *(planned)* |

This repository holds the documents that don't belong to any single stage.

## Contents

- [`docs/formal-methods-proposal.md`](docs/formal-methods-proposal.md) — a proposal for
  where TLA+ and Alloy would fit and be beneficial across the engine's lifecycle.
- [`docs/engine-comparison-findings.md`](docs/engine-comparison-findings.md) — a
  consolidated comparison of DB25 against FoundationDB, DuckDB, Polars, and SQLite,
  with ranked, actionable inspiration for the next layers.
