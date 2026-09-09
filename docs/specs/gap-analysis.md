# VaireDB SQL Gap Analysis

What a PostgreSQL client can and cannot do against VaireDB today — commands, data types,
functions, operators and literals — with every partial and missing behaviour named, and the
limitations and next phases that would close them.

This is the consolidated record. It is a compaction of six detailed analyses, which remain
the authority for any single row and hold the measurements, root causes and rejected
hypotheses behind every verdict here:

| Axis | Detailed document | Rows measured |
|---|---|---:|
| Statements | [`gap-analysis-command.md`](gap-analysis-command.md) | 36 |
| Data types | [`gap-analysis-data-type.md`](gap-analysis-data-type.md) | 24 + 8 refused |
| Aggregate functions | [`gap-analysis-aggregate-function.md`](gap-analysis-aggregate-function.md) | 66 |
| Window functions | [`gap-analysis-window-function.md`](gap-analysis-window-function.md) | 42 |
| Operators & literals | [`gap-analysis-operator-literal.md`](gap-analysis-operator-literal.md) | 80 |
| Joins & set operations | [`gap-analysis-join.md`](gap-analysis-join.md) | 43 |

The join axis was added last, and it is the one that measures *combinators* rather than a
catalog: it is the only surface whose correctness depends on how the data is laid out, so its
rows are measured in both the **co-located** and the **shuffle** layout. Writing it found
seven wrong answers, and closing one of those found an eighth — the widest of all of them, found
only because the fix for another behaved differently on a cluster than in a unit test. A join
with **no equijoin key whose result is decided on its build side** lost exactly those rows:
`EXISTS` and `NOT EXISTS` answered constant false for every subquery not correlated by an
equality, and a keyless `LEFT`/`FULL JOIN` quietly became an inner join. **All eight are
closed**, that one included; no single-process test could see it, which is why the join axis is
measured on a cluster and nowhere else. What the axis still records as not-✅ is three
*residues* — each the part of a rule a fix could not reach, written down rather than folded
away. One of those residues has since been closed too, and how is worth noting: it had been
recorded as impossible on the grounds that two plan shapes were indistinguishable, and when the
two plans were finally printed rather than reasoned about, they were not.

Everything below was measured against a live 5-node e2e cluster (1 coordinator, 1
scheduler, 3 cores) and byte-diffed against a real **PostgreSQL 16.15** oracle in a
throwaway container, with a **DuckDB 1.5.5** CLI as a second oracle for the forms PG cannot
parse. Result types and column labels come from extended-protocol `Describe`, not from
inspecting source. See *How this was measured* in each source document.

## Verdict legend

| Status | Meaning |
|---|---|
| ✅ | **Works, and agrees with PostgreSQL.** |
| 🟡 | **Partial** — correct value under a wrong advertised type, one path only, degraded semantics, or an honest error in the wrong SQLSTATE. |
| ⛔ | **Silently wrong** — parses, executes, returns a plausible value that is not PostgreSQL's. The most dangerous class. |
| 🚫 | **Not planned**, with a written rationale — the statement has no meaning in a shared-nothing cluster, or is DuckDB-only. |
| ❌ | **Rejected loudly** with a SQLSTATE, and no target behaviour agreed yet. |

`🚫` and `❌` differ only in intent: a `🚫` row is a decision, an `❌` row is an open gap.

## Totals

| Axis | Rows | ✅ | 🟡 | ⛔ | 🚫 | ❌ |
|---|---:|---:|---:|---:|---:|---:|
| Statements | 36 | 5 | 14 | — | 8 | 9 |
| Data types (Arrow round-trip) | 24 | 20 | 2 | — | 1 | 1 |
| Data types (refused by name at DDL) | 8 | — | — | — | 8 | — |
| Aggregate functions | 66 | 46 | 8 | — | — | 12 |
| Window functions | 42 | 27 | 4 | 1 | — | 10 |
| Operators & literals | 80 | 47 | 8 | 5 | — | 20 |
| Joins & set operations | 43 | 36 | 2 | 2 | — | 3 |
| **Total** | **299** | **181** | **38** | **8** | **17** | **55** |

Each axis's counts are that source document's own tallies, where a single row can group
several spellings (`+ - *` is one row, `regr_*` is one row) — so the table compares axes,
not individual constructs.

**The operator axis's counts moved without anything regressing.** It went from
77 rows / 53 ✅ to 80 rows / 47 ✅ because v0.2 re-tallied it at *row* granularity: its
summary used to count spellings and disagreed with its own tables in two places. Three rows
were also split apart (notably `1.0 / 0` from `1.0::float8 / 0`, which behave differently).
Read the movement on that axis as a measurement correction, not a change in behaviour; the
per-row verdicts are the authority.

The read path's *statement* list, expression surface and type layer are done, including live
predicate and `LIMIT` push-down to the shards. The v0.2 pass closed the write path's
expression divergences by **translating** them rather than refusing them (§ 4.3), so **all ten
split-brain rows are closed** — including the one the translation itself opened, a zero divisor
that stored NULL and reported success; it closed the aggregate axis's last silently wrong row
and its last two missing percentiles; and it made the streaming `COPY` sub-protocol work, which
is what an analytical client needs first. It also made a division-by-zero raised *inside an
executor* arrive as `22012` rather than `XX000`, which was the last read-path SQLSTATE that
crossed the Ballista boundary misclassified. What it opened it also closed: the join axis
found that a keyless join whose result is decided on its build side answered no rows on a
cluster, which is now repaired in the scheduler's physical plan (§ 6.1 item 10). Every
silently wrong row that remains is a literal form, one float division, one advertised result
type, or one of the two narrow join rows — none of them a write.

---

## 1 — How VaireDB decides

Three type systems meet here, and they are not peers:

- **PostgreSQL is the contract.** It is the entry point, and what a client is promised.
- **DataFusion 54.1** plans and executes every `SELECT`, distributed over Ballista.
  Compatibility with it is not optional.
- **DuckDB 1.5.5** is the landing technology, one instance per shard. It may be restricted:
  a DuckDB-only form is out of scope unless PostgreSQL has it too.

### Two engines, two verdicts

A statement takes one of two entirely different routes:

| Path | Statements | Who evaluates the expressions |
|---|---|---|
| **Read** | `SELECT` | DataFusion, on the Ballista executors. Each shard runs only `SELECT <cols> FROM <shard_table> [WHERE <pushed predicate>] [LIMIT n]` — **no `GROUP BY`, and no aggregate is ever pushed into DuckDB.** |
| **Write** | `INSERT` / `UPDATE` / `DELETE` / `MERGE` / DDL | The statement is re-rendered to SQL text and executed **verbatim by DuckDB** on each shard. |

That split is the single structural defect in this analysis: **the same SQL fragment can get
two different verdicts.** Ten such rows have been identified, and **all ten are closed** — the
v0.2 pass translated the divergent expression on the write path rather than refusing it. The
tenth was *opened* by one of those fixes and then closed on its own terms: putting DuckDB in
`integer_division` mode to make `7/2` truncate also changed what a zero divisor does there, so
the divisor is now checked in the re-render rather than left to a setting that decides both
behaviours at once (§ 6.2, item 4).

### The parse and rewrite chain

| Stage | Where | What it does |
|---|---|---|
| **E1** Parse | `PostgresCompatibilityParser` (sqlparser 0.62, PG dialect) | One parse, PostgreSQL's dialect. Also where `COLLATE` is stripped and checked. |
| **E2** Rewrite | `pgwire_handler/pg_operators.rs`, `compat_rewrite.rs`, `pg_aggregate_widening.rs`, `pg_param_types.rs`, `column_labels.rs`, `anonymized_reads.rs` | The PostgreSQL expression layer: operators DataFusion spells differently, aggregate widening, placeholder typing, PG column labels, and the refusals. |
| **E3** Plan | datafusion-sql | Logical plan from the rewritten AST. |
| **E4** Execute | datafusion-proto → Ballista | Optimize, serialize to the executors, run. |

Type-changing read-path rewrites must run on the **logical plan inside `plan_select`, before
the analyzer** — the type a client is told is read off the *unanalyzed* plan, and
`TypeCoercion` erases the argument type PostgreSQL resolves overloads on.

Rewrites are AST-level rather than UDF registrations on purpose: a function registered on
the coordinator's planner but not on every Ballista executor resolves at planning time and
then fails on stage deserialization.

### Where a statement is refused

| Rejection point | Error | SQLSTATE |
|---|---|---|
| Parse (E1) | `SqlSyntaxError` | `42601` |
| Classification (`classify_statement`) | `FeatureNotSupported` | `0A000` |
| Coordinator rewrite (E2) | `[VDB-1004] … is not supported: …` | `0A000` |
| Logical / physical planning | `Error during planning` / `Physical plan does not support …` | `XX000` |
| Distributed execution | `[VDB-5001] Job … failed` | `XX000` |

`classify_statement` (`pgwire_handler/query_router.rs`) sorts a statement into 21
`QueryType`s; anything else is `QueryType::Other` and refused.

### Rules that apply to every statement

- A statement is either **correct on every shard or refused**. Nothing is applied partially
  by design.
- A write must have a **determinable shard key** before it leaves the coordinator.
- **Uniqueness is only enforceable when it includes the shard key.** `FOREIGN KEY` is never
  enforced; `CHECK` always is.
- **No cross-shard atomicity.** There is no 2PC. A transaction block spanning more than one
  node set is refused at `COMMIT` unless `allow_cross_shard_transactions` is set; a partial
  multi-shard write reports `40003` stating how much was written.
- **DDL is refused inside a transaction block.**
- Every object kind resolves through its own namespace: wrong kind is `42809`, nothing is
  `42P01`.
- A **schema qualifier is part of the catalog key** and folds into the physical name
  (`sales.t` → `sales_t_shard2`). There is no search path.
- A decoration that only affects **performance** is stripped; anything that changes
  **results** is refused **by name**.

---

## 2 — Implemented

### 2.1 Statements

Fully supported, no sharding caveat beyond the rules above:

| Statement | Notes |
|---|---|
| `SELECT` | The full read path, distributed via Ballista. `pg_catalog` / `vairedb_catalog` introspection answered from a local context. Its expression coverage is §§ 2.3–2.5. |
| `INSERT` | Shard key must be present and non-NULL per row. Multi-row inserts split per shard; a query source (`SELECT`, `UNION`, CTE) is materialized and re-emitted as literal `VALUES`. `ON CONFLICT DO UPDATE` works when the arbiter includes the shard key and is index-backed. |
| `UPDATE` | Except mutating the shard-key column — row relocation is refused. |
| `DELETE` | Routed to the owning shards, broadcast when the predicate's shard key is not a literal. |
| `CREATE TABLE` | VaireDB-extended with `WITH (shards, replication_factor, shard_by, anonymized_columns)`. The name is claimed atomically. `AS SELECT` supported when `shard_by` is stated before the query. |

Supported **with sharding restrictions** — each is real, and each refuses by name the forms
a sharded cluster cannot answer truthfully:

| Statement | What works | What is refused |
|---|---|---|
| `ALTER TABLE` | Column ops (ADD/DROP/RENAME, type, nullability, default), `RENAME TO`, `ADD`/`DROP CONSTRAINT … UNIQUE (<shard key>)`. | Dropping the shard key, touching anonymized columns, retyping a constrained column, most `ADD`/`DROP CONSTRAINT` forms (the shard engine implements neither `ADD … CHECK` nor `DROP CONSTRAINT`). While an index exists, only adding a column and changing a default. |
| `DROP` | Table, index, view, schema — each through its own namespace. `IF EXISTS` honored. | Several names in one statement, `CASCADE` / `RESTRICT`. |
| `ALTER VIEW` | `AS <query>`, validated like a `CREATE`. | `RENAME TO` (fails at parse). |
| `COPY` | Bulk CSV import/export, over a **file on the coordinator** *or* streamed over the wire — `FROM STDIN` and `TO STDOUT` both work, which is what makes `psql`'s `\copy` and every driver's bulk-load API work. `TO` gathers from every shard; `FROM` routes each row by its shard key through the same lane a client `INSERT` uses, so a load lands on all shards rather than one. `CopyData` chunks are buffered and flushed a batch at a time, so a load is not bounded by memory. `HEADER`/`DELIMITER`/`QUOTE` honored. A load that cannot work is refused at **Parse**, before the client is told to stream, and the connection leaves copy mode when the copy completes — both are needed for a driver's bulk-load API, which uses the extended protocol; see the command axis for what each one prevents. | `PROGRAM`, non-CSV formats. A `COPY FROM` that fails part-way leaves earlier per-shard batches committed — the same guarantee a multi-row `INSERT` has (§ 6.1.1). ⚠️ **No privilege check exists** — VaireDB has no role model. |
| `CREATE INDEX` / `DROP INDEX` | One real index per shard, on every replica. `UNIQUE` over the shard key doubles as an `ON CONFLICT` arbiter. | `UNIQUE` off the shard key, a partial or `NULLS NOT DISTINCT` unique index, an unnamed index, an expression index. |
| `CREATE SCHEMA` / `DROP SCHEMA` | Coordinator-local namespace. `42P06` taken, `3F000` missing, `2BP01` non-empty. | `CASCADE`, `AUTHORIZATION`, dropping `public` or a metadata schema. `ALTER SCHEMA` fails at parse. |
| `CREATE VIEW` | Coordinator-local: the text is stored and inlined as a CTE ahead of every query naming it, so it is planned fresh and never stale. `OR REPLACE`, `IF NOT EXISTS`, column lists, views over views. | `MATERIALIZED`, dialect decorations, a cyclic definition, a view named after a `pg_catalog` table. Views are read-only (`42809`). |
| `MERGE INTO` | All four `WHEN` kinds with their conditions and actions, applied shard by shard. Requires `ON` to equate the target's shard key with a co-located source or an inline `VALUES` list. | Any other source, an `ON` that does not pin the shard key, `UPDATE SET <shard key>`, `RETURNING`, per-action `WHERE`, a merge into an anonymized table. |
| Transactions | `BEGIN`/`START`, `COMMIT`/`END`, `ROLLBACK`/`ABORT`, `SAVEPOINT`, `ROLLBACK TO`, `RELEASE`, on both protocols. Writes are **buffered in the coordinator** and shipped at `COMMIT` as one atomic batch per node set, which makes `ROLLBACK` exact. | Inside a block: `UPDATE`/`DELETE` (unknowable row count), DDL, and reads of a table the block has written. `READ ONLY` honored (`25006`); a failed block stays failed (`25P02`). Isolation levels accepted and ignored. |
| `SET` | Every parameter drivers issue on connect: `client_encoding`, `application_name`, `extra_float_digits`, `DateStyle`, `TimeZone`, `IntervalStyle`, `standard_conforming_strings`, `statement_timeout`, `client_min_messages`. Each declares the values the coordinator's behaviour already matches; a value outside that set is refused **by value** (`22023`) naming what VaireDB does instead. | `search_path` **by name** (`0A000`), `SET LOCAL`, `SET ROLE`, `SET SESSION AUTHORIZATION`, `SET TRANSACTION`, `SET NAMES`. Unknown parameter `42704`, startup-fixed `55P02`. |
| `SHOW` | The read half of the same registry, so it always agrees with the startup `ParameterStatus`. `SHOW <name>`, `SHOW ALL`, `SHOW TIME ZONE`, `SHOW TRANSACTION ISOLATION LEVEL` (answers `read uncommitted`, which JDBC calls). | `SHOW TABLES`/`DATABASES`/`SCHEMAS` — a different statement, and PostgreSQL has none either. |
| `RESET` | `RESET <name>` and `RESET ALL`, which is what a pooler issues on checkout. Recognized before the parser and rewritten to `SET … TO DEFAULT`. | — (a `RESET` of a refused parameter **succeeds**: the default is what VaireDB already does). |
| `EXPLAIN` / `EXPLAIN ANALYZE` | The plan a read would actually run — views inlined, rewrites applied, per-shard remote scans visible. PostgreSQL's output shape: one `text` column named `QUERY PLAN`. `ANALYZE` runs distributed and reports per-stage metrics. | `EXPLAIN` of a write (there is no coordinator plan — the shard engine plans it), every option but `ANALYZE`/`VERBOSE` by name, the dialect plan modes, `EXPLAIN <relation>`. `PRAGMA` stays refused. |
| `DESCRIBE` / `DESC` | Answered by describing `SELECT * FROM <relation>`, which reaches a **view** for free. One row per result column with name, type, nullability. `DESCRIBE <query>` too. | `FORMATTED` / `EXTENDED` — they promise storage detail the coordinator does not hold. |

### 2.2 Data types

**Clean end-to-end** — the DDL lands, literal and parameterized writes both store exactly,
the read advertises an Arrow type matching DuckDB's plus a faithful PostgreSQL OID, and
values are correct in **both** text and binary format:

| Declared type | Arrow | PG OID |
|---|---|---|
| `BOOLEAN`, `BOOL` | `Boolean` | `bool` |
| `SMALLINT`, `INT2` | `Int16` | `int2` |
| `INTEGER`, `INT`, `INT4` | `Int32` | `int4` |
| `BIGINT`, `INT8` | `Int64` | `int8` |
| `TINYINT` | `Int8` | `int2` |
| `UTINYINT`, `USMALLINT`, `UINTEGER` | `UInt8/16/32` | widened |
| `REAL`, `FLOAT4`, `FLOAT` | `Float32` | `float4` |
| `DOUBLE PRECISION`, `FLOAT8` | `Float64` | `float8` |
| `NUMERIC(p,s)`, `DECIMAL(p,s)`, p ≤ 38 | `Decimal128(p,s)` | `numeric` |
| `VARCHAR`, `TEXT`, `CHAR`, `STRING` | `Utf8` | `text` |
| `BYTEA` (and `BINARY`, `VARBINARY`) | `Binary` | `bytea` |
| `DATE` | `Date32` | `date` |
| `TIME` | `Time64(µs)` | `time` |
| `TIMESTAMP` | `Timestamp(µs, None)` | `timestamp` |
| `TIMESTAMPTZ`, `TIMESTAMP WITH TIME ZONE` | `Timestamp(µs, "UTC")` | `timestamptz` |
| `INTERVAL` | `Interval(MonthDayNano)` | `interval` |
| `T[]` | `List(T)` | array of T |
| `T[n]` | `List(T)` — length dropped, as PostgreSQL does | array of T |

What made the last eight of those clean is worth stating, because each was a separate
decision: **declared parameters are part of the type** (a bare `DECIMAL`/`NUMERIC` becomes
`(18,3)`, DuckDB's own default); time and timestamp resolve to **microseconds, not
nanoseconds**; `TIMESTAMPTZ` carries `Some("UTC")` because the zone is the point of the
type; an array's declared length is **dropped** (`T[n]` → `T[]`), matching PostgreSQL and
avoiding an unencodable `FixedSizeList`; the schema-rebuild cast is **checked**, not safe;
and `interval` and `timestamptz` render in PostgreSQL's text form (`1 year 2 mons`,
`2024-01-01 10:00:00+00`) rather than Arrow's or ISO's.

Also accepted, deliberately, on the **`Utf8` fallback**: `UUID`, `CHAR`/`BPCHAR`, `JSON`,
`ENUM`, `STRUCT`. Values round-trip faithfully as text; only the advertised OID is `text`
rather than the type's own. An **unknown** declared type degrades the same way rather than
being refused, which is what keeps VaireDB forward-compatible with any type DuckDB adds
whose values are faithful as text.

**Aliases resolved** so DuckDB's spellings do not silently become `Utf8`: `BINARY`,
`VARBINARY` → `BLOB`; `LONG` → `BIGINT`; `SIGNED` → `INTEGER`; `SHORT` → `SMALLINT`;
`INT1` → `TINYINT`; `DATETIME` → `TIMESTAMP`; `LOGICAL` → `BOOLEAN`.

### 2.3 Aggregate functions

**There is no missing-coverage gap and no split-brain row.** Every aggregate is computed by
DataFusion; DuckDB never evaluates one. The surface is DataFusion's 38 default
`AggregateUDF`s plus 7 aliases, plus **two VaireDB UDAFs** — `percentile_cont`, which shadows
DataFusion's by name to remove its 5-decimal interpolation quantization, and `percentile_disc`,
which DataFusion does not have. Both are registered in `register_postgres_functions`, which
runs on the client context, the scheduler state *and* every executor; that is what makes them
survive a stage boundary, since datafusion-proto resolves an aggregate with an empty codec
payload by **name** from the executor's own registry, needing no codec change.

The distributed merge is correct **structurally**, not by testing luck:
`scheduler/remote_scan_exec.rs` declares `UnknownPartitioning(1)` and empty
`EquivalenceProperties`, so DataFusion cannot prove the shard streams are already
hash-partitioned and is *forced* to insert a shuffle:

```
AggregateExec{Partial} → RepartitionExec(Hash) → AggregateExec{FinalPartitioned}
```

There is **no hand-rolled recombination anywhere**, which structurally rules out
average-of-averages, a summed-per-shard `COUNT(DISTINCT)`, and a per-shard `MEDIAN` — the
partial aggregate never emits a finished value.

Correct, with PostgreSQL's result type:

| Family | Functions |
|---|---|
| Counting & sums | `count(*)`, `count(x)`, `count(DISTINCT x)`, `sum(int4)`, `sum(int8)` → **`numeric`, exact past `i64::MAX`**, `sum(float8)`, `sum(numeric)` |
| Averages | `avg(float8)`, `avg(numeric)`, `avg(int2/int4/int8)` → **`numeric`, exact past 2⁵³** |
| Extremes & selection | `min`, `max` (type-preserving for every type probed), `median` (exact, not approximate), `first_value`, `last_value`, `nth_value` in their aggregate forms with inner `ORDER BY` |
| Ordered-set | `percentile_disc(f) WITHIN GROUP (ORDER BY x)` — VaireDB's own UDAF, PostgreSQL's index rule with no interpolation, **returning the sort column's own type** (`int4`, `numeric`, `text` all verified) |
| Collection | `array_agg` (with inner `ORDER BY` and PG's NULLS-FIRST-on-DESC default), `string_agg` |
| Boolean & bitwise | `bit_and`, `bit_or`, `bit_xor`, `bool_and`, `bool_or` |
| Grouping | `grouping` — correct bitmask under `ROLLUP`, `CUBE`, `GROUPING SETS` |
| Statistical | `corr`, `covar_samp`/`covar`, `covar_pop`, and all eight `regr_*` (`slope`, `intercept`, `r2`, `avgx`, `avgy`, `sxx`, `syy`, `sxy`) |
| Approximate | `approx_distinct`, `approx_median`, `approx_percentile_cont`, `approx_percentile_cont_with_weight` |

Modifiers and clauses that work: `DISTINCT` inside any aggregate; `FILTER (WHERE …)` on a
plain aggregate; `ORDER BY` inside an aggregate; `GROUP BY` / `HAVING` (including `HAVING`
with no `GROUP BY`); `ROLLUP`, `CUBE`, `GROUPING SETS ((a),())`; `DISTINCT ON (col)`; an
aggregate used as a window function with an inline `OVER (PARTITION BY … ORDER BY …)`.

Three PostgreSQL spellings DataFusion lacks are **answered by AST rewrite**: `variance` →
`var_samp`, `every` → `bool_and`, `any_value` → `min` (`min` satisfies PostgreSQL's "an
arbitrary value **among the non-null inputs**" and is deterministic besides; `first_value`
is the closer-looking choice and the wrong one, since without `ORDER BY` it can return the
NULL PostgreSQL promises to skip).

An **unaliased result column carries PostgreSQL's bare function name** (`count`, `sum`,
`rank`), supplied by `pgwire_handler/column_labels.rs`, not DataFusion's rendered plan
expression.

An **untyped `$N` beside an aggregate takes the aggregate's type**
(`pg_param_types.rs`), so `HAVING sum(n) > $1` compares as a number rather than
lexicographically. `LIMIT $1` / `OFFSET $1` are typed `bigint` outright.

### 2.4 Window functions

**All 11 PostgreSQL window functions are present and correct**, verified against
`pg_proc WHERE prokind = 'w'`: `row_number`, `rank`, `dense_rank`, `ntile`, `percent_rank`,
`cume_dist`, `lag`, `lead`, `first_value`, `last_value`, `nth_value`. The ranking functions
advertise `int8` as PostgreSQL promises — the coordinator widens the `UInt64` result column
and checked-casts the payload above arrow-pg, which maps `UInt64 => NUMERIC`.

The **frame engine is healthy**: all five frame units (`ROWS`, `RANGE`, `GROUPS`, default,
and every bound combination), correct peer-group semantics (`RANGE` is *not* collapsed into
`ROWS` on ties, verified against duplicate ordering values straddling shard boundaries), and
integer, float and `INTERVAL` offsets. `RESPECT NULLS` is accepted, because it asks for the
behaviour DataFusion already has.

**Distribution is correct, including the cases that look most dangerous:**

| Scenario | Result |
|---|---|
| Global window, no `PARTITION BY` | Correct and **stable** — 10 consecutive runs identical, byte-identical at 1, 3 and 5 shards. Ballista cuts stages at `CoalescePartitionsExec`/`SortPreservingMergeExec`, so the window runs on one gathered partition. |
| `PARTITION BY <col>` | Correct and shard-count-invariant. Verified with skewed groups, empty tables, and — by reading `core.duckdb` out of a container with the DuckDB CLI — with every group physically spanning all 3 shards. |
| `PARTITION BY <col>` **+ equality on that same `<col>`** | Correct, and it used to be a hard failure. `scheduler/window_partition_sort.rs` puts the partition columns into the window's own sort, because **an equivalence property is a fact about a plan, not about the data, and it does not cross a stage boundary** — the knowledge that `g` was constant stayed behind in the stage holding the filter. A sort is only ever added, so no plan gets worse and no result changes. |
| `PARTITION BY <anonymized col>` | Correct, and **sound** — HMAC is deterministic and injective, so digest equality is plaintext equality. |

`OVER <name>` (a bare named-window reference, including several references to one window)
and `QUALIFY` both work.

### 2.5 Operators, literals and casts

Correct on **both** paths unless noted:

| Family | Operators |
|---|---|
| Arithmetic | `+` `-` `*` (incl. unary), `/` (integer division truncates on both paths — DuckDB is put in `integer_division` mode rather than rewritten, and the write path's re-render then guards the divisor, because that mode also makes a zero divisor answer NULL), `%` (sign follows the dividend, PG-aligned; accepts floats; a zero divisor guarded the same way), `^` (exponentiation — E2 rewrites to `power()`, left-associative as PostgreSQL specifies, so `2^3^2` is 64) |
| Comparison | `=` `<>` `!=` `<` `<=` `>` `>=`, `IS [NOT] DISTINCT FROM`, `BETWEEN` / `NOT BETWEEN`, `IN (list)` / `NOT IN`, `IS NULL` / `IS NOT NULL`, `IS TRUE`/`FALSE`/`UNKNOWN` (+ `NOT`) |
| Pattern | `~~` `~~*` `!~~` `!~~*` (LIKE/ILIKE aliases, identical in both engines); `~` `!~` `~*` `!~*` on **both** paths — the write path rewrites all four to `regexp_matches`, DuckDB's partial-match function, with an `'i'` flag for the case-insensitive pair, because DuckDB's own `~` is `regexp_full_match` |
| Logical & bitwise | `AND` `OR` `NOT` (three-valued logic correct), `&` `\|` `<<` `>>`, `#` (XOR — read-path only, DuckDB has no `#`), unary `~` (E2 rewrites to `x # -1`, exact for every two's-complement width) |
| String & array | `\|\|` (string, NULL-propagating as PG), `@>` `<@` (arrays), `&&` overlap (E2 → `array_has_any`), `^@` starts-with (E2 → `starts_with()`), `arr[n]` **1-based**, `arr[a:b]` inclusive, `struct['field']`, `ANY (array)` |
| Subquery | `EXISTS` / `IN (subquery)` in `WHERE`, including correlated — the optimizer decorrelates them into joins before serialization. A **scalar** subquery in the SELECT list also works. |
| Casts & zones | `::` / `CAST` / `TRY_CAST` (which returns NULL on failure), `AT TIME ZONE` |
| Collation | `C`, `POSIX`, `ucs_basic`, `default` — these *name* byte order, which is what VaireDB does, so they are dropped without changing an answer. `default` is load-bearing: `psql`'s `\d` sends `COLLATE pg_catalog.default`. |

Literals: `'…'` with `''` escaping, `E'…'`, `$$…$$` / `$tag$…$tag$`, `1e3`, `.5`, `TRUE` /
`FALSE` / `NULL`, `DATE '…'`, `TIME '…'`, `INTERVAL '1 day'` / `INTERVAL '1' DAY`,
`ARRAY[1,2,3]` and `[1,2,3]`, `ROW(1,2)` / `STRUCT(1,2)`, and `$1` placeholders.

**Decimal literals are exact.** `parse_float_as_decimal` is set on every read-path context,
so `1.5` is `Decimal128` rather than `f64`, `0.1 + 0.2 = 0.3` is **true**, and a 30-digit
literal reads back exactly instead of silently dropping its low digits. An explicitly-typed
`double` is still binary floating point, as in PostgreSQL.

`LIKE` / `ILIKE` / `NOT LIKE` / `NOT ILIKE` are correct on both paths, and each path needed a
different fix to get there. The *default* escape differed — `\` in DataFusion and PostgreSQL,
none in DuckDB, which is the shape every ORM emits when it escapes `_`/`%` in user input — so
the write path appends an explicit `ESCAPE '\'` whenever the pattern contains a backslash and
none was given. A *client-chosen* escape (`LIKE 'a!_%' ESCAPE '!'`) was the mirror image:
DuckDB honors any escape character, DataFusion honors only the backslash and fails at
execution on the rest, so the read path re-spells the pattern with backslash escaping before
planning. The pattern has to be a literal for that, since it happens before evaluation; a
computed pattern with a non-backslash `ESCAPE` is refused `0A000` rather than answered by the
wrong rules.

`SIMILAR TO` is correct on both paths, and it is worth saying why it needed work at all:
**`SIMILAR TO` is not a regex.** `%` and `_` are its wildcards and every other
metacharacter is literal. Both engines used to hand the pattern to a regex engine unchanged,
which was wrong in *both* directions at once. E2 now compiles it to an anchored regex by
PostgreSQL's own algorithm and calls `regexp_like`, and the write path reuses that same
translation rather than a second implementation. A pattern that is not a literal — a bound
parameter, say — is refused `0A000` on both paths rather than passed through under the wrong
rules.

**Filter and `LIMIT` push-down** into the shards is live, and it is a whitelist of shapes
whose meaning is identical in both engines — necessarily, because DataFusion's `Inexact`
contract keeps its own `FilterExec` as authority and therefore covers a shard returning too
*many* rows, not too *few*, and a shard that *errors* not at all. Four narrowings:

1. Nothing is pushed onto an **opaque text column** (one whose declared type degraded to
   `Utf8`).
2. **No ordering comparison on any text column** — collation is not pinned across the two
   engines.
3. `LIKE` only with a **literal, backslash-free** pattern — the default escape differs.
4. Only literal kinds that **render as the value they stand for**. `Date64` is excluded: it
   unparses as `CAST('2022-01-01 01:20:00' AS DATETIME)`, a *timestamp*, so a value carrying
   a time of day would be truncated by the coordinator and compared exactly by the shard,
   dropping rows. `Date32` renders as a real `CAST(… AS DATE)` and is what a PostgreSQL
   `date` parameter actually arrives as.

### 2.6 Anonymization, and the line it draws on reads

A column in `anonymized_columns` stores an **HMAC-SHA256 digest**. Anonymization is
structurally **write-path only** (`anonymization/rewrite.rs` falls through for everything
else), so a `SELECT` reads digests. The read path therefore refuses exactly what the hash
destroys and accepts everything it preserves:

| The hash **keeps** — accepted | The hash **destroys** — refused `0A000` |
|---|---|
| Equality against a 64-hex digest (the documented lookup), `GROUP BY`, `DISTINCT`, `count`, `count(DISTINCT)`, joins, `IS NULL`, `PARTITION BY`, projecting the digest | `ORDER BY`, `min`/`max`, `<` `<=` `>` `>=`, `BETWEEN`, a window's `ORDER BY` (inline or through a named `WINDOW`), an aggregate's `ORDER BY`, `LIKE`/`ILIKE`/`SIMILAR TO`/`~` and their negations |

Equality against a literal that **cannot** be a digest is refused with a message naming the
digest to send instead. There was no third option here: the plaintext is genuinely not on
the server, so the digest order is the only order a read can see, and a window is *defined*
by its ordering.

### 2.7 Joins and set operations

36 of the 43 rows of [`gap-analysis-join.md`](gap-analysis-join.md) are ✅, and every one was
measured in **both** layouts the shard map produces — *co-located*, where the join key is the
shard key on both sides and each shard can be joined where it lives, and *shuffle*, where the
rows have to be repartitioned across the cluster before they can meet. No row differs between
the two, which is the property that matters: the shard map may change the plan, never the
answer.

| Group | Works |
|---|---|
| Join types | `INNER`, `LEFT`, `RIGHT`, `FULL`, `CROSS`, the comma spelling and comma-plus-`WHERE`, with or without the `OUTER` keyword |
| Key syntax | `ON` (equi, non-equi `<`, disjunctive `OR`), `USING` on **every** join type — including the *merged* output column, asserted at `Describe`, and including the outer joins where the merge is the only way to see the key of an unmatched row — and `NATURAL`, which merges every common column |
| Shapes | self join, three-way join, join keys of different types (`int4`↔`int8`, `text`), `ON` vs `WHERE` on an outer join (the distinction that decides whether an outer join stays outer) |
| Composition | `LATERAL` and `LEFT JOIN LATERAL`, a join feeding `GROUP BY`/`HAVING`/`ORDER BY`/`LIMIT`, a join over a derived table or CTE, a one-sided shard-key predicate that prunes shards on that side only |
| Semi / anti | `IN`, `= ANY`, `EXISTS`, `NOT EXISTS`, `NOT IN`, `<> ALL`, a correlated scalar subquery and an uncorrelated aggregate subquery — all six agreeing, including on NULL, and no longer only when the subquery is correlated by an equality: a keyless semi or anti join used to answer nothing at all, which was the widest defect the join axis found and is closed (§ 6.1 item 10). `NOT IN` is null-correct in the clauses that test a predicate for truth; the two shapes it is not are § 4.3 row 3 |
| Set operations | `UNION` / `UNION DISTINCT` / `UNION ALL`, `INTERSECT`, `EXCEPT`, NULL identity across all three, `ORDER BY` by ordinal and by the first branch's label, `LIMIT` at both scopes, left-to-right chaining, branch type resolution over *every* branch in either order — including the `42804` refusal when the branches have no common PostgreSQL type, for all three operators — a `VALUES` branch, a bare-`NULL` branch, and nesting inside a derived table, a CTE or one side of a join |
| Aggregates over a join | `COUNT(*)` and `COUNT(<col>)` over a cross join, self cross join and three-way cross join — the empty-projection scan, which used to fail outright |

Seven of those closed in the pass that measured them, and each had been *silently wrong* or
outright broken: `COUNT(*)` over a join (`XX000`, a scan answering `SELECT *` for an empty
projection), `NOT IN (subquery)`'s NULL-awareness (only `HashJoinExec`'s anti join is
null-aware and Ballista does not plan it — closed by respelling the predicate before planning,
since the operator cannot be changed; see § 6.1 item 9), a set operation's advertised type
following its first branch instead of resolving over all of them, a `UNION` over branches with
**no common PostgreSQL type** answering rows PostgreSQL refuses (§ 6.1 item 12), the same
mismatch under `INTERSECT` and `EXCEPT` failing *mid-execution* with `XX000` and a leaked
DataFusion cast error (§ 6.1 item 13), `USING` on a full or right join reporting a key that
could not be told from a NULL (§ 6.1 item 11), and the widest of the seven — a join with **no
equijoin key** losing exactly the rows its build side decides, which made `EXISTS`/`NOT EXISTS`
constant false and a keyless `LEFT`/`FULL JOIN` an inner join (§ 6.1 item 10). That last one is
why the `NOT IN` respelling looks the way it does, and it was found because the respelling's
first draft ran into it.

---

## 3 — Partially implemented (🟡)

Correct behaviour under a wrong advertised type, one path only, or an honest error in the
wrong SQLSTATE. The 14 sharding-restricted statements in § 2.1 are supported-but-constrained
and are not repeated here.

### Types

| Item | Divergence |
|---|---|
| `UInt64` column | Advertised `numeric`, not `bigint` — arrow-pg has no unsigned PostgreSQL type. |
| `STRUCT` column | Advertised as `text`. Values faithful; the OID is wrong. |

### Aggregates — all eight are correct values under a divergent type

| Item | VaireDB | PostgreSQL |
|---|---|---|
| `stddev` / `stddev_samp` / `stddev_pop` | `float8` | `numeric` for int/numeric input — PG stays exact over a `numeric` column, VaireDB does not |
| `var` / `var_samp` / `var_pop` (and `variance`, which rewrites to `var_samp`) | `float8` | `numeric` |
| `regr_count` | `numeric` (from `UInt64`) | `int8` |
| `percentile_cont` / `quantile_cont` | **Value now exact** — VaireDB's own UDAF shadows DataFusion's, whose interpolation weight was quantized to 5 decimals (`9.099999` for an exact `9.1`). What remains is the type: always `float8`, where PostgreSQL returns `numeric` over a `numeric` or integer sort column. `DISTINCT` inside it is refused rather than ignored. PostgreSQL's **array-of-fractions** overload (`percentile_cont(ARRAY[0.25,0.5])`) is missing on both percentile functions and they refuse it inconsistently — `0A000` from `percentile_cont`, `XX000` from `percentile_disc`, whose own fraction check runs on an executor and so crosses the boundary in item 2. A fraction that is a plain **literal** does not cross it: an out-of-range one is refused on the coordinator with `22023`. | `numeric` for a `numeric`/integer input, `float8` for `float8` |
| `GROUPING SETS (())` alone | **0 rows** | 1 grand-total row. Narrow — the same set works combined with a non-empty one. |
| `count(DISTINCT a, b)` comma form | `XX000 NotImplemented`. Workaround `count(DISTINCT (a,b))` plans as `count(DISTINCT struct(a,b))` and **is correct**. | works |
| Nested aggregate (`max(sum(m))`) | `XX000`. The `Signature { … }` dump is now truncated, so the message is readable; the class is still wrong, because the error is raised past the Ballista scheduler — see § 6.2, item 2. | `42803` at parse |

### Window functions

| Item | Divergence |
|---|---|
| `ntile(n)` result type | `int8` where PG promises `int4`. Buckets are correct. A driver decodes it successfully into the wrong width — the last 🟡 on this axis VaireDB can close alone, in the same place the `UInt64` widening already lives. |
| Window fn in `WHERE`/`GROUP BY`/`HAVING`/nested | **Correctly rejected** (PG rejects too) but with `XX000` instead of `42P20`/`42803`. The debug dump is now truncated; the class is still wrong for the same reason as the nested aggregate above. Wrong error class breaks client error handling. |
| Duplicate unaliased labels | `SELECT sum(a), sum(b)` is two columns both called `sum` in PostgreSQL, which DataFusion refuses to plan, so those keep the verbose rendered label. `AS` is a full workaround. |
| Bind-parameter offsets (`ntile($1)`, `lag(x,$1)`) | Correct **provided the client declares a parameter OID**. |

### Operators and literals

| Item | Divergence |
|---|---|
| `<=>` | MySQL null-safe equality; **no PostgreSQL equivalent**. In DuckDB `<=>` is *vector distance* on `FLOAT[]`, so on a float-array column the two paths would diverge. |
| `\|\|` on arrays | DataFusion also overloads it as append/prepend by dimension; DuckDB rejects the element-to-list form PostgreSQL accepts. The **string** form is ✅ on both paths — only the array overload is partial. |
| `1_000` | Answers `1000`, but typed `numeric` where PostgreSQL types an underscore-separated literal `integer`. It works only incidentally: `parse_float_as_decimal` routes the token down the decimal parser, which tolerates the separator. Nothing strips separators anywhere. |
| `X'DEADBEEF'` | → `Binary`. PG gives `bit(32)`; DuckDB gives the VARCHAR `'xDEADBEEF'`. Three engines, three answers. |
| `TIMESTAMP '…'` literal | Nanosecond typing bounds a literal to **1677–2262**; outside that it fails in `simplify_expressions`. A DataFusion planner limit. |
| `TIMESTAMPTZ '…+02'` literal | Normalized to UTC correctly, but the offset is not shown back to the client. |
| `'{1,2,3}'::INT[]` | PostgreSQL's canonical array text form works on read; DuckDB renders lists as `[1, 2, 3]` on write. |
| `COLLATE` | The four byte-order names (`C`, `POSIX`, `ucs_basic`, `default`) work on both paths — they name the comparison VaireDB already performs, so the clause is dropped. Any other collation is now refused on **both** paths (`0A000`, naming it); it used to be silently ignored on write, which was the one row where a client could think a locale ordering had been applied. |

### Joins and set operations

| Item | Divergence |
|---|---|
| A join on a **pseudonymized** column | Equality survives HMAC-SHA256 — equal values have equal digests — so an equi-join on such a column relates two tables by it correctly, including under a `GROUP BY`/`SUM` above the join. Everything the digest does not preserve is refused `0A000` rather than answered: an ordering join (`a.email < b.email`), an `ORDER BY` on the column, and a comparison against **plaintext**, which would match nothing and say so nowhere. The restriction is enforced, not assumed, which is what keeps this 🟡 rather than ⛔ — see § 2.6. |

---

## 4 — Not implemented

### 4.1 Decided — not planned (🚫), with the reason

These are decisions, not backlog. Each is refused with a SQLSTATE and, where one exists, a
named alternative.

**No sequences** — `CREATE SEQUENCE`, `nextval()`, `SERIAL`. One monotonic counter is
precisely what a shared-nothing cluster cannot maintain cheaply. Use an application-side
UUID, ULID or snowflake ID.

**No search path** — `SET search_path` is refused **by name**, and `USE` likewise. A
qualifier here is *part of the catalog key*, not a lookup hint: it folds into the physical
name. There is nothing to search.

**No user-defined types** — `CREATE TYPE`, `CREATE DOMAIN`, `ALTER TYPE`. Cluster-wide state
with no catalog replay path and no `pg_type` OID to advertise. A `CHECK` constraint covers
the common case.

**No dialect-only statements** — PostgreSQL is the contract, so a form only DuckDB has is
out of scope: `PIVOT` (`CASE` inside aggregates plus `GROUP BY`), `UNPIVOT` (`UNION ALL`, or
a `LATERAL` over a `VALUES` list), `SUMMARIZE` (the aggregates written out), `SET VARIABLE`
(nothing in the read path would read one back, so accepting it would be a fake `OK`).

**No storage maintenance** — `ANALYZE` (no planner-statistics surface to populate) and
`VACUUM` (a per-shard storage concern) have nothing to act on at the coordinator.

**Eight data types refused by name at DDL** (`0A000`, at `CREATE TABLE`, `ADD COLUMN` and
`ALTER COLUMN TYPE`), each naming what to write instead. Every one of these previously
*accepted* the DDL and then failed or lied on read:

| Refused | Use instead |
|---|---|
| `HUGEINT` / `INT128` | `BIGINT`, or `DECIMAL(38,0)` |
| `UHUGEINT` / `UINT128` | same |
| `BIT` / `BITSTRING` / `VARBIT` / `BIT VARYING` | `BOOLEAN`, or `VARCHAR` for the `'0'`/`'1'` spelling |
| `BIGNUM` / `VARINT` | `DECIMAL(38,s)` |
| `UNION` | one nullable column per member, or `JSON` |
| `VARIANT` | `JSON` |
| `MAP` | `JSON`, or a pair of array columns |
| `TIMETZ` / `TIME WITH TIME ZONE` | `TIMESTAMPTZ`, or `TIME` with the offset in its own column |

An array is refused for the same reason as its element type.

### 4.2 Rejected, no target behaviour agreed (❌)

**Statements** — 4 fail at classification, 3 at parse, 2 across both:

| Statement | Why |
|---|---|
| `ATTACH` / `DETACH` | Single-node DuckDB attachment; meaningless for a cluster. |
| `CHECKPOINT` | Per-shard storage concern, not coordinator-exposed. |
| `EXPORT` / `IMPORT DATABASE` | Whole-DB dump/load. Use `COPY`. |
| `INSTALL` / `LOAD` | Extension management is a per-node concern. |
| `CREATE SECRET` | DuckDB's secrets manager. VaireDB has its own path (`INSERT INTO vairedb_catalog.anonymization_secret`). |
| `USE` | One database per cluster, and no search path to switch. |
| `CALL` | No stored or table procedures. |
| `COMMENT ON` | No catalog comment storage and nowhere to read one back — accepting it would be a fake `OK`. |
| `CREATE MACRO` | DuckDB-only. |

**Aggregates** — 8 PostgreSQL spellings DataFusion lacks, each needing a new UDAF:

- `mode() WITHIN GROUP (…)`.
- `rank()`, `dense_rank()`, `percent_rank()`, `cume_dist()` as **hypothetical-set**
  aggregates (`WITHIN GROUP`). The **window** spellings of all four work, so only the
  `WITHIN GROUP` form is missing.
- `json_agg`, `jsonb_agg` — blocked behind the JSON type gap below.
- `xmlagg` — no XML type. Out of scope.

Plus two clause combinations refused because a discarded clause is a wrong answer:
`FILTER` on a **window** aggregate, and `OVER (<named_window> …)`. Both are § 4.3.

**Window clauses** — 10 rows, all refusals, and 8 of them are ⛔ rows *converted into
refusals* rather than left returning a plausible number:

| Clause | Status |
|---|---|
| `count(*) FILTER (WHERE …) OVER (…)` | Refused `0A000`. `FILTER` is lost by **Ballista's plan serialization**, not by DataFusion: `PhysicalWindowExprNode` has no filter field and the deserializer hardcodes `None`. Single-node DataFusion is correct. **This is the one defect VaireDB owns *because* it is distributed** — no upstream release closes it. Rewrite with a `CASE` inside the aggregate, or aggregate a subquery. |
| `IGNORE NULLS` | Refused `0A000`. Was a **silent no-op** — `IGNORE NULLS` and `RESPECT NULLS` returned byte-identical columns. One hardcoded `false` in `to_proto.rs`, with a stale comment claiming the field is unused. The worst of the discarded clauses, because the NULL it asked to skip comes back looking like data. |
| `OVER (w)` | Refused `0A000`. Redundant parens dropped `PARTITION BY` **and** `ORDER BY`; the frame widened to the whole table. |
| `OVER (w <extra clauses>)` | Refused `0A000`. Adding `ORDER BY` lost the partition; adding a **frame** made it **nondeterministic** — five consecutive runs on unchanged data returned five different answers. The only unstable row in any VaireDB gap analysis, and the reason this is refused rather than patched: an unstable answer is worse than no answer. |
| `WINDOW w2 AS (w1 …)` | Refused `0A000`. Chained inheritance in the `WINDOW` list, which the `OVER`-clause check structurally cannot see, so it needs its own check. |
| `EXCLUDE {CURRENT ROW \| GROUP \| TIES \| NO OTHERS}` | `42601` — **unparseable**. sqlparser's `WindowFrame` has a literal `// TBD: EXCLUDE`. The only ❌ in this analysis that needs parser work first, and the only genuinely *missing feature*. Rare in practice. |
| Window fn in outer `ORDER BY`, written inline | `XX000` — **legal PostgreSQL, rejected**. Ordering by the output alias or position is a full and general workaround. |
| Order-sensitive reads over an anonymized column (3 rows) | Refused `0A000` — § 2.6. |

Root causes upstream: `WindowSpec::window_name` is parsed by sqlparser and **never read** by
datafusion-sql; `IGNORE NULLS` is one hardcoded line; the filter field needs a proto change
plus both codecs.

**Operators and literals** — 20 rows:

| Rejected | Why |
|---|---|
| `**` | `42601`. **No sqlparser 0.62 dialect parses it** — needs upstream work, not a dialect change. |
| `//`, `DIV` | `42601`. `//` is dialect-gated (PG's dialect rejects it); DataFusion's `IntegerDivide` is unimplemented anyway. |
| `->` `->>` `#>` `#>>` `@?` `@@` | Present in DataFusion's `Operator` enum, unimplemented in type coercion. **The whole `jsonb` operator family is absent**, and unreachable regardless because `::json` fails at planning. `@@` used to leak a raw gRPC `Status { … }` payload on top of that; the sanitizer now strips that shape on every path (§ 6.2 item 5). |
| `'{"a":1}'::json`, `'…'::uuid` | `0A000`. Both work as **column** types but not as cast targets — which is what makes every JSON-operator probe unreachable. |
| `'abcde'::VARCHAR(2)` | `0A000` naming the cast. The length used to be **silently discarded** where PostgreSQL truncates; it is not enforced anywhere on the read path, so E2 refuses it and names `substr()`. All four spellings covered. `NUMERIC(p,s)` is unaffected — that precision *is* applied. |
| `MAP {'a':1}`, `{'a': 1}` | `42601`, dialect-gated at E1. `MAP(['a'],[1])` parses and plans, then dies at `XX000 Unsupported Datatype Map`. |
| `str[n]`, `str[a:b]` | `0A000`. DuckDB supports string subscripting; PostgreSQL does not. |
| `struct.field` | `0A000 Dot access not supported` — the class is now right; the form is still unavailable. Both PG (with parens) and DuckDB support it. `struct['field']` works. |
| `LIKE ANY (array)` | `0A000` naming the clause (it used to be `XX000`). `ALL (array)` and `= ANY (subquery)` were in this table and both now **answer** — the first ordinary DataFusion, the second because `= ANY (subquery)` is normalized to `IN` on the verbatim AST before the array rule can claim it. |
| `EXISTS` / `IN (subquery)` in the **SELECT list** | `XX000 … Expr::Exists { .. } not supported`. Not decorrelated there, so the expression reaches `datafusion-proto`, which cannot encode a subquery. The `WHERE` spelling works, including correlated, and a **scalar** subquery in the SELECT list works. |
| `GLOB` / `~~~` | `42601`. DuckDB-only. |
| `U&'…'`, `N'…'`, `B'…'` | `0A000`. Unimplemented in both engines. DuckDB silently turns `B'1010'` into the *string* `'b1010'`, so the loud rejection is the better behaviour. |
| `INTERVAL '1-2' YEAR TO MONTH` | `0A000`. Unimplemented in both engines; PostgreSQL has it. |

**Joins and set operations** — 3 rows. Two of the three are **wrong answers converted into
refusals** rather than gaps that were never implemented; the third was uncovered by closing a
wrong answer, and was always there:

| Rejected | Why |
|---|---|
| An unqualified `USING` key in a `WHERE` clause — `… USING (c) WHERE c > 2` | `42703` (PostgreSQL's code for an ambiguous column is `42702`, and it does not refuse this at all): `Ambiguous reference to unqualified field c`. DataFusion's name resolution does not see the join's `USING` set from a `WHERE` predicate, so two fields named `c` are ambiguous to it where PostgreSQL sees one merged column. It refuses on **every** `USING` and `NATURAL` join, inner included, so it is not a cost of the merge in § 6.1 item 11. Every other clause resolves the merged column correctly — `GROUP BY`, `HAVING`, `ORDER BY`, an aggregate argument, the select list — and the qualified spelling `WHERE l.c > 2` works. |
| `x <op> ANY \| SOME \| ALL (subquery)` for every `<op>` except `= ANY` and `<> ALL` | `0A000`. Each plans to a **mark** join, whose output column is named `mark` on both sides of the join above it, and serializing that plan for an executor fails — `Schema contains duplicate unqualified field name mark`. It used to reach the client as `XX000` carrying a raw gRPC `Status { … }`. The refusal names the `max()`/`min()` rewrite over the same subquery, or `EXISTS`, **and states that the aggregate form answers differently for an empty subquery and for one containing NULLs** — which is why it is named and not applied. `= ANY` and `<> ALL` are normalized to `IN`/`NOT IN` and work. |
| `INTERSECT ALL`, `EXCEPT ALL` (and `MINUS ALL`) | `0A000`. These count **multiplicity**; both were answered as the plain semi/anti join the `DISTINCT` forms are built from, so with left `1,1,1,2` and right `1,1,3`, `INTERSECT ALL` answered `1,1,1` where PostgreSQL answers `1,1` and `EXCEPT ALL` answered `2` where PostgreSQL answers `1,2`. Neither the right count nor a subset of it — and a row count is exactly what an analytical client goes on to aggregate. The refusal names the `DISTINCT` form. `UNION ALL` is untouched. |

**Types** — one broken: `Decimal256` (a `NUMERIC` with precision > 38). arrow-pg has no arm
for it, so it is refused `XX000` rather than rounded.

### 4.3 Silently wrong (⛔) — everything that remains

The dangerous class, and the shortest list in this document. It used to be led by five
**write-path** rows sharing one fix; all five are now closed — four by translating the
divergent expression rather than refusing it, the fifth by guarding it — and what is left
divides cleanly: two literal forms, one read-path float row, and two read-path rows the
join axis found. Four join rows have been here and left. The widest was a join with no
equijoin key answering no rows, which made `EXISTS` and `NOT EXISTS` constant false and a
keyless outer join an inner one; it left in the same pass it entered (§ 6.1 item 10). Two more
were `FULL JOIN … USING (c)` reporting a key that could not be told from a NULL, and a `UNION`
over branches with no common PostgreSQL type answering rows PostgreSQL refuses; row 2 below is
what closing the first of those could not reach (§ 6.1 items 11 and 12). The fourth was a row
of this section that closing the `UNION` row created and then removed again: `INTERSECT` and
`EXCEPT` over the same mismatch, which had been recorded here as unclosable because the plan
shape was believed indistinguishable from a client's own `IN (subquery)`. Printing the two
plans showed it is not, and it is now refused with the rest (§ 6.1 item 13).

| # | Construct | Returns | PostgreSQL |
|---|---|---|---|
| 1 | `1.0::float8 / 0` **on the read path** | `inf` — a poison value that flows into aggregates. The integer and `numeric` forms now raise `22012`, PostgreSQL's own class, even though the error is raised on an executor and crosses the Ballista boundary as text; float division is the one that still answers. Arrow's float division follows IEEE 754, so there is no error to classify. | `22012 division_by_zero` |
| 2 | `SELECT a.id, b.id FROM a FULL JOIN b USING (id)` — the key column reached by an **explicit qualifier** | the merged `COALESCE` value under *both* qualifiers, where PostgreSQL keeps the raw per-side values and their NULLs. The unqualified `id` and `SELECT *` are now correct (they were the wrong answer this row replaced), and this is what that fix cannot represent: PostgreSQL's join output has three addressable names — `id`, `a.id`, `b.id` — where a DataFusion schema has two fields, so the merged value has to occupy whichever fields every other consumer reads. Rare and expert; the `ON` spelling answers it exactly. | the raw `a.id` and `b.id`, NULL where the row is unmatched |
| 3 | `HAVING max(k) NOT IN (SELECT …)`, and `WHERE k NOT IN (<correlated subquery>)` | one row too many, in both cases the one whose key is NULL. These are the two shapes the `NOT IN` rewrite cannot enter: DataFusion will not plan a correlated subquery whose outer reference is an aggregate of the group, and a derived table cannot see the outer row without `LATERAL`. It used to need one thing more — with `LATERAL`, what survives has no bare equality, so it would have landed on the keyless-join defect and answered no rows at all — but that is closed (§ 6.1 item 10), leaving `LATERAL` as the only obstacle. Everywhere else — `WHERE`, `HAVING`, `QUALIFY`, `ON`, through `AND`/`OR`, and under a `NOT` — `NOT IN` is null-correct (§ 2.7, § 6.1 item 9). `NOT EXISTS` expresses both correctly today. | the NULL-keyed row excluded: the predicate is NULL, and NULL is not true |
| 4 | `0b101` | **`0`**. Not a parse error: sqlparser tokenizes it as `0` aliased `b101`, so the planner is handed `SELECT 0 AS b101`. Confirmed in all four dialects and both versions — a tokenizer defect. DuckDB mangles it identically. | `5` |
| 5 | `'\xDEADBEEF'::bytea` | the **10 ASCII bytes of the literal text** — PostgreSQL's hex-escape input format is not decoded; DataFusion casts `Utf8`→`Binary` bytewise. Note this does **not** contradict `Binary` being rated clean: that holds for parameterized writes, not for this literal form. DuckDB's `::BLOB` *does* decode `\x`. | 4 bytes |

Closed in the v0.2 pass, and worth recording because the *shape* of the fix was not the one
planned: `7/2`, `s ~ '^a'`, `s LIKE 'a\_b'` and `SIMILAR TO` on the write path were all going
to be **refused** — the cheap way to stop being silently wrong. Three of the four turned out
to be translatable instead (`write_sql_cl/dialect.rs`: `~` and its three variants to
`regexp_matches`, `LIKE`/`ILIKE` given PostgreSQL's `\` escape explicitly, `SIMILAR TO`
compiled by the read path's own algorithm), and `/` fell to one session setting on the shard
connection (`SET GLOBAL integer_division = true`). So a client gets the right answer rather
than a refusal, and only what cannot be translated is refused (`write_sql_cl/reject.rs`: a
locale collation, a length-bearing character cast, and a `SIMILAR TO` whose pattern is a
parameter and therefore not visible at parse time).

The fifth was the one that setting *opened*, and it closed the same way: a zero divisor on
the write path stored NULL and reported success, because the flag that makes `7/2` answer `3`
is the same flag that turns a zero divisor into NULL, and DuckDB has no second flag to
separate them. So the divisor is checked in the re-render instead — every write-path `/` and
`%` is wrapped in a guard that raises PostgreSQL's own message, which the core node
classifies back into `22012`. One shape is refused rather than guarded, and recorded here
because it is a real narrowing: a divisor that is *itself* a division. The guard has to name
the divisor twice, so nesting one inside another would duplicate its guard, and a right-nested
chain would grow the rendered statement exponentially. A division in the dividend is
unaffected, and so is every divisor that is not a division.

And three where the divergence is known and the fix is **deliberately deferred**, each for a
stated reason rather than by oversight:

- `nth_value(x, 0)` returns NULL for every row; PostgreSQL raises `22016`. One guard
  upstream, and the narrowest row in this analysis.
- `arr[-1]` returns the last element on both paths; PostgreSQL returns `NULL`. Consistent
  and silently non-PG — a decision, not yet made.
- `0x1F` reads as `Binary` (bytes `1f`) where PG 16 gives the integer `31`; on write the
  render rewrites it to `X'1F'` and the shard fails `42804`. That render is irreducible.

---

## 5 — Supersets that must NOT be "fixed"

VaireDB accepts these and PostgreSQL does not. Narrowing to PG parity would be a regression
against DataFusion and DuckDB, and each is recorded so nobody "corrects" it:

- `count(DISTINCT x) OVER (…)` — PG rejects `DISTINCT` in a window aggregate (`42P20`).
- `QUALIFY` — DuckDB/Snowflake syntax PostgreSQL does not have.
- Negative `nth_value` offsets.
- `DISTINCT ON (col)` is the reverse case: a PostgreSQL extension, and it works.
- `INTERVAL 1 DAY` unquoted — DuckDB-only; PG rejects it.
- `count(DISTINCT (a,b))` — plans as `count(DISTINCT struct(a,b))` and is correct.

---

## 6 — Limitations, issues and next phases

### 6.1 Standing architectural limitations

These are properties of the design, not backlog items. A client should expect them.

1. **No cross-shard atomicity.** There is no two-phase commit. A transaction spanning more
   than one node set is refused at `COMMIT` unless `allow_cross_shard_transactions`; a
   partial multi-shard write reports `40003` with how much was written. This is the standing
   limitation beyond every statement row.
2. **Uniqueness only over the shard key.** A `UNIQUE` or `PRIMARY KEY` constraint is
   enforceable only when it includes the shard key. `FOREIGN KEY` is never enforced.
3. **The shard key is immutable.** `UPDATE` of the shard-key column is refused; row
   relocation does not exist.
4. **A transaction block buffers.** Only statements the coordinator can answer truthfully
   *without running them* are allowed inside one, so `UPDATE`/`DELETE`, DDL, and reads of a
   table the block has written are refused.
5. **No role model, therefore no privilege checks.** Most visibly, `COPY` reads and writes
   any path the coordinator process can. Tracked on the roadmap as *Security: TLS, users,
   groups*.
6. **Anonymization is one-way and write-path only.** The plaintext of a pseudonymized column
   is not on the server. Order and value are unrecoverable; equality and grouping are exact
   (§ 2.6).
7. **No sequences, no search path, no user-defined types** — § 4.1.
8. **Text ordering is byte order.** Collation is not pinned across the two engines, which is
   also why no ordering comparison on a text column is pushed to a shard.
9. **A null-aware anti join cannot be shipped by this Ballista version, so `NOT IN` is
   respelled instead.** Ballista sets `datafusion.optimizer.prefer_hash_join = false`, for a
   resource reason: DataFusion's hash join cannot spill while its sort-merge join can. But
   only `HashJoinExec` carries the `null_aware` flag that makes an anti join agree with
   PostgreSQL when a key is NULL, and `NOT IN (subquery)` decorrelates to exactly that anti
   join — so under sort-merge it returned rows PostgreSQL excludes. Turning the option on
   looked like a one-line correctness fix and is **unshippable**, measured on the cluster: a
   null-aware anti join is only correct as a broadcast, so `JoinSelection` stamps it
   `CollectLeft`; Ballista's planner refuses to broadcast a join driven by its build side,
   demotes it to a shuffle and swaps the sides; `HashJoinExec` then refuses to build a
   null-aware `RightAnti`; the job dies inside the scheduler and **the client hangs**,
   because the failed stage reports no status. So the operator is left alone and the
   predicate is respelled in the AST before planning
   (`compat_rewrite::rewrite_not_in_subqueries`). `NOT EXISTS` is two-valued, so the rewrite
   applies only where NULL and false are indistinguishable — see § 4.3 row 3 for the two
   shapes that leaves — and its *form* was dictated by item 10 below.
10. **An operator that coordinates its partitions through shared memory cannot be
    distributed, and one of them is the nested-loop join.** The standing limitation; the
    defect it caused is closed. Ballista runs each partition of a stage as a separate task in
    a separate **process**, so any operator that decides its output by counting how many
    partitions have finished is counting inside one task only. DataFusion's
    `NestedLoopJoinExec` is exactly that: for the join types whose result comes from a match
    bitmap over the collected build side — `Left`, `Full`, `LeftSemi`, `LeftAnti`, `LeftMark`
    — it emits nothing until a shared counter, seeded with the probe's partition count,
    reaches zero. Every task seeded its own and decremented once, so the emission happened
    nowhere: a keyless `EXISTS` and a keyless `NOT EXISTS` were both false for every row, and
    a keyless `LEFT`/`FULL JOIN` silently dropped its unmatched rows. The counter cannot be
    made to span processes without changing Ballista, so the coordinator removes the need for
    it — `scheduler::nested_loop_join_one_task` coalesces the probe side of those five join
    types to one partition, at the cost of the probe's parallelism and only for the shapes
    that were broken. Two things about the class outlive the fix. First, **no unit test over a
    plan built from SQL can catch a regression in it** — in one process the unrepaired plan
    answers correctly, so the rule's tests construct the operator by hand and the e2e suite is
    the only end-to-end guard. Second, it is the reason item 9's respelling of `NOT IN`
    produces an anti join on a **bare equality**: at the time that was the only shape the
    cluster answered, and it remains the cheaper one, since an equality plans as a partitioned
    hash join where the disjunction plans as a coalesced nested loop. A logically equivalent
    one-join rewrite was written first, passed every unit test, and returned no rows on the
    cluster — which is how this whole item was found.
11. **A `USING` key has three names in PostgreSQL and two fields in a plan, so the merge is
    projected and the qualified spelling pays for it.** `USING (c)` does not only name a join
    predicate: it names one *merged* output column whose value is `COALESCE(left.c, right.c)`,
    while `l.c` and `r.c` stay reachable and raw. DataFusion's join keeps both sides' columns
    and fakes the merge twice, in name resolution and in wildcard expansion, each time by
    *picking* one side rather than combining them — and the two disagree about which. On an
    inner or left join that is invisible, because the left value is the answer; on a full or
    right join the unmatched row reported NULL where PostgreSQL reports its key, which was
    silently wrong and is closed: the coordinator projects the `COALESCE` over the join, into
    the join's own schema so nothing above it sees a new shape
    (`pgwire_handler/pg_using_join_merge.rs`). What does not follow is the third name. Both
    fakes have to be overwritten for `SELECT c` and `SELECT *` to agree whatever the tables are
    called, so the merged value occupies both fields, and an explicit `l.c`/`r.c` on a full or
    right join now reports it too (§ 4.3 row 2). The `ON` spelling with the client's own
    `COALESCE` addresses all three exactly and is the documented alternative. Separately, and
    also upstream: an **unqualified** `USING` key in a `WHERE` clause is refused as an
    ambiguous reference on every join type, inner included — loud, and unrelated to the merge.
12. **Arrow can always find a common type, so a `UNION` has to be refused on PostgreSQL's rule
    rather than on whether one exists.** DataFusion asks a set operation's branches for *a*
    common type and Arrow always has one, because everything casts to a string. PostgreSQL asks
    for one reachable by **implicit** coercion, and between two type categories there is none —
    so `int ∪ text` is a plan-time `42804` there and was seven rows of text here. The rule is
    now applied before coercion, on the branch types the planner has not yet reconciled
    (`pgwire_handler/pg_set_op_types.rs`), which is the only point at which the disagreement
    still exists. Two things about it are permanent. First, PostgreSQL's `UNKNOWN` has to be
    modelled and Arrow does not have it: a bare string literal or `NULL` in a branch's select
    list has no type there and takes the other branches', so the *literal* is recognized in the
    plan instead — and only in a select list, because a `VALUES` clause resolves its own columns
    to text first, which makes two spellings that look interchangeable differ. Second, the
    literal is *recognized* but not *retyped*, so the rows are PostgreSQL's while the advertised
    column type is not: `… UNION ALL SELECT '9'` comes back as `text` where PostgreSQL says
    `integer`, which is the one thing this leaves and is recorded as such.
13. **`INTERSECT` and `EXCEPT` do not survive planning as set operations, and the reason that
    was thought to make item 12 unreachable there was a guess.** DataFusion lowers them to a
    `LeftSemi` and a `LeftAnti` join, so item 12's check saw no set operation and the mismatch
    reached the executors, where it failed with `XX000` and a leaked
    `CastError("Cannot cast string 'x' to value of Int32 type")` — work done, nothing usable
    returned. The written reason for leaving it was that the lowered join is indistinguishable
    from the one a client's own `IN (subquery)` produces, which PostgreSQL refuses with a
    *different* code (`42883`). Printing both plans disproved it: at the point the check runs a
    subquery is still an expression inside a `Filter` and has not become a join at all, and an
    explicit `LEFT SEMI JOIN` — not PostgreSQL syntax — carries its equality in the join's
    filter where a lowered set operation carries it in the join's keys. So all three operators
    are now refused under one rule, each named in its own message. The transferable lesson is
    the method, not the shape: a claim about a plan is worth what printing the plan costs.

### 6.2 Open issues, ranked by consequence then cost

**1. A join with no equijoin key answered no rows.** **Closed.** It held the top of this list
for one pass, and it was the widest defect the join axis found: `EXISTS` and `NOT EXISTS` were
unusable for every subquery not correlated by an equality — which includes the two most
ordinary spellings an analytical client writes, an existence check with a constant predicate
and an inequality correlation as a "has a later row" test — and both directions answered
false. Closing it found that it was wider still: a keyless `LEFT` or `FULL JOIN` was dropping
its unmatched rows too, silently turning an outer join into an inner one, which no row of any
axis had asked about. One root cause covers all five join types, and it is not in the
scheduler's plan rewrite where this list first pointed: DataFusion's nested-loop join emits
those rows only after a **shared, in-process** counter of finished probe partitions reaches
zero, and Ballista gives each partition its own process (§ 6.1 item 10). The repair is a
physical optimizer rule in the coordinator's own scheduler state,
`scheduler::nested_loop_join_one_task`, coalescing the probe side of exactly those five join
types so the count is always one. The second option this item offered — planning the shape as
a nested-loop semi join, correct if slow — is in effect what shipped; the first, refusing it,
was not needed. The cost is the probe's parallelism, on those shapes only.

**2. An error raised past the Ballista scheduler loses its SQLSTATE.** The residue of what
used to be this list's second item, and now the whole of it. The classifier itself is fixed:
it matches on `DataFusionError` variants rather than message substrings, so a version bump
cannot rot it, and `sanitize.rs`'s prefix list is re-derived from DataFusion 54.1's own
`error_prefix()`. Coordinator-local errors now classify correctly — `'x'::int` is `22P02`,
`struct.field` and `LIKE ANY` are `0A000`, `1_000` answers `1000` outright — and the
`~1.5 KB Signature { … }` debug dump is truncated on every path.

What is left is structural rather than a mapping table. An error raised on an **executor**
reaches the coordinator as the scheduler's own text — `Job <id> failed: Job failed due to
stage N failed: … DataFusionError(Execution("ArrowError(DivideByZero)"))` — so there is no
typed error left to classify and it lands `XX000`. A nested aggregate is still `XX000` instead
of `42803` for exactly that reason.

**Division by zero is the one named exception**, and it is worth reading as a scoped
concession rather than a pattern to copy. `reclassify_transported_data_error` in
`error_enrichment.rs` fires *only* when the typed classifier already returned `EngineError` or
`InternalError` — i.e. only when there was nothing to classify — and *only* for the
divide-by-zero spellings, including Arrow's spaceless `Debug` form `DivideByZero`. That makes
it independent of which `DataFusionError` variant the wrapper happens to be, which matters
because the arriving variant is not the one Ballista's source constructs. `1 / 0`,
`1.0 / 0` and `7 % 0` all report `22012` live. The general fix is unchanged: carry a
structured error across the scheduler's gRPC surface. Re-parsing that text for every class is
exactly the substring matching the typed classifier was written to get rid of, which is why
this exception is one function with one predicate and not a fallback table.

**3. `ntile` advertises `int8` where PostgreSQL promises `int4`.** A one-function widening in
the same place the `UInt64` widening already lives — the last 🟡 on the window axis VaireDB
can close alone.

**4. Division by zero on the write path stored NULL and reported success.** **Closed.** It
was the one split-brain row the write path's expression work left behind, and it was
introduced *by* that work: `SET GLOBAL integer_division = true` is what makes `7/2` answer `3`
as PostgreSQL does, and the same setting turns a zero divisor from DuckDB's `inf` into NULL. A
narrower lie than `inf`, since NULL does not poison an aggregate, but a lie with a success
report attached.

The setting could not be asked to fix it, because one flag decides both behaviours, and
DuckDB 1.5.5 has no second one — measured, not assumed: with `integer_division` on, `7/0`,
`7.0/0` and `7 % 0` are all NULL, and nothing in `duckdb_settings()` turns any of them into an
error. So the divisor is checked where the statement is built instead: every `/` and `%` in a
write is re-rendered wrapped in a guard that raises PostgreSQL's own message when the divisor
is zero, and the core node classifies that message back into `22012` — the same class the read
path reports for the same expression. Three properties of the guard were measured rather than
assumed, and all three are what make it safe to apply unconditionally: it does not change the
result type (so `7/2` still stores `3` and `7.0/2` still stores `3.5`), it is evaluated per
row rather than folded when the statement is bound (so an ordinary division costs nothing and
an empty table raises nothing), and a repeated placeholder still binds once.

One shape is refused instead of guarded, and it is a real narrowing rather than an oversight:
a divisor that is itself a division or a modulo. Raising from a DuckDB expression is only
possible through `error()` inside a `CASE`, which has nowhere to bind the divisor once, so the
guard names it twice — and a divisor carrying a guard of its own would be duplicated with it,
`a / (b / (c / d))` doubling once per level. A division in the *dividend* is unaffected and
grows linearly.

**5. `format_type` has no logical codec, and `psql`'s `\gdesc` is what finds it.** **Closed.**
`format_type` was registered as a coordinator scalar UDF, so `SELECT format_type(23, NULL)`
answered `integer` and `\d` worked, while any plan carrying it that had to be **serialized to
the scheduler** could not be — and `\gdesc` falls back to exactly such a query (`VALUES` plus
`pg_catalog.format_type`). Two independent defects arrived in that one message, and closing
them separately is what the item asked for.

1. **The codec arm was the wrong fix, and finding that out is the useful part.** A scalar
   function crosses the Ballista wire as **a name and nothing else** — the plan carries
   `ScalarUdfExprNode { fun_name, args }` with no definition attached, and the decoding side
   looks the name up in *its own* registry, falling back to `try_decode_udf` only if that
   misses. So the defect was never a missing codec arm; it was a registry that three nodes
   were supposed to share and only one had. An arm in `VaireLogicalCodec` would have fixed the
   logical plan and then needed a second arm in the physical codec, which lives in
   `vairedb-core` and would have needed these constructors anyway. Making the name resolve is
   the same fix in one place: `vairedb_common::pg_udf::register_pg_catalog_scalar_functions`,
   called from the coordinator's catalog contexts, from the scheduler's own state
   (`scheduler::register_postgres_functions`) and from the executor — the invariant the
   registration seam already stated, that every context which plans *or* executes needs the
   identical set. The audit this item asked for came with it: the set is now defined by
   "can this appear in a serialized plan", not by "did a client tool trip over it".

   Four of `setup_pg_catalog`'s functions are deliberately left out, and that is a property
   rather than an omission. All of these declare `Immutable` or `Stable` volatility, so a call
   whose arguments are all literals is const-folded *before* serialization — which is why
   `format_type(23, NULL)` always worked. A function that can **only** be called with no
   arguments therefore can never cross the wire, and it is session-scoped, which makes an
   executor precisely the wrong place to answer it. Measured, not assumed: `current_database(),
   current_schema(), session_user, pg_backend_pid()` over a `VALUES` row answers, while
   `current_schemas(b)` over a column does not — so `current_schemas` is registered, for its
   one-argument form only.

2. **The raw gRPC `Status { … }` leak is closed at the sanitizer**, not at the call site.
   `sanitize.rs` now recognises the `Status { code: …, message: …, metadata: MetadataMap { … } }`
   shape and replaces the dump with the message it carries, so it does not matter which path
   fails at serialization or which tonic layer wraps it. Live, no error reply contains
   `Status {` or `MetadataMap` — that is asserted as a property over the whole error corpus
   rather than for this one statement, which is the shape item 2's general fix will need too.

Both halves are verified on a live cluster, not just in unit tests:
`test_a_pg_catalog_function_over_a_column_survives_distribution` covers the `\gdesc` shape
(a `VALUES` list, so there is no table to route by and the whole statement is distributed) and
the same projection over a sharded table, where it really does run on a core node; and
`test_no_error_reply_leaks_transport_internals` guards the sanitizer.

**Residue, and it is not the one the window axis predicted.** That axis recorded `\gdesc` as
failing on a `format_type(Utf8, Int64)` coercion and treated the command and the coercion as
one finding. They are two, and only one closed. `\gdesc` itself works — measured with
`psql` 18.6 against the live five-node cluster, over both a literal projection
(`SELECT 1 AS a, now() AS b \gdesc` answers `bigint` and `timestamp(0) without time zone`) and
a sharded table. The coercion gap is untouched: `SELECT format_type('23', 0)` still answers
`0A000`, because the UDF's signature is `OneOf(Exact(Int32, Int32), Exact(Int32, Int64),
Exact(Int64, Int32), Exact(Int64, Int64))` with no arm that accepts an untyped string literal,
where PostgreSQL coerces one to `oid`. It is narrow and it now has no known client-tool
reproducer, so it belongs with item 13's cosmetic work rather than at this rank — but it is
recorded here because the axis that found it will otherwise read as closed.

**6. The join axis's two remaining wrong answers**, in order of width: `NOT IN` is still
null-unaware over an aggregate left side and over a correlated subquery (§ 4.3 row 3 — both
blocked on DataFusion plan support, and the correlated one additionally on item 1); and a
`USING` key reached by an **explicit qualifier** on a full or right join reports the merged value
where PostgreSQL keeps the raw per-side one (§ 4.3 row 2 — not representable in a plan whose
schema has two fields for PostgreSQL's three names). A third was here — `INTERSECT`/`EXCEPT`
over incompatible branch types — and is closed (item 13 of § 6.1); it is worth remembering that
what removed it was printing the plan the reasoning had assumed. **Both remaining are residues
of a fix rather than untouched defects**, which is how this axis has settled: each closure is
narrower than the rule it implements and the difference is written down. The axis also has one
**loud** gap of its own now — an unqualified `USING` key in a `WHERE` clause is refused as an
ambiguous reference by DataFusion's own name resolution, for every join type — and one mild
🟡: an untyped literal branch of a set operation resolves to `text` where PostgreSQL resolves to
the typed branch's type, so the rows are right and the advertised column type is not.

**7. `STRUCT` DDL re-render.** The top remaining item on the type axis. sqlparser re-renders
`STRUCT(a INTEGER, b VARCHAR)` as `STRUCT(a, INTEGER, b, VARCHAR)`, which DuckDB cannot
parse, so `STRUCT` cannot be mapped to a PostgreSQL composite and stays on the `Utf8`
fallback.

**8. `CAST(… AS JSON)` and `CAST(… AS UUID)`.** Both types work as columns; neither works as
a cast target, which is what makes the entire JSON operator family unreachable. Closing this
also unblocks `json_agg` / `jsonb_agg`.

**9. Function-level coverage.** `datafusion-pg-functions` 0.1.0 is registered on every
context that plans or executes, but only its **`math`** category is populated (~18 UDFs).
The format and datetime families are not covered. `pg_typeof()` is the smallest visible
example.

**10. Session-scoped `pg_settings`.** `SHOW` works because it reads the session registry, but
`SELECT … FROM pg_settings` cannot reflect session parameters — the catalog table has no
per-session context to read from.

**11. `ALTER SCHEMA` and `ALTER TABLE … SET SCHEMA`.** Parser plus routing. Both write-path.

**12. Result types that are still not PostgreSQL's.** All of them return the right *value*;
the OID a client binds its buffer from is wrong. `percentile_cont` always answers `float8`,
where PostgreSQL returns `numeric` over a `numeric` or integer sort column; the `stddev`/`var`
family answers `float8` where PostgreSQL stays exact over `numeric`; and `ntile` is item 3.
`percentile_cont` is VaireDB's own UDAF now, so its type is VaireDB's to fix — it is the
cheaper of the two, since the statistics family is DataFusion's. (`percentile_disc` already
returns the sort column's own type and is **not** on this list.)

**13. Cosmetic and narrow, in order:** `JSON` / `UUID` OIDs; `ENUM` sorting in declaration
order rather than by decoded string (which is why it is *not* mapped to
`Dictionary(UInt8, Utf8)`); the duplicate-unaliased-label residue; `GROUPING SETS (())`
alone; the `min`/`max` `simplify()` shortcut DataFusion applied to `percentile_cont(0)` and
`percentile_cont(1)`, which VaireDB's UDAF does not reproduce; and `format_type`'s missing
`(Utf8, …)` signature arm — an untyped string literal for the OID is `0A000` where PostgreSQL
coerces it to `oid` (§ 6.2 item 5's residue).

**14. Deferred upstream, deliberately:** `Decimal256` (no arrow-pg arm); `NUMERIC` above 29
digits in **binary** format (`22003`, from arrow-pg's `rust_decimal` 96-bit mantissa);
timestamp literals outside the nanosecond range 1677–2262 (a DataFusion planner limit);
`nth_value(x, 0)`; `IGNORE NULLS`; the `PhysicalWindowExprNode` filter field; `**`; `0b101`.

**15. Undecided, needs a call:** `arr[-1]` (last element vs PostgreSQL's `NULL`); whether to
revisit the three push-down narrowings — the **text ordering** one is cheapest, needing only
DuckDB's `default_collation` pinned and inspected.

**16. A core node never re-registered, so a restarted coordinator had an empty cluster.**
**Closed.** *(Out of the ranking above: it is not a SQL-surface gap but a cluster-lifecycle
one, and it was the highest-consequence item in this section.)* Measured deliberately on the
e2e cluster, three times: recreate the coordinator container with all five cores left running,
and `SELECT count(*) FROM vairedb_catalog.nodes` reported **0** indefinitely, so every
`CREATE TABLE … replication_factor = 3` failed `[VDB-1004] replication_factor 3 exceeds the
number of available core nodes (0)` while five healthy nodes were polling for work. The cause
was a gap between the two halves of the protocol, on both sides. A core sent `Register` once,
at startup: `reconnect_loop` re-established only the heartbeat *stream*, which the log showed —
"heartbeat session established" with no second "registered with coordinator successfully". And
the coordinator had nothing to say about it, because its heartbeat handler updated the named
node's `last_heartbeat` and answered `HEARTBEAT_ACTION_NONE` regardless; an unknown `node_id`
only logged a WARN, and the only other action the proto defined was `DRAIN`. Registration
itself *is* durable — `put_node` writes to the redb catalog — so a plain process restart over
the same data directory always recovered. What did not recover was a coordinator that starts
with a **fresh catalog**: a recreated container, a new data directory, or a standby with its
own store.

Both sides now close it, and both is better than either, because each covers a case the other
cannot see:

- **Registration is part of attaching, not of starting up.** The core remembers the shard list
  its first `Register` carried and re-sends it before every stream it opens, so a coordinator
  that was replaced between two streams is told about the node by the reconnect that noticed
  it. The write it costs is an idempotent upsert.
- **`HEARTBEAT_ACTION_REGISTER`.** A heartbeat naming a node the catalog has no record of is
  answered with it instead of a bare ack, and the node ends the session and registers before
  reattaching. This is the only recovery path when the *stream* is healthy — a catalog emptied
  underneath a live connection — where nothing on the core side would otherwise ever notice.

Verified live on the e2e cluster: recreating the coordinator container brings
`count(*) FROM vairedb_catalog.nodes` back to **5** within one reconnect (~1 s after the
stream breaks, each core logging "registered with coordinator successfully" before
"heartbeat session established"), and `CREATE TABLE … replication_factor = 3`, an `INSERT` and
a distributed `SELECT` all succeed against the replacement. Guarded by integration tests on
both sides: `crates/vairedb-core/tests/heartbeat_tests.rs` (a `REGISTER` action produces a
second registration carrying the same shards; a reopened stream is preceded by one) and
`crates/vairedb-coordinator/tests/node_service_tests.rs` (an unknown node is told to register,
a registered one is acked). Bears directly on the roadmap's *Coordinator HA* line: a standby
taking over is now told who the cores are by the cores themselves.

### 6.3 Next phases

**Phase A — stop lying about errors. Largely shipped, with one residue.** The classifier is
now derived from `DataFusionError` variants rather than from substrings of its display text,
the SQLSTATE table gained `22012`, `22P02`, `42803` and `42P20`, the `Signature { … }` dump
truncates, and `= ANY (subquery)` and `ALL (array)` — which were being misrouted to
`array_has` and answered *wrongly* — now answer instead of needing a refusal. What remains is
item 2: the classifier only sees a typed error while the error is still coordinator-local.
Past the Ballista scheduler every failure arrives as the text `Job <id> failed: …` and lands
`XX000` whatever it was. So `'x'::int` classifies `22P02` and a nested aggregate does not.
Division by zero is carved out by name — a text match scoped to fire only after the typed
classifier has already given up — so all three of its forms report `22012`; that is a
concession for the one class an analytical client hits daily, not the shape of the general
fix. Closing it properly means carrying the code across the gRPC boundary, not another arm in
the matcher.

**Phase B — the write path's expression gap. Shipped, by translation rather than refusal.**
This phase was planned twice: first as the mirror of the read path's rewrite, then — for
v0.2 — as refusals, on the argument that a loud `0A000` is most of the value for a fraction
of the cost. Neither is what landed. `write_sql_cl/dialect.rs` translates: all four PostgreSQL
regex operators to `regexp_matches` (with `'i'` for the case-insensitive pair), an explicit
`'\\'` escape onto every `LIKE`/`ILIKE`, `SIMILAR TO` through the read path's own
`similar_to_regex_from_ast`, and a byte-order `COLLATE` dropped as the no-op it is. Integer
division came free from the engine side — `SET GLOBAL integer_division = true` at connection
open — so `7/2` answers `3` on both paths without touching the AST. `reject.rs` remains for
the forms that have no faithful translation. The refusal scaffolding was worth building
anyway: it is what made it safe to translate, because a form the translator does not handle
is refused rather than passed through. **The one gap this phase created, it also closed**:
item 4, a zero divisor storing NULL, which was the price of the setting and is now paid by a
guard around the divisor in `dialect.rs` — with a single nested shape refused in `reject.rs`,
because the guard has to name the divisor twice.

**Phase C — finish the type and result-type contract.** `avg(bigint)` now widens to
`numeric` the way `sum(bigint)` already did, in the same pre-`TypeCoercion` logical-plan pass
and for both the plain and the `OVER ()` form; `percentile_cont` and `percentile_disc` are
VaireDB UDAFs, exact rather than truncated to five decimals, registered by name on every
context so they cross a Ballista stage boundary with no codec change. Remaining: items 3, 7,
8, 12 and 13 — `ntile`'s narrowing, the `STRUCT` re-render, JSON/UUID as cast targets (which
also unblocks `json_agg`), `percentile_cont`'s and the statistics family's result types, then
the cosmetic OIDs.

**Phase D — function-level coverage.** Item 9, unchanged. This is the roadmap's remaining
`IN PROGRESS` surface: scalar and special functions, measured the same way, with the same
distributed-UDF constraint in force (**AST rewrite or register on every executor — never
coordinator-only**). The two percentile UDAFs were the first use of the by-name registry
resolution that makes this affordable, and § 6.2 item 5 turned it into the seam this phase
runs on: `vairedb_common::pg_udf` registers the `pg_catalog` scalar set on every context that
plans or executes, so a scalar function is added in one place rather than in a logical and a
physical codec arm.

**Phase E — the remaining write-path statements.** `COPY … FROM STDIN` / `TO STDOUT` now
stream, reusing the INSERT lane so every row is routed by shard key, which is what makes
`psql`'s `\copy` work in both directions. Remaining: item 11, `ALTER SCHEMA` and
`ALTER TABLE … SET SCHEMA`.

**Phase F — the distributed keyless join. Closed.** Item 1. It was its own phase because it
was the only item whose fix was not a rewrite, a refusal or a type change: a plan that is
provably equivalent in one process answered nothing at all once Ballista cut it into stages,
and no unit test built from SQL in this repository could see that. It closed as a physical
optimizer rule on the scheduler's own session state (`scheduler::nested_loop_join_one_task`),
which is the second such repair on that seam — `scheduler::window_partition_sort` was the
first — and the pair is now the house pattern for *any* operator whose correctness assumes one
process. Everything built on top of a semi or anti join — `EXISTS`, `NOT EXISTS`, the
null-aware `NOT IN` rewrite, and any future correlated-subquery decorrelation — sat downstream
of this and is unblocked; § 4.3 row 3's correlated `NOT IN` in particular now has only
`LATERAL` in front of it.

The axis's other two wrong answers closed on the opposite seam, and the contrast is the point:
`USING` on a full or right join reported a key indistinguishable from a NULL, which needed no
cluster to reproduce and no scheduler rule to fix — a schema-preserving projection of
`COALESCE(left.c, right.c)` over the join, on the coordinator, after the planner has resolved
names and expanded wildcards (`pgwire_handler/pg_using_join_merge.rs`). It is recorded as a
standing limitation rather than a finished job because of what it cannot represent: § 6.1
item 11 and § 4.3 row 2, the qualified spelling that now reads the merged value.

The last two closed by **refusing**, and are the one place on this axis where the right answer
was to stop answering: a set operation whose branches have no common PostgreSQL type returned
rows PostgreSQL refuses, and it is now `42804` at plan time, checked before coercion because
coercion is the pass that removes the disagreement (`pgwire_handler/pg_set_op_types.rs`).
Modelling PostgreSQL's `UNKNOWN` was the substance of the `UNION` half rather than the type
partition — without it the refusal would catch `… UNION ALL SELECT '9'`, which PostgreSQL
answers. The `INTERSECT`/`EXCEPT` half had been written off in this document as unreachable,
because they are joins by the time a plan exists and that join was assumed identical to a
client's own `IN (subquery)`; the assumption cost nothing to check and was wrong, which is the
one methodological note worth carrying out of this axis. Remaining: § 4.3 row 3 for `NOT IN`,
and the 🟡 on the untyped branch's advertised type. See § 6.1 items 12 and 13.

Cross-shard atomicity (§ 6.1.1) is out of all six phases. It is a design decision, and
changing it means adding 2PC.

**Deferred out of v0.2, deliberately, not dropped.** Recorded here so none of it is lost:
`GROUPING SETS (())` alone; `json_agg`/`jsonb_agg` and the `CAST(… AS JSON)` /
`CAST(… AS UUID)` unblock they wait on; the `STRUCT` DDL re-render; `mode()` and the
hypothetical-set `WITHIN GROUP` forms; `ntile`'s `int4`; function coverage beyond
`datafusion-pg-functions`' `math` category; session-scoped `pg_settings`; `ALTER SCHEMA`;
`arr[-1]`; `nth_value(x, 0)`; `0x1F`; the three push-down narrowings; cross-shard atomicity.
TLS, users and groups stay their own roadmap line rather than a gap row.

---

## 7 — Executable counterpart

The gap map is executable. The convention throughout is a **passing** test pinning today's
actual behaviour, plus an `#[ignore = "gap (row N): …"]` test asserting the PostgreSQL-correct
target. `make e2e` runs only the passing set, so the gap map never blocks CI.

| Suite (`tests/e2e/tests/`) | Covers |
|---|---|
| `sql_command_select.rs` | The read path, including push-down |
| `sql_expression_gaps.rs` | Expression, window and aggregate closures — organized by the **seam** a gap sits on (a refusal, a rewrite, a distributed case) rather than by function |
| `sql_command_dml.rs`, `sql_command_ddl.rs`, `sql_command_transaction.rs` | Writes, DDL, transaction blocks |
| `sql_command_unsupported.rs` | Every 🚫 and ❌ statement row — plus `COPY`'s streaming pair, which lives here because this is where its refusal used to be tested: a `FROM STDIN` load, a `TO STDOUT` dump, a `STDOUT`→`STDIN` round-trip into an empty table, a mid-copy failure leaving the connection usable, and a chunk large enough to span many `CopyData` messages |
| `sql_join_gaps.rs` | The join and set-operation axis, each test naming the seam it holds — a **distributed** case, a **three-valued** case, or a **refusal** |
| `data_types_round_trips.rs`, `data_types_dialect_gaps.rs` | The type axis, both formats |
| `anonymization.rs` | § 2.6, refusals beside the reads that must keep answering |
| `shard_key_hazards.rs`, `identifier_rewrite.rs`, `concurrency.rs` | The sharding rules |

Two properties of the suite are load-bearing. First, **`describe_result_types` and the
label helper live in `tests/e2e/src/lib.rs`**, so a result OID and a column label are
assertable from any suite — several rows in § 3 exist only because they are visible over
`Describe`. Second, **every refusal is probed for over-reach**: each one asserts, in the
same test, that the neighbouring form which loses no clause still answers. That is what
keeps a refusal honestly scoped rather than a blanket rejection.

Still proposed and not yet written: `sql_function_aggregate.rs`,
`sql_function_aggregate_types.rs`, `sql_function_aggregate_distributed.rs` (the merge
invariants that would catch a wrong "optimization" if `remote_scan_exec.rs` ever gained a
partitioning claim), `sql_expression_operators.rs`, `sql_expression_literals.rs`,
`sql_expression_split_brain.rs`.

## 8 — Four lessons worth carrying forward

All four were learned by being wrong first, and all four generalize past the rows that
taught them.

**An equivalence property is a fact about a plan, not a fact about the data, and it does not
cross a stage boundary.** `PARTITION BY g` + `WHERE g = 1` failed because `FilterExec` told
the plan above it that `g` was constant, `EnforceSorting` **correctly** concluded no sort was
needed, and then Ballista cut the plan into stages — leaving the window in a stage whose
input is a shuffle reader reporting no equivalences at all. The plan was valid as one piece
and invalid once split. Reproducing it on a single shard was read as exonerating
distribution; it does not, because a one-shard VaireDB still cuts the plan into stages.

**A logically equivalent rewrite is not a correct rewrite until it has run on a cluster.**
The first fix for null-aware `NOT IN` was a textbook rewrite — a left anti join with the null
cases spelled out — and it passed every unit test, because in one process it *is* right. On
five nodes it returned no rows, and the reason had nothing to do with `NOT IN`: a join carrying
no equality answered nothing once the plan was cut into stages (§ 6.1 item 10). The shipped
rewrite is deliberately clumsier than the equivalent one — three references to the subquery,
two of them uncorrelated `count(*)`s, and only the bare-equality predicate left as a join —
because the elegant form was the one distribution rejected. The general shape of the mistake:
reason about equivalence over relational algebra, verify it over the physical plan the scheduler
actually builds. This is the same defect class as the lesson above, arrived at from the other
direction, which is why it deserves its own entry rather than a footnote. **And the discrepancy
was worth more than the fix.** Running down why one rewrite behaved differently on five nodes
than on one turned up the keyless-join defect, which no row of any axis had asked about; closing
*that* turned up the keyless outer joins, which no row had asked about either. Two silently
wrong shapes reached only by refusing to route around a puzzle, which is the argument for
treating an unexplained cluster/unit-test divergence as a finding rather than an obstacle.

**A refusal is neither a local fix nor an upstream wait.** It was the third option this
analysis kept missing. It costs little, it is not thrown away when upstream lands — deleting
a refusal is trivial — and it converts silent wrongness into something a client can see.
That is how 8 of the window axis's 10 ❌ rows and 4 of the aggregate axis's got there, and it
is most of why the silently-wrong class shrank from more than twenty rows to § 4.3. The one case where it is unambiguously *better*
than a fix rather than a stopgap is the nondeterministic `OVER (w <frame>)`: an unstable
answer produces bug reports nobody can reproduce and no single-run test can catch.

**A text fallback behind a typed classifier is only ever exercised by errors that crossed a
process boundary — so write it against `Debug` spellings, not `Display` ones.** The typed
SQLSTATE classifier is the right design, and the text arm behind it looked like dead weight.
It was not: everything raised on an executor arrives as the scheduler's own string, so that arm
is the *only* thing those errors meet. Its divide-by-zero rule matched the prose `"divide by
zero"`, and the wire carries Arrow's `Debug` spelling `DivideByZero`, spaceless — a string no
distributed query ever produces was what the unit test asserted on. Two further points the
second attempt turned up. First, matching on the *wrapper* variant does not work either: the
`DataFusionError` variant that arrives is not the one Ballista's source constructs, so the
predicate has to be variant-independent. Second, scope such a fallback by *when* it may fire —
only after the typed classifier has already returned `EngineError`/`InternalError`, i.e. only
when there was nothing to classify — so it can never shadow a correct typed answer. That keeps
it one function with one predicate instead of a second, competing classifier.

A corollary on citations: **cite a file and a symbol, not a versioned path.** `ntile.rs`
under a `-53.1.0` prefix is a dead link one bump later — and `ntile`'s own bucket defect was
closed by the 53.1 → 54.1 bump with nothing written locally, so a versioned citation can
outlive the defect it describes.
