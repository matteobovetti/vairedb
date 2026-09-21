# VaireDB SQL Gap Analysis

Everything a PostgreSQL client **cannot** fully do against VaireDB today: statements, data
types, aggregate and window functions, operators, literals, casts, joins and set operations.

This is the single gap record. It lists only what diverges — what is partially supported,
what is refused, and what is not implemented because VaireDB is a distributed database.
Anything absent from this document works and agrees with PostgreSQL — on the six axes § 2
enumerates. Scalar functions are not one of them and have never been measured as an axis, so the
rule above is asserted over them without evidence — and an unmeasured axis can hold a ⛔.

**PostgreSQL is the contract.** DataFusion 54.1 (read path, over Ballista) and DuckDB 1.5.5
(write path, one instance per shard) are implementation layers; a form only they have is out
of scope. Everything below was measured against a live 5-node e2e cluster (1 coordinator,
1 scheduler, 3 cores), byte-diffed against a **PostgreSQL 16.15** oracle with a **DuckDB
1.5.5** CLI as a second oracle for the forms PG cannot parse, with result types and column
labels read from extended-protocol `Describe`.

## Verdict legend

| Status | Meaning |
|---|---|
| 🟡 | **Partial** — right value under a wrong advertised type, one path only, degraded semantics, or an honest error in the wrong SQLSTATE. |
| ⛔ | **Silently wrong** — parses, executes, returns a plausible value that is not PostgreSQL's. The dangerous class; collected in § 2.7. |
| ❌ | **Rejected loudly** with a SQLSTATE, and no target behaviour agreed yet. An open gap. |
| 🚫 | **Not planned**, with a written rationale — no meaning in a shared-nothing cluster, or DuckDB-only. A decision, not backlog (§ 3). |

## Totals

Counted as rows of the tables below; a row can group several spellings (`+ - *` is one row,
`regr_*` is one row), so this compares axes rather than individual constructs. Six axes, which
is not every axis PostgreSQL has: scalar functions are not among them and have never been
measured.

| Axis | 🟡 | ⛔ | ❌ | 🚫 |
|---|---:|---:|---:|---:|
| Statements (§ 2.1) | 15 | — | 9 | 8 |
| Data types (§ 2.2) | 6 | — | — | 8 |
| Aggregate functions (§ 2.3) | 2 | — | 3 | — |
| Window functions (§ 2.4) | 1 | — | 5 | 1 |
| Operators, literals, casts (§ 2.5) | 12 | — | 19 | — |
| Joins and set operations (§ 2.6) | 2 | — | 4 | — |
| **Total** | **38** | **—** | **40** | **17** |

The read path's expression surface and type layer are closed, including live predicate and
`LIMIT` push-down to the shards. § 2.2 has no ❌ left either: no value a shard can store is
refused on the wire any more, so what remains in the type table is an advertised
name or a length, not a value. The **statement** list gained a ❌ this revision and lost it
again in the same one: a filtered read of a schema-qualified relation failed `42703`,
present since qualified relations were and found while closing the qualifier rewrite, and it
now answers. All ten historical *split-brain* rows — one SQL fragment answered
differently by a `SELECT` and by an `UPDATE` — are closed, by translating the divergent
expression on the write path rather than refusing it.

Every divergence that was ranked as a wrong answer is closed, which is why the ⛔ column is
empty: nothing in this document returns a plausible answer that is not PostgreSQL's. These
counts are therefore **not comparable** to the previous revision's. The closed rows left the
tables, and the residues those closures left entered them, each as a 🟡 or a ❌ of its own — a
residue a query can see belongs in the census exactly as much as the divergence it came from,
since anything absent from this document is a claim that VaireDB agrees with PostgreSQL.

Some rows are **settled** rather than fixed, and the difference matters: nothing is left to do on
them, and that is not a claim of agreement. Each open item on such a row is either blocked
upstream — in DuckDB, sqlparser, pgwire or `datafusion-pg-catalog` — or a correction to the row's
own premise, or a divergence held back deliberately because the mechanism that would repair it
cannot. Each stays in the tables above as the 🟡 it is.

**An empty ⛔ column is not an MVP.** Two things above should be read carefully rather than as a
finish line. The **scalar-function axis has never been measured**, so it contributes no rows to a
table that otherwise looks complete — and a function both engines have under PostgreSQL's name but
evaluate by a different rule is exactly a ⛔. And a **`VARCHAR(n)` length is reported but not
enforced** (§ 2.2, last row): the one divergence anywhere in this document that is neither fixed
nor settled, because the check simply does not exist.

---

## 1 — Where a gap comes from

### 1.1 Two execution paths

| Path | Statements | Who evaluates the expressions |
|---|---|---|
| **Read** | `SELECT` | DataFusion, on the Ballista executors. Each shard runs only `SELECT <cols> FROM <shard_table> [WHERE <pushed predicate>] [LIMIT n]` — **no `GROUP BY`, and no aggregate is ever pushed into DuckDB.** |
| **Write** | `INSERT` / `UPDATE` / `DELETE` / `MERGE` / DDL | The statement is re-rendered to SQL text and executed **verbatim by DuckDB** on each shard. |

The same fragment therefore has two verdicts, and every expression fix has to be checked on
both. Read-path rewrites live at **E2** (`pgwire_handler/pg_operators.rs`, `compat_rewrite.rs`,
`pg_aggregate_widening.rs`, `pg_param_types.rs`, `column_labels.rs`, `anonymized_reads.rs`);
write-path ones at **W3** (`write_sql_cl/dialect.rs`, `reject.rs`).

Two invariants constrain every fix:

- A **type-changing** read-path rewrite must run on the logical plan inside `plan_select`,
  **before the analyzer** — the type a client is told is read off the *unanalyzed* plan, and
  `TypeCoercion` erases the argument type PostgreSQL resolves overloads on.
- A function must be **AST-rewritten or registered on every executor — never
  coordinator-only.** datafusion-proto carries a function as a *name* and resolves it from the
  decoding node's own registry, so a coordinator-only registration plans fine and then fails
  on stage deserialization.

### 1.2 Where a statement is refused

| Rejection point | Error | SQLSTATE |
|---|---|---|
| Parse (E1, sqlparser 0.62 PG dialect) | `SqlSyntaxError` | `42601` |
| Classification (`classify_statement`, 21 `QueryType`s) | `FeatureNotSupported` | `0A000` |
| Coordinator rewrite (E2) | `[VDB-1004] … is not supported: …` | `0A000` |
| Logical / physical planning | `Error during planning` / `Physical plan does not support …` | the class of the refusal — `0A000`, `42803`, `42P20`, `42601`, `42703` |
| Distributed execution | `[VDB-5001] Job … failed` | the class the executor raised (§ 1.3); `XX000` only when the failure carries none |

### 1.3 Distributed-execution constraints that produce gaps

Each of these is a property of running a plan across processes, and each is the direct cause
of rows below.

| Constraint | Rows it produces |
|---|---|
| **An error raised past the Ballista scheduler arrives as text, not as a typed error.** Ballista's `FailedTask` holds a single `String` (it is a published dependency, so the proto is not VaireDB's to extend), and the scheduler renders the whole failure into it: `Job <id> failed: … DataFusionError(Execution(…))`. The **class** no longer travels with the error, so it has to travel *inside* the text — which is what the error-classification fix shipped, and why this no longer produces rows. VaireDB's own guards on an executor write a `[VDB-<code>]` tag at the raise site; for everything else the variant is read back out of the rendering, and only when the typed classifier has already given up. | **No open rows** — closed. The residue is upstream-shaped: a rendering that stopped spelling the variant name would put an untagged executor failure back at `XX000`. |
| **Ballista's `PhysicalWindowExprNode` has no filter field** (the deserializer hardcodes `None`), so a window aggregate's `FILTER` is lost in plan serialization. Single-node DataFusion is correct. **The one defect VaireDB owns *because* it is distributed** — no upstream release closes it. | `FILTER (WHERE …) OVER (…)` refused. |
| **A null-aware anti join cannot be shipped.** Ballista sets `prefer_hash_join = false` (its sort-merge join can spill, the hash join cannot), and only `HashJoinExec` carries the `null_aware` flag `NOT IN` needs. Turning the option on is unshippable — measured: the job dies inside the scheduler and the client hangs, because the failed stage reports no status. So the predicate is respelled in the AST instead (`compat_rewrite::rewrite_not_in_subqueries`). | The two `NOT IN` shapes the respelling cannot enter (§ 2.7 row 7). |
| **An operator that coordinates its partitions through shared memory cannot be distributed.** Ballista runs each partition as a separate process, so `NestedLoopJoinExec`'s probe-partition counter counts inside one task only. Repaired by coalescing the probe side (`scheduler::nested_loop_join_one_task`); the class outlives the fix, and no unit test built from SQL can catch a regression in it. | Constrains the *form* of the `NOT IN` respelling: a bare equality, because that is the shape the cluster answers. |
| **An equivalence property is a fact about a plan, not about the data, and does not cross a stage boundary.** A shuffle reader reports no equivalences, so a sort decision made above a `FilterExec` is invalid once the plan is cut. Repaired by `scheduler::window_partition_sort`. | Constrains every future optimizer assumption; no open row. |
| **Anonymization is write-path only.** A pseudonymized column stores an HMAC-SHA256 digest and the plaintext is not on the server, so the digest order is the only order a read can see. | The order-sensitive reads refused in § 2.4 and § 2.6. |
| **No cross-shard atomicity** (no 2PC), **uniqueness only over the shard key**, **immutable shard key**, **transaction blocks buffer in the coordinator**. | Most 🟡 statement restrictions in § 2.1. |

---

## 2 — The gap table

Each axis table carries its 🟡 and ❌ rows. **⛔ rows are collected once, in § 2.7**, and not
repeated here.

### 2.1 Statements

🟡 — routed and correct, with restrictions that follow from sharding. The restriction *is*
the gap; each is refused by name rather than silently under-applied.

| Statement | What is refused |
|---|---|
| `ALTER TABLE` | Dropping the shard key; touching an anonymized column; retyping or dropping a column a declared constraint covers; every `ADD`/`DROP CONSTRAINT` form but `… UNIQUE (<shard key>)` — the shard engine implements neither `ADD … CHECK` nor `DROP CONSTRAINT`. On an indexed table: dropping or retyping a column an index is built on, any column change while a **unique** index or index-backed constraint exists, and `RENAME TO` while any index exists — every other column change is shipped as one per-shard transaction that drops the indexes, applies it and builds them again. `RENAME TO` / `SET SCHEMA` must be the only action (`42601`), and a qualified `RENAME TO` destination may change the schema or the name but not both (`42601`). A column declared `COLLATE` anything but byte order (`0A000`). |
| `DROP` | Several names in one statement; `CASCADE` / `RESTRICT`. `DROP SEQUENCE` reaches the table handler and reports a missing table. |
| `ALTER VIEW` | `RENAME TO` — fails at parse (`42601`). |
| `COPY` | `PROGRAM`; any format but CSV and PARQUET, and the format must be stated (PG's default is `TEXT`); any option but `HEADER`/`DELIMITER`/`QUOTE`, and those only for `FORMAT CSV` — a Parquet file names and types its own columns, so there is nothing for them to act on. `FROM STDIN`/`TO STDOUT` are CSV-only (`0A000`, at Parse): a Parquet footer is written last and has to be read first, so the bytes are not the row-at-a-time stream the copy sub-protocol carries. A Parquet import reads the file's own types, so a column whose type has no SQL literal form (`BYTEA`, a list, a struct) is refused **by name** where CSV would carry it as text. A load that fails part-way leaves the batches already sent committed — the same guarantee a multi-row `INSERT` gives. ⚠️ **No privilege check exists**: VaireDB has no role model, so any client reads and writes any path the coordinator process can. |
| `CREATE INDEX` / `DROP INDEX` | `UNIQUE` off the shard key; a `UNIQUE` index narrowed by `WHERE` or `NULLS NOT DISTINCT`; an unnamed index (nothing for `DROP` to resolve); an index over an expression. |
| `CREATE SCHEMA` / `ALTER SCHEMA` / `DROP SCHEMA` | `CASCADE`, `AUTHORIZATION`; dropping `public` or a metadata schema. `sales.t` and `sales_t` cannot coexist (`42P07`), since the qualifier folds into the physical name. `ALTER SCHEMA`: every action but `RENAME TO` (`0A000`, a schema carries no properties of its own); renaming `public` or a metadata schema; renaming a schema that still holds a relation (`2BP01`) — the qualifier is part of each shard table's physical name, so moving the relations is the client's call. |
| `CREATE VIEW` | `MATERIALIZED`; the dialect decorations (`TEMPORARY`, `WITH (…)`, `SECURE`, `CLUSTER BY`, `TO`, `COMMENT`, a typed column); a view over a metadata schema or named after a `pg_catalog` table; a cyclic definition. Views are read-only (`42809`). |
| `MERGE INTO` | Any source but a co-located table sharded by the join column or an inline `VALUES` list; an `ON` that does not pin the shard key; `UPDATE SET <shard key>`; an `INSERT` that omits or contradicts it; `RETURNING`; per-action `WHERE`; `NOT MATCHED BY SOURCE` over a `VALUES` source; a merge into a table with anonymized columns. |
| Transactions | Inside a block: `UPDATE`/`DELETE` (unknowable row count), DDL, and reads of a table the block has written. Writes buffer in the coordinator and ship at `COMMIT` as one atomic batch per node set. Isolation levels are accepted and ignored. A block spanning node sets is refused at `COMMIT` unless `allow_cross_shard_transactions`; a partial multi-shard write reports `40003` with how much was written. |
| `SET` | `search_path` **by name** (`0A000`); `SET LOCAL`, `SET ROLE`, `SET SESSION AUTHORIZATION`, `SET TRANSACTION`, `SET NAMES`. A value outside the set the coordinator's behaviour already matches is refused **by value** (`22023`) naming what VaireDB does instead. Unknown parameter `42704`, startup-fixed `55P02`. |
| `SHOW` | `SHOW TABLES` / `DATABASES` / `SCHEMAS` and that family (`0A000`) — a different statement, and PostgreSQL has none either. |
| `RESET` | Nothing, but note the asymmetry: a `RESET` of a *refused* parameter **succeeds**, because the default is the behaviour VaireDB already has. |
| `EXPLAIN` / `EXPLAIN ANALYZE` | `EXPLAIN` of a write — a write is planned by each shard's engine, so there is no coordinator plan; every utility option but `ANALYZE`/`VERBOSE`, each by name; a non-boolean option value (`22023`); `EXPLAIN QUERY PLAN` / `ESTIMATE`; `EXPLAIN <relation>`. `PRAGMA` stays refused. |
| `DESCRIBE` / `DESC` | `FORMATTED` / `EXTENDED` — they promise storage detail the coordinator does not hold. |
| `TRUNCATE` | Several tables in one statement; `CASCADE` / `RESTRICT`; `RESTART IDENTITY`. |

❌ — no target behaviour agreed.

| Statement | SQLSTATE | Why |
|---|:-:|---|
| `ATTACH` / `DETACH` | `0A000` / `42601` | Single-node DuckDB attachment; meaningless for a cluster. |
| `CHECKPOINT` | `42601` | Per-shard storage concern, not coordinator-exposed. |
| `EXPORT` / `IMPORT DATABASE` | `42601` | Whole-DB dump/load. Use `COPY`. |
| `INSTALL` / `LOAD` | `42601` / `0A000` | Extension management is a per-node concern. |
| `CREATE SECRET` | `0A000` | DuckDB's secrets manager. VaireDB has its own path (`INSERT INTO vairedb_catalog.anonymization_secret`, which is append-only — there is no documented retraction path, so a fixture cannot clean up after itself). |
| `USE` | `0A000` | One database per cluster, and no search path to switch. |
| `CALL` | `0A000` | No stored or table procedures. |
| `COMMENT ON` | `0A000` | No catalog comment storage and nowhere to read one back — accepting it would be a fake `OK`. |
| `CREATE MACRO` | `42601` | DuckDB-only. |

(`SET datafusion.*` being refused `42704` is *correct*, not a gap: VaireDB models PostgreSQL's
parameter set, not DataFusion's.)

### 2.2 Data types

Values round-trip faithfully in every row here; what diverges is the advertised type — with one
exception, the last row, where the divergence is that a length the type *declares* is not applied
to the value.

| Item | Divergence | Where the fix lives |
|---|---|---|
| 🟡 `UInt64` column | Advertised `numeric`, not `bigint` — arrow-pg has no unsigned PostgreSQL type. The three ranking functions are already widened above it; this is the general case. | VaireDB (widen), or arrow-pg |
| 🟡 `STRUCT` column | Advertised `text`. Blocked on sqlparser re-rendering `STRUCT(a INTEGER, b VARCHAR)` as `STRUCT(a, INTEGER, b, VARCHAR)`, which DuckDB cannot parse, so `STRUCT` cannot be mapped to a PostgreSQL composite. | sqlparser, then VaireDB |
| 🟡 `ENUM` | The last `Utf8` fallback: values faithful, advertised OID `text` rather than the type's own. `UUID`, `JSON` and `CHAR`/`VARCHAR` now advertise their own OIDs; an enum cannot follow them, because a PostgreSQL enum has **no fixed OID** to advertise — every `CREATE TYPE` allocates a `pg_type` row — so it needs a VaireDB-owned catalog entry first. An **unknown** declared type still degrades to `text` the same way, which is what keeps VaireDB forward-compatible with any type DuckDB adds whose values are faithful as text. | VaireDB |
| 🟡 `ENUM` ordering | Sorts by decoded string, not declaration order — which is why it is *not* mapped to `Dictionary(UInt8, Utf8)`. Out of reach at this layer rather than merely unimplemented: PostgreSQL's declaration order governs **every** comparison on the type, so teaching `ORDER BY` alone would leave `e < 'b'` disagreeing with the sort it sits beside. | VaireDB |
| 🟡 `VARCHAR(n)` / `CHAR(n)` typmod | The OID is now the column's own (`varchar`, `bpchar`), but the length is not on the wire, so `psql`'s `\d` prints `character varying` without its `(64)`. Blocked twice downstream: pgwire's `RowDescription` hardcodes `type_modifier: -1`, and `\d` reads `pg_attribute`, where datafusion-pg-catalog derives `atttypid` from the Arrow type alone, hardcodes `atttypmod = -1` and keeps its OID allocator `pub(crate)`. The catalog still holds the declared string verbatim, so the length is recoverable for introspection. | pgwire, datafusion-pg-catalog |
| 🟡 `VARCHAR(n)` / `CHAR(n)` length not enforced | The other half of "reported, not enforced": a value longer than the declared length is **stored and returned**, where PostgreSQL raises `22001`. The shards' engine ignores the length and the coordinator does not check it, so the `n` is documentation. Found while closing the OID row above, pinned by an `#[ignore]`d test. | VaireDB |

Eight further types are refused **by name at DDL** — a decision, in § 3.3.

### 2.3 Aggregate functions

There is no missing-coverage gap and no split-brain row: every aggregate is computed by
DataFusion, DuckDB never evaluates one, and the distributed merge is correct *structurally*
(`remote_scan_exec.rs` declares `UnknownPartitioning(1)` and empty `EquivalenceProperties`, so
DataFusion is forced to insert a shuffle and no partial aggregate ever emits a finished value).

The ordered-set and hypothetical-set family, both percentiles' array-of-fractions overload,
`json_agg`, the exact-numeric result types and their scale, and the wrong SQLSTATE a nested
aggregate used to carry are all closed. What is below is what those closures did not reach.

| Item | Divergence |
|---|---|
| 🟡 The exact-numeric aggregates — `avg(int2\|int4\|int8)`, `stddev*`, `var*`, `variance` | Value, type and PostgreSQL's sixteen decimal places, but the **scale is a constant** where PostgreSQL chooses one per value: the average of `1` and `2` prints `1.5000000000000000`. An Arrow column has one scale for every row, so this is a choice rather than an oversight. And a variance above `10²²` raises `22003` instead of rounding, where PostgreSQL's unbounded `numeric` answers — visible either way, and `stddev` is unaffected because it is measured on the square root. |
| 🟡 `json_agg` / `jsonb_agg` of a bare `JSONB` **column** | Quotes the document as a string instead of embedding it. A `JSONB` column is Arrow `Utf8` at the coordinator, so nothing tells a stored document from stored text; `json_agg(v::json)` and `json_agg(v::jsonb)` both embed, and the cast is the spelling to write. |
| ❌ The multi-column hypothetical-set form — `rank(a, b) WITHIN GROUP (ORDER BY x, y)` | `0A000`. datafusion-sql answers `Only a single ordering expression is permitted in a WITHIN GROUP clause` before any aggregate is consulted, so no signature VaireDB could register is ever reached. The one-column form of all five ordered-set aggregates answers, as do both percentiles' fraction and array overloads. |
| ❌ `GROUPING SETS ((), ())` — the empty set **repeated** | `0A000`. PostgreSQL emits the grand total once per empty set, and a query grouped by nothing lowers here to the plain aggregate the cluster distributes correctly, which emits one row. `GROUPING SETS (())` alone answers, and combined with a non-empty set is untouched. |
| ❌ `xmlagg` | No XML type. Out of scope. |

### 2.4 Window functions

All 11 PostgreSQL window functions are present, and the frame engine (all five units, peer
groups, integer/float/`INTERVAL` offsets) is correct. The clause surface around them is closed
too — named windows and both sites their abbreviation appears, chained inheritance, a windowed
`FILTER`, a window function in the outer `ORDER BY`, `ntile`'s result type, and the wrong
SQLSTATE an illegal placement carried. What is left is one unparseable frame
option, one class of aggregate a windowed `FILTER` cannot be folded into, and the anonymization
refusals.

| Item | Divergence |
|---|---|
| 🟡 Bind-parameter offsets — `ntile($1)`, `lag(x,$1)`, `nth_value(x,$1)` | Correct **provided the client declares a parameter OID**. Values byte-identical to literal controls otherwise. |
| ❌ `FILTER (WHERE …) OVER (…)` on a **collecting** aggregate — `array_agg`, `string_agg` and their kind | `0A000`. § 1.3 — Ballista's proto has no filter field, so a windowed `FILTER` is answered by folding the predicate into the argument (`agg(CASE WHEN p THEN x END)`), which is exact only for an aggregate that *skips* a null argument. A collecting aggregate would gain one null element per excluded row, so the pair stays refused, naming the subquery spelling. Every null-skipping aggregate answers: `count` (including `count(*)`), `sum`, `avg`, `min`, `max`, the boolean, bit, `stddev` and `variance` families. A plain aggregate's `FILTER` is unaffected. |
| ❌ `EXCLUDE {CURRENT ROW \| GROUP \| TIES \| NO OTHERS}` | `42601` — **unparseable**. sqlparser's `WindowFrame` carries a literal `// TBD: EXCLUDE`. The only gap in this document that needs parser work first, and the only genuinely *missing feature*. Rare in practice. |
| ❌ `rank`/`row_number`/`dense_rank`/`percent_rank`/`cume_dist` `OVER (ORDER BY <anon col>)` | `0A000`. A window *is* an ordering and a digest has no plaintext order on the server; it was returning `3,1,2` for PostgreSQL's `1,2,3`. |
| ❌ `first_value`/`last_value`/`nth_value`/`lag`/`lead` over an anonymized column ordered by it | `0A000`. Was returning the digest of the wrong row. |
| ❌ Plaintext predicate beside a window over an anonymized column | `0A000`. Was returning 0 rows. Still unguarded in one shape: `WHERE email = $1` bound to plaintext, since the value never appears in the AST — catching it needs a check at `Bind`. |

`PARTITION BY <anonymized col>` is **sound and stays allowed**: HMAC is deterministic and
injective, so digest equality is plaintext equality. Only order and value are destroyed.

### 2.5 Operators, literals and casts

| Item | Divergence |
|---|---|
| 🟡 `<=>` | MySQL null-safe equality; **no PostgreSQL equivalent**. In DuckDB `<=>` is *vector distance* on `FLOAT[]`, so on a float-array column the two paths would diverge. |
| 🟡 `\|\|` on arrays | DataFusion also overloads it as append/prepend by dimension; DuckDB rejects the element-to-list form PostgreSQL accepts. The **string** form is correct on both paths. |
| 🟡 `COLLATE` | `C`, `POSIX`, `ucs_basic` and `default` name byte order, which is what VaireDB does, so they are dropped without changing an answer — and `default` is load-bearing, because `psql`'s `\d` sends `COLLATE pg_catalog.default`. **Every other collation is refused by name** (`0A000`) on both paths, not applied. Refused at E1, not E2: the compat parser's `StripCollate` deletes the clause before E2 could see it. |
| 🟡 `X'DEADBEEF'` | → `Binary`. PostgreSQL gives `bit(32)`; DuckDB gives the VARCHAR `'xDEADBEEF'`. Three engines, three answers. |
| 🟡 `1_000` | Answers `1000` but typed `numeric` where PostgreSQL types it `integer`. Works only incidentally: `parse_float_as_decimal` routes the token down the decimal parser, which tolerates the separator. Nothing strips separators anywhere. |
| 🟡 `TIMESTAMP '…'` literal | Nanosecond typing bounds a literal to **1677–2262**; outside that it fails in `simplify_expressions`. A DataFusion planner limit. Column-level `TIMESTAMP` is microseconds and unaffected. |
| 🟡 `TIMESTAMPTZ '…+02'` literal | Normalized to UTC correctly, but the offset is not shown back to the client. |
| 🟡 `'{1,2,3}'::INT[]` | PostgreSQL's canonical array text form works on read; DuckDB renders lists as `[1, 2, 3]` on write. |
| 🟡 An integer literal | Typed `bigint` where PostgreSQL types it `integer`: DataFusion types the literal `Int64`, so `SELECT 1` is described as `bigint` and `pg_typeof(1)` answers `bigint`. The **value** is PostgreSQL's, and a cast or a column reference carries its own type — only the bare literal is wide. Distinct from `1_000` above, which is typed `numeric` for a different reason. |
| 🟡 `now()` / `current_timestamp` / `transaction_timestamp()` | The instant is right and it is read **once, on the coordinator** — three shards reading three clocks is what reading it on the coordinator exists to prevent — but `now()` carries no time zone where PostgreSQL's is `timestamptz`, and `transaction_timestamp()` is per-statement, because a transaction block here buffers in the coordinator (§ 3.4). `clock_timestamp()` and `timeofday()` are per-call as PostgreSQL's are. |
| 🟡 `'{"a":1}'::json` / `::jsonb` and the four accessors | Casts validate (`22P02` on text that is not a document) and `->`, `->>`, `#>`, `#>>` answer, including negative indices and the `'{a,b}'` path spelling. Two residues: `::jsonb` does **not** normalize — key order and whitespace survive, because a `JSONB` column does not normalize either and one reading everywhere beats two — and a missing key or index is `NULL` where PostgreSQL's `json` accessors raise. The advertised OID is still `text` (§ 2.2). |
| 🟡 `'\xDEADBEEF'::bytea` | The cast is PostgreSQL's `byteain`, with both its SQLSTATEs, and a `::bytea` over anything but a literal, `NULL` or a parameter is refused `0A000` naming the two spellings that work. Four residues, each pinned by an `#[ignore]`d test: an **implicit** string→`bytea` column coercion, a `CREATE TABLE` `DEFAULT`, text-format `bytea` **output**, and `length()` over a `Binary` column. |
| ❌ `**` | `42601`. **No sqlparser 0.62 dialect parses it** — needs upstream work, not a dialect change. DuckDB itself supports it. |
| ❌ `//`, `DIV` | `42601`. `//` is dialect-gated (PG's dialect rejects it, Generic and DuckDB accept it); DataFusion's `Operator::IntegerDivide` is unimplemented anyway. |
| ❌ `@?` | `0A000`. The other four accessors (`->`, `->>`, `#>`, `#>>`) answer; this one is **jsonpath**, a second language, and a subset of it would answer some paths and mis-answer others. The refusal names it and the four accessors. |
| ❌ `@@` | `0A000 Invalid function 'to_tsvector'`. Full-text search has no functions at all. |
| ❌ `'abcde'::VARCHAR(2)` | `0A000` naming the cast, all four spellings (`VARCHAR(n)`, `CHAR(n)`, `CHARACTER(n)`, `CHARACTER VARYING(n)`). The length used to be silently discarded where PostgreSQL truncates; it is not enforced anywhere on the read path, so it is refused and `substr()` named instead. An unbounded character cast is fine, and `NUMERIC(p,s)` is unaffected — that precision *is* applied. |
| ❌ `CAST(x AS T ARRAY)` | `42601`. |
| ❌ `{'a': 1}`, `MAP {'a': 1}` | `42601`, dialect-gated at E1: DataFusion supports brace struct literals, sqlparser's PG dialect refuses them. |
| ❌ `MAP(['a'],[1])` | `XX000 Unsupported Datatype Map(…)`. Parses and plans; arrow-pg cannot map `Map` to a PostgreSQL OID. |
| ❌ `str[n]`, `str[a:b]` | `0A000 array_element does not support type Utf8`. DuckDB supports string subscripting; PostgreSQL does not. |
| ❌ `struct.field` | `0A000 Dot access not supported for non-string expr`. Both PostgreSQL (with parens) and DuckDB support it. `struct['field']` works. |
| ❌ `LIKE ANY (array)` | `0A000` naming the clause. |
| ❌ `EXISTS` / `IN (subquery)` in the **SELECT list**, correlated by more than an equality | `0A000`. The select-list forms answer by respelling as `count(*)` subqueries, which only `ScalarSubqueryToJoin` decorrelates — and it decorrelates an **equality** correlation over a filtered projection, nothing else. So these stay refused, each naming the spelling that answers: a `q` correlated by an inequality or an expression, a correlated `q` whose own plan is more than a filtered projection, a `q` not projecting exactly one column for `IN`, and the positions where even PostgreSQL's planner refuses a correlated scalar subquery — `ORDER BY EXISTS (q)` and `GROUP BY EXISTS (q)`, which the message answers with the select-list alias that reaches both. |
| ❌ `GLOB` / `~~~` | `42601`. DuckDB-only. |
| ❌ `U&'\0041'` | `0A000 Unsupported Value 'UnicodeStringLiteral'`. |
| ❌ `N'foo'` | `0A000 Unsupported Value 'NationalStringLiteral'`. |
| ❌ `B'1010'` | `0A000`. DuckDB silently turns it into the *string* `'b1010'`, so the loud rejection is the better behaviour. |
| ❌ `INTERVAL '1-2' YEAR TO MONTH` | `0A000 Unsupported Interval Expression with last_field`. Unimplemented in both engines; PostgreSQL has it. |
| ❌ A **computed** `SIMILAR TO` pattern, or a computed `LIKE … ESCAPE '<non-backslash>'` | `0A000` on both paths. Both are answered by translating the *literal* pattern before evaluation — `SIMILAR TO` is compiled to an anchored regex by PostgreSQL's own algorithm, and a non-backslash escape is re-spelled with backslash escaping — and a pattern that only exists at run time cannot be. Refused rather than passed through under the wrong matching rules. |
| ❌ `to_number(text, text)` | `42883`. Deliberately absent rather than approximated: the whole format family it belongs to is written out (`format`, `quote_ident`, `quote_literal`, `quote_nullable`, `%s`/`%I`/`%L`/`%%` with positions and widths), but a `numeric` here has a fixed Arrow scale, so `to_number('1234', '9999')` would print `1234.000…` where PostgreSQL prints `1234` — and a wrong spelling is less visible to a client than an absence. `to_char` is unaffected. |

**Labels, cutting across both tables:** an unaliased expression column now carries the name
PostgreSQL derives, or `?column?` where PostgreSQL has none, and a label that
**repeats** is sent as often as PostgreSQL sends it — `SELECT sum(a), sum(b)` is two columns both
called `sum`. One residue: `'{1,2}'::int[]` is labelled `array`, because upstream's
`FixArrayLiteral` has already replaced the string with an `ARRAY[…]` constructor by the time the
label is read — a spelling PostgreSQL itself labels `array`.

**One push-down narrowing** is left, where there were four. Each of them cost an optimization
rather than an answer, and each existed because DataFusion's `Inexact` contract covers a shard
returning too *many* rows but not too *few* and not one that *errors*. Three are closed, and
what closed two of them is the same move: stop asking whether the two engines happen to agree
and make the pushed fragment **say which reading applies**.

* **`LIKE`'s escape character — closed.** PostgreSQL escapes with `\` when no `ESCAPE` clause
  is present and DuckDB escapes with nothing, so `'a\_b'` asked a shard for a literal
  backslash and matched no row. The fragment now spells out `ESCAPE '\'`, which is exactly
  what the write path's re-render already did for the same reason, so the two paths agree by
  construction rather than by coincidence. It is emitted unconditionally — one rule, no
  branch on whether the pattern happens to contain a backslash — and the clause states which
  engine's default applies rather than adding a rule of its own.
* **The literal kinds that render as themselves — closed.** `Date64` was the exclusion, and
  it did not need one: Arrow's `Date64` is a whole number of days expressed in milliseconds,
  so a whole-day value is **renumbered to the `Date32` naming the same day** and renders as a
  real `CAST('YYYY-MM-DD' AS DATE)` instead of the timestamp the unparser would have written.
* **Text ordering — closed**, and it was the one narrowing about a *setting*
  rather than an expression: both engines compare text by byte value, but nothing pinned
  DuckDB's `default_collation`. It is now enforced at every layer that can name a collation —
  a shard pins and verifies the setting when it opens its database, an expression `COLLATE`
  naming anything else is refused on both paths, and a **column** declared with one is refused
  at DDL — so `<`, `<=`, `>`, `>=` are pushed, `BETWEEN` with them.
* **The opaque `Utf8` column stays, narrowed to what it is actually about.** A declared type
  that degraded to `Utf8` (`UUID`, `JSON`, `ENUM`, `STRUCT`, and every type
  `parse_data_type` does not recognize) is stored as something else by the shard, so a pushed
  `u = 'notauuid'` is a shard-side conversion *error* — the one outcome `Inexact` cannot
  repair. No predicate that puts a **value** beside such a column is pushed. `IS [NOT] NULL`
  now is, and it is exempt for a reason that holds for any declared type rather than for the
  four measured ones: it names no value, so there is nothing to convert, and null-ness is the
  one thing a faithful read cannot disagree about. Closing the value shapes too would mean
  pushing `CAST(<col> AS VARCHAR) = '…'` and asserting that DuckDB's text rendering of the
  stored type is byte-identical to the text its Arrow export produces — checkable for the
  types that have been measured, not checkable for the open-ended fallback that keeps VaireDB
  forward-compatible with a type DuckDB has not added yet.

Two residues, both measured rather than assumed. A `LIKE` pattern ending in an **unpaired
backslash** stays at the coordinator: `arrow-string` reads the trailing `\` as a literal
backslash and DuckDB refuses the pattern outright (`Like pattern must not end with escape
character`), and an error is neither too many rows nor too few. A `Date64` **carrying a time of
day** stays too — truncating it to its day would not widen the answer but move it, which
`Inexact` cannot repair for `<`. Neither is reachable from a PostgreSQL client: `date` arrives
as `Date32`, and PostgreSQL itself rejects the dangling-backslash pattern.

### 2.6 Joins and set operations

Every row on this axis was measured in **both** layouts the shard map produces — co-located
and shuffle — and no row differs between them: the shard map may change the plan, never the
answer.

| Item | Divergence |
|---|---|
| 🟡 A join on a **pseudonymized** column | Equality survives HMAC-SHA256, so an equi-join on such a column relates two tables correctly, including under a `GROUP BY`/`SUM` above the join. Everything the digest does not preserve is refused `0A000` rather than answered: an ordering join (`a.email < b.email`), an `ORDER BY` on the column, and a comparison against **plaintext**, which would match nothing and say so nowhere. |
| 🟡 An untyped literal branch of a set operation | `… UNION ALL SELECT '9'` comes back as `text` where PostgreSQL resolves to the typed branch's `integer`. The rows are PostgreSQL's; the advertised column type is not, so an `ORDER BY` over it sorts lexicographically. PostgreSQL's `UNKNOWN` has no Arrow equivalent, so the *literal* is recognized in the plan rather than retyped — and only in a select list, because a `VALUES` clause resolves its own columns to text first. |
| ❌ `x <op> ANY \| SOME \| ALL (subquery)` outside a predicate `AND` chain | `0A000`. Every operator answers in a `WHERE`, `HAVING`, `QUALIFY` or join `ON`, as an operand of a top-level `AND` — the only positions where the three-valued reading collapses onto the semi/anti join the cluster already ships. A select list, `ORDER BY`, under a `NOT`, inside a `CASE` and under an `OR` are refused naming that spelling; `OR` is refused although the collapse *is* valid there, because the plan behind it is the one carrying three upstream defects. One shape inside a covered position goes with them: a join `ON` whose comparison reaches its column through a table alias, where a hand-written `EXISTS` fails identically — the message names the two spellings that answer. `= ANY`, `<> ALL`, `IN` and `EXISTS` are untouched. |
| ❌ `WHERE k NOT IN (<correlated subquery>)` | `0A000` — and it used to answer **one row too many** (§ 2.7 row 7). PostgreSQL's three-valued rule is evaluated over an `array_agg` of the candidates, which a correlated subquery cannot be: DataFusion will not plan an outer reference that a derived table needs `LATERAL` to see. The refusal names `NOT EXISTS` **and** the `IS NULL` test it needs, since `NOT EXISTS` is two-valued. A wildcard subquery and a `NOT IN` inside a `CASE` are refused with it. `NOT IN` over `NOT NULL` columns still answers, correlated or not, and `HAVING max(k) NOT IN (q)` — the other half of that row — answers without a join at all. |
| ❌ The `USING` shapes three names cannot fit in two fields | `0A000`. A `FULL`/`RIGHT JOIN … USING (c)` reached by a qualifier is respelled so each side reports its own key, which leaves two shapes refused, each naming the spelling that answers: a wildcard beside a qualified key, and the merged key beside the same key per side under **one** name — PostgreSQL answers that with two result columns both called `c`, and a `DFSchema` holds neither two fields of a name nor an unqualified `c` beside a qualified `l.c`. |
| ❌ A bare `USING` / `NATURAL` key in `WHERE` over a side a rewrite cannot read | `42703`. The bare key is rewritten into the expression PostgreSQL's merged column *is*, which needs to know which side to name. Three shapes it cannot: a left side that is itself a join, two joins in one query block sharing a key name, and a key reached from a nested block. PostgreSQL resolves all three; the qualified spelling answers here. |

### 2.7 Silently wrong (⛔) — all eight, all closed

The dangerous class: a plausible answer that is not PostgreSQL's, with no error and no
warning. This list was the whole of it, and **all eight rows are now closed** — each fix is in
the code and pinned by its own test. Row 7's last
shape, the correlated `NOT IN`, is closed as the refusal now listed in § 2.6; everything else
answers what PostgreSQL answers.

The table is kept as the census **as first measured**, and deliberately not deleted: a row's
shape is the record of why it was invisible, which is what § 5's rule for a ⛔ test is derived
from, and what a future silent-wrong-answer row will be recognized by. Row 5's "none of these is
a write" held only for the list as written: rows 4 and 5 were wrong on the write path too, since
a write is rendered back to SQL text and executed verbatim by a shard.

| # | Construct | Returns | PostgreSQL | Note |
|---|---|---|---|---|
| 1 | `1.0::float8 / 0` on the **read path** | `inf` — a poison value that flows into aggregates | `22012 division_by_zero` | The integer and `numeric` forms raise `22012` on both paths. Arrow's float division follows IEEE 754, so there is no error to classify. The write path is loud, since its zero-divisor guard does not consult operand types. |
| 2 | `0b101` | **`0`** | `5` | Not a parse error: sqlparser tokenizes it as `0` aliased `b101`, so the planner is handed `SELECT 0 AS b101`. Confirmed in all four dialects and both versions — a tokenizer defect. DuckDB mangles it identically. |
| 3 | `0x1F` | **`Binary`**, bytes `1f`, on read | `31` | On write the irreducible W4 render rewrites it to `X'1F'` and the shard fails `42804`. Loud on write, silently a byte string on read. |
| 4 | `arr[-1]` | the **last element** | `NULL` | Consistent on both paths; DataFusion and DuckDB agree with each other and not with PostgreSQL. Needs either a rewrite or a decision to document it as intentional. |
| 5 | `'\xDEADBEEF'::bytea` | the **10 ASCII bytes** of the literal text | 4 bytes | PostgreSQL's hex-escape input format is not decoded; DataFusion casts `Utf8`→`Binary` bytewise. This does not contradict `Binary` being clean in § 2.2 — that holds for parameterized writes, not for this literal form. DuckDB's `::BLOB` *does* decode `\x`. |
| 6 | `nth_value(x, 0)` | **NULL for every row** | `22016 argument of nth_value must be greater than zero` | One guard upstream, and the narrowest row here. Correct for `n ≥ 1`; negative `n` is a deliberate superset (§ 4). |
| 7 | `HAVING max(k) NOT IN (SELECT …)`, and `WHERE k NOT IN (<correlated subquery>)` | **one row too many** — the one whose key is NULL | the NULL-keyed row excluded | The two shapes the null-aware `NOT IN` respelling cannot enter: DataFusion will not plan a correlated subquery whose outer reference is an aggregate of the group, and a derived table cannot see the outer row without `LATERAL`. Everywhere else — `WHERE`, `HAVING`, `QUALIFY`, `ON`, through `AND`/`OR`, under a `NOT` — `NOT IN` is null-correct. `NOT EXISTS` expresses both correctly today. |
| 8 | `SELECT a.id, b.id FROM a FULL JOIN b USING (id)` — the key reached by an **explicit qualifier** | the merged `COALESCE` value under *both* qualifiers | the raw `a.id` and `b.id`, NULL where unmatched | The unqualified `id` and `SELECT *` are correct. PostgreSQL's join output has three addressable names (`id`, `a.id`, `b.id`) where a DataFusion schema has two fields, so the merged value has to occupy whichever fields every other consumer reads. Rare and expert; the `ON` spelling with the client's own `COALESCE` answers all three exactly. |

---

## 3 — Not implemented by decision (🚫)

Each is refused with a SQLSTATE and, where one exists, a named alternative. These are
decisions, not backlog.

### 3.1 Because the cluster is shared-nothing

**No sequences** — `CREATE`/`DROP SEQUENCE`, `nextval()`, `SERIAL`/`BIGSERIAL`/`SMALLSERIAL`
(`0A000`; `ALTER SEQUENCE` `42601`). One monotonic counter is precisely what a shared-nothing
cluster cannot maintain cheaply: broadcasting gives every shard its own counter, and because
replication is statement shipping a surviving `nextval()` would evaluate differently on each
replica. A coordinator-allocated counter is correct but turns every insert into a round trip
through one serialized allocator and makes the coordinator a hard failure point for writes,
which is the opposite of what sharding buys. Use an application-side UUID, ULID or snowflake.

**No search path** — `SET search_path` and `USE` are refused **by name** (`0A000`). A qualifier
here is *part of the catalog key*, not a lookup hint: `sales.t` keys as `sales.t` and lands as
`sales_t_shard2`. A search path would make one written name resolve to different keys depending
on session state, while the coordinator resolves a name once, at planning time, for every shard
at once — so the same statement replayed on a replica could reach a different table.

**No user-defined types** — `CREATE TYPE` (enum, composite, range), `CREATE DOMAIN`,
`ALTER TYPE` (`0A000`). Cluster-wide state with no catalog replay path, so a node that joins or
is rebuilt would come back without the type; and no `pg_type` row to give it an OID over the
wire. What an enum or domain buys is a value check, which is shard-local — a `CHECK` constraint
at `CREATE TABLE` is the in-database version of the same guarantee.

**No storage maintenance** — `ANALYZE` (`0A000`, no planner-statistics surface to populate) and
`VACUUM` (`42601`, a per-shard storage concern, like `CHECKPOINT`). Either could only report an
`OK` for work that never happened.

### 3.2 Because PostgreSQL is the contract

A form only DuckDB has has no client asking for it, and accepting one would publish a second
dialect that clients would then depend on. The same reasoning covers a form the **standard** has
and PostgreSQL does not implement: there is no PostgreSQL answer to match, so answering it at all
would be inventing one.

| Refused | SQLSTATE | Write instead |
|---|:-:|---|
| `PIVOT` | `42601` | `CASE` inside aggregates plus `GROUP BY` |
| `UNPIVOT` | `42601` | `UNION ALL`, or a `LATERAL` over a `VALUES` list |
| `SUMMARIZE` | `42601` | the aggregates it wraps, written out |
| `SET VARIABLE` | `42601` | — nothing in the read path would read one back, so accepting it would be a fake `OK` |
| `IGNORE NULLS` on a window function — counted on § 2.4's axis | `0A000` | nothing: PostgreSQL does not implement the standard's null-treatment option (§ 9.22) and always behaves as `RESPECT NULLS`, which is what VaireDB answers. It used to be a **silent no-op** here, which was worse than a refusal — the NULL it asked to skip came back looking like data. `RESPECT NULLS` is accepted, since it asks for the behaviour both engines already have. |

### 3.3 Types refused by name at DDL

`0A000` at `CREATE TABLE`, `ADD COLUMN` and `ALTER COLUMN TYPE`, each naming what to write
instead. Every one of these previously *accepted* the DDL and then failed or lied on read. An
array is refused for the same reason as its element type.

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

### 3.4 Standing limitations a client must expect

Properties of the design rather than backlog items:

1. **No cross-shard atomicity.** No 2PC. A transaction spanning more than one node set is
   refused at `COMMIT` unless `allow_cross_shard_transactions`; a partial multi-shard write
   reports `40003` with how much was written. Out of every phase below — changing it means
   adding 2PC.
2. **Uniqueness only over the shard key.** A `UNIQUE` or `PRIMARY KEY` constraint is
   enforceable only when it includes the shard key. `FOREIGN KEY` is never enforced; `CHECK`
   always is.
3. **The shard key is immutable.** `UPDATE` of the shard-key column is refused; row relocation
   does not exist. A write must have a determinable shard key before it leaves the coordinator.
4. **A transaction block buffers.** Only statements the coordinator can answer truthfully
   *without running them* are allowed inside one.
5. **No role model, therefore no privilege checks.** Most visibly, `COPY` reads and writes any
   path the coordinator process can. Tracked on the roadmap as *Security: TLS, users, groups*.
6. **Anonymization is one-way and write-path only.** Order and value are unrecoverable;
   equality and grouping are exact.
7. **Text ordering is byte order.** Collation is not pinned across the two engines.
8. **A statement is either correct on every shard or refused.** Nothing is applied partially by
   design, and a decoration that only affects performance is stripped while anything that
   changes results is refused **by name**.

---

## 4 — Supersets that must NOT be "fixed"

VaireDB accepts these and PostgreSQL does not. Narrowing to PG parity would be a regression
against DataFusion and DuckDB:

- `count(DISTINCT x) OVER (…)` — PostgreSQL rejects `DISTINCT` in a window aggregate (`42P20`).
- `QUALIFY` — DuckDB/Snowflake syntax PostgreSQL does not have.
- Negative `nth_value` offsets.
- `INTERVAL 1 DAY` unquoted — DuckDB-only; PostgreSQL rejects it.
- `count(DISTINCT (a,b))` — plans as `count(DISTINCT struct(a,b))` and is correct.
- `DISTINCT ON (col)` is the reverse case: a PostgreSQL extension, and it works.
- `ALTER TABLE t RENAME TO sales.t` — PostgreSQL's grammar has no production for a qualified
  `RENAME TO` destination. Here a qualifier *is* part of a shard table's physical name, so the
  statement is the same work `SET SCHEMA` asks for and is honored when it changes only the
  schema; changing the schema **and** the name in one statement is refused (`42601`).

---

## 5 — Executable counterpart

The convention throughout: a **passing** test pinning today's actual behaviour, plus an
`#[ignore = "gap: …"]` test asserting the PostgreSQL-correct target, which fails by
construction and is the definition of done. `make e2e` runs only the passing set, so the gap
map never blocks CI. A ⛔ row's ignored test must assert the **correct value**, not merely that
no error occurred — a silent-wrong-answer gap is invisible to an error-shape assertion.

| Suite (`tests/e2e/tests/`) | Covers |
|---|---|
| `sql_command_select.rs` | The read path, including push-down |
| `sql_expression_gaps.rs` | Expression, window and aggregate rows — organized by the **seam** a gap sits on (a refusal, a rewrite, a distributed case) rather than by function |
| `sql_command_dml.rs`, `sql_command_ddl.rs`, `sql_command_transaction.rs`, `catalog_ddl.rs` | Writes, DDL, transaction blocks |
| `sql_command_unsupported.rs` | Every 🚫 and ❌ statement row, plus `COPY`'s streaming pair and its Parquet round trip |
| `sql_join_gaps.rs` | § 2.6 and § 2.7 rows 7–8 |
| `data_types_round_trips.rs`, `data_types_dialect_gaps.rs` | § 2.2, both wire formats |
| `anonymization.rs` | The refusals in § 2.4 and § 2.6, beside the reads that must keep answering |
| `errors.rs` | The rejection points of § 1.2 and the classes of § 1.3, including a refusal raised on a core node |
| `extended_protocol.rs` | `Describe`-time result and parameter OIDs — what § 2.4's bind-parameter row is measured over |
| `shard_key_hazards.rs`, `identifier_rewrite.rs`, `concurrency.rs`, `sharding.rs`, `shard_routing.rs`, `replication_fault_tolerance.rs` | The sharding rules in § 3.4 |

Two properties of the suite are load-bearing. `describe_result_types` and the column-label
helper live in `tests/e2e/src/lib.rs`, so a result OID and a label are assertable from any
suite — several rows above exist only because they are visible over `Describe`. And **every
refusal is probed for over-reach**: each asserts, in the same test, that the neighbouring form
which loses no clause still answers. That is what keeps a refusal honestly scoped rather than a
blanket rejection.

One residue of consolidating six per-axis documents into this one: an `#[ignore]` reason or
comment that cites a bare *"row N"* refers to the retired per-axis numbering, and the row it
means is identified by its construct, not by that number. Only § 2.7 is numbered here.

Still to write: `sql_expression_operators.rs`, `sql_expression_literals.rs`,
`sql_function_aggregate_distributed.rs` (the merge invariants that would catch a wrong
"optimization" if `remote_scan_exec.rs` ever gained a partitioning claim).
