# VaireDB SQL Gap Analysis

Everything a PostgreSQL client **cannot** fully do against VaireDB today: statements, data
types, aggregate and window functions, operators, literals, casts, joins and set operations.

This is the single gap record. It lists only what diverges — what is partially supported,
what is refused, and what is not implemented because VaireDB is a distributed database.
Anything absent from this document works and agrees with PostgreSQL.

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
`regr_*` is one row), so this compares axes rather than individual constructs.

| Axis | 🟡 | ⛔ | ❌ | 🚫 |
|---|---:|---:|---:|---:|
| Statements (§ 2.1) | 15 | — | 10 | 8 |
| Data types (§ 2.2) | 6 | — | 1 | 8 |
| Aggregate functions (§ 2.3) | 8 | — | 10 | — |
| Window functions (§ 2.4) | 4 | 1 | 10 | — |
| Operators, literals, casts (§ 2.5) | 8 | 4 | 20 | — |
| Joins and set operations (§ 2.6) | 2 | 3 | 3 | — |
| **Total** | **43** | **8** | **54** | **16** |

The read path's statement list, expression surface and type layer are closed, including live
predicate and `LIMIT` push-down to the shards. All ten historical *split-brain* rows — one SQL
fragment answered differently by a `SELECT` and by an `UPDATE` — are closed, by translating the
divergent expression on the write path rather than refusing it. No remaining ⛔ row is a write.

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
| Logical / physical planning | `Error during planning` / `Physical plan does not support …` | `XX000` |
| Distributed execution | `[VDB-5001] Job … failed` | `XX000` |

### 1.3 Distributed-execution constraints that produce gaps

Each of these is a property of running a plan across processes, and each is the direct cause
of rows below.

| Constraint | Rows it produces |
|---|---|
| **An error raised past the Ballista scheduler loses its SQLSTATE.** It arrives as the scheduler's own text (`Job <id> failed: … DataFusionError(Execution(…))`), so there is no typed error left to classify and it lands `XX000`. Division by zero is the one class carved out by name, scoped to fire only after the typed classifier has already given up. | Nested aggregate `XX000` not `42803`; window function in an illegal position `XX000` not `42P20`; `percentile_disc(ARRAY[…])` `XX000`. |
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
| `ALTER TABLE` | Dropping the shard key; touching an anonymized column; retyping or dropping a column a declared constraint covers; every `ADD`/`DROP CONSTRAINT` form but `… UNIQUE (<shard key>)` — the shard engine implements neither `ADD … CHECK` nor `DROP CONSTRAINT`. While the table carries an index or index-backed constraint, only `ADD COLUMN` and changing a default. `RENAME TO` must be the only action (`42601`) and stays inside the table's schema. |
| `DROP` | Several names in one statement; `CASCADE` / `RESTRICT`. `DROP SEQUENCE` reaches the table handler and reports a missing table. |
| `ALTER VIEW` | `RENAME TO` — fails at parse (`42601`). |
| `COPY` | `PROGRAM`, non-CSV formats; `FORMAT CSV` must be stated (PG's default is `TEXT`); any option but `HEADER`/`DELIMITER`/`QUOTE`. A load that fails part-way leaves the batches already sent committed — the same guarantee a multi-row `INSERT` gives. ⚠️ **No privilege check exists**: VaireDB has no role model, so any client reads and writes any path the coordinator process can. |
| `CREATE INDEX` / `DROP INDEX` | `UNIQUE` off the shard key; a `UNIQUE` index narrowed by `WHERE` or `NULLS NOT DISTINCT`; an unnamed index (nothing for `DROP` to resolve); an index over an expression. |
| `CREATE SCHEMA` / `DROP SCHEMA` | `CASCADE`, `AUTHORIZATION`; dropping `public` or a metadata schema. `sales.t` and `sales_t` cannot coexist (`42P07`), since the qualifier folds into the physical name. |
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
| `ALTER SCHEMA`, `ALTER TABLE … SET SCHEMA` | `42601` | **The one open write-path statement gap.** Parser plus routing: sqlparser omits `ALTER SCHEMA` although PostgreSQL has it. |
| `ATTACH` / `DETACH` | `0A000` / `42601` | Single-node DuckDB attachment; meaningless for a cluster. |
| `CHECKPOINT` | `42601` | Per-shard storage concern, not coordinator-exposed. |
| `EXPORT` / `IMPORT DATABASE` | `42601` | Whole-DB dump/load. Use `COPY`. |
| `INSTALL` / `LOAD` | `42601` / `0A000` | Extension management is a per-node concern. |
| `CREATE SECRET` | `0A000` | DuckDB's secrets manager. VaireDB has its own path (`INSERT INTO vairedb_catalog.anonymization_secret`, which is append-only — there is no documented retraction path, so a fixture cannot clean up after itself). |
| `USE` | `0A000` | One database per cluster, and no search path to switch. |
| `CALL` | `0A000` | No stored or table procedures. |
| `COMMENT ON` | `0A000` | No catalog comment storage and nowhere to read one back — accepting it would be a fake `OK`. |
| `CREATE MACRO` | `42601` | DuckDB-only. |

Also missing, and function-level rather than statement-level: **`pg_typeof()`**. It is how a
client asks what VaireDB thinks a column is, which is the question every type-layer surprise
starts with. (`SET datafusion.*` being refused `42704` is *correct*, not a gap: VaireDB models
PostgreSQL's parameter set, not DataFusion's.)

### 2.2 Data types

Values round-trip faithfully in every row here; what diverges is the advertised type.

| Item | Divergence | Where the fix lives |
|---|---|---|
| 🟡 `UInt64` column | Advertised `numeric`, not `bigint` — arrow-pg has no unsigned PostgreSQL type. The three ranking functions are already widened above it; this is the general case. | VaireDB (widen), or arrow-pg |
| 🟡 `STRUCT` column | Advertised `text`. Blocked on sqlparser re-rendering `STRUCT(a INTEGER, b VARCHAR)` as `STRUCT(a, INTEGER, b, VARCHAR)`, which DuckDB cannot parse, so `STRUCT` cannot be mapped to a PostgreSQL composite. | sqlparser, then VaireDB |
| 🟡 `UUID`, `JSON`, `ENUM`, `CHAR`/`BPCHAR` | Deliberate `Utf8` fallback: values faithful, advertised OID `text` rather than the type's own. An **unknown** declared type degrades the same way, which is what keeps VaireDB forward-compatible with any type DuckDB adds whose values are faithful as text. | VaireDB |
| 🟡 `ENUM` ordering | Sorts by decoded string, not declaration order — which is why it is *not* mapped to `Dictionary(UInt8, Utf8)`. | VaireDB |
| 🟡 `VARCHAR(n)` typmod | Not advertised, so `psql`'s `\d` prints `text` where PostgreSQL prints `character varying(64)`. Arrow has nowhere to put a typmod; the catalog still holds the declared string verbatim, so it is recoverable for introspection. | VaireDB |
| 🟡 `NUMERIC` above 29 digits, **binary** format | `22003`, from arrow-pg's `rust_decimal` 96-bit mantissa. Text format is exact. | arrow-pg |
| ❌ `Decimal256` (`NUMERIC` with precision > 38) | `XX000 Unsupported Datatype` — arrow-pg has no arm for it, so it is refused rather than rounded. | arrow-pg |

Eight further types are refused **by name at DDL** — a decision, in § 3.3.

### 2.3 Aggregate functions

There is no missing-coverage gap and no split-brain row: every aggregate is computed by
DataFusion, DuckDB never evaluates one, and the distributed merge is correct *structurally*
(`remote_scan_exec.rs` declares `UnknownPartitioning(1)` and empty `EquivalenceProperties`, so
DataFusion is forced to insert a shuffle and no partial aggregate ever emits a finished value).

| Item | Divergence |
|---|---|
| 🟡 `avg(int4)` / `avg(int8)` | Exact `numeric` as PostgreSQL promises, but carries **10 decimal places where PG prints 16** — the accumulator is `Decimal128(38,6)`, so the result is `Decimal128(38,10)`; scale 6 was chosen so the row ceiling stays around 10¹³. |
| 🟡 `stddev` / `stddev_samp` / `stddev_pop` | `float8` where PostgreSQL returns `numeric` for integer or `numeric` input, and stays exact over it. |
| 🟡 `var` / `var_samp` / `var_pop` (and `variance`, which rewrites to `var_samp`) | Same. |
| 🟡 `regr_count` | `numeric` (from `UInt64`) where PostgreSQL says `int8`. The other eight `regr_*` are correct. |
| 🟡 `percentile_cont` / `quantile_cont` | Value is exact (VaireDB's own UDAF shadows DataFusion's, whose interpolation weight was quantized to 5 decimals), but the type is always `float8` where PostgreSQL returns `numeric` over a `numeric` or integer sort column. PostgreSQL's **array-of-fractions** overload (`percentile_cont(ARRAY[0.25,0.5])`) is missing on **both** percentile functions and they refuse it inconsistently: `0A000` from `percentile_cont` at coordinator signature resolution, `XX000` from `percentile_disc`, whose own fraction check runs on an executor and so crosses the boundary in § 1.3. A plain literal fraction out of range is correctly `22023` on the coordinator. |
| 🟡 `GROUPING SETS (())` alone | **0 rows** where PostgreSQL returns 1 grand-total row. Narrow — the same set works combined with a non-empty one. |
| 🟡 `count(DISTINCT a, b)` comma form | `XX000 NotImplemented`. `count(DISTINCT (a,b))` plans as `count(DISTINCT struct(a,b))` and **is correct**. |
| 🟡 Nested aggregate (`max(sum(m))`) | `XX000` where PostgreSQL raises `42803` at parse. The `Signature { … }` dump is truncated; the class is wrong for the § 1.3 boundary reason. |
| ❌ `mode() WITHIN GROUP (…)` | `0A000`. Needs a new UDAF. |
| ❌ `rank()` `WITHIN GROUP` | `0A000`. Hypothetical-set form; the **window** spelling works. |
| ❌ `dense_rank()` `WITHIN GROUP` | Same. |
| ❌ `percent_rank()` `WITHIN GROUP` | Same. |
| ❌ `cume_dist()` `WITHIN GROUP` | Same. |
| ❌ `json_agg` | `0A000`. Blocked behind the `CAST(… AS JSON)` gap in § 2.5. |
| ❌ `jsonb_agg` | Same. |
| ❌ `xmlagg` | No XML type. Out of scope. |
| ❌ `FILTER (WHERE …)` on a **window** aggregate | `0A000`. § 1.3 — Ballista's proto has no filter field. Rewrite with a `CASE` inside the aggregate, or aggregate a subquery. A plain aggregate's `FILTER` is applied correctly and stays accepted, so the refusal is scoped to the pair, not the keyword. |
| ❌ `OVER (<named_window> …)` | `0A000`. § 2.4. |

### 2.4 Window functions

All 11 PostgreSQL window functions are present, and the frame engine (all five units, peer
groups, integer/float/`INTERVAL` offsets) is correct. The gap is the clause surface around
them, plus one result type.

| Item | Divergence |
|---|---|
| 🟡 `ntile(n)` result type | `int8` where PostgreSQL promises `int4`. Buckets are correct. A driver decodes it successfully into the wrong width — the last 🟡 on this axis VaireDB can close alone, in the same place the `UInt64` widening already lives. |
| 🟡 Window fn in `WHERE` / `GROUP BY` / `HAVING` / nested in another window fn | **Correctly rejected** — PostgreSQL rejects these too — but `XX000` instead of `42P20` / `42803`. The debug dump is truncated; the class is wrong for the § 1.3 boundary reason. Wrong class breaks client error handling. |
| 🟡 Duplicate unaliased labels | `SELECT sum(a), sum(b)` is two columns both called `sum` in PostgreSQL, which DataFusion refuses to plan, so these keep the verbose rendered label instead of PostgreSQL's name. `AS` is a full workaround. |
| 🟡 Bind-parameter offsets — `ntile($1)`, `lag(x,$1)`, `nth_value(x,$1)` | Correct **provided the client declares a parameter OID**. Values byte-identical to literal controls otherwise. |
| ❌ `count(*) FILTER (WHERE …) OVER (…)` | `0A000`. § 1.3. Was silently dropping the filter and returning a plain unfiltered count. |
| ❌ `IGNORE NULLS` | `0A000`. Was a **silent no-op** — `IGNORE NULLS` and `RESPECT NULLS` returned byte-identical columns. One hardcoded `false` in datafusion-proto's `to_proto.rs`, with a stale comment claiming the field is unused. The worst of the discarded clauses, because the NULL it asked to skip comes back looking like data. `RESPECT NULLS` stays accepted — it asks for the behaviour DataFusion already has. |
| ❌ `OVER (w)` | `0A000`. Redundant parens dropped `PARTITION BY` **and** `ORDER BY`, widening the frame to the whole table. Root cause: `WindowSpec::window_name` is parsed by sqlparser and never read by datafusion-sql. `OVER w` without parens is correct and is what the refusal points at. |
| ❌ `OVER (w <extra clauses>)` | `0A000`. Adding `ORDER BY` lost the partition; adding a **frame** made it **nondeterministic** — five consecutive runs on unchanged data returned five different answers. The only unstable row ever measured here, and the reason this is refused rather than patched: an unstable answer is worse than no answer, and it cannot be caught by a test that runs once. |
| ❌ `WINDOW w2 AS (w1 …)` | `0A000`. Chained inheritance declared in the select's `WINDOW` list, which the `OVER`-clause check structurally cannot see, so it needs its own check. Two independent definitions in one `WINDOW` list lose nothing and are answered. |
| ❌ `EXCLUDE {CURRENT ROW \| GROUP \| TIES \| NO OTHERS}` | `42601` — **unparseable**. sqlparser's `WindowFrame` carries a literal `// TBD: EXCLUDE`. The only gap in this document that needs parser work first, and the only genuinely *missing feature*. Rare in practice. |
| ❌ Window fn in the outer `ORDER BY`, written inline | `XX000` — **legal PostgreSQL, rejected.** Ordering by the output alias or by position is a full and general workaround. |
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
| ❌ `**` | `42601`. **No sqlparser 0.62 dialect parses it** — needs upstream work, not a dialect change. DuckDB itself supports it. |
| ❌ `//`, `DIV` | `42601`. `//` is dialect-gated (PG's dialect rejects it, Generic and DuckDB accept it); DataFusion's `Operator::IntegerDivide` is unimplemented anyway. |
| ❌ `->` `->>` `#>` `#>>` `@?` | `0A000`. Present in DataFusion's `Operator` enum, unimplemented in type coercion. **The whole `jsonb` operator family is absent**, and unreachable regardless while `::json` fails. |
| ❌ `@@` | `0A000 Invalid function 'to_tsvector'`. Full-text search has no functions at all. |
| ❌ `'{"a":1}'::json` | `0A000 Unsupported SQL type JSON`. Works as a **column** type but not as a cast target — which is what makes every JSON-operator probe unreachable and blocks `json_agg`. |
| ❌ `'…'::uuid` | `0A000`. Same shape as JSON. |
| ❌ `'abcde'::VARCHAR(2)` | `0A000` naming the cast, all four spellings (`VARCHAR(n)`, `CHAR(n)`, `CHARACTER(n)`, `CHARACTER VARYING(n)`). The length used to be silently discarded where PostgreSQL truncates; it is not enforced anywhere on the read path, so it is refused and `substr()` named instead. An unbounded character cast is fine, and `NUMERIC(p,s)` is unaffected — that precision *is* applied. |
| ❌ `CAST(x AS T ARRAY)` | `42601`. |
| ❌ `{'a': 1}`, `MAP {'a': 1}` | `42601`, dialect-gated at E1: DataFusion supports brace struct literals, sqlparser's PG dialect refuses them. |
| ❌ `MAP(['a'],[1])` | `XX000 Unsupported Datatype Map(…)`. Parses and plans; arrow-pg cannot map `Map` to a PostgreSQL OID. |
| ❌ `str[n]`, `str[a:b]` | `0A000 array_element does not support type Utf8`. DuckDB supports string subscripting; PostgreSQL does not. |
| ❌ `struct.field` | `0A000 Dot access not supported for non-string expr`. Both PostgreSQL (with parens) and DuckDB support it. `struct['field']` works. |
| ❌ `LIKE ANY (array)` | `0A000` naming the clause. |
| ❌ `EXISTS` / `IN (subquery)` in the **SELECT list** | `XX000 … Expr::Exists { .. } not supported`. Not decorrelated there, so the expression reaches datafusion-proto, which cannot encode a subquery. The `WHERE` spelling works, including correlated, and a **scalar** subquery in the SELECT list works. |
| ❌ `GLOB` / `~~~` | `42601`. DuckDB-only. |
| ❌ `U&'\0041'` | `0A000 Unsupported Value 'UnicodeStringLiteral'`. |
| ❌ `N'foo'` | `0A000 Unsupported Value 'NationalStringLiteral'`. |
| ❌ `B'1010'` | `0A000`. DuckDB silently turns it into the *string* `'b1010'`, so the loud rejection is the better behaviour. |
| ❌ `INTERVAL '1-2' YEAR TO MONTH` | `0A000 Unsupported Interval Expression with last_field`. Unimplemented in both engines; PostgreSQL has it. |
| ❌ A **computed** `SIMILAR TO` pattern, or a computed `LIKE … ESCAPE '<non-backslash>'` | `0A000` on both paths. Both are answered by translating the *literal* pattern before evaluation — `SIMILAR TO` is compiled to an anchored regex by PostgreSQL's own algorithm, and a non-backslash escape is re-spelled with backslash escaping — and a pattern that only exists at run time cannot be. Refused rather than passed through under the wrong matching rules. |

**Cosmetic, cutting across both tables:** an expression column with no `AS` alias is labelled
with DataFusion's **plan rendering** rather than PostgreSQL's `?column?` — `SELECT 5 # 3` is
labelled `Int64(5) BIT_XOR Int64(3)`, and a rewritten expression renders as its expansion, so
`1 < ALL (ARRAY[2,3])` produces a ~400-character `CASE WHEN make_array(…)` header. Function
columns already get PostgreSQL's name (`pgwire_handler/column_labels.rs`); this is the rest.

**Four push-down narrowings**, each of which costs an optimization rather than an answer, and
each of which exists because DataFusion's `Inexact` contract covers a shard returning too
*many* rows but not too *few* and not one that *errors*: nothing is pushed onto an **opaque
`Utf8` column** (a declared type that degraded to `Utf8` — a pushed `u = 'notauuid'` is a
shard-side conversion *error*, and an error is the one outcome `Inexact` cannot repair); **no
ordering comparison on any text column**, because DuckDB's `default_collation` is neither
pinned nor inspected; `LIKE` only with a **literal, backslash-free** pattern, since the default
escape differs; and only literal kinds that **render as the value they stand for** (`Date64` is
excluded — it unparses as a *timestamp*, so a value carrying a time of day would be truncated
by the coordinator and compared exactly by the shard, dropping rows).

### 2.6 Joins and set operations

Every row on this axis was measured in **both** layouts the shard map produces — co-located
and shuffle — and no row differs between them: the shard map may change the plan, never the
answer.

| Item | Divergence |
|---|---|
| 🟡 A join on a **pseudonymized** column | Equality survives HMAC-SHA256, so an equi-join on such a column relates two tables correctly, including under a `GROUP BY`/`SUM` above the join. Everything the digest does not preserve is refused `0A000` rather than answered: an ordering join (`a.email < b.email`), an `ORDER BY` on the column, and a comparison against **plaintext**, which would match nothing and say so nowhere. |
| 🟡 An untyped literal branch of a set operation | `… UNION ALL SELECT '9'` comes back as `text` where PostgreSQL resolves to the typed branch's `integer`. The rows are PostgreSQL's; the advertised column type is not, so an `ORDER BY` over it sorts lexicographically. PostgreSQL's `UNKNOWN` has no Arrow equivalent, so the *literal* is recognized in the plan rather than retyped — and only in a select list, because a `VALUES` clause resolves its own columns to text first. |
| ❌ An unqualified `USING` key in a `WHERE` clause — `… USING (c) WHERE c > 2` | `42703 Ambiguous reference to unqualified field c`, on **every** `USING` and `NATURAL` join, inner included. PostgreSQL does not refuse this at all (and its code for a genuinely ambiguous column is `42702`). DataFusion's name resolution does not see the join's `USING` set from a `WHERE` predicate. Every other clause resolves the merged column correctly — `GROUP BY`, `HAVING`, `ORDER BY`, an aggregate argument, the select list — and `WHERE l.c > 2` works, and since § 4's Tier 1 row 5 closed it filters on the left side's own key the way PostgreSQL does. |
| ❌ `x <op> ANY \| SOME \| ALL (subquery)` for every `<op>` but `= ANY` and `<> ALL` | `0A000`. Each plans to a **mark** join whose output column is named `mark` on both sides of the join above it, and serializing that plan fails — `Schema contains duplicate unqualified field name mark`. The refusal names the `max()`/`min()` rewrite over the same subquery, or `EXISTS`, **and states that the aggregate form answers differently for an empty subquery and for one containing NULLs** — which is why it is named and not applied. `= ANY` and `<> ALL` are normalized to `IN`/`NOT IN` and work. |
| ❌ `INTERSECT ALL`, `EXCEPT ALL` (and `MINUS ALL`) | `0A000`. These count **multiplicity**, which DataFusion lacks: both were answered as the plain semi/anti join the `DISTINCT` forms are built from, so with left `1,1,1,2` and right `1,1,3`, `INTERSECT ALL` answered `1,1,1` where PostgreSQL answers `1,1`, and `EXCEPT ALL` answered `2` where PostgreSQL answers `1,2`. Neither the right count nor a subset of it — and a row count is exactly what an analytical client goes on to aggregate. The refusal names the `DISTINCT` form. `UNION ALL` is untouched. |

### 2.7 Silently wrong (⛔) — all eight

The dangerous class: a plausible answer that is not PostgreSQL's, with no error and no
warning. This list is the whole of it. None of these is a write.

Rows 1, 2–5, 6, 8, and 7 apart from its correlated shape, are now **closed** — the whole list but
one shape of one row. The `Returns` column
below stays the census as first measured — kept for the record, since a row's shape is why it
was invisible — and § 4's Tier 1 table with [`gap-closure-status.md`](gap-closure-status.md)
carry the current state. Row 5's "none of these is a write" held only for the list as written:
rows 4 and 5 were wrong on the write path too, since a write is rendered back to SQL text and
executed verbatim by a shard.

| # | Construct | Returns | PostgreSQL | Note |
|---|---|---|---|---|
| 1 | `1.0::float8 / 0` on the **read path** | `inf` — a poison value that flows into aggregates | `22012 division_by_zero` | The integer and `numeric` forms raise `22012` on both paths. Arrow's float division follows IEEE 754, so there is no error to classify. The write path is loud, since its zero-divisor guard does not consult operand types. |
| 2 | `0b101` | **`0`** | `5` | Not a parse error: sqlparser tokenizes it as `0` aliased `b101`, so the planner is handed `SELECT 0 AS b101`. Confirmed in all four dialects and both versions — a tokenizer defect. DuckDB mangles it identically. |
| 3 | `0x1F` | **`Binary`**, bytes `1f`, on read | `31` | On write the irreducible W4 render rewrites it to `X'1F'` and the shard fails `42804`. Loud on write, silently a byte string on read. |
| 4 | `arr[-1]` | the **last element** | `NULL` | Consistent on both paths; DataFusion and DuckDB agree with each other and not with PostgreSQL. Needs either a rewrite or a decision to document it as intentional. |
| 5 | `'\xDEADBEEF'::bytea` | the **10 ASCII bytes** of the literal text | 4 bytes | PostgreSQL's hex-escape input format is not decoded; DataFusion casts `Utf8`→`Binary` bytewise. This does not contradict `Binary` being clean in § 2.2 — that holds for parameterized writes, not for this literal form. DuckDB's `::BLOB` *does* decode `\x`. |
| 6 | `nth_value(x, 0)` | **NULL for every row** | `22016 argument of nth_value must be greater than zero` | One guard upstream, and the narrowest row here. Correct for `n ≥ 1`; negative `n` is a deliberate superset (§ 5). |
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
dialect that clients would then depend on.

| Refused | SQLSTATE | Write instead |
|---|:-:|---|
| `PIVOT` | `42601` | `CASE` inside aggregates plus `GROUP BY` |
| `UNPIVOT` | `42601` | `UNION ALL`, or a `LATERAL` over a `VALUES` list |
| `SUMMARIZE` | `42601` | the aggregates it wraps, written out |
| `SET VARIABLE` | `42601` | — nothing in the read path would read one back, so accepting it would be a fake `OK` |

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

## 4 — Priorities

Ranked for the two things an analytical distributed database is judged on: **stability** — a
query either answers correctly or fails visibly, and the same query answers the same way twice
— and **usability** — a driver, a BI tool and an ORM work without the client knowing which
engine is underneath. Tier order is by consequence; within a tier, by cost.

### ✅ **CLOSED** - Tier 1 — a wrong answer the client cannot detect

Highest priority by definition: everything else in this document is visible to the client.

A closed row keeps its number and its place here, marked ✅ with a pointer to
[`gap-closure-status.md`](gap-closure-status.md), until the next revision of this document
retires it from § 2.7 and the totals. Numbering is continuous across all four tiers, so
deleting a row would renumber every tier below it.

| # | Gap | Why it matters here | Fix lives in |
|---:|---|---|---|
| 1 | ✅ **CLOSED** — `1.0::float8 / 0` → `inf` (§ 2.7 row 1) now raises `22012` | A poison value flowed into every aggregate above it, so one bad row silently corrupted a whole report. The integer and `numeric` forms already raised `22012`, so the divergence was also *inconsistent* across types — the type of a literal decided whether a client was told about a bad divisor. | VaireDB — an E2 rewrite of float division, the way `^` and `SIMILAR TO` were translated. Shipped: `vairedb_common::float_div` raises where PostgreSQL raises and `pgwire_handler::pg_float_division` rewrites the operator on the logical plan, so the advertised type does not move. NaN and NULL dividends still follow PostgreSQL rather than raising. Details in [`gap-closure-status.md`](gap-closure-status.md) |
| 2 | ✅ **CLOSED** — `NOT IN` over an aggregate left side or a correlated subquery (§ 2.7 row 7) no longer returns the extra row | `NOT IN` is ordinary analytical SQL, the error is one extra row, and the shapes that are wrong look identical to the shapes that are right. Which is why the resolution takes both routes this row named: the aggregate shape **answers**, and the correlated one is **refused**. | VaireDB. Shipped: `vairedb_common::not_in` evaluates PostgreSQL's three-valued rule over an `array_agg` of the candidates, so `HAVING MAX(k) NOT IN (q)` answers without a join at all; `pgwire_handler::pg_not_in_nulls` refuses, `0A000`, every remaining shape a NULL can reach — a correlated `q`, a wildcard `q`, a `NOT IN` inside a `CASE` — naming `NOT EXISTS` **and** the `IS NULL` test it needs, since `NOT EXISTS` is two-valued. A `NOT IN` over `NOT NULL` columns is still answered, correlated or not. The correlated shape *answering* remains open — a refusal, so it belongs with Tier 3's at the next revision — and needs DataFusion (`LATERAL`, or an outer reference that survives plan serialization). Details in [`gap-closure-status.md`](gap-closure-status.md) |
| 3 | ✅ **CLOSED** — `0b101` → `5`, `0x1F` → `31`, `arr[-1]` → NULL, `'\x…'::bytea` → its bytes (§ 2.7 rows 2–5) | Four literal and subscript forms where DataFusion and DuckDB agreed with each other and not with PostgreSQL — consistency is exactly why they were easy to miss, and why no split-brain probe caught them. Two were understated by the census: rows 2 and 3 are a *tokenizer* defect rather than a divergence, and rows 4 and 5 were wrong on the **write** path too. | VaireDB. Shipped: `pgwire_handler::pg_integer_literals` respells every `0b`/`0o`/`0x` literal in the statement **text**, before either parse, since after parsing there is no literal left to fix; `pgwire_handler::pg_subscripts` clamps the index and the bounds *inside* the brackets, one rule both engines execute, called from the read path and the write path's dialect pass alike; `vairedb_common::bytea_in` is PostgreSQL's `byteain` with both of its SQLSTATEs (`22P02` and `22023`), reached on read as a UDF registered on both sides of the Ballista wire and on write as `unhex('…')`. A `::bytea` over anything but a literal, `NULL` or a parameter is refused `0A000` at parse time, naming both ways out. `column_labels` moved with the rewrites, so a driver's result map still keys on PostgreSQL's names. Four residues stay open with `#[ignore]`d tests — an *implicit* string→`bytea` column coercion, a `CREATE TABLE DEFAULT`, text-format `bytea` output, and `length()` over `Binary`. Details in [`gap-closure-status.md`](gap-closure-status.md) |
| 4 | ✅ **CLOSED** — `nth_value(x, 0)` (§ 2.7 row 6) now raises `22016` instead of answering NULL | Narrowest row here, and one of the worst-shaped: NULL is this function's *legitimate* answer for an offset past the end of the frame, so a column of NULLs could not be told apart from a query with the wrong offset in it. | VaireDB. Shipped: `vairedb_common::nth_value` registers a `WindowUDF` under DataFusion's **own** name that refuses a literal integer offset of exactly zero with PostgreSQL's wording and delegates everything else — evaluator, declared field, coercion, `reverse_expr` — to DataFusion's `nth_value`, so no answer and no result OID moves. The check sits in the per-partition evaluator rather than in the planner, which is what keeps `SELECT nth_value(n, 0) OVER () FROM <empty>` returning no rows and no error the way PostgreSQL does; `error_enrichment` gains a third named exception so a refusal raised on a core node keeps `22016` across the scheduler boundary (§ 1.3). Because it shadows, a registration missing on any executor silently restores the wrong answer — hence one on the coordinator and one in `build_session_state`. The negative offset stays a § 5 superset, asserted. Details in [`gap-closure-status.md`](gap-closure-status.md) |
| 5 | ✅ **CLOSED** — `FULL JOIN … USING (c)` reached by an explicit qualifier (§ 2.7 row 8) now reports each side's own key | Called here "the one Tier 1 row whose honest resolution may be documentation", on the grounds that PostgreSQL's three names do not fit a plan schema's two fields. They do not — but the third name was missing from the *statement*, and a statement is what a rewrite gets to choose. It also mattered more than "rare and expert": `WHERE l.c > 2` filtered on the merged value, so a predicate naming one side changed which **rows** came back — and row 17 below is what pushes a client into writing it. | VaireDB. Shipped: `pgwire_handler::pg_using_join_qualifiers` respells, on the AST before planning, a full or right `USING` join whose key that query block reaches through a qualifier into `ON <l>.<c> = <r>.<c>` plus `COALESCE(<l>.<c>, <r>.<c>)` for every unqualified reference — three names for three values, with the rows and the merged column's label unmoved. `pg_using_join_merge` is unchanged and now reached only by the statements it answers correctly (`SELECT c`, `SELECT *`, `GROUP BY c`), so the two divide the surface by which of them can be right. Two shapes are refused `0A000`, each naming the spelling that answers: a wildcard beside a qualified key, and the merged key beside the same key per side under **one** name — the last, irreducible piece of three-names-in-two-fields, since PostgreSQL answers that one with two result columns both called `c` and a `DFSchema` holds neither two fields of a name nor an unqualified `c` beside a qualified `l.c`. `NATURAL` is left as it was (its key set is a catalog fact, not a statement one) and pinned. The write path needed nothing: DuckDB 1.5.5 already answers as PostgreSQL does. Details in [`gap-closure-status.md`](gap-closure-status.md) |

### Tier 2 — errors and types a client programs against

| # | Gap | Why it matters here | Fix lives in |
|---:|---|---|---|
| 6 | **Carry a structured error code across the Ballista scheduler boundary** (§ 1.3) | The single highest-leverage item in this document: it is the only fix that closes a whole *class* rather than a row. Today any failure raised on an executor arrives as text and lands `XX000` — `internal_error` — so a client that retries on `XX000` retries a syntax error, and a client that reports it to a user reports a bug in VaireDB. It also removes the divide-by-zero text carve-out and closes the wrong classes on the nested aggregate, illegal window placement and `percentile_disc(ARRAY[…])` rows in one move. | Ballista's `FailedTask` proto, then VaireDB |
| 7 | **Result-type OIDs** — `ntile` `int4` (§ 2.4), `regr_count` `int8`, the `stddev`/`var` family and `percentile_cont` `numeric` (§ 2.3), `UInt64` `bigint` (§ 2.2) | A driver binds its receive buffer from the advertised OID. Wrong-but-decodable (`int8` for `int4`) is a silent width change; wrong category (`numeric` for `int8`) has already made a conforming driver fail its decode outright. `ntile` and `percentile_cont` are VaireDB's own and cheapest; the statistics family is DataFusion's. | VaireDB, then DataFusion |
| 8 | `avg(int4)`/`avg(int8)` carries 10 decimal places where PostgreSQL prints 16 (§ 2.3) | The value is exact and the type is right, so this is a rendering difference — but it is a rendering difference in the single most-used analytical aggregate, and it shows up in every diff against a PostgreSQL baseline. | VaireDB — the accumulator scale in `pg_aggregate_widening` |
| 9 | Unaliased expression columns labelled with the plan rendering, not `?column?` (§ 2.5) | A client that keys on column names sees a name no PostgreSQL client would produce, and a rewritten expression makes it worse rather than better (a ~400-character header). | VaireDB — `column_labels.rs`, which already labels function columns |

### Tier 3 — the analytical SQL surface

| # | Gap | Why it matters here | Fix lives in |
|---:|---|---|---|
| 10 | `CAST(… AS JSON)` and `CAST(… AS UUID)` (§ 2.5) | The largest missing *feature* surface behind one planner type-name gap: it blocks the entire `jsonb` operator family (`->`, `->>`, `#>`, `#>>`, `@?`) and `json_agg`/`jsonb_agg`. Both types already work as column types. | VaireDB / DataFusion planner |
| 11 | `INTERSECT ALL`, `EXCEPT ALL` (§ 2.6) | Multiplicity is what an analytical client goes on to aggregate, and there is no rewrite that preserves it. | DataFusion |
| 12 | `x <op> ANY \| ALL (subquery)` beyond `= ANY` / `<> ALL` (§ 2.6) | Ordinary PostgreSQL. Blocked on a duplicate `mark` field in plan serialization, so the fix is upstream and narrow. | DataFusion / datafusion-proto |
| 13 | `EXISTS` / `IN (subquery)` in the **SELECT list** (§ 2.5) | The `WHERE` spelling works, so this is a projection-shaped hole a reporting query hits when it wants a boolean column. datafusion-proto cannot encode a subquery expression. | datafusion-proto |
| 14 | The window clause surface: inline window fn in the outer `ORDER BY` (legal PG, rejected), `OVER (w …)` named-window inheritance, `IGNORE NULLS`, `FILTER … OVER`, `EXCLUDE` | Window functions are the core of analytical SQL, and these are all currently **loud** — so this is usability, not stability. Ordered by cost: the outer `ORDER BY` and the named-window AST expansion are VaireDB's; `IGNORE NULLS` is one line upstream; `FILTER … OVER` needs a proto field and both codecs; `EXCLUDE` needs the parser taught first. | VaireDB, then DataFusion / Ballista / sqlparser |
| 15 | The `WITHIN GROUP` family: `mode()`, hypothetical-set `rank`/`dense_rank`/`percent_rank`/`cume_dist`, and the percentile **array-of-fractions** overload (§ 2.3) | Standard PostgreSQL ordered-set aggregates. Each needs a UDAF; the array overload would close both percentile functions' inconsistent refusal at once, and refusing it in the coordinator would at least make the class consistent today. | VaireDB |
| 16 | `count(DISTINCT a, b)` comma form and `GROUPING SETS (())` alone (§ 2.3) | Both have exact workarounds (`count(DISTINCT (a,b))`, a non-empty set alongside) and both are one narrow fix. | DataFusion / VaireDB |
| 17 | Unqualified `USING` key in a `WHERE` clause (§ 2.6) | PostgreSQL does not refuse this at all, and every other clause resolves it, so it reads as arbitrary to a client. The qualified spelling works, and since Tier 1 row 5 closed it answers PostgreSQL's rows rather than filtering on the merged value — so the workaround is now a correct one. | DataFusion name resolution |
| 18 | Function-level coverage — `pg_typeof()` first, then the format and datetime families | `datafusion-pg-functions` 0.1.0 populates only its `math` category (~18 UDFs), so the other families are simply absent. `pg_typeof()` is how a client asks what VaireDB thinks a column is. Every addition must be AST-rewritten or registered on every executor (§ 1.1). | VaireDB — `vairedb_common::pg_udf`, the seam that already registers the `pg_catalog` scalar set on every context that plans or executes |

### Tier 4 — write path, introspection and cosmetics

| # | Gap | Why it matters here | Fix lives in |
|---:|---|---|---|
| 19 | `ALTER SCHEMA` and `ALTER TABLE … SET SCHEMA` (§ 2.1) | The one open write-path statement gap. Parser plus routing. | sqlparser, then VaireDB |
| 20 | `ALTER TABLE ADD … CHECK` and `DROP CONSTRAINT`, and the index dependency that narrows `ALTER TABLE` to `ADD COLUMN` and defaults (§ 2.1) | Schema evolution is what an analytical table needs most after loading, and today a table with an index is nearly frozen. | The shard engine |
| 21 | Session-scoped `pg_settings` | `SHOW` works because it reads the session registry, but `SELECT … FROM pg_settings` cannot reflect session parameters — the catalog table has no per-session context to read from. Introspection tools read the table, not `SHOW`. | VaireDB |
| 22 | `STRUCT` DDL re-render → a PostgreSQL composite; `Decimal256`; `NUMERIC` above 29 digits in binary format (§ 2.2) | Type-layer completeness. Each is blocked on a named upstream limitation. | sqlparser, arrow-pg |
| 23 | Remaining cosmetics: `JSON`/`UUID`/`ENUM`/`CHAR` OIDs, `ENUM` declaration-order sorting, `VARCHAR(n)` typmod in `\d`, duplicate unaliased labels, `format_type`'s missing `(Utf8, …)` signature arm | All correct values under a wrong advertised type or name. Real for introspection tooling, invisible to a query. | VaireDB |
| 24 | The three push-down narrowings (§ 2.5) | Pure throughput, no answer changes. **Text ordering is cheapest** — it needs only DuckDB's `default_collation` pinned and inspected. | VaireDB |

### Not on this list, deliberately

Cross-shard atomicity (2PC) and the role model (TLS, users, groups) are roadmap lines rather
than gap rows. Everything in § 3 is a decision. `SET datafusion.*` returning `42704` is
correct behaviour. And § 5 must not be "fixed".

---

## 5 — Supersets that must NOT be "fixed"

VaireDB accepts these and PostgreSQL does not. Narrowing to PG parity would be a regression
against DataFusion and DuckDB:

- `count(DISTINCT x) OVER (…)` — PostgreSQL rejects `DISTINCT` in a window aggregate (`42P20`).
- `QUALIFY` — DuckDB/Snowflake syntax PostgreSQL does not have.
- Negative `nth_value` offsets.
- `INTERVAL 1 DAY` unquoted — DuckDB-only; PostgreSQL rejects it.
- `count(DISTINCT (a,b))` — plans as `count(DISTINCT struct(a,b))` and is correct.
- `DISTINCT ON (col)` is the reverse case: a PostgreSQL extension, and it works.

---

## 6 — Executable counterpart

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
| `sql_command_unsupported.rs` | Every 🚫 and ❌ statement row, plus `COPY`'s streaming pair |
| `sql_join_gaps.rs` | § 2.6 and § 2.7 rows 7–8 |
| `data_types_round_trips.rs`, `data_types_dialect_gaps.rs` | § 2.2, both wire formats |
| `anonymization.rs` | The refusals in § 2.4 and § 2.6, beside the reads that must keep answering |
| `shard_key_hazards.rs`, `identifier_rewrite.rs`, `concurrency.rs` | The sharding rules in § 3.4 |

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
