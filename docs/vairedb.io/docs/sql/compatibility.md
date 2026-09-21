# PostgreSQL Compatibility

VaireDB speaks the PostgreSQL wire protocol, and **PostgreSQL is the contract**: where VaireDB
and PostgreSQL disagree about what a statement means, PostgreSQL is right and VaireDB has a gap.
DataFusion (read path) and DuckDB (write path, one instance per shard) are implementation
layers, not part of the promise — a form only they have is out of scope.

This page is the public rendering of the project's gap analysis: what a PostgreSQL client
**cannot** fully do today, axis by axis. Anything not listed here works and agrees with
PostgreSQL, on the six axes below — with [two caveats](#two-caveats) that are worth reading
before you rely on that sentence.

!!! info "The full record"
    The census this page summarizes lives in the repository, one row per divergence with its
    cause and its SQLSTATE:
    [`docs/specs/gap-analysis.md`](https://github.com/matteobovetti/vairedb/blob/main/docs/specs/gap-analysis.md).

## How to read the status labels

| Status | What it means for your client |
|---|---|
| 🟡 **Partial** | The statement runs and the rows are PostgreSQL's, but something around them is not: an advertised column type, a decoration that is refused rather than applied, or an error under the wrong SQLSTATE. |
| ⛔ **Silently wrong** | Parses, runs, returns a plausible value that is not PostgreSQL's — no error, no warning. The dangerous class. **There are none.** |
| ❌ **Refused** | Rejected loudly with a SQLSTATE, and the message names the spelling that does work where one exists. |
| 🚫 **Not planned** | Refused on purpose, with a reason: it has no meaning in a shared-nothing cluster, or it is a DuckDB-only form PostgreSQL does not have. |

!!! success "Nothing returns a wrong answer quietly"
    Every construct that used to — eight of them — is closed. On the six axes below, a query
    either agrees with PostgreSQL or fails visibly. That is the property to design against: you
    do not need to double-check results, you need to handle errors.

## Status by axis

Counted as rows of the census, where one row can cover several spellings, so this compares axes
rather than individual constructs.

| Axis | 🟡 Partial | ⛔ Silently wrong | ❌ Refused | 🚫 Not planned |
|---|---:|---:|---:|---:|
| [Statements](#statements) | 15 | — | 9 | 8 |
| [Data types](#data-types) | 6 | — | — | 8 |
| [Aggregate functions](#aggregate-functions) | 2 | — | 3 | — |
| [Window functions](#window-functions) | 1 | — | 5 | 1 |
| [Operators, literals and casts](#operators-literals-and-casts) | 12 | — | 19 | — |
| [Joins and set operations](#joins-and-set-operations) | 2 | — | 4 | — |
| **Total** | **38** | **—** | **40** | **17** |

For what *is* supported on each axis, see the
[supported SQL surface](index.md#the-supported-sql-surface).

## Statements

Every statement is either routed and correct, or refused by name with a SQLSTATE. The 🟡 rows
are not half-built statements: the statement works, and a decoration of it that sharding cannot
honour is refused rather than quietly under-applied.

| Statement | What is refused |
|---|---|
| `ALTER TABLE` | Dropping the shard key; touching a pseudonymized column; retyping or dropping a column a constraint or index covers; every `ADD`/`DROP CONSTRAINT` form but `UNIQUE (<shard key>)`; `RENAME TO` while an index exists. `RENAME TO` / `SET SCHEMA` must be the only action in the statement. |
| `DROP` | Several names in one statement; `CASCADE` / `RESTRICT`. |
| `TRUNCATE` | Several tables in one statement; `CASCADE` / `RESTRICT`; `RESTART IDENTITY`. |
| `CREATE INDEX` / `DROP INDEX` | `UNIQUE` off the shard key; a `UNIQUE` index narrowed by `WHERE` or `NULLS NOT DISTINCT`; an unnamed index; an index over an expression. |
| `CREATE` / `ALTER` / `DROP SCHEMA` | `CASCADE`, `AUTHORIZATION`; dropping `public`; renaming a schema that still holds a relation. `sales.t` and `sales_t` cannot coexist — the qualifier folds into the physical shard-table name. |
| `CREATE VIEW` / `ALTER VIEW` | `MATERIALIZED`; the dialect decorations (`TEMPORARY`, `WITH (…)`, a typed column); a view over a metadata schema; a cyclic definition; `ALTER VIEW … RENAME TO`. Views are read-only. |
| `MERGE INTO` | Any source but a co-located table sharded by the join column or an inline `VALUES` list; an `ON` that does not pin the shard key; `UPDATE SET <shard key>`; `RETURNING`; per-action `WHERE`; a merge into a table with pseudonymized columns. |
| `COPY` | `PROGRAM`; any format but CSV and PARQUET, and the format must be stated (PostgreSQL's default is `TEXT`); any option but `HEADER` / `DELIMITER` / `QUOTE`, and those only for CSV — a Parquet file carries its own names and types. `FROM STDIN` / `TO STDOUT` are CSV-only: a Parquet footer is written last and read first, so it is not a row-at-a-time stream. |
| Transaction blocks | Inside a block: `UPDATE` / `DELETE`, DDL, and reads of a table the block has written. Writes buffer in the coordinator and ship at `COMMIT`. Isolation levels are accepted and ignored. |
| `SET` / `SHOW` / `RESET` | `search_path` by name; `SET LOCAL`, `SET ROLE`, `SET SESSION AUTHORIZATION`, `SET TRANSACTION`, `SET NAMES`; `SHOW TABLES` and that family. A value outside the set the coordinator's behaviour already matches is refused **by value**, naming what VaireDB does instead. |
| `EXPLAIN` | `EXPLAIN` of a write — a write is planned by each shard's engine, so there is no coordinator plan; every option but `ANALYZE` / `VERBOSE`. |
| `DESCRIBE` | `FORMATTED` / `EXTENDED` — they promise storage detail the coordinator does not hold. |

❌ **Refused outright**, because they are single-node DuckDB utilities or catalog features with
no cluster meaning: `ATTACH` / `DETACH`, `CHECKPOINT`, `EXPORT` / `IMPORT DATABASE` (use
`COPY`), `INSTALL` / `LOAD`, `CREATE SECRET` (VaireDB has its own path for anonymization
secrets), `USE`, `CALL`, `COMMENT ON`, `CREATE MACRO`.

## Data types

**No type gap refuses a value.** Every value on this axis round-trips faithfully; what diverges
is the type VaireDB *advertises* for it — with one exception, the last row.

| Item | Divergence |
|---|---|
| 🟡 `UBIGINT` column | Advertised `numeric`, not `bigint` — PostgreSQL has no unsigned integer type. |
| 🟡 `STRUCT` column | Advertised `text`. |
| 🟡 `ENUM` column | Advertised `text`. A PostgreSQL enum has no fixed type OID to advertise. `UUID`, `JSON` and `CHAR`/`VARCHAR` advertise their own. |
| 🟡 `ENUM` ordering | Sorts by string value, not declaration order. |
| 🟡 `VARCHAR(n)` / `CHAR(n)` length | The type OID is right, but the length is not on the wire, so `psql`'s `\d` prints `character varying` without its `(64)`. |
| 🟡 `VARCHAR(n)` / `CHAR(n)` not enforced | A value **longer than the declared length is stored and returned**, where PostgreSQL raises `22001`. Treat `n` as documentation and validate in the application. |

Any declared type VaireDB does not recognize degrades to `text` with its values intact, which is
what keeps it forward-compatible with types DuckDB adds.

🚫 Eight types are **refused by name at `CREATE TABLE`**, `ADD COLUMN` and `ALTER COLUMN TYPE`,
each naming what to write instead — they used to be accepted and then fail or lie on read:

| Refused | Use instead |
|---|---|
| `HUGEINT` / `INT128`, `UHUGEINT` / `UINT128` | `BIGINT`, or `DECIMAL(38,0)` |
| `BIT` / `BITSTRING` / `VARBIT` / `BIT VARYING` | `BOOLEAN`, or `VARCHAR` for the `'0'`/`'1'` spelling |
| `BIGNUM` / `VARINT` | `DECIMAL(38,s)` |
| `UNION` | one nullable column per member, or `JSON` |
| `VARIANT` | `JSON` |
| `MAP` | `JSON`, or a pair of array columns |
| `TIMETZ` / `TIME WITH TIME ZONE` | `TIMESTAMPTZ`, or `TIME` with the offset in its own column |

## Aggregate functions

The strongest axis, for one structural reason: **DuckDB never computes an aggregate.** Every
aggregate is evaluated by DataFusion above the shards, and the distributed merge cannot emit a
partial result as a finished one.

| Item | Divergence |
|---|---|
| 🟡 `avg`, `stddev*`, `var*`, `variance` over exact integers | Value, type and PostgreSQL's sixteen decimal places, but the **scale is constant** where PostgreSQL picks one per value: the average of `1` and `2` prints `1.5000000000000000`. A variance above 10²² raises `22003` instead of rounding. |
| 🟡 `json_agg` / `jsonb_agg` of a bare `JSONB` column | Quotes the document as a string instead of embedding it. Write `json_agg(v::json)` — the cast embeds. |
| ❌ `rank(a, b) WITHIN GROUP (ORDER BY x, y)` | The multi-column hypothetical-set form. The one-column form of all five ordered-set aggregates answers, as do both percentiles' fraction and array overloads. |
| ❌ `GROUPING SETS ((), ())` | The empty set *repeated*. `GROUPING SETS (())` alone answers. |
| ❌ `xmlagg` | No XML type. |

## Window functions

All 11 PostgreSQL window functions are present, and the frame engine is correct: all five units,
peer groups, and integer, float and `INTERVAL` offsets.

| Item | Divergence |
|---|---|
| 🟡 `ntile($1)`, `lag(x,$1)`, `nth_value(x,$1)` | Correct provided your client declares a parameter type OID. |
| ❌ `FILTER (WHERE …) OVER (…)` on a **collecting** aggregate — `array_agg`, `string_agg` | Every null-skipping aggregate answers: `count` (including `count(*)`), `sum`, `avg`, `min`, `max`, and the boolean, bit, `stddev` and `variance` families. A plain (non-windowed) `FILTER` is unaffected. Use a subquery for the collecting pair. |
| ❌ `EXCLUDE {CURRENT ROW \| GROUP \| TIES \| NO OTHERS}` | Not parseable yet — the one genuinely missing feature on this axis. |
| ❌ `rank` / `row_number` / `dense_rank` / `percent_rank` / `cume_dist` `OVER (ORDER BY <pseudonymized col>)` | A window *is* an ordering, and a digest has no plaintext order on the server. |
| ❌ `first_value` / `last_value` / `nth_value` / `lag` / `lead` over a pseudonymized column ordered by it | Same reason — it would return the digest of the wrong row. |
| 🚫 `IGNORE NULLS` | PostgreSQL does not implement the standard's null-treatment option and always behaves as `RESPECT NULLS`, which is what VaireDB does. `RESPECT NULLS` is accepted. |

!!! tip "`PARTITION BY` over a pseudonymized column is sound"
    HMAC is deterministic and injective, so digest equality *is* plaintext equality. Grouping and
    equality over a pseudonymized column are exact; only order and value are destroyed. See
    [Column Pseudonymization](pseudonymization.md).

## Operators, literals and casts

The widest axis. The read path's expression surface and type layer are closed, including
predicate and `LIMIT` push-down into the shards.

| Item | Divergence |
|---|---|
| 🟡 An integer literal | Typed `bigint` where PostgreSQL types it `integer`, so `SELECT 1` is described as `bigint`. The **value** is PostgreSQL's, and a cast or a column reference carries its own type. |
| 🟡 `1_000` | Answers `1000`, typed `numeric` where PostgreSQL types it `integer`. |
| 🟡 `TIMESTAMP '…'` literal | A *literal* is bounded to 1677–2262 by nanosecond typing. A `TIMESTAMP` **column** is microseconds and unaffected. |
| 🟡 `TIMESTAMPTZ '…+02'` literal | Normalized to UTC correctly, but the offset is not shown back. |
| 🟡 `now()` / `current_timestamp` | The instant is right and read **once, on the coordinator**, but carries no time zone where PostgreSQL's is `timestamptz`. `transaction_timestamp()` is per-statement, because a transaction block buffers. `clock_timestamp()` and `timeofday()` are per-call, as PostgreSQL's are. |
| 🟡 `::json` / `::jsonb` and `->`, `->>`, `#>`, `#>>` | Casts validate and all four accessors answer, including negative indices and the `'{a,b}'` path spelling. Two residues: `::jsonb` does not normalize key order or whitespace, and a missing key is `NULL` where PostgreSQL's `json` accessors raise. |
| 🟡 `'\xDEADBEEF'::bytea` | The cast is PostgreSQL's, with both its SQLSTATEs, over a literal, `NULL` or a parameter. An implicit string→`bytea` coercion, a `DEFAULT`, and text-format `bytea` *output* are residues. |
| 🟡 `COLLATE` | `C`, `POSIX`, `ucs_basic` and `default` name byte order, which is what VaireDB does, so they are accepted. **Every other collation is refused by name** rather than parsed and ignored. |
| 🟡 `\|\|` on arrays | The **string** form is correct on both paths; the array overloads differ between the two engines. |
| 🟡 `'{1,2,3}'::INT[]` | PostgreSQL's array text form works on read; DuckDB renders lists as `[1, 2, 3]` on write. |
| 🟡 `X'DEADBEEF'` | Answers a binary value; PostgreSQL gives `bit(32)`. |
| 🟡 `<=>` | MySQL null-safe equality — no PostgreSQL equivalent, and DuckDB reads it as vector distance. |

❌ **Refused**, grouped by cause — the message names the spelling that answers where one exists:

- **No parser support yet**: `**`, `//` / `DIV`, `CAST(x AS T ARRAY)`, `{'a': 1}` / `MAP {'a': 1}`,
  `U&'\0041'`, `N'foo'`, `B'1010'`, `INTERVAL '1-2' YEAR TO MONTH`.
- **DuckDB-only, so out of scope**: `GLOB` / `~~~`, `str[n]` and `str[a:b]` string subscripting.
- **Meaning cannot be reproduced exactly, so it is refused rather than approximated**: `@?`
  (jsonpath is a second language — the four accessors above answer), `@@` and full-text search,
  `'abcde'::VARCHAR(2)` (the length is enforced nowhere, so `substr()` is named instead),
  `LIKE ANY (array)`, a **computed** `SIMILAR TO` pattern or `LIKE … ESCAPE '<non-backslash>'`
  (a literal pattern is translated before evaluation; a runtime one cannot be),
  `to_number(text, text)` (`to_char` is unaffected), `struct.field` (write `struct['field']`),
  `MAP(['a'],[1])`, and correlated `EXISTS` / `IN (subquery)` in the select list beyond an
  equality correlation.

!!! note "Column labels"
    An unaliased expression column carries the name PostgreSQL derives, or `?column?` where
    PostgreSQL has none, and a label that **repeats** is sent as often as PostgreSQL sends it —
    `SELECT sum(a), sum(b)` is two columns both called `sum`.

## Joins and set operations

Every row on this axis was measured in **both** layouts the shard map produces, co-located and
shuffle, and none differs between them: the shard map may change the plan, never the answer.

| Item | Divergence |
|---|---|
| 🟡 A join on a **pseudonymized** column | An equi-join relates two tables correctly, including under a `GROUP BY` above it. What the digest does not preserve is refused rather than answered: an ordering join (`a.email < b.email`), an `ORDER BY` on the column, and a comparison against **plaintext**. |
| 🟡 An untyped literal branch of a set operation | `… UNION ALL SELECT '9'` comes back as `text` where PostgreSQL resolves to the typed branch's `integer`. The rows are PostgreSQL's; an `ORDER BY` over the column sorts lexicographically. |
| ❌ `x <op> ANY \| SOME \| ALL (subquery)` outside a predicate `AND` chain | Every operator answers in a `WHERE`, `HAVING`, `QUALIFY` or join `ON`, as an operand of a top-level `AND`. A select list, `ORDER BY`, `NOT`, `CASE` and `OR` are refused. `= ANY`, `<> ALL`, `IN` and `EXISTS` are untouched. |
| ❌ `WHERE k NOT IN (<correlated subquery>)` | Write `NOT EXISTS` plus the `IS NULL` test it needs — `NOT EXISTS` is two-valued. `NOT IN` over `NOT NULL` columns answers, correlated or not. |
| ❌ Two `USING` shapes | A wildcard beside a qualified join key, and the merged key beside the same key per side under one name — PostgreSQL answers with two result columns of the same name, which a DataFusion schema cannot hold. |
| ❌ A bare `USING` / `NATURAL` key in `WHERE` over a side the rewrite cannot read | Three shapes: a left side that is itself a join, two joins in one query block sharing a key name, and a key reached from a nested block. The qualified spelling answers. |

## Supersets: things VaireDB accepts and PostgreSQL does not

These are deliberate, and will not be narrowed to PostgreSQL parity:

- `QUALIFY` — DuckDB/Snowflake syntax PostgreSQL does not have.
- `count(DISTINCT x) OVER (…)` — PostgreSQL rejects `DISTINCT` in a window aggregate.
- `count(DISTINCT (a,b))`, and negative `nth_value` offsets.
- `INTERVAL 1 DAY` unquoted.
- `DISTINCT ON (col)` is the reverse case — a PostgreSQL extension, and it works.

## Standing limitations

Properties of the design rather than backlog, and the cause of most of the 🟡 statement rows:

1. **No cross-shard atomicity.** No two-phase commit. A transaction spanning more than one node
   set is refused at `COMMIT` unless `allow_cross_shard_transactions` is set; a partial
   multi-shard write reports `40003` with how much was written.
2. **Uniqueness only over the shard key.** `UNIQUE` and `PRIMARY KEY` are enforceable only when
   they include the shard key. `FOREIGN KEY` is never enforced; `CHECK` always is.
3. **The shard key is immutable.** `UPDATE` of the shard-key column is refused; row relocation
   does not exist.
4. **A transaction block buffers.** Only statements the coordinator can answer truthfully
   without running them are allowed inside one.
5. **No role model, therefore no privilege checks.** Most visibly, `COPY` reads and writes any
   path the coordinator process can.
6. **Pseudonymization is one-way and write-path only.** Order and value are unrecoverable;
   equality and grouping are exact.
7. **Text ordering is byte order.**
8. **A statement is either correct on every shard or refused.** Nothing is applied partially: a
   decoration that only affects performance is stripped, and anything that changes results is
   refused **by name**.

Also not planned, each refused with its reason: sequences and `SERIAL`, `SET search_path`,
user-defined types and domains, `ANALYZE` and `VACUUM`, and the DuckDB-only `PIVOT`, `UNPIVOT`,
`SUMMARIZE` and `SET VARIABLE`. See the [roadmap](../roadmap.md#known-limitations-v01).

## Two caveats

!!! warning "Scalar functions have not been measured"
    The six axes above do **not** include PostgreSQL's scalar functions — the string, math,
    pattern, conditional, array and datetime families. They have never been run against the
    PostgreSQL oracle the way the six axes were, so "not listed here" is weaker evidence for
    them than for anything else on this page. A function that does not exist answers `42883` and
    is loud; a function both engines have under PostgreSQL's name but evaluate by a different
    rule would not be. That measurement pass has not run yet.

!!! warning "`VARCHAR(n)` is not enforced"
    A value longer than the declared length is stored and returned, where PostgreSQL raises
    `22001`. It is the one divergence on this page that is neither fixed nor deliberate — the
    check does not exist yet. Validate lengths in the application until it does.

## How this was measured

Against a live 5-node cluster (1 coordinator, 1 scheduler, 3 core nodes), byte-diffed against a
**PostgreSQL 16.15** oracle, with a **DuckDB 1.5.5** CLI as a second oracle for forms PostgreSQL
cannot parse, and result types and column labels read from the extended protocol's `Describe`.

Every entry on this page has a test behind it, and every refusal is probed for over-reach: each
test asserts, alongside the refusal, that the neighbouring form which loses no clause still
answers. That is what keeps a refusal honestly scoped rather than a blanket rejection.
