# SQL Guide

VaireDB speaks the **PostgreSQL wire protocol**, so you interact with it using
standard SQL through any PostgreSQL client. SQL submitted to the coordinator is
parsed by DataFusion, translated to DuckDB's dialect, and dispatched to the
core nodes that own the relevant shards.

<div class="grid cards" markdown>

-   [:material-connection: __Connecting__](connecting.md)

    Connect with `psql` and other PostgreSQL clients.

-   [:material-table-cog: __Tables & Schema__](tables.md)

    `CREATE TABLE` with sharding, `ALTER TABLE`, `DROP TABLE`, constraints, indexes,
    and catalog introspection.

-   [:material-table-search: __Querying Data__](querying.md)

    `INSERT`, `MERGE INTO` and `SELECT` against sharded tables, plus bulk load
    and export with `COPY`.

-   [:material-shield-lock: __Column Pseudonymization__](pseudonymization.md)

    Declare columns to be HMAC-SHA256 hashed in the coordinator for compliance.

-   [:material-script-text: __Worked Example__](examples.md)

    An end-to-end session from empty cluster to query results.

-   [:material-check-decagram: __PostgreSQL Compatibility__](compatibility.md)

    Per-axis status of what diverges from PostgreSQL, and what to write instead.

</div>

!!! note "Dialect"
    Table DDL accepts a `WITH (...)` clause for sharding options (`shards`,
    `replication_factor`, `shard_by`). Statement-level SQL is
    PostgreSQL-compatible and translated to DuckDB internally.

---

## The supported SQL surface

What follows is the SQL that **works and agrees with PostgreSQL**, grouped the way the project's
compatibility census groups it. Each section ends with the divergences on that axis, which are
listed in full on the [PostgreSQL Compatibility](compatibility.md) page.

Two things make this list readable as a promise rather than a sales pitch. Every construct here
was byte-diffed against a real PostgreSQL 16 oracle, and **nothing on these axes returns a wrong
answer quietly** — a construct either matches PostgreSQL or is refused with a SQLSTATE. So the
failure mode to handle is an error, not a plausible-looking number.

!!! warning "One gap in the coverage itself"
    PostgreSQL's **scalar functions** — the string, math, pattern, conditional, array and
    datetime families — are not one of the axes below and have not been measured against the
    oracle. They largely work, but "not listed as a gap" is weaker evidence for a scalar function
    than for anything else on this page. See [the caveats](compatibility.md#two-caveats).

### Statements

| Area | Supported |
|---|---|
| Queries | `SELECT` with `WHERE`, `GROUP BY`, `HAVING`, `ORDER BY`, `LIMIT` / `OFFSET`, `DISTINCT`, `DISTINCT ON`, `QUALIFY`, common table expressions, subqueries, derived tables and `LATERAL`. Predicates and `LIMIT` are pushed down into the shards. |
| Grouping | `GROUP BY`, `GROUPING SETS`, `ROLLUP`, `CUBE`, `grouping()`, `HAVING`. |
| Writes | `INSERT` (single and multi-row `VALUES`, `INSERT … SELECT`), `UPDATE`, `DELETE`, `MERGE INTO` against a co-located source or a `VALUES` list. |
| Table DDL | `CREATE TABLE … WITH (shards, replication_factor, shard_by)`, `CREATE TABLE … AS SELECT`, `ALTER TABLE` (add / drop / rename / retype a column, nullability, defaults, `RENAME TO`, `SET SCHEMA`), `DROP TABLE`, `TRUNCATE`. |
| Constraints | `PRIMARY KEY` and `UNIQUE` over the shard key (enforced), `CHECK` (always enforced), `NOT NULL`, `DEFAULT`, `FOREIGN KEY` (recorded, not enforced). |
| Indexes | `CREATE INDEX`, `CREATE UNIQUE INDEX` over the shard key, `DROP INDEX` — one physical index per shard. |
| Schemas & views | `CREATE` / `ALTER` / `DROP SCHEMA`, `CREATE [OR REPLACE] VIEW`, `ALTER VIEW … AS`, `DROP VIEW`. Views are read-only. |
| Bulk data | `COPY … TO` / `COPY … FROM` a CSV or Parquet file on the coordinator, and CSV over the copy protocol (`STDIN` / `STDOUT`, so `\copy`). CSV honours `HEADER`, `DELIMITER` and `QUOTE`; Parquet carries its own names and types and takes no options. |
| Sessions | `BEGIN` / `COMMIT` / `ROLLBACK` / `SAVEPOINT` / `RELEASE`, `SET` / `SHOW` / `RESET` of PostgreSQL runtime parameters. |
| Introspection | `pg_catalog` and `information_schema` reads, `vairedb_catalog` tables, `EXPLAIN`, `EXPLAIN ANALYZE`, `DESCRIBE`. |
| Protocol | Simple and extended query protocols, prepared statements, bound parameters with declared type OIDs, `Describe` of result and parameter types. |

Divergences: [Statements](compatibility.md#statements) — 15 statements refuse a *decoration*
sharding cannot honour (`CASCADE`, dropping the shard key, DDL inside a transaction block), and
9 single-node DuckDB utilities are refused outright.

### Data types

| Category | Declared as |
|---|---|
| Boolean | `BOOLEAN`, `BOOL` |
| Signed integers | `TINYINT` / `INT1`, `SMALLINT` / `INT2`, `INTEGER` / `INT` / `INT4`, `BIGINT` / `INT8` |
| Unsigned integers | `UTINYINT`, `USMALLINT`, `UINTEGER`, `UBIGINT` |
| Exact numeric | `DECIMAL(p,s)` / `NUMERIC(p,s)` — the precision and scale **are** applied |
| Floating point | `REAL` / `FLOAT4`, `DOUBLE PRECISION` / `FLOAT8` |
| Character | `VARCHAR` / `TEXT` / `STRING`, `CHAR` / `CHARACTER`, `VARCHAR(n)` / `CHAR(n)` (see the note below) |
| Binary | `BYTEA`, `BLOB`, `BINARY`, `VARBINARY` |
| Date & time | `DATE`, `TIME`, `TIMESTAMP` (microseconds), `TIMESTAMPTZ`, `TIMESTAMP_S` / `_MS` / `_NS`, `INTERVAL` |
| Structured | `UUID`, `JSON`, `JSONB`, arrays (`INT[]`, `TEXT[]`, …), `STRUCT`, `ENUM` |

Values round-trip faithfully in every row above, and a declared type VaireDB does not recognize
degrades to `text` with its values intact. `UUID`, `JSON`/`JSONB` and the character types
advertise their own PostgreSQL type OID.

!!! danger "`VARCHAR(n)` is not enforced"
    The length is advertised but not applied: a longer value is **stored and returned** where
    PostgreSQL raises `22001`. Validate lengths in the application.

Divergences: [Data types](compatibility.md#data-types) — no type gap refuses a value; six rows
are about the advertised type, and eight DuckDB-specific types are refused at DDL with a
replacement named.

### Aggregate functions

Every aggregate is evaluated above the shards, never by DuckDB, so the distributed merge cannot
turn a partial result into a finished one.

| Family | Functions |
|---|---|
| Basic | `count` (including `count(*)` and `count(DISTINCT …)`), `sum`, `avg`, `min`, `max` |
| Statistical | `stddev`, `stddev_pop`, `stddev_samp`, `var_pop`, `var_samp`, `variance`, `covar_pop`, `covar_samp`, `corr`, the `regr_*` family |
| Boolean & bitwise | `bool_and` / `every`, `bool_or`, `bit_and`, `bit_or` |
| Collecting | `array_agg`, `string_agg`, `json_agg`, `jsonb_agg` |
| Ordered-set | `percentile_cont` and `percentile_disc` — both the single-fraction and array-of-fractions overloads — and `mode() WITHIN GROUP (ORDER BY …)` |
| Hypothetical-set | `rank`, `dense_rank`, `percent_rank`, `cume_dist` `WITHIN GROUP (ORDER BY x)`, single column |
| Modifiers | `DISTINCT` and `FILTER (WHERE …)` on any of the above |

PostgreSQL's alternate spellings are accepted for the three that differ: `variance` and `every`
resolve to `var_samp` and `bool_and`, and `any_value` returns an arbitrary non-null input.

Divergences: [Aggregate functions](compatibility.md#aggregate-functions) — a constant decimal
scale where PostgreSQL picks one per value, `json_agg` of a bare `JSONB` column (write
`json_agg(v::json)`), the multi-column hypothetical-set form, a repeated empty `GROUPING SETS`,
and `xmlagg`.

### Window functions

All **11** PostgreSQL window functions:

| Family | Functions |
|---|---|
| Ranking | `row_number`, `rank`, `dense_rank`, `percent_rank`, `cume_dist`, `ntile` |
| Offset | `lag`, `lead`, `first_value`, `last_value`, `nth_value` |

Plus the clause surface around them:

- `OVER (PARTITION BY … ORDER BY …)`, and any aggregate used as a window function.
- A named `WINDOW` clause, the abbreviated `OVER (w …)` form, and chained window inheritance.
- All five frame units with peer groups, `UNBOUNDED` / `CURRENT ROW` bounds, and integer, float
  and `INTERVAL` offsets.
- `FILTER (WHERE …)` over a null-skipping aggregate, a window function in the outer `ORDER BY`,
  `QUALIFY`, and `count(DISTINCT x) OVER (…)` — which PostgreSQL itself rejects.
- `PARTITION BY` over a pseudonymized column, which is exact because HMAC preserves equality.

Divergences: [Window functions](compatibility.md#window-functions) — `EXCLUDE` is not parseable
yet, a windowed `FILTER` over `array_agg` / `string_agg` is refused, and ordering by a
pseudonymized column is refused because a digest has no plaintext order.

### Operators

| Family | Operators |
|---|---|
| Comparison | `=`, `<>` / `!=`, `<`, `<=`, `>`, `>=`, `IS [NOT] NULL`, `IS [NOT] DISTINCT FROM`, `BETWEEN`, `IN`, `NOT IN`, `EXISTS`, `NOT EXISTS` |
| Quantified | `= ANY`, `<> ALL` and every comparison with `ANY` / `SOME` / `ALL (subquery)` in a `WHERE`, `HAVING`, `QUALIFY` or join `ON` |
| Logical | `AND`, `OR`, `NOT`, and the three-valued rules over `NULL` |
| Arithmetic | `+`, `-`, `*`, `/`, `%`, unary `-` — integer and `numeric` division by zero raises `22012` |
| String | `\|\|`, `LIKE`, `NOT LIKE`, `ILIKE`, `SIMILAR TO` with a literal pattern, `ESCAPE` |
| JSON | `->`, `->>`, `#>`, `#>>`, including negative indices and the `'{a,b}'` path spelling |
| Array | `ARRAY[…]` construction, subscripting `arr[n]`, slicing `arr[a:b]` |
| Conditional | `CASE`, `COALESCE`, `NULLIF`, `GREATEST`, `LEAST` |

`<`, `<=`, `>`, `>=`, `BETWEEN` and `LIKE` are **pushed down into the shards**, with the escape
character spelled out in the pushed fragment so both engines read the pattern the same way.

Divergences: [Operators](compatibility.md#operators-literals-and-casts) — `**`, `//`, `@?`
jsonpath, `@@` full-text search, `LIKE ANY`, `GLOB`, string subscripting, and a *computed*
`SIMILAR TO` pattern.

### Literals

| Kind | Forms |
|---|---|
| Numeric | integers, decimals, floats, `1e10` |
| String | `'…'` with `''` escaping |
| Boolean & null | `TRUE`, `FALSE`, `NULL` |
| Date & time | `DATE '2024-01-01'`, `TIME '…'`, `TIMESTAMP '…'`, `TIMESTAMPTZ '…+02'`, `INTERVAL '1 day'`, and the unquoted `INTERVAL 1 DAY` DuckDB form |
| Arrays | `ARRAY[1,2,3]` and PostgreSQL's canonical text form `'{1,2,3}'::INT[]` |
| Binary | `'\xDEADBEEF'::bytea`, using PostgreSQL's own hex-escape input rules and SQLSTATEs |
| JSON | `'{"a":1}'::json` / `::jsonb`, validated on cast |
| Current time | `now()`, `current_timestamp`, `transaction_timestamp()` — evaluated **once, on the coordinator**, so three shards never read three clocks — plus the per-call `clock_timestamp()` and `timeofday()` |

Divergences: [Literals](compatibility.md#operators-literals-and-casts) — `B'1010'`, `U&'\0041'`,
`N'foo'`, `INTERVAL '1-2' YEAR TO MONTH`, brace struct literals, and the type of a bare integer
literal (`bigint`, not `integer`) or of `1_000` (`numeric`).

### Casts

- Both spellings, `x::T` and `CAST(x AS T)`, over every type in the table above.
- `NUMERIC(p,s)` — the precision and scale are applied.
- `::json` / `::jsonb`, which validate and raise `22P02` on text that is not a document.
- `::uuid` and `::bytea`, with PostgreSQL's input rules.
- Array casts such as `'{1,2,3}'::INT[]`.
- `COLLATE C`, `POSIX`, `ucs_basic` and `default`, which name the byte ordering VaireDB uses.

Divergences: [Casts](compatibility.md#operators-literals-and-casts) — `'abc'::VARCHAR(2)` is
refused rather than silently unbounded (use `substr()`), `CAST(x AS T ARRAY)` does not parse, and
any collation other than byte order is refused by name instead of ignored.

### Joins and set operations

| Kind | Supported |
|---|---|
| Joins | `INNER`, `LEFT`, `RIGHT`, `FULL`, `CROSS`, `NATURAL`, and `JOIN LATERAL` / `LEFT JOIN LATERAL` |
| Join conditions | `ON <predicate>` including non-equality and multi-column, `USING (c)`, self-joins, three or more relations |
| Semi / anti | `EXISTS`, `NOT EXISTS`, `IN (subquery)`, `NOT IN` over `NOT NULL` columns — null-correct everywhere the census reaches |
| Set operations | `UNION`, `UNION ALL`, `INTERSECT`, `INTERSECT ALL`, `EXCEPT`, `EXCEPT ALL`, with PostgreSQL's multiplicity rules |
| Pseudonymized columns | An equi-join on a pseudonymized column, including under a `GROUP BY` above it |

Every construct on this axis was measured in **both** shard layouts — co-located and shuffle —
and none differs between them: the shard map may change the plan, never the answer.

Divergences: [Joins and set operations](compatibility.md#joins-and-set-operations) — a correlated
`NOT IN` (write `NOT EXISTS` plus its `IS NULL` test), quantified subqueries outside a predicate
`AND` chain, two `USING` shapes that need three addressable column names, and the type of an
untyped literal branch of a set operation.
