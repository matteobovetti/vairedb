# Data Type Gap — DataFusion ↔ DuckDB ↔ PostgreSQL Wire Protocol

Gap analysis of the **data types** VaireDB carries end-to-end: from a PostgreSQL
wire-protocol client, down the write path into the per-shard DuckDB engines, and
back up the read path through DataFusion/Ballista to the client.

## Scope and framing

Three type systems meet in VaireDB, and they are not peers:

1. **PostgreSQL is the entry point.** Clients declare and send PostgreSQL types.
   Every type a client can declare must land somewhere in DuckDB on the write
   path and come back as a faithful DataFusion/Arrow type on the read path.
2. **DuckDB is the landing technology and may be restricted.** VaireDB does not
   need to expose DuckDB's full type surface. A DuckDB type with no clean
   DataFusion representation is better *rejected at DDL time* than accepted and
   silently degraded.
3. **DataFusion compatibility is not optional.** Every read goes through a
   DataFusion plan, and so does every DML statement's parameter inference.
   A type that DataFusion cannot represent faithfully cannot be served
   faithfully, no matter what DuckDB stores.

This document therefore takes **DataFusion's type vocabulary as the starting
point** and asks, for each type: *is it clean end-to-end — write path and read
path — with no data loss, no precision loss, and no read that degrades to
`NULL`?*

### References

- **DataFusion:** [Data Types](https://datafusion.apache.org/user-guide/sql/data_types.html) — SQL name → Arrow `DataType` mappings.
- **DuckDB:** [Data Types overview](https://duckdb.org/docs/current/sql/data_types/overview) — 25 general-purpose + 6 nested/composite types.
- **Paths:** [`distributed-query-processing.md`](distributed-query-processing.md).

### One clarification about the DataFusion table

DataFusion's page maps *SQL type names its parser accepts* to Arrow types. That
table is the **floor of what DataFusion can execute on, not the ceiling.**
DataFusion executes over any Arrow type its kernels support, which is a strictly
larger set — `Timestamp(µs, Some(tz))`, `Time64(µs)`, `Dictionary(UInt8, Utf8)`
and the unsigned integers are all fully supported in plans even where the SQL
parser has no name that produces them.

This matters because VaireDB never asks DataFusion's parser for a column type.
It builds the schema itself, in
[`parse_data_type`](../../crates/vairedb-coordinator/src/scheduler/scheduler.rs)
(`scheduler.rs:236`), from the type string the catalog stored. VaireDB is
free to choose the Arrow type that matches DuckDB exactly — the binding
constraint is not DataFusion's parser but **arrow-pg's** ability to map that
Arrow type to a PostgreSQL OID and encode its values.

## The type chain

A type crosses five boundaries. It can be lost at any of them.

### Write path

| # | Boundary | Code | What can go wrong |
|---|---|---|---|
| W1 | Parse the statement under `PostgreSqlDialect` (sqlparser 0.58) | `sql_compat::parse_sql` | Unknown type names parse as `DataType::Custom`; nothing is rejected. `STRUCT`/`UNION` field lists are **mangled on re-render**. |
| W2 | Rewrite PG → DuckDB, broadcast shard-local DDL/DML | `sql_compat/dialect.rs` (`transform_data_type`), `pgwire_handler/ddl.rs:453` | Only `BYTEA`→`BLOB` and `JSONB`→`JSON` are rewritten. A type DuckDB cannot parse fails the DDL on every shard. |
| W3 | Store the declared type string in the catalog | `table_meta_ops.rs:238` — stores `col.data_type.to_string()`, **untransformed** | The catalog keeps the PostgreSQL spelling (`BYTEA`, `JSONB`), not what DuckDB received. Both spellings must be handled downstream. |
| W4 | Convert extended-protocol bind parameters for transport | `write_router.rs:124` (`scalar_to_write_param`) → `WriteParam` oneof → `param_conversion.rs` (`write_param_to_duckdb_value`) | The `WriteParam` oneof carries only `bool / i64 / f64 / string / bytes`. Everything else falls through `other => StringVal(other.to_string())` — see [The write-path parameter defect](#the-write-path-parameter-defect). |

Inline literals (simple protocol) are pass-through: the coordinator hands
rewritten SQL text to DuckDB, which parses the literals itself. **Only
parameterized DML crosses W4**, which is why the defect there is easy to miss.

### Read path

| # | Boundary | Code | What can go wrong |
|---|---|---|---|
| R1 | Map the stored type string to Arrow for the coordinator's DataFusion schema | `scheduler.rs:203` → `parse_data_type` | Ends in `_ => DataType::Utf8`: **any unrecognized type silently becomes text.** |
| R2 | Coerce DuckDB's Arrow batches to that schema on the core node | `vairedb-core/src/table_provider/scan_exec.rs:212` (`coerce_batch_to_schema`) | A **safe** `arrow::compute::cast`: unsupported casts error the query; out-of-range or invalid values return **`NULL` instead of failing**. |
| R3 | Advertise result/parameter types | `parser.rs:165` (`get_parameter_types`), `parser.rs:200` (`get_result_schema`) → `arrow_pg::datatypes::into_pg_type` | An Arrow type with no arm in `into_pg_type` fails the statement with `XX000 Unsupported Datatype`. |
| R4 | Encode cells to the wire | `pgwire_handler/encoding.rs` (text) / `arrow_pg::encoder::encode_value` (binary + arrays) | Text and binary have **different code paths and different failure modes** (see [Text vs binary divergence](#text-vs-binary-divergence)). |

The schema itself crosses the coordinator→core boundary as **Arrow IPC**
(`scheduler/codec.rs`, `encode_schema_ipc`/`decode_schema_ipc`), which is
type-agnostic — it round-trips every Arrow type tested, including
`Decimal256`, `Map`, `Struct` and `FixedSizeList`. That boundary is not a
constraint.

## Verdict legend

| Status | Meaning |
|---|---|
| ✅ | **Clean end-to-end.** Literal and parameterized writes both store exactly; the read advertises an Arrow type matching DuckDB's, a faithful PG OID, and correct values in both text and binary format. |
| 🟡 | **Works, but lossy or partial.** Values survive but precision, format, range or the advertised type is wrong; or one of the two wire formats misbehaves. |
| ❌ | **Broken.** The DDL fails, the write fails, the `SELECT` fails, or values read back as `NULL`. |

## Summary

Measured against DataFusion's type vocabulary:

| Verdict | Count | Arrow types |
|---|---:|---|
| ✅ Clean | 11 | `Boolean`, `Int8`, `Int16`, `Int32`, `Int64`, `Float32`, `Float64`, `Utf8`, `Binary`, `Date32`, `List(T)` |
| 🟡 Lossy | 1 | `Timestamp(µs, None)` |
| ❌ Broken | 12 | `Decimal128`, `Decimal256`, `Timestamp(_, tz)`, `Time64`, `Interval`, `UInt8`, `UInt16`, `UInt32`, `UInt64`, `FixedSizeList(n,T)`, `Map`, `Struct` |

Only the types a client rarely thinks about are safe. **Every temporal type
except `DATE`, every exact-numeric type, and every unsigned integer is broken or
lossy** — and `NUMERIC`, `TIMESTAMP` and `TIME` are among the most common column
types in real PostgreSQL schemas.

## Master table — DataFusion types end-to-end

`DuckDB Arrow` is what the DuckDB driver returns on a scan. `VaireDB advertises`
is what `parse_data_type` derives today. `PG OID` is what the client is told in
`RowDescription`/`ParameterDescription`.

### Boolean

| DataFusion Arrow | PG declaration | DuckDB landing | DuckDB Arrow | VaireDB advertises | PG OID | Verdict |
|---|---|---|---|---|:--:|:--:|
| `Boolean` | `BOOLEAN`, `BOOL` | `BOOLEAN` | `Boolean` | `Boolean` | `bool` | ✅ |

Notes: the type mapping is clean and no value is lost, but the **text** wire form
diverges: `arrow_array_value_to_string` emits Rust's `true`/`false` where
PostgreSQL's `boolout` emits `t`/`f`. Binary format is correct. Most drivers
accept both spellings on input, so this is cosmetic — see
[Text vs binary divergence](#text-vs-binary-divergence).

### Character

| DataFusion Arrow | PG declaration | DuckDB landing | DuckDB Arrow | VaireDB advertises | PG OID | Verdict |
|---|---|---|---|---|:--:|:--:|
| `Utf8View` (default) / `Utf8` | `VARCHAR`, `TEXT`, `CHAR`, `STRING` | `VARCHAR` | `Utf8` | `Utf8` | `text` | ✅ |

Notes: VaireDB advertises `Utf8` while DataFusion's parser produces `Utf8View`
for string literals; DataFusion's coercion rules bridge the two, so this is not
a fault. `VARCHAR(n)`/`CHAR(n)` reach `Utf8` through the catch-all — correct by
accident. Declared length is enforced by neither the coordinator nor DuckDB, and
`CHAR(n)` is not blank-padded; the column is advertised `text`, not
`varchar`/`bpchar`.

### Integer

| DataFusion Arrow | PG declaration | DuckDB landing | DuckDB Arrow | VaireDB advertises | PG OID | Verdict |
|---|---|---|---|---|:--:|:--:|
| `Int8` | — (`TINYINT`) | `TINYINT` | `Int8` | `Int8` | `int2` | ✅ |
| `Int16` | `SMALLINT`, `INT2` | `SMALLINT` | `Int16` | `Int16` | `int2` | ✅ |
| `Int32` | `INTEGER`, `INT`, `INT4` | `INTEGER` | `Int32` | `Int32` | `int4` | ✅ |
| `Int64` | `BIGINT`, `INT8` | `BIGINT` | `Int64` | `Int64` | `int8` | ✅ |
| `UInt8` | — (`UTINYINT`) | `UTINYINT` | `UInt8` | **`Utf8`** | `text` | ❌ |
| `UInt16` | — (`USMALLINT`) | `USMALLINT` | `UInt16` | **`Utf8`** | `text` | ❌ |
| `UInt32` | — (`UINTEGER`) | `UINTEGER` | `UInt32` | **`Utf8`** | `text` | ❌ |
| `UInt64` | — (`UBIGINT`) | `UBIGINT` | `UInt64` | **`Utf8`** | `text` | ❌ |

`Int8` is widened to `int2` on the wire because PostgreSQL has no one-byte
integer; that is faithful, not lossy.

The unsigned integers have no PostgreSQL declaration, but they are legal DuckDB
and legal DataFusion, and a client can create such a column. Today all four fall
through to `Utf8`: values are readable as digits but ordering and comparison go
lexicographic, and typed drivers see `text`. **All four are viable targets** —
arrow-pg maps `UInt8`→`int2`, `UInt16`→`int4`, `UInt32`→`int8`, `UInt64`→`numeric`,
and all four encode correctly in both formats.

### Floating point

| DataFusion Arrow | PG declaration | DuckDB landing | DuckDB Arrow | VaireDB advertises | PG OID | Verdict |
|---|---|---|---|---|:--:|:--:|
| `Float32` | `REAL`, `FLOAT4`, `FLOAT` | `FLOAT` | `Float32` | `Float32` | `float4` | ✅ |
| `Float64` | `DOUBLE PRECISION`, `FLOAT8` | `DOUBLE` | `Float64` | `Float64` | `float8` | ✅ |

### Exact numeric

| DataFusion Arrow | PG declaration | DuckDB landing | DuckDB Arrow | VaireDB advertises | PG OID | Verdict |
|---|---|---|---|---|:--:|:--:|
| `Decimal128(p,s)`, p ≤ 38 | `NUMERIC(p,s)`, `DECIMAL(p,s)` | `DECIMAL(p,s)` | `Decimal128(p,s)` | **`Decimal128(38,10)`** | `numeric` | ❌ |
| `Decimal256(p,s)`, p > 38 | `NUMERIC(p,s)` with p > 38 | *rejected by DuckDB* | — | — | — | ❌ |

`Decimal128` fails on four counts:

1. **Declared precision and scale are discarded.** Every decimal is advertised
   as `(38,10)`, so `1.5` renders `1.5000000000` and `coerce_batch_to_schema`
   must rescale on every batch instead of taking its "fields already match" fast
   path.
2. **Large values read back `NULL`.** The safe rescale from `Decimal128(38,0)` to
   `(38,10)` overflows for values with more than 28 integer digits, and the safe
   cast turns overflow into `NULL` — a wrong answer, not an error. On a `NOT NULL`
   column the manufactured NULLs are then caught one step later, when
   `coerce_batch_to_schema` rebuilds the batch:
   `Invalid argument error: Column 'big' is declared as non-nullable but contains
   null values`, which fails the whole query (`XX000`). So the symptom depends on
   the column's nullability — a silent wrong answer when nullable, a confusing
   internal error when not.
3. **Parameterized writes fail.** See [below](#the-write-path-parameter-defect).
4. **Binary-format clients hit `22003` at the top of the range.** arrow-pg
   encodes `numeric` through `rust_decimal`, whose 96-bit mantissa caps at
   `79228162514264337593543950335` (29 digits) with a maximum scale of 28.
   Values above that raise SQLSTATE `22003` from
   `encoder::get_numeric_128_value`. Text-format clients are unaffected because
   VaireDB renders text cells itself.

Honoring the declared `(p,s)` fixes 1 and 2 and is exact: **DuckDB caps `DECIMAL`
width at 38** (`DECIMAL(39,0)` is rejected with
`Binder Error: DECIMAL type width must be between 1 and 38`), so `Decimal128`
holds every legal DuckDB decimal without loss. It does convert fault 2 from a
silent `NULL` into an explicit `22003` for binary clients on >29-digit
`DECIMAL(38,0)` values — a strict improvement, but a visible behavior change.

`Decimal256` is unreachable and unusable: DuckDB rejects `p > 38` outright, and
arrow-pg 0.14 has **no `Decimal256` arm** in `into_pg_type` (only `Decimal128`),
so advertising it fails every `SELECT` on that column with
`XX000 Unsupported Datatype Decimal256(...)`. See
[Why not `Decimal256`?](#why-not-decimal256).

### Temporal

| DataFusion Arrow | PG declaration | DuckDB landing | DuckDB Arrow | VaireDB advertises | PG OID | Verdict |
|---|---|---|---|---|:--:|:--:|
| `Date32` | `DATE` | `DATE` | `Date32` | `Date32` | `date` | ✅ |
| `Timestamp(ns, None)` (SQL) / `Timestamp(µs, None)` | `TIMESTAMP` | `TIMESTAMP` | `Timestamp(µs, None)` | `Timestamp(µs, None)` | `timestamp` | 🟡 |
| `Timestamp(_, Some(tz))` | `TIMESTAMPTZ`, `TIMESTAMP WITH TIME ZONE` | `TIMESTAMPTZ` | `Timestamp(µs, "UTC")` | **`Utf8`** | `text` | ❌ |
| `Time64(ns)` (SQL) / `Time64(µs)` | `TIME` | `TIME` | `Time64(µs)` | **`Utf8`** | `text` | ❌ |
| `Interval(MonthDayNano)` | `INTERVAL` | `INTERVAL` | `Interval(MonthDayNano)` | **`Utf8`** | `text` | ❌ |

**`TIMESTAMP` is 🟡, not ✅.** The column type and OID are right and values
round-trip, but DataFusion's SQL parser produces **nanosecond** timestamp
literals, and the nanosecond epoch only spans ≈`1677-09-21 .. 2262-04-11`. Any
literal outside that window fails the whole query inside the
`simplify_expressions` optimizer rule, even when the stored data is fine:

```
-- DuckDB stores and projects this correctly:
SELECT ts FROM t;                          -- 1500-01-01 00:00:00  ✓
-- But any out-of-window literal kills the query:
SELECT ts FROM t WHERE ts < TIMESTAMP '1600-01-01';
  Optimizer rule 'simplify_expressions' failed caused by
  Arrow error: Cast error: Overflow converting 1600-01-01
SELECT TIMESTAMP '3000-01-01';             -- same overflow
```

So VaireDB accepts and stores dates DuckDB supports but cannot be *queried*
about them. This is a DataFusion-side range limit, not a VaireDB mapping bug.

`TIMESTAMPTZ`, `TIME` and `INTERVAL` are all pure `parse_data_type` gaps —
DuckDB returns exactly the Arrow type DataFusion wants, and arrow-pg maps all
three correctly (`timestamptz`, `time`, `interval`). Today the `Utf8` fallback
produces:

- `TIMESTAMPTZ` → `2024-01-01T10:00:00Z`, the ISO `T`/`Z` form
  `encoding.rs` deliberately avoids for real timestamps because libpq and JDBC
  reject it. Offsets are normalized to UTC and predicates compare
  lexicographically.
- `TIME` → correct value (`12:34:56`), wrong type.
- `INTERVAL` → Arrow's rendering (`1 days`), not PostgreSQL's (`1 day`).

Also note DataFusion 53's parser **drops the timezone** when `TIMESTAMPTZ` is
used as a cast target (`CAST(x AS TIMESTAMPTZ)` yields `Timestamp(ns, None)`).
That does not affect column typing, which VaireDB controls, but it does affect
explicit casts written by clients.

### Binary

| DataFusion Arrow | PG declaration | DuckDB landing | DuckDB Arrow | VaireDB advertises | PG OID | Verdict |
|---|---|---|---|---|:--:|:--:|
| `Binary` | `BYTEA` | `BLOB` (rewritten at W2) | `Binary` | `Binary` | `bytea` | ✅ |
| `Binary` | `BINARY`, `VARBINARY` | `BLOB` (DuckDB alias) | `Binary` | **`Utf8`** | `text` | ❌ |

`BYTEA` and `BLOB` are clean. The DuckDB aliases `BINARY`/`VARBINARY` are not in
`parse_data_type`, so they degrade to `Utf8` and the safe `Binary`→`Utf8` cast
sees invalid UTF-8: **every non-ASCII value reads back `NULL`.** This is the
worst failure mode in the document — silent data loss on a type that works
correctly under a different spelling.

### Nested

| DataFusion Arrow | PG declaration | DuckDB landing | DuckDB Arrow | VaireDB advertises | PG OID | Verdict |
|---|---|---|---|---|:--:|:--:|
| `List(T)` | `T[]` | `LIST` (`T[]`) | `List(T)` | `List(T)` | `<T>[]` | ✅ |
| `FixedSizeList(n,T)` | `T[n]` | `ARRAY` (`T[n]`) | `FixedSizeList(T,n)` | `List(T)` | `<T>[]` | ❌ |
| `Map(k,v)` | — | `MAP(k,v)` | `Map(...)` | **`Utf8`** | — | ❌ |
| `Struct(...)` | — | `STRUCT(...)` | `Struct(...)` | **`Utf8`** | — | ❌ |

- **`List`** works: `parse_data_type` recurses on the element type, and arrow-pg
  renders PostgreSQL array literals (`{1,2,3}`). The element type comes from the
  same map, so an unsupported element nests the text fallback inside the array.
- **`FixedSizeList`** does not merely lose its declared length — **every read of a
  `T[n]` column fails today.** The `FixedSizeList(T,n)`→`List(T)` cast itself
  succeeds, but it produces a list whose child field is unnamed, and
  `coerce_batch_to_schema`'s rebuild then rejects the batch against the advertised
  schema: `column types must match schema types, expected List(Int32) but found
  List(Int32, field: '')`. Fixing it means normalizing the child field (name
  `item`, nullable) after the cast. **Do not "fix" it by advertising
  `FixedSizeList`:** arrow-pg's `encoder.rs:488` downcasts
  `List | FixedSizeList | LargeList` unconditionally to `ListArray`, so it
  **panics** (`Option::unwrap()` on `None`) on a `FixedSizeList` in binary
  format. `LargeList` is worse — `encoding.rs`'s `is_list` check matches
  `List | LargeList` only, so `LargeList` is routed to arrow-pg in *both*
  formats and panics in both.
- **`Map`** fails on every read: `Casting from Map(...) to Utf8 not supported`,
  surfaced as `schema coercion cast failed`. Advertising `Map` would not help —
  arrow-pg has no `Map` arm at all.
- **`Struct`** never reaches DuckDB intact: sqlparser re-renders
  `STRUCT(a INTEGER, b VARCHAR)` as `STRUCT(a, INTEGER, b, VARCHAR)` and DuckDB
  rejects it with `syntax error at or near ","`. Unlike `Map`, `Struct` *is*
  viable in principle — arrow-pg maps it to `record` (OID 2249) and encodes it —
  so fixing the W1 re-render is the only blocker.

### DataFusion-unsupported SQL types

DataFusion's parser rejects `UUID`, `CLOB`, `REGCLASS`, `ENUM`, `SET`, `CUSTOM`
and `DATETIME`, and DataFusion has no JSON type at all. Three of these have
DuckDB columns behind them and are worth naming explicitly:

| Type | DuckDB Arrow | VaireDB advertises | Verdict |
|---|---|---|:--:|
| `JSON` (`JSONB` rewritten at W2) | `Utf8` | `Utf8` | 🟡 — values faithful; the field is advertised `text`, not `json` (OID 114). |
| `UUID` | `Utf8` (DuckDB's bridge already stringifies) | `Utf8` | 🟡 — values correct, advertised `text` not `uuid` (OID 2950). |
| `ENUM(...)` | `Dictionary(UInt8, Utf8)` | `Utf8` | 🟡 — values correct via a clean dictionary→text cast. `Dictionary(UInt8, Utf8)` is a fully viable target (arrow-pg resolves a dictionary to its value type) if a distinguishable type is ever wanted. |

Because DataFusion has no richer representation for any of the three, text is
the correct *value* carrier; only the advertised OID is wrong, and only
cosmetically.

## The write-path parameter defect

This is the highest-severity finding and it is currently shipping.
`tests/e2e/tests/extended_protocol.rs::test_parameterized_insert_typed_columns`
exercises only `INTEGER`, `DOUBLE PRECISION`, `BOOLEAN` and `VARCHAR`, so it never
reaches the defect; the `*_bind_parameter_insert` xfails in
`tests/e2e/tests/data_types_round_trips.rs` now pin it down.

`WriteParam` (`proto/vairedb/v1/write_service.proto`) carries only
`is_null | bool | int64 | double | string | bytes`. `scalar_to_write_param`
(`write_router.rs:124`) has explicit arms for the booleans, `Int8..Int64`,
`UInt8..UInt32`, `Float32/64`, the three UTF-8 variants and the three binary
variants — and then:

```rust
other => write_param::Value::StringVal(other.to_string()),   // write_router.rs:145
```

`ScalarValue`'s `Display` is a **debug/diagnostic** rendering, not a SQL
literal. For the types that reach this arm it produces strings DuckDB cannot
bind into the target column:

| PG parameter type | `ScalarValue` variant | `to_string()` renders | DuckDB bind |
|---|---|---|:--:|
| `NUMERIC` / `DECIMAL` | `Decimal128(v,p,s)` | `Some(12345600000000),38,10` | ❌ fails |
| `TIMESTAMP` | `TimestampMicrosecond` | `1704106800000000` (raw µs) | ❌ fails |
| `TIMESTAMPTZ` | `TimestampMicrosecond(_, tz)` | `1704106800000000` | ❌ fails |
| `TIME` | `Time64Microsecond` | `45296000000` (raw µs) | ❌ fails |
| `INTERVAL` | `IntervalMonthDayNano` | `IntervalMonthDayNano { months: 0, days: 1, .. }` | ❌ fails |
| `DATE` | `Date32` | `2024-01-01` | ✅ works — `Date32`'s `Display` is ISO, so it parses by luck |
| `UBIGINT` | `UInt64` | `42` | ✅ works by luck |

So `INSERT INTO t (amount) VALUES ($1)` against a `NUMERIC` column — the single
most ordinary parameterized write in PostgreSQL — fails at DuckDB bind time.
Measured end to end (`amount NUMERIC(10,2)`, `$2 = 1234.56`):

```
42804 [VDB-1002] node execution failed: Could not convert string
"Some(12345600000000),38,10" to DECIMAL(10,2)
```

The unscaled value and the advertised `(38,10)` are visible in the error, which
confirms both this defect and [fault 1](#exact-numeric) in a single message.

### Sequencing warning

The read-path and write-path fixes are coupled, and doing them in the wrong
order causes a regression.

Parameter OIDs are inferred from a DataFusion plan over the *advertised* schema
(`get_parameter_types`, `parser.rs:165`). So today:

- A `NUMERIC` column advertises `Decimal128(38,10)` → `$1` is described
  `numeric` → the client sends numeric → `deserialize_parameters` yields
  `ScalarValue::Decimal128` → **broken today.**
- A `TIME`/`TIMESTAMPTZ`/`INTERVAL` column advertises `Utf8` → `$1` is described
  `text` → the client sends text → `ScalarValue::Utf8` → `StringVal` → DuckDB
  casts the text itself → **works today, accidentally.**

The `Utf8` fallback is self-consistent for writes and destructive for reads.
**Fixing `parse_data_type` first would convert currently-working parameterized
writes into hard failures** for `TIME`, `TIMESTAMPTZ` and `INTERVAL`. Fix
`scalar_to_write_param` in the same change or before it.

The correct fix is to extend the `WriteParam` oneof with typed variants
(date/time/timestamp/interval as their integer representations, decimal as
unscaled-value + scale) and map them in `param_conversion.rs` to
`duckdb::types::Value::{Date32, Time64, Timestamp, Interval, Decimal}`. A
narrower interim fix is to render SQL-parseable strings per variant instead of
using `Display`, and to fail loudly on any variant without an explicit arm
rather than stringifying it.

## Text vs binary divergence

`encoding.rs` renders text cells with VaireDB's own
`arrow_array_value_to_string`, and routes binary cells (and all arrays) to
`arrow_pg::encoder::encode_value`. The two paths do not fail together, so a
column can work in `psql` and break in JDBC:

| Condition | Text format | Binary format |
|---|---|---|
| `Boolean` value | renders `true`/`false`; PostgreSQL renders `t`/`f` | correct |
| `Decimal128` value > 29 digits | renders correctly | `22003` error |
| `FixedSizeList` column | renders via `ArrayFormatter` | **panic** in arrow-pg |
| `LargeList` column | **panic** in arrow-pg | **panic** in arrow-pg |

The panics are an availability concern, not just a correctness one: they unwind
inside the pgwire connection task.

## DuckDB types to restrict

Per framing point 2, these DuckDB types have no clean path to DataFusion or to
the wire and should be **rejected at DDL time** (`0A000`) rather than accepted
and broken on first read:

| DuckDB type | Why it cannot be served |
|---|---|
| `HUGEINT` | duckdb-rs's own Arrow bridge narrows 128-bit to `Decimal128(38,0)` **before the coordinator sees the batch** — the 39-digit max `…884105727` silently comes back as `…88410572`. Unfixable in `parse_data_type`. `Decimal256(39,0)` would hold it, but neither duckdb-rs nor arrow-pg supports `Decimal256`. |
| `UHUGEINT` | Same narrowing, worse: the 39-digit maximum `340282366920938463463374607431768211455` reads back as `-1`. |
| `BIT` / `BITSTRING` | DuckDB returns the packed bitstring as `Binary`; the safe `Binary`→`Utf8` cast sees invalid UTF-8 and **every value reads `NULL`** (and on a `NOT NULL` column the rebuild then fails the query outright — same mechanism as [`Decimal128` fault 2](#exact-numeric)). |
| `BIGNUM` | Same `Binary`→`Utf8` NULL-ification. |
| `UNION` | sqlparser mangles the DDL re-render (`UNION(num, INTEGER, str, VARCHAR)`) → DuckDB parse error. Even with valid DDL, no Arrow `Union` path exists through arrow-pg. |
| `VARIANT` | Unusable on the shard databases as deployed: `CREATE TABLE` is rejected with `Invalid Input Error: VARIANT columns are not supported in storage versions prior to v1.5.0 (database "core" is using storage version v1.0.0+)`. Even on a v1.5.0+ store the read fails inside the DuckDB driver: `decoding Variant columns is not supported`. |
| `MAP` | No arrow-pg mapping and no cast to text. |
| `TIMETZ` / `TIME WITH TIME ZONE` | DuckDB's Arrow bridge drops the offset (`12:34:56+02` → `Time64(µs)` `12:34:56`) — silent data loss upstream of the coordinator. |
| `TIMESTAMP_S` / `_MS` / `_NS` | No `parse_data_type` arm; read back as ISO `T`-separated text. Cheap to support properly (all three are valid Arrow `Timestamp` units), so restrict *or* map — do not leave as text. |

Rejecting at DDL is strictly better than the status quo for all of these: today
`MAP`, `VARIANT`, `HUGEINT` and `BIT` are accepted at `CREATE TABLE`, accept
`INSERT`s, and only break on the first `SELECT` — after the data is written.

## The alias gap

`parse_data_type` matches the declared string, so an unrecognized **alias** of a
supported type degrades to text even though the canonical name works. Recognized
today: `INT8`, `INT4`, `INT`, `INT2`, `BOOL`, `FLOAT4`, `FLOAT8`, `REAL`,
`DOUBLE PRECISION`, `BYTEA`, `TEXT`, `STRING`, `NUMERIC`, `JSONB`
(`CHAR`/`BPCHAR` work via the `Utf8` catch-all). Missing:

| Alias | Canonical | Effect |
|---|---|---|
| `BINARY`, `VARBINARY` | `BLOB` | **Silent data loss** — non-UTF-8 bytes read back `NULL`. |
| `LONG` | `BIGINT` | Numbers arrive as strings. |
| `SIGNED` | `INTEGER` | Numbers arrive as strings. |
| `SHORT` | `SMALLINT` | Numbers arrive as strings. |
| `INT1` | `TINYINT` | Numbers arrive as strings. |
| `DATETIME` | `TIMESTAMP` | Timestamps arrive as ISO `T`-separated strings. |
| `LOGICAL` | `BOOLEAN` | Booleans arrive as `true`/`false` strings. |
| `BITSTRING` | `BIT` | Same NULL-ification as `BIT`. |

## Consequences of the `Utf8` catch-all

`_ => DataType::Utf8` is not a cosmetic mismatch:

- **Typed drivers break.** `Describe`/`RowDescription` is built from the same
  Arrow schema (`get_result_schema`), so JDBC `getTimestamp()`/`getLong()` on a
  fallback column sees `text`.
- **Predicates and ordering go lexicographic.** `SchedulerTableProvider` does
  not override `supports_filters_pushdown`, so `filter_exprs` is always empty
  and DataFusion evaluates every filter itself — over the advertised type.
  `WHERE ts > '2024-01-01 12:00:00'` on a `TIMESTAMPTZ` column compares strings
  against DuckDB's `2024-01-01T10:00:00Z` rendering; `ORDER BY` on a `UBIGINT`
  column sorts digit-by-digit.
- **Failures are silent.** `coerce_batch_to_schema` uses the *safe* cast, so
  overflow and invalid UTF-8 become `NULL` rather than errors.
- **Unsupported types fail late**, after data has been written.
- **It hides the write-path defect**, as described above.

## Recommended target mapping

What `parse_data_type` should return. Every target below was verified end-to-end
— select, filter, sort, aggregate, `arrow_cast`, `into_pg_type`, `encode_value`
in both formats, and Arrow IPC round-trip.

| Declared type | Target Arrow type | Resulting PG OID |
|---|---|---|
| `UTINYINT`, `USMALLINT`, `UINTEGER`, `UBIGINT` | `UInt8` / `UInt16` / `UInt32` / `UInt64` | `int2` / `int4` / `int8` / `numeric` |
| `TIMESTAMPTZ`, `TIMESTAMP WITH TIME ZONE` | `Timestamp(µs, "UTC")` | `timestamptz` |
| `TIME` | `Time64(µs)` | `time` |
| `INTERVAL` | `Interval(MonthDayNano)` | `interval` |
| `BINARY`, `VARBINARY` | `Binary` | `bytea` |
| `DECIMAL(p,s)`, `NUMERIC(p,s)` | `Decimal128(p,s)` — parse the declared parens | `numeric` |
| `ENUM(...)` | `Dictionary(UInt8, Utf8)` | `text` |
| `TIMESTAMP_S/_MS/_NS` | `Timestamp(s\|ms\|ns, None)` | `timestamp` |
| aliases `LONG`/`SIGNED`/`SHORT`/`INT1`/`DATETIME`/`LOGICAL` | as canonical | as canonical |

**Do not target:** `Decimal256` (no arrow-pg arm → `XX000`), `Map` (no arm),
`FixedSizeList` (panics in binary), `LargeList` (panics in both formats),
`Union` (no path).

Choosing microseconds for `Timestamp` and `Time64` — matching DuckDB rather
than DataFusion's SQL-parser default of nanoseconds — is deliberate: it makes
`coerce_batch_to_schema` take its "fields already match" fast path instead of
casting, and it avoids the nanosecond range limit on stored values.

## Prioritized remediation

1. **Fix `scalar_to_write_param` / extend `WriteParam`** — parameterized writes
   into `NUMERIC`, `TIMESTAMP`, `TIMESTAMPTZ`, `TIME` and `INTERVAL` columns fail
   outright today, on the most ordinary statement shape there is. Must land with
   or before item 2. Add e2e coverage for every type in the recommended mapping.
2. **Extend `parse_data_type`** per the recommended target mapping — one change
   that closes the unsigned integers, all three broken temporal types,
   `BINARY`/`VARBINARY` and every missing alias.
3. **Honor declared `DECIMAL(p,s)`** — removes the trailing-zero rendering and
   the silent `NULL` on large decimals, and enables the coercion fast path.
4. **Reject unsupported types at DDL time** (`0A000`) — everything in
   [DuckDB types to restrict](#duckdb-types-to-restrict). Fail the
   `CREATE TABLE`, not the first `SELECT`. Decide explicitly whether an unknown
   declared type should degrade to text at all, or be rejected.
5. **Guard the encoder against panicking types** — never advertise
   `FixedSizeList` or `LargeList`; consider extending `is_list` in
   `encoding.rs` so a `LargeList` column cannot reach arrow-pg's text path.
6. **Consider raising `TIMESTAMP` to ✅** by documenting or working around
   DataFusion's nanosecond literal range (rewriting out-of-range literals to
   microsecond-typed literals in the read-path AST transform).
7. **Fix the `STRUCT` DDL re-render** (or bypass sqlparser rendering for
   shard-local DDL) — the only blocker to `Struct` support, which arrow-pg
   already handles as `record`.
8. **Wire-type fidelity for `JSON` and `UUID`** — correct OIDs (`json`, `uuid`)
   once the Arrow side carries a distinguishable type. Both are cosmetic today:
   values are already faithful.

## Why not `Decimal256`?

[`Decimal256`](https://docs.rs/arrow/latest/arrow/datatypes/struct.Decimal256Type.html)
(precision up to 76, and DataFusion's documented mapping for `DECIMAL(p,s)` with
p > 38) looks like the natural way to stop losing precision. Measured against
this stack it is the wrong tool for `DECIMAL` and currently unusable for
anything reaching the wire:

- **DuckDB `DECIMAL` never exceeds 128 bits.** Width is capped at 38, so
  `Decimal128` holds every legal DuckDB decimal *exactly*. The `NULL` is caused
  by the hardcoded `(38,10)` target, not by 128-bit capacity.
- **arrow-pg 0.14 cannot map it.** `into_pg_type` has a `Decimal128` arm and no
  `Decimal256`, so it falls to the catch-all:
  `XX000 Unsupported Datatype Decimal256(48, 10)` (and `Unsupported List
  Datatype` inside a list). That function backs both `Describe` and result
  encoding, so advertising `Decimal256` fails every `SELECT` on the column.
  `encoder.rs` has no `Decimal256` value path either.
- **It *is* the correct type for `HUGEINT`/`UHUGEINT`** (39 and 40 digits) — but
  the truncation happens inside duckdb-rs's Arrow bridge before the coordinator
  sees the batch, so it cannot be fixed by changing `parse_data_type`. End-to-end
  support needs `Decimal256` in both duckdb-rs (which also has no `Decimal256`
  *write* path: `arrow_interop/schema.rs:46-48`) and arrow-pg. Until then, the
  lossless option is to emit `c::VARCHAR` in the shard scan — all 39 digits
  survive — and keep advertising `text`.

## How this was measured

Empirically, with seven probes rather than by reading mapping tables. 78 type
declarations were pushed through the full chain:

1. `CREATE TABLE t (c <TYPE>)` parsed with `sql_compat::parse_sql`, then
   `transform_to_duckdb` + re-render — what DuckDB actually receives, and what
   string the catalog stores.
2. That DDL plus an `INSERT` of a representative literal executed against an
   in-memory DuckDB — what DuckDB accepts.
3. `SELECT c` through `query_arrow` — the Arrow type the driver returns.
4. `arrow::compute::cast` from that type to `parse_data_type`'s target (exactly
   what `coerce_batch_to_schema` does), then
   `arrow_pg::datatypes::arrow_schema_to_pg_fields` on the target — whether the
   value survives and what OID the client is told.
5. Each of 21 candidate Arrow targets registered as a `MemTable` and driven
   through select / filter / sort / group-by / `max` / `arrow_cast`, then
   `into_pg_type`, then `encode_value` in both formats (wrapped in
   `catch_unwind`, which is how the `FixedSizeList` panic was found), then an
   Arrow IPC schema round-trip.
6. `ScalarValue::to_string()` for every variant reaching the `other` arm, bound
   into a typed DuckDB column as `Value::Text` — how the write-path defect was
   confirmed. Cross-checked against `datafusion-common`'s `Display` impl.
7. Timestamp range: nanosecond literal handling in DataFusion's optimizer
   against DuckDB's wider microsecond storage range.

Every verdict above is now also pinned down by the e2e suite against a live
5-node cluster. `tests/e2e/tests/data_types_round_trips.rs` holds one test per type:
working types are ordinary tests, and each 🟡/❌ type is an `#[ignore]`d xfail
asserting the PostgreSQL-correct behavior. Run

```
cd tests/e2e && cargo test --test data_types_round_trips -- --ignored --test-threads=1
```

to reproduce the gap map; un-`#[ignore]` a test when its gap closes. The
end-to-end run is what corrected three probe-level predictions recorded above:
`FixedSizeList` reads fail outright (they do not merely lose the length),
`VARIANT` is rejected at `CREATE TABLE` on the deployed storage version, and the
`NULL`-ification faults surface as query errors on `NOT NULL` columns.
