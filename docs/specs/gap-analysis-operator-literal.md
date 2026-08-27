# Operator & Literal Gap — DataFusion ↔ DuckDB ↔ PostgreSQL Wire Protocol

Gap analysis of the **operators and literals** VaireDB can evaluate: from a
PostgreSQL wire-protocol client, through the coordinator's parse/plan chain into
DataFusion on the read path and into the per-shard DuckDB engines on the write
path.

This document is the executable-spec counterpart of the roadmap's only
`IN PROGRESS` item — *"Close the GAP with Datafusion [Operation and Literals,
Aggregate, Window, Scalar, Special]"*. It covers the **Operators and Literals**
half of that item; aggregate/window/scalar/special *functions* are a separate axis
— aggregates are covered by
[`gap-analysis-aggregate-function.md`](gap-analysis-aggregate-function.md).

### References

- **DataFusion:** [Operators and Literals](https://datafusion.apache.org/user-guide/sql/operators.html) — 32 documented operators plus a Literals section. DataFusion 53 (`Cargo.toml:33`).
- **DuckDB:** [Pattern matching](https://duckdb.org/docs/current/sql/functions/pattern_matching.html), [Numeric operators](https://duckdb.org/docs/current/sql/functions/numeric.html), [Literal types](https://duckdb.org/docs/current/sql/data_types/literal_types.html), and DuckDB's own [PostgreSQL compatibility](https://duckdb.org/docs/current/sql/dialect/postgresql_compatibility.html) page. DuckDB **v1.5.5**, bundled by `duckdb` 1.10505.0.
- **PostgreSQL:** [Math functions and operators](https://www.postgresql.org/docs/current/functions-math.html), [Lexical structure](https://www.postgresql.org/docs/current/sql-syntax-lexical.html).
- **Sibling analyses:** [`gap-analysis-data-type.md`](gap-analysis-data-type.md), [`gap-analysis-command.md`](gap-analysis-command.md), [`gap-analysis-aggregate-function.md`](gap-analysis-aggregate-function.md).

## Scope and framing

The two sibling analyses each had a single spine: one type system to map, one
statement list to route. Operators have **two**, because VaireDB evaluates
expressions in two different engines depending on the *statement kind*:

- **On a `SELECT`, DuckDB never sees a client expression.** The core node runs
  only `SELECT <cols> FROM <shard_table>`
  (`vairedb-core/src/table_provider/scan_exec.rs:93`). Every operator and literal
  is evaluated by **DataFusion**, on the Ballista executor.
- **On an `INSERT`/`UPDATE`/`DELETE`, DataFusion never sees the expression.** The
  statement is re-rendered and executed **verbatim by DuckDB**
  (`write_router/write_router.rs:94` → `vairedb-core/src/write_service/write_service.rs:89`).

So the same SQL fragment has **two independent verdicts**, and where DataFusion
and DuckDB disagree on an operator's meaning, VaireDB returns *both* answers —
one on read, one on write. That is not a theoretical concern:
[six confirmed cases](#the-split-brain-defect) exist today where a `SELECT` and
an `UPDATE` carrying the **identical predicate** diverge — four by returning
different rows or different values, two by the `SELECT` failing outright.

### The parse chain

A read-path expression is parsed **once** and never re-rendered:

| # | Stage | Parser / dialect | Code |
|---|---|---|---|
| E1 | The single parse: `PostgresCompatibilityParser` — tokenize, substitute the pg-compat blacklist, parse, apply 12 rewrite rules | sqlparser **0.61**, `PostgreSqlDialect` | `sql_compat/mod.rs` → `sql_compat::parse_sql` |
| E2 | AST rewrites (`to_char` format, schema collapse) | — | `sql_compat/dialect.rs:122`, `:146` |
| E3 | Plan **from that AST** — `statement_to_plan(DFStatement::Statement(…))` | — no parse | `handler.rs:433`, `parser.rs:133` |
| E4 | Plan, optimize, serialize to proto, execute | DataFusion kernels | `scheduler/`, Ballista |

This is the state **after** the parser unification. Before it, the chain was three
parses and two renders: a `datafusion-pg-catalog` parse rendered back to text, a
second parse by VaireDB's own sqlparser **0.58**, a re-render via
`statement_to_sql`, and finally a third parse *inside* DataFusion — which uses
`Dialect::Generic` by default (`datafusion-common/src/config.rs:283`) and which
nothing in the workspace overrode. The reachable operator surface was therefore
the **intersection** of sqlparser's PostgreSQL and Generic dialects, strictly
smaller than either DataFusion's or DuckDB's. Three defects traced to that seam
and all three are now gone; see
[what unification changed](#what-the-parser-unification-changed).

There is now exactly **one** sqlparser in the tree, taken from DataFusion's own
re-export (`vairedb_coordinator::sqlparser` → `datafusion::sql::sqlparser`), so a
version or dialect split is structurally impossible: the AST VaireDB rewrites is
by construction the type DataFusion plans. What remains is a *single-dialect*
limitation rather than an intersection — syntax that sqlparser's PG dialect does
not accept is unreachable even when DataFusion and DuckDB both support it
(`{'a': 1}`, `MAP {…}`, `**`, `//` → `42601`).

One cost was accepted in the trade: `PostgresCompatibilityParser::parse_tokens`
rebuilds every token with `Span::empty()`, so DataFusion's `Diagnostic` source
positions are now empty. Previously the render-and-re-parse regenerated them by
accident. VaireDB's error enrichment (`error_enrichment.rs`) matches on message
text, not spans, so nothing client-visible regressed.

The write path adds a shard-local rewrite and the one surviving render:

| # | Stage | Code |
|---|---|---|
| W1 | E1 as above — the same single parse | `sql_compat::parse_sql` |
| W2 | Shard-local relation rewrite | `sql_compat/mod.rs:49` |
| W3 | `transform_to_duckdb` — PG→DuckDB rewrite | `sql_compat/dialect.rs:15` |
| W4 | Render `.to_string()`, ship, execute verbatim on DuckDB | `write_router.rs:94` |

**W4 is irreducible** — `write_router` ships SQL *text* over gRPC for DuckDB to
execute — so the write path is 1 parse and 1 render. Nothing VaireDB owns ever
re-parses its own output.

**W3 rewrites almost nothing in an expression.** `transform_exprs_in_statement`
(`dialect.rs:58`) visits only `INSERT … VALUES` rows, an INSERT-source `SELECT`'s
top-level `selection`, and `UPDATE … SET` values — it never visits an `UPDATE`
or `DELETE` `WHERE` clause, and even where it does visit, it only rewrites
`Expr::Function` (`dialect.rs:93`). **No `BinaryOp` is ever translated.** Every
operator in a write statement reaches DuckDB with PostgreSQL spelling and DuckDB
semantics.

### What the parser unification changed

Re-probed against the live cluster after the change. Three rows moved; the rest of
this document was re-verified as still accurate.

| Probe | Before (two dialects) | After (one parse) | |
|---|---|---|---|
| `SELECT 5 # 3` | `XX000 ParserError("No infix parser for token Sharp")` | **`6`** | ✅ fixed |
| `SELECT 2 ^ 10` | **`8`** — silently XOR | `0A000 [VDB-1004] Unsupported binary operator: PGExp` | ⛔ → ❌ |
| `SELECT 1_000` | **`1`** — silently truncated | `XX000 [VDB-5001] ParserError("Cannot parse 1_000 as f64")` | ⛔ → 🟡 |
| `SELECT 0b101` | `0` | `0` | unchanged |
| `SELECT 7 / 2`, `1.5 + 1.5`, `'abc' ~ '^a'`, `'a%b' LIKE 'a\%b'`, `'abc' ^@ 'a'` | as tabled below | byte-identical | unchanged |

Two of the three moves are not the ones the change was expected to produce, and the
mechanism is worth recording, because a text-level comparison of what the two
dialects *render* does not reveal either of them.

**`^` was never a DataFusion-semantics problem on VaireDB's read path — it was a
dialect problem wearing a DataFusion costume.** The two dialects render `^` to
*byte-identical text* (`SELECT 2 ^ 10`) while producing **different AST nodes**:
PG yields `BinaryOperator::PGExp` (exponentiation, agreeing with PostgreSQL *and*
DuckDB), Generic yields `BitwiseXor`. The old chain parsed the client's `^` as
exponentiation, threw that reading away in the render, and let DataFusion re-read
it as XOR. Now the `PGExp` node reaches DataFusion's planner, which does not
implement it, and the client gets an honest `0A000`. This also makes
[remediation #1](#prioritized-remediation) unambiguous: rewrite the `PGExp` node
to `power()` and the operator is correct on both paths, with no reinterpretation
of anyone's semantics.

**`1_000` moved from a silent wrong answer to a loud one, not to a right one.** PG's
dialect keeps `1_000` as a single `Number` token; Generic used to split the
rendered text into `1 AS _000`, which is where the silent `1` came from. That token
now reaches DataFusion's expression planner, which parses number literals with
`parse::<f64>()` and rejects the underscore. PostgreSQL 16 and DuckDB both return
`1000`, so this is still a gap — but a visible one, and it now belongs to
DataFusion's literal handling rather than to a dialect seam.

## Verdict legend

| Status | Meaning |
|---|---|
| ✅ | **Works, and agrees with PostgreSQL.** |
| ⛔ | **Silently wrong.** Parses, executes, returns a value — a *different* value than PostgreSQL would. No error, no warning. The most dangerous class. |
| 🟡 | **Works, but partially** — one path only, degraded semantics, or an honest error in an unexpected SQLSTATE. |
| ❌ | **Rejected.** Fails loudly with a SQLSTATE. |

## Summary

Measured against the 32 operators on DataFusion's Operators and Literals page,
plus the operator-like constructs it supports elsewhere (subscripts, casts,
`AT TIME ZONE`, quantified comparisons, subquery operators) — **77 operator and
literal probes executed against the live 5-node cluster**:

| Verdict | Count | Highlights |
|---|---:|---|
| ✅ Correct | 42 | `+ - *`, all six comparisons, `<=>`, `IS [NOT] DISTINCT FROM`, `AND/OR/NOT`, `& \| << >> #`, `LIKE`/`ILIKE`/`~~` family, `~ ~* !~ !~*`, `BETWEEN`, `IN`, `IS NULL`, `\|\|`, `@> <@`, `::`/`CAST`/`TRY_CAST`, `AT TIME ZONE`, `CASE`, array subscript/slice, most literal forms |
| ⛔ Silently wrong | 9 | **`/` on integers**, **decimal literals**, **large integer literals**, **`0b101`**, `SIMILAR TO`, `COLLATE`, `VARCHAR(n)` length, `'\x…'::bytea`, `arr[-1]` |
| 🟡 Partial | 6 | `1_000`, subquery operators in the SELECT list, `1/0`, `1.0/0`, `X'…'`, `0x…` |
| ❌ Rejected | 20 | `^` (read only), `**`, `//`/`DIV`, unary `~`, `&&`, `^@` (read only), `-> ->> #> #>> @? @@`, `MAP {…}`, `{a: 1}`, string subscript, `GLOB`, `ALL(array)`, `LIKE ANY`, `U&'…'`, `N'…'`, `B'…'`, `INTERVAL '1-2' YEAR TO MONTH`, `::json`, `::uuid`, `.field` access |

The ✅ column is genuinely broad — the ordinary comparison, logical, string-match
and array surface is in good shape. **The problem is concentrated entirely in the
⛔ column**, and specifically in the six cases below, where the read and write
paths disagree.

Counts reflect the state after the parser unification: `#` moved to ✅, `^` to ❌
and `1_000` to 🟡, per
[what unification changed](#what-the-parser-unification-changed).

## The split-brain defect

All six were reproduced against the running cluster. In each, a `SELECT` and an
`UPDATE` carrying the **byte-identical predicate** select different rows, or an
`INSERT` stores a value that the same expression cannot find on read.

| # | SQL fragment | Read path (DataFusion) | Write path (DuckDB) | PostgreSQL |
|---|---|---|---|---|
| 1 | `2 ^ 10` | `0A000 Unsupported binary operator: PGExp` | `1024` — `^` is **exponentiation** | `1024` |
| 2 | `7 / 2` | `3` — integer division truncates | `3.5` — **float division** | `3` |
| 3 | `0.1 + 0.2` | `0.30000000000000004` — literals are `Float64` | `0.3` — literals are `DECIMAL` | `0.3` |
| 4 | `s ~ '^a'` | matches — **partial** match | no match — DuckDB `~` is `regexp_full_match` | matches |
| 5 | `s LIKE 'a\_b'` | matches — `\` is the default escape | no match — DuckDB has **no default escape** | matches |
| 6 | `s ^@ 'a'` | `0A000 Unsupported binary operator: PGStartsWith` | matches — DuckDB supports `^@` | matches |

The splits fall into two shapes, and the parser unification moved case 1 from the
first shape to the second:

- **Cases 2–5 diverge silently** — both paths answer, with different answers.
- **Cases 1 and 6 diverge loudly** — the `SELECT` is rejected `0A000` while the
  `UPDATE` carrying the identical predicate succeeds. Still a split, but a client
  cannot act on a wrong answer it never received.

### The two consequences, both reproduced

**A row you cannot `SELECT` can still be `UPDATE`d.** With one row where
`num = 0.3`, written by `INSERT … VALUES (0.1 + 0.2)`:

```sql
SELECT count(*) FROM litx WHERE id = 1 AND num = 0.1 + 0.2;          -- 0
UPDATE litx SET txt = 'matched' WHERE id = 1 AND num = 0.1 + 0.2;    -- UPDATE 1
```

Cases 2 and 3 produce this direction silently, and cases 1 and 6 produce it with
the `SELECT` erroring instead of returning zero rows. Either way it defeats the
universal safety practice of previewing a mutation with a `SELECT` before running
it: the preview is not the same query. Case 1 is the sharpest — before the parser
unification, `UPDATE t SET … WHERE 2 ^ 10 = 1024` updated all 3 rows of a table
whose `SELECT … WHERE 2 ^ 10 = 1024` returned none; the `UPDATE` still applies, and
the `SELECT` now fails rather than lying.

**A value written by an expression cannot be found by that expression.**
Cases 2 and 3 make the write path compute a *different value* than the read path,
with no error on either side:

```sql
INSERT INTO opx (id, n) VALUES (4, 7 / 2);   -- DuckDB stores 3.5
SELECT 7 / 2;                                -- DataFusion returns 3
```

Cases 4 and 5 run the other way — the `SELECT` matches rows the `UPDATE`/`DELETE`
cannot reach, so a mutation silently under-applies and reports `UPDATE 0`.

### `^` is the cheapest of the six to fix

`^` is the only case where **PostgreSQL and DuckDB agree with each other** and
VaireDB's read path satisfies neither. It is also the only one where VaireDB
already holds the correct reading and merely fails to act on it: the single parser
produces `BinaryOperator::PGExp` — exponentiation, exactly what PostgreSQL and
DuckDB mean — and DataFusion's planner then rejects that node because it maps `^`
to `Operator::BitwiseXor` instead (interchangeable with `#`, per its own operators
page). Rewriting `PGExp` to `power()` in the read-path AST transform closes the
split in both directions without reinterpreting anyone's semantics. It needs no
unusual types, no NULLs and no edge-case data to trigger, and any client that has
ever written `x ^ 2` hits it.

Cases 2, 4 and 5 are the inverse shape: DataFusion is **PostgreSQL-correct** and
DuckDB is the outlier, so the *read* path is right and the *write* path is wrong.
That matters for remediation — 1 and 3 must be fixed in the read path, 2/4/5/6 in
the write path.

## Master operator table

Read-path results are what the coordinator returned; write-path results are what
DuckDB did with the same fragment in an `UPDATE … WHERE`. `—` means not
separately probed because the read path already rejects it at parse (E1), so it
cannot reach DuckDB either.

### Numerical operators

| Operator | Read path | Write path | Verdict | Notes |
|---|---|---|---|---|
| `+` `-` `*` | ✅ | ✅ | ✅ | Incl. unary `-`/`+`. |
| `/` | `7/2` → `3` | `7/2` → `3.5` | ⛔ | **Split #2.** DataFusion truncates (PG-correct); DuckDB does float division. DuckDB documents this divergence itself. |
| `%` | ✅ `-7 % 3` → `-1` | ✅ | ✅ | Sign follows dividend, PG-aligned. Accepts floats (`7.5 % 2` → `1.5`). `% 0` → `NULL` in DuckDB vs error in PG. |
| `^` | `0A000 Unsupported binary operator: PGExp` | `2^10` → `1024` | ❌ | **Split #1.** The parser yields `PGExp` (exponentiation, PG- and DuckDB-correct); DataFusion's planner maps `^` to `BitwiseXor` and rejects the node. Before the parser unification this silently returned `8`. Use `power()`, or rewrite `PGExp` → `power()` — see [remediation #1](#prioritized-remediation). |
| `**` | `42601` | — | ❌ | Dies at E1. **No sqlparser 0.61 dialect parses `**`** (PG, Generic, DuckDB, SQLite all reject it), so this needs upstream work, not a dialect change. DuckDB itself supports it. |
| `//` | `42601` | — | ❌ | Dies at E1 because `PostgreSqlDialect` rejects it; `Generic` and `DuckDbDialect` both accept it. **Dialect-gated** — the single parser uses PG's dialect, so this stays unreachable. DataFusion's `Operator::IntegerDivide` is "not yet supported" anyway. |
| `DIV` | `42601` | — | ❌ | Dies at E1. No sqlparser 0.61 dialect parses it. |

### Comparison operators

| Operator | Read path | Write path | Verdict | Notes |
|---|---|---|---|---|
| `=` `<>` `!=` `<` `<=` `>` `>=` | ✅ | ✅ | ✅ | DuckDB implicitly casts across types PG would reject (`'1.1' = 1` → true), so a write predicate can succeed where a read predicate errors. |
| `<=>` | ✅ `NULL <=> NULL` → true | not probed | 🟡 | MySQL null-safe equality; **no PostgreSQL equivalent**. In DuckDB `<=>` is *vector distance* on `FLOAT[]`, so on a float-array column the two paths would diverge. |
| `IS [NOT] DISTINCT FROM` | ✅ | ✅ | ✅ | PG-aligned on both. |
| `~` `!~` | ✅ **partial** match | **full** match | ⛔ | **Split #4.** DuckDB's `~` is `regexp_full_match`; even `'abcd' ~ '^ab'` is false there. A write predicate silently matches nothing. |
| `~*` `!~*` | ✅ | ❌ DuckDB has no `~*` | 🟡 | Read-only. A write predicate fails loudly in the shard. |
| `~~` `~~*` `!~~` `!~~*` | ✅ | ✅ | ✅ | LIKE/ILIKE aliases; identical mapping in DataFusion and DuckDB. |
| `LIKE` `ILIKE` `NOT LIKE` `NOT ILIKE` | ✅ | 🟡 | ⛔ | **Split #5.** `ESCAPE c` is honored on both paths, but the *default* escape differs: `\` in DataFusion/PG, none in DuckDB. Every ORM that escapes `_`/`%` in user input emits exactly this shape. |
| `SIMILAR TO` | `'abc' SIMILAR TO 'a%'` → **false**; `'a.*'` → **true** | same | ⛔ | Both engines implement it as a **POSIX regex**; PostgreSQL uses SQL wildcards, where `%` matches anything and `.` is literal. Wrong in **both** directions — under-matches on `%`, over-matches on `.`. Consistent read/write, so no split, but silently non-PG. `SIMILAR TO … ESCAPE` is unimplemented in DuckDB. |
| `BETWEEN` / `NOT BETWEEN` | ✅ | ✅ | ✅ | `BETWEEN SYMMETRIC` is unimplemented in DuckDB. |
| `IN (list)` / `NOT IN` | ✅ | ✅ | ✅ | NULL semantics PG-aligned. |
| `IS NULL` / `IS NOT NULL` | ✅ | ✅ | ✅ | |
| `IS TRUE/FALSE/UNKNOWN` (+`NOT`) | ✅ | ✅ | ✅ | Full PG set present. |

### Logical and bitwise operators

| Operator | Read path | Write path | Verdict | Notes |
|---|---|---|---|---|
| `AND` `OR` `NOT` | ✅ | ✅ | ✅ | Three-valued logic correct on both. |
| `&` `\|` `<<` `>>` | ✅ | ✅ | ✅ | `5 << 3` → 40, `5 >> 3` → 0. |
| `#` (bitwise XOR) | ✅ `5 # 3` → `6` | — | ✅ | **Fixed by the parser unification.** Previously `XX000 ParserError("No infix parser for token Sharp")`: PG's dialect parsed it, then DataFusion's **Generic** dialect re-parsed the render and had no infix `#`. Now the PG-parsed node goes straight to the planner, which implements it. DuckDB has no `#` (it uses `xor()`), so this is read-path only. |
| unary `~` (bitwise NOT) | `0A000 Unsupported SQL unary operator BitwiseNot` | — | ❌ | DataFusion handles only `NOT`, `+`, `-` as unary. DuckDB and PG both support `~`. |

### Other operators

| Operator | Read path | Write path | Verdict | Notes |
|---|---|---|---|---|
| `\|\|` (string) | ✅ NULL-propagating | ✅ | ✅ | `'a' \|\| NULL` is NULL on both; `concat('a', NULL)` is `'a'`. PG-aligned. |
| `\|\|` (array) | ✅ `array_concat` | 🟡 | 🟡 | DataFusion also overloads it as append/prepend by dimension; DuckDB rejects element-to-list `\|\|` that PG accepts. |
| `@>` `<@` | ✅ arrays | ✅ | ✅ | Arrays only. Neither engine supports the `jsonb`/range/`inet` overloads PG has. |
| `&&` (overlap) | `0A000 Unsupported binary operator: PGOverlap` | — | ❌ | DuckDB has it (`list_has_any`); DataFusion does not. |
| `^@` (starts with) | `0A000 … PGStartsWith` | ✅ matches | ⛔ | **Split #6.** Works in `UPDATE`/`DELETE`, rejected in `SELECT`. Use `starts_with()`. |
| `->` `->>` `#>` `#>>` `@?` | `XX000 … Operator -> is not yet supported` | not reachable — `::json` fails at planning | ❌ | Present in DataFusion's `Operator` enum but unimplemented in type coercion. DuckDB *does* implement `->`/`->>`. **The whole `jsonb` operator family is absent.** Note `XX000` — should be `0A000`. |
| `@@` | `0A000` with a raw `Status { code: InvalidArgument … }` gRPC payload in the message | — | ❌ | Error-hygiene bug: internal transport detail leaks to the client. |
| `::` / `CAST` / `TRY_CAST` | ✅ | ✅ | ✅ | `TRY_CAST` returns NULL on failure. `CAST(x AS T ARRAY)` → `42601`. |
| `AT TIME ZONE` | ✅ | not probed | ✅ | Semantics agree between the engines. |
| `COLLATE` | Parsed, **silently ignored** | ignored | ⛔ | `SELECT 'B' COLLATE "en_US" < 'a'` → **true**. PostgreSQL under `en_US` says false. Neither engine applies the collation, and neither reports that it didn't. Also means `ORDER BY` on text is byte order, not locale order. |
| `arr[n]` | ✅ 1-based | — | ✅ | Base is PG-aligned. |
| `arr[-1]` | → last element | — | ⛔ | PostgreSQL returns `NULL`; DataFusion and DuckDB both return the last element. Consistent read/write, silently non-PG. |
| `arr[a:b]` | ✅ 1-based inclusive | — | ✅ | |
| `str[n]`, `str[a:b]` | `0A000 array_element does not support type Utf8` | — | ❌ | DuckDB supports string subscripting; PG does not. |
| `struct['field']` | ✅ | — | ✅ | `ROW(1,2)['c0']` → 1. |
| `struct.field` | `XX000 Dot access not supported for non-string expr` | — | ❌ | Both PG (with parens) and DuckDB support it. |
| `ANY (array)` | ✅ | — | ✅ | `2 = ANY(ARRAY[1,2,3])` → true. |
| `ANY (subquery)` | `0A000 array_has does not support type Int64` | — | ❌ | `= ANY (SELECT …)` — ordinary PostgreSQL — is misrouted to `array_has`. |
| `ALL (array)` | `XX000 ALL only supports subquery comparison currently` | — | ❌ | |
| `LIKE ANY (…)` | `XX000 ANY in LIKE expression` | — | ❌ | |
| `EXISTS` / `IN (subquery)` **in `WHERE`** | ✅ | — | ✅ | The optimizer decorrelates these into joins before plan serialization, so the proto limitation below never bites. Correlated `EXISTS` works. |
| `EXISTS` / `IN (subquery)` **in the SELECT list** | `XX000 failed to serialize logical plan: … Expr::Exists { .. } not supported` | — | ❌ | Not decorrelated, so it reaches `datafusion-proto`, which cannot encode subquery expressions. `SELECT EXISTS (SELECT 1 FROM t)` and `SELECT 1 IN (SELECT 1)` both fail. **Scalar** subqueries in the SELECT list *do* work (`scalar_subquery_to_join` decorrelates them). The error also advises the user to file a bug with the DataFusion project. |
| `GLOB` / `~~~` | `42601` | — | ❌ | DuckDB-only, dies at E1. |
| `1 / 0` | `XX000 … ArrowError(DivideByZero)` | DuckDB returns `inf` | 🟡 | PG raises `22012 division_by_zero`. Wrong SQLSTATE on read, and a *value* instead of an error on write. |
| `1.0 / 0` | `inf` | `inf` | ⛔ | PostgreSQL raises `division_by_zero` for the numeric case. A poison value flows into aggregates instead. |

## Master literal table

| Literal | Read path | Verdict | Notes |
|---|---|---|---|
| `'…'`, `''` escape | ✅ | ✅ | |
| `E'…'` | ✅ real newline | ✅ | DuckDB lacks `\uXXXX` inside `E'…'`. |
| `$$…$$`, `$tag$…$tag$` | ✅ | ✅ | Undocumented in DataFusion, but works. |
| `U&'\0041'` | `0A000 Unsupported Value 'UnicodeStringLiteral'` | ❌ | Unimplemented in both engines. |
| `N'foo'` | `0A000 Unsupported Value 'NationalStringLiteral'` | ❌ | |
| `B'1010'` | `0A000 Unsupported Value 'SingleQuotedByteStringLiteral'` | ❌ | DuckDB silently turns `B'1010'` into the **string** `'b1010'` — the loud rejection here is the better behavior. |
| `X'DEADBEEF'` | → `Binary` | 🟡 | PG gives `bit(32)`; DuckDB silently gives the VARCHAR `'xDEADBEEF'`. Three engines, three answers. |
| `0x1F` | → **`Binary`** (bytes `1f`) | ⛔/🟡 | PG 16 gives the integer `31`. DuckDB silently parses `0x1F` as `0 AS x1F`. On the **write** path the W4 render rewrites `0x1F` to `X'1F'`, which fails in the shard: `42804 Could not convert string 'x1F' to DOUBLE`. Loud on write, silently a byte string on read. That render is irreducible (DuckDB is handed SQL text), so unification did not change this. |
| `0b101` | → **`0`** | ⛔ | Not a parse error: sqlparser tokenizes it as `0` with the alias `b101`, so the AST handed to the planner is `SELECT 0 AS b101`. PG 16 returns `5`. DuckDB mangles it the same way. Confirmed in all four sqlparser dialects and both versions — a single-parser tokenizer defect, unaffected by unification. |
| `1_000` (underscores) | `XX000 [VDB-5001] ParserError("Cannot parse 1_000 as f64")` | 🟡 | PG's dialect keeps `1_000` as one `Number` token; DataFusion's expression planner then parses number literals with `parse::<f64>()` and rejects the underscore. PG 16 and DuckDB both return `1000`. Before the parser unification this **silently returned `1`**, because `Dialect::Generic` re-parsed the render as `1` aliased `_000`. Now loud — but the SQLSTATE should be `42601`, not `XX000`. |
| `1e3`, `.5` | ✅ | ✅ | |
| `1.5` (decimal literal) | → **`Float64`** | ⛔ | **Split #3.** PostgreSQL types unsuffixed decimals as exact `numeric`; DuckDB as `DECIMAL`. DataFusion's `parse_float_as_decimal` defaults to false, so `0.1 + 0.2 = 0.3` is **false** on read and **true** on write. |
| `123456789012345678901234567890` | → `Float64`, reads back `123456789012345680000000000000` | ⛔ | Silent precision loss on any integer literal beyond `i64`/`u64`. PG uses `numeric`, DuckDB `HUGEINT`. |
| `TRUE` / `FALSE` / `NULL` | ✅ | ✅ | |
| `'\xDEADBEEF'::bytea` | → the **10 ASCII bytes** of the literal text | ⛔ | PostgreSQL's hex-escape input format is not decoded: DataFusion casts `Utf8`→`Binary` bytewise. PG yields 4 bytes; DuckDB's `::BLOB` *does* decode `\x`. Cross-check `gap-analysis-data-type.md`, which rates `Binary` clean — that holds for parameterized writes, not for this literal form. |
| `DATE '2024-01-15'` | ✅ `Date32` | ✅ | |
| `TIME '12:34:56'` | ✅ `Time64(ns)` | ✅ | Column-level `TIME` is a separate gap — see the data-type analysis. |
| `TIMESTAMP '…'` | ✅ `Timestamp(ns)` | 🟡 | Nanosecond typing bounds literals to 1677–2262; an out-of-range literal fails in `simplify_expressions`. Already tracked in `gap-analysis-data-type.md`. |
| `TIMESTAMPTZ '…+02'` | ✅ normalized to UTC | 🟡 | The offset is not shown back to the client. |
| `INTERVAL '1 day'`, `INTERVAL '1' DAY`, `INTERVAL 1 DAY` | ✅ `Interval(MonthDayNano)` | ✅ | The unquoted `INTERVAL 1 DAY` spelling is DuckDB-only — PG rejects it, VaireDB accepts it. Rendering normalizes to `14 mons 3 days` rather than PG's `1 year 2 mons 3 days`. |
| `INTERVAL '1-2' YEAR TO MONTH` | `0A000 Unsupported Interval Expression with last_field` | ❌ | Unimplemented in both engines; PG supports it. |
| `ARRAY[1,2,3]`, `[1,2,3]` | ✅ `List(Int64)` | ✅ | |
| `'{1,2,3}'::INT[]` | ✅ `List(Int32)` | 🟡 | PostgreSQL's canonical array **text** form works on read. On write DuckDB 1.5.5 accepts it but renders lists as `[1, 2, 3]` where PG/DataFusion render `{1,2,3}`. |
| `ROW(1,2)`, `STRUCT(1,2)` | ✅ → struct, fields `c0`, `c1` | ✅ | |
| `{'a': 1}` | `42601` | ❌ | **Dialect-gated at E1.** DataFusion supports brace struct literals; sqlparser's PG dialect — the single parser's dialect — refuses them. |
| `MAP {'a': 1}` | `42601` | ❌ | Same cause. |
| `MAP(['a'],[1])` | `XX000 Unsupported Datatype Map(…)` | ❌ | Parses and plans; arrow-pg cannot map `Map` to a PG OID. Same root cause as the `Map` row in the data-type analysis. |
| `'{"a":1}'::json` | `0A000 Unsupported SQL type JSON` | ❌ | `JSON` works as a *column* type but not in a `CAST`, which is why every JSON-operator probe is unreachable. |
| `'…'::uuid` | `0A000 Unsupported SQL type UUID` | ❌ | Same shape as JSON. |
| `'abcde'::VARCHAR(2)` | → `'abcde'`, type `Utf8` | ⛔ | The length is silently discarded — no truncation, no error. PG truncates on explicit cast. |
| `$1` placeholders | ✅ | ✅ | Well covered by `extended_protocol.rs`. |

## Root causes

Five defects account for every ⛔ and most 🟡 rows:

1. **DataFusion's planner maps `^` to `Operator::BitwiseXor`** and does not
   implement the `PGExp` node its own parser's PG dialect produces, while
   PostgreSQL and DuckDB both define `^` as exponentiation. Read-path fix only, and
   VaireDB's read-path AST transform is the natural owner.
2. **Decimal and large-integer literals are typed `Float64`** by DataFusion's
   default `parse_float_as_decimal = false`, while DuckDB uses `DECIMAL`/`HUGEINT`.
   Causes split #3 and the precision loss.
3. **`transform_to_duckdb` never translates operators** — `dialect.rs:58` skips
   `UPDATE`/`DELETE` `WHERE` clauses entirely and `dialect.rs:93` only rewrites
   `Expr::Function`. This is the single fix point for splits #2, #4, #5 and #6:
   there is no PG→DuckDB operator rewrite layer at all.
4. **The surviving W4 render is lossy.** `write_router.rs:94` rewrites the client's
   literal text on its way to DuckDB. Caught empirically: `0x1F` becomes `X'1F'`
   and then fails in the shard. Irreducible as long as shards are handed SQL text
   rather than a plan.
5. **`COLLATE` is parsed and discarded** with no diagnostic, by both engines.

**Resolved by the parser unification** (kept here because the analysis above and
several roadmap items were written against it): the read path used *two* sqlparser
dialects — VaireDB's own `PostgreSqlDialect` and DataFusion's default
`Dialect::Generic` — so the reachable surface was their intersection, and two of
the three lossy `.to_string()` round-trips sat between them. That cost `#`
outright, silently truncated `1_000`, and silently converted `^` from
exponentiation to XOR. There is now one parser, one dialect, and no read-path
render.

### SQLSTATE mapping is inconsistent

Probing surfaced a systematic error-code problem, independent of any operator gap.
The same *class* of failure reaches the client under different SQLSTATEs depending
on which pipeline stage raises it:

| Failure | Observed | Correct PG code |
|---|---|---|
| Syntax error at the parse (`**`, `DIV`, `GLOB`, `CAST(… AS T ARRAY)`) | `42601` ✅ | `42601` |
| Literal the planner cannot convert (`1_000`) | **`XX000`**, message `ParserError("Cannot parse 1_000 as f64")` | `42601` |
| Unsupported operator at logical planning (`^`, `^@`, `&&`) | `0A000` ✅ | `0A000` |
| Unsupported operator at type coercion (`->`, `@?`) | **`XX000`** | `0A000` |
| Cast failure (`'x'::int`) | **`42804`**, message `Optimizer rule 'simplify_expressions' failed` | `22P02` |
| Division by zero | **`XX000`**, message `ArrowError(DivideByZero)` | `22012` |
| `@@` | `0A000` with a raw gRPC `Status { … }` in the message | `0A000` |

`XX000` is `internal_error`. A client that retries on `XX000` and reports `42601`
to the user will do the wrong thing for half these cases, and three of the
messages leak DataFusion or gRPC internals (including one that asks the user to
file a bug against DataFusion).

### Latent: filter pushdown will break the moment it is enabled

Not a gap today, but it belongs here because it is the same
DataFusion-`Expr`-to-DuckDB-SQL boundary. `SchedulerTableProvider`
(`scheduler/scheduler.rs:319`) does not implement `supports_filters_pushdown`, so
DataFusion's default returns `Unsupported` for every predicate and `filters` is
always empty — **no predicate is ever pushed to DuckDB.** (Note that
`docs/vairedb.io/docs/concepts/query-processing.md` claims filter push-down
happens; for the DuckDB source it does not.) `_limit` is likewise discarded
(`scheduler.rs:338`), so `LIMIT 1` full-scans every shard.

The dormant code path is wrong, however. `scheduler.rs:346` serializes filters
with `filters.iter().map(|f| f.to_string())` — `Display for Expr`, DataFusion's
*plan-display* format, whose literals go through `ScalarValue`'s `Debug`. So
`col("id").gt(lit(10))` renders as `id > Int32(10)`, and `col("s").eq(lit("a"))`
as `s = Utf8("a")` — neither is valid DuckDB SQL, and columns render
fully-qualified against a table name that does not exist on the shard. There is
no match on `Expr` variants and no error for unsupported ones. The only test
asserts `contains("id")` and `contains("10")` (`tests/scheduler_tests.rs:318`),
so it passes on the broken output. Whoever implements `supports_filters_pushdown`
must replace this with `datafusion::sql::unparser` and a rejection list — and
must not push any operator in the ⛔ table, since pushing `^` or `LIKE` would
change results.

## Prioritized remediation

1. **Fix `^`** — the only case where PostgreSQL and DuckDB agree and VaireDB's
   read path satisfies neither, and the easiest to hit. Rewrite the `PGExp` binary
   op to a `power()` call in the read-path AST transform (`sql_compat/dialect.rs`),
   which is already the right hook and already runs on every SELECT. Cheap,
   self-contained, and closes split #1 in both directions. The parser unification
   made this strictly simpler: the AST now carries `PGExp`, so the rewrite
   *preserves* the parsed meaning instead of overriding DataFusion's XOR reading.
2. **Add a PG→DuckDB operator rewrite layer to the write path** — the single fix
   for splits #2, #4, #5 and #6. `transform_exprs_in_statement` (`dialect.rs:58`)
   must (a) visit `UPDATE`/`DELETE` `WHERE` clauses and nested expressions, and
   (b) rewrite `Expr::BinaryOp`, not just `Expr::Function`: `/` on integers →
   `//` or an explicit cast, `~`/`!~` → `regexp_matches`, `LIKE` → an explicit
   `ESCAPE '\'`. Without this, no amount of read-path work makes reads and writes
   agree.
3. ~~**Set `datafusion.sql_parser.dialect` to PostgreSQL**~~ — **done, and
   superseded.** Rather than aligning the second parser's dialect, the second parse
   was removed: `PostgresCompatibilityParser` is now the only parser, and its AST is
   planned directly (`statement_to_plan`) instead of being re-rendered for
   DataFusion to re-parse. `datafusion.sql_parser.dialect` is moot on the read path
   — DataFusion no longer parses read-path SQL at all — and the whole class of
   latent failure where PG's dialect accepted a token that `Generic` then rejected
   with `XX000` or silently re-read as an alias is gone with it. This recovered `#`
   and converted the `1_000` and `^` silent-wrong answers into honest errors; see
   [what unification changed](#what-the-parser-unification-changed).
4. **Decide the decimal-literal policy** — either enable
   `parse_float_as_decimal` (aligning read with write and with PostgreSQL, at the
   cost of decimal-vs-float coercion changes throughout the read path), or reject
   the split explicitly. This one is a real trade-off and should be an explicit
   decision, not a default. It interacts with the `Decimal128` work in
   `gap-analysis-data-type.md` and should land with it.
5. **Fix the SQLSTATE mapping** — `42601` for every syntax error regardless of
   stage, `0A000` for every unsupported operator, `22012` for division by zero,
   `22P02` for cast failures. Strip the gRPC `Status` and the "file a bug with
   DataFusion" advice from client-visible messages. Independent of every item
   above and improves every error a client sees today.
6. **Reject `COLLATE` instead of ignoring it** (`0A000`) until collations are
   real. A silently-ignored `COLLATE` is worse than an unsupported one, because
   `ORDER BY` results look plausible and are in byte order.
7. **Make `CAST(… AS JSON/UUID)` work** — both types already exist as column
   types, so this is a planner type-name gap that currently makes the entire JSON
   operator surface unreachable.
8. **Close the cheap read-path rejections** — unary `~`, `&&`, `^@`,
   `struct.field`, `= ANY (subquery)`, `ALL (array)`. `^@` and `= ANY (subquery)`
   rank highest: the first is a live split, the second is ordinary PostgreSQL that
   a client would not expect to fail.
9. **Warn or reject on discarded cast lengths** (`VARCHAR(n)`) and on `arr[-1]`,
   or document them as intentional DuckDB-flavored divergences.
10. **Before implementing filter pushdown**, replace `Expr::to_string()` with
    `datafusion::sql::unparser` plus an explicit allow-list, and exclude every
    operator in the ⛔ table.

## Executable counterpart

There is **no operator or literal coverage today** beyond four incidental tests
(`||` and `ILIKE` in `data_types_dialect_gaps.rs:121`/`:101`, `BETWEEN` in
`sql_command_select.rs:310`, `CAST` as an error probe in `errors.rs:148`). No test
anywhere issues a `SELECT` without a `FROM`, which is the cheapest probe axis for
pure expression semantics — it needs no DDL and no shard setup.

Proposed suite, following the `<doc-topic>_<facet>.rs` convention established by
`sql_command_*` and `data_types_*`:

| File | Contents |
|---|---|
| `sql_expression_operators.rs` | The master operator table, one test per row, FROM-less where possible. |
| `sql_expression_literals.rs` | The master literal table. |
| `sql_expression_split_brain.rs` | The six splits. Each is a **paired** test: the same predicate through `SELECT` and through `UPDATE`, asserting they agree. For splits #1 and #6 the current `SELECT` half is a `0A000` rejection, so the passing test pins that and the `#[ignore]`d one asserts agreement. These are the highest-value tests in the whole suite. |

Follow the existing convention: a **passing** test pinning today's honest
rejection (`assert_unsupported` for `0A000`, `assert_rejected` where the stage is
uncertain), plus an `#[ignore = "gap (<section>): …"]` test asserting the
PostgreSQL-correct behavior, which fails by construction and is the definition of
done. The ⛔ rows need the ignored test to assert the **correct value**, not merely
that no error occurred — a silent-wrong-answer gap is invisible to an
error-shape assertion.

```sh
cd tests/e2e && cargo test --test sql_expression_operators -- --ignored --test-threads=1
```

## How this was measured

Empirically, against the live 5-node e2e cluster (`make e2e-up`, DuckDB v1.5.5 on
the core nodes), over `psql` on the coordinator's PostgreSQL wire port — not by
reading capability tables. 77 operator and literal fragments were probed:

1. **Read path, FROM-less** — `SELECT <expr>`, which `extract_select_table_name`
   (`query_router/query_router.rs:93`) resolves to no table, so it plans on
   `session_ctx` and isolates pure DataFusion semantics.
2. **Read path, table-backed** — the same fragment in a `WHERE` over a 3- and
   5-shard table, confirming the FROM-less result holds under distribution.
3. **Write path** — the same fragment in an `UPDATE … WHERE` and in
   `INSERT … VALUES`, with the result read back, which is what isolates DuckDB's
   answer from DataFusion's.
4. **`arrow_typeof(<literal>)`** for every literal form, to see the Arrow type
   DataFusion assigns rather than inferring it from the rendered text.
5. **`VERBOSITY=verbose`** on every probe, to capture the SQLSTATE and the
   `[VDB-NNNN]` enrichment code and so attribute each failure to a pipeline stage.

DuckDB's standalone semantics were cross-checked against a local DuckDB CLI, which
corrected three assumptions taken from documentation — notably that `^` is
exponentiation in DuckDB (so the `^` divergence is DataFusion's alone), that
`*~~`/`*~~*` do not exist in DuckDB, and that `-2^2` is `4` in both PG and DuckDB
rather than being a divergence.

Every ⛔ row and all six splits were reproduced at least twice.

After the parser unification the affected rows were re-probed the same way, and
the dialect-sensitive cases were additionally reduced to a standalone harness that
parses one fragment under `PostgreSqlDialect` and `Dialect::Generic` and compares
**both** the rendered text and the AST. That last part matters: `^` renders to
byte-identical text under both dialects while producing different AST nodes, so a
render-only comparison reports "no difference" on the very case that was silently
wrong.
