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

- **DataFusion:** [Operators and Literals](https://datafusion.apache.org/user-guide/sql/operators.html) — 32 documented operators plus a Literals section. DataFusion **54.1** (`Cargo.toml:33`).
- **DuckDB:** [Pattern matching](https://duckdb.org/docs/current/sql/functions/pattern_matching.html), [Numeric operators](https://duckdb.org/docs/current/sql/functions/numeric.html), [Literal types](https://duckdb.org/docs/current/sql/data_types/literal_types.html), and DuckDB's own [PostgreSQL compatibility](https://duckdb.org/docs/current/sql/dialect/postgresql_compatibility.html) page. DuckDB **v1.5.5**, bundled by `duckdb` 1.10505.0.
- **PostgreSQL:** [Math functions and operators](https://www.postgresql.org/docs/current/functions-math.html), [Lexical structure](https://www.postgresql.org/docs/current/sql-syntax-lexical.html).
- **Compatibility layer:** [`datafusion-pg-functions`](https://github.com/datafusion-contrib/datafusion-postgres/tree/master/datafusion-pg-functions) — the PostgreSQL built-ins DataFusion does not ship, **0.1.0** (`math` category only; see below).
- **Sibling analyses:** [`gap-analysis-data-type.md`](gap-analysis-data-type.md), [`gap-analysis-command.md`](gap-analysis-command.md), [`gap-analysis-aggregate-function.md`](gap-analysis-aggregate-function.md). All five are consolidated in [`gap-analysis.md`](gap-analysis.md).

## Scope and framing

The two sibling analyses each had a single spine: one type system to map, one
statement list to route. Operators have **two**, because VaireDB evaluates
expressions in two different engines depending on the *statement kind*:

- **On a `SELECT`, DuckDB sees a client expression only where both engines agree on
  it.** The core node runs `SELECT <cols> FROM <shard_table>` plus the pushed-down
  predicate and limit (`vairedb-core/src/table_provider/scan_exec.rs`,
  `build_query`). Every operator and literal is still evaluated by **DataFusion** on
  the Ballista executor — that evaluation is what decides the answer — but a copy of
  the simplest predicate shapes also runs on the shard, to avoid streaming rows the
  query will discard. The allow-list that decides which shapes qualify is derived from
  the ✅/⛔ columns of the table below, so no operator the two engines read differently
  is ever pushed. See [the closed pushdown
  section](#-closed-filter-pushdown-which-would-have-broken-the-moment-it-was-enabled).
- **On an `INSERT`/`UPDATE`/`DELETE`, DataFusion never sees the expression.** The
  statement is re-rendered and executed **verbatim by DuckDB**
  (`write_router/write_router.rs:94` → `vairedb-core/src/write_service/write_service.rs:89`).

So the same SQL fragment has **two independent verdicts**, and where DataFusion and
DuckDB disagree on an operator's meaning, VaireDB would return *both* answers — one on
read, one on write. That was not a theoretical concern:
[nine confirmed cases](#the-split-brain-defect) existed, where a `SELECT` and an
`UPDATE` carrying the **identical predicate** diverged. All nine are closed as of
v0.2, because the write path now has its own expression layer (W3 below) mirroring the
read path's (E2). **One new split opened** in the process, and it is instructive about
how: it came from closing a divergence by changing a DuckDB *setting* rather than by
translating the expression.

The two-verdict structure is not something a fix removes, though. It is a property of
executing reads in DataFusion and writes in DuckDB, so every operator added on either
side has to be checked on both — which is what the two verdict columns in the master
tables are for.

### The parse chain

A read-path expression is parsed **once** and never re-rendered:

| # | Stage | Parser / dialect | Code |
|---|---|---|---|
| E1 | The single parse: `PostgresCompatibilityParser` — tokenize, substitute the pg-compat blacklist, parse, apply 12 rewrite rules. Plus the one expression check that has to happen here, because rule 8 (`StripCollate`) destroys what it looks at: a non-byte-order `COLLATE` is refused against a parse of the client's own text | sqlparser **0.62**, `PostgreSqlDialect` | `pgwire_handler/parser.rs` → `pgwire_handler::parser::parse_sql` |
| E2 | AST rewrites (`to_char` format, schema collapse) and the **PostgreSQL expression layer** — rewrite the operators DataFusion has no node for, refuse the ones it would half-ignore | — | `pgwire_handler/parser.rs:301`, `pgwire_handler/pg_operators.rs` |
| E3 | Plan **from that AST** — `statement_to_plan(DFStatement::Statement(…))` | — no parse | `parser.rs:322` (`plan_select`, shared by both protocols) |
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
| W1 | E1 as above — the same single parse | `pgwire_handler::parser::parse_sql` |
| W2 | Shard-local relation rewrite | `write_sql_cl/mod.rs:43` |
| W3 | `transform_to_duckdb` — PG→DuckDB rewrite | `write_sql_cl/dialect.rs:13` |
| W4 | Render `.to_string()`, ship, execute verbatim on DuckDB | `write_router.rs:94` |

**W4 is irreducible** — `write_router` ships SQL *text* over gRPC for DuckDB to
execute — so the write path is 1 parse and 1 render. Nothing VaireDB owns ever
re-parses its own output.

**W3 is the write path's expression layer, and it is new in v0.2.** It used to rewrite
almost nothing: `transform_exprs_in_statement` visited only `INSERT … VALUES` rows, an
INSERT-source `SELECT`'s top-level `selection` and `UPDATE … SET` values — never an
`UPDATE` or `DELETE` `WHERE` clause — and even where it did visit, it rewrote only
`Expr::Function`. No `BinaryOp` was ever translated, so every operator in a write
statement reached DuckDB with PostgreSQL spelling and DuckDB semantics. It now visits
`WHERE` clauses and nested expressions and rewrites `BinaryOp`, which is what closed
the splits.

Two things about W3 that follow from W4 being a *render* rather than a plan
handoff, and that make it a mirror of E2 rather than a copy:

- **Every rewrite has to survive `.to_string()`.** E2 hands DataFusion a tree it will
  plan; W3 hands DuckDB text. So a fix cannot be a planner flag — `LIKE`'s missing
  default escape becomes a literal `ESCAPE '\'` clause in the emitted SQL, and the
  regex operators become function calls.
- **A refusal is a legitimate rewrite.** Where DuckDB would silently ignore what
  PostgreSQL enforces — a non-byte-order `COLLATE`, a character cast length — W3's
  counterpart refuses at the pre-translation seam in `parser.rs` instead of translating.
  Both paths then agree by both rejecting, which is agreement a client can act on.

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

### What the PostgreSQL expression layer changed

The unification left the read path holding correctly-parsed PostgreSQL nodes that
DataFusion's planner then refused, plus a handful of forms it accepted while
ignoring part of what they asked for. A rewrite stage at E2
(`pgwire_handler/pg_operators.rs`) now closes both, in the only two shapes a gap
can honestly take: **rewrite** to an equivalent DataFusion expression, or
**refuse** by name. Nine rows moved.

| Probe | Before | After | |
|---|---|---|---|
| `SELECT 2 ^ 10` | `0A000 … PGExp` | **`1024`** | ❌ → ✅ |
| `SELECT ~5` | `0A000 … BitwiseNot` | **`-6`** | ❌ → ✅ |
| `SELECT ARRAY[1,2] && ARRAY[2,3]` | `0A000 … PGOverlap` | **`t`** | ❌ → ✅ |
| `SELECT 'abc' ^@ 'a'` | `0A000 … PGStartsWith` | **`t`** | ⛔ → ✅ |
| `SELECT 'abc' SIMILAR TO 'a%'` | **`f`** — POSIX regex | **`t`** — SQL wildcards | ⛔ → read-correct |
| `SELECT 'B' COLLATE "en_US" < 'a'` | **`t`** — collation discarded | `0A000` naming `COLLATE` | ⛔ → ❌ |
| `SELECT 'abcdef'::VARCHAR(3)` | **`abcdef`** — length discarded | `0A000` naming the cast | ⛔ → ❌ |
| `SELECT 0.1 + 0.2 = 0.3` | **`f`** — `Float64` literals | **`t`** — exact `numeric` | ⛔ → ✅ |
| `SELECT 123456789012345678901234567890` | reads back `…5680000000000000` | the digits it was given | ⛔ → ✅ |

Three things are worth recording about the shape of that layer, because each was a
choice with a cheaper alternative:

**`SIMILAR TO` is translated, not passed through.** It is not a regex — `%` and `_`
are its only wildcards and every other regex metacharacter is literal — so the
pattern is compiled to an anchored regular expression at E2 and handed to
`regexp_like`. The translation reproduces PostgreSQL's own algorithm including its
quirks, because PostgreSQL is the contract. It needs the pattern as a literal, so a
pattern computed at run time is refused rather than passed through under the wrong
matching rules.

**`COLLATE` is refused selectively, and one stage earlier than the rest.** `C`,
`POSIX`, `ucs_basic` and `default` name byte order, which is what VaireDB already
does, so those are dropped without changing an answer — which is also what keeps the
driver introspection queries that sort under `COLLATE "C"` working. `default`
(`pg_catalog.default`) is the load-bearing one: it names the *database's own*
collation, which here is byte order, and it is what `psql`'s `\d` sends, so refusing
it broke table introspection for the reference client while the whole e2e suite stayed
green — the reason `test_psql_describe_table_introspection` now drives that shape.
Every other collation is refused by name. The decision cannot be made at E2 with the
others: the
compatibility parser at E1 deletes every `COLLATE` clause on its way past, so by the
time the read path holds an AST there is nothing left to refuse. It is therefore
taken at parse time, against a parse of the client's own text
(`pgwire_handler/parser.rs`, `pg_operators::reject_unsupported_collation`) — a
reminder that a rewrite stage upstream can erase the evidence a later stage would
need, and that the check has to live where the clause still exists.

**The three aggregate spellings are rewritten in the AST, not registered as
aliases.** A name registered on the coordinator's planner but not on every Ballista
executor resolves at planning time and then fails when the stage is deserialized —
after the client's query was accepted. Rewriting `variance`/`every`/`any_value` to
`var_samp`/`bool_and`/`min` at E2 means only names both sides already know cross
the wire. See [`gap-analysis-aggregate-function.md`](gap-analysis-aggregate-function.md).

**The PostgreSQL functions DataFusion does not ship come from
`datafusion-pg-functions`, registered on every context that plans or executes.** For
the same reason the aggregates are rewritten rather than aliased: a UDF crosses the
wire as a name, so it is installed on the client context, on the scheduler's own
state, and on the executor's (`scheduler/scheduler.rs`,
`vairedb-core/src/ballista_exec/executor.rs`) — a function known to the planner alone
would resolve and then fail on the node running the stage. What that buys is smaller
than the crate's documentation suggests: at **0.1.0** only the `math` category is
populated, roughly 18 UDFs, and the other categories are empty modules. Enabling
their features would register nothing while implying VaireDB had gained them, so they
are left off and revisited per category as upstream fills them in. The format and
datetime families are therefore *not* covered by this dependency.

That layer was read-path only, which changed the split-brain picture rather than
simply shrinking it. v0.2 gave the write path its mirror; see
[the split-brain defect](#the-split-brain-defect).

### What v0.2 changed

Two things: the write path got the expression layer the read path had, and three
read-path rows that were rejected for shallow reasons stopped being rejected. Eleven
rows moved.

| Probe | Before | After | |
|---|---|---|---|
| `UPDATE … SET n = 7/2` | stores `3.5` | stores **`3`** | ⛔ → ✅ |
| `UPDATE … WHERE s ~ '^ab'` | matches nothing | **matches** | ⛔ → ✅ |
| `UPDATE … WHERE s ~* '^ABCD$'` | shard error — no `~*` in DuckDB | **matches** | 🟡 → ✅ |
| `UPDATE … WHERE s LIKE 'x\_y'` | matches nothing | **matches** | ⛔ → ✅ |
| `UPDATE … WHERE s SIMILAR TO 'AB%'` | POSIX regex | **SQL wildcards** | ⛔ → ✅ |
| `UPDATE … WHERE s COLLATE "en_US" < 'a'` | answers in byte order, silently | `0A000`, the read path's message | 🟡 → 🟡, split closed |
| `UPDATE … SET s = 'abcde'::VARCHAR(2)` | stores the value whole | `0A000`, the read path's message | ❌ → ❌, split closed |
| `SELECT n = ANY (SELECT …)` | `0A000 array_has does not support type Int64` | **answers** | ❌ → ✅ |
| `SELECT 1 < ALL (ARRAY[2,3])` | `XX000 ALL only supports subquery comparison` | **`t`** | ❌ → ✅ |
| `SELECT 1_000` | `XX000 Cannot parse 1_000 as f64` | **`1000`**, typed `numeric` | ❌ → 🟡 |
| `SELECT 1 / 0` | `XX000 … ArrowError(DivideByZero)` | **`22012`** | 🟡 → ✅ on read |

Three observations about the write-path layer, since it is the mirror of E2 rather
than a copy of it:

**It rewrites an AST, but the AST is on its way to SQL text, not to a planner.** E2
hands DataFusion an expression tree it will plan. `write_sql_cl/dialect.rs` hands
DuckDB a string, so every rewrite has to survive `.to_string()` — which is why the
`LIKE` fix appends an explicit `ESCAPE '\'` clause rather than adjusting a planner
flag, and why the regex operators become function calls rather than staying operators.

**Two of the six were closed by refusing, not translating.** `COLLATE` and
`CAST(… AS VARCHAR(n))` needed no translation at all — the read path already had the
refusal, and the write path simply never ran it. The seam was one `continue` in
`parser.rs` that skipped statements wanting a verbatim AST, which is exactly the set
of write statements. Moving the check ahead of that branch covers every write
statement at a single point.

**One of the six was closed by configuration, and that is the one that opened a new
split.** Integer `/` truncation comes from DuckDB's `integer_division` setting rather
than from a rewrite, and the setting carries the rest of its semantics with it:
division by zero yielded NULL on the write path instead of `inf`. Split #10 was the
bill for that shortcut, and it was settled the way the shortcut could not be — by
checking the divisor in the re-render, since one flag decides both behaviours and there
is no second flag to separate them.

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
`AT TIME ZONE`, quantified comparisons, subquery operators) — **80 rows, every one
probed against the live 5-node cluster**:

| Verdict | Count | Rows |
|---|---:|---|
| ✅ Correct | 47 | `+ - *`, `/`, `%`, `^`, all six comparisons, `IS [NOT] DISTINCT FROM`, the whole `LIKE`/`ILIKE`/`~~` family, `~ !~ ~* !~*`, `SIMILAR TO`, `BETWEEN`, `IN`, `IS NULL`, `IS TRUE/FALSE/UNKNOWN`, `AND/OR/NOT`, `& \| << >> #`, unary `~`, `\|\|` on strings, `@> <@`, `&&`, `^@`, `::`/`CAST`/`TRY_CAST`, `AT TIME ZONE`, array subscript/slice, `struct['field']`, `ANY (array)`, `ANY (subquery)`, `ALL (array)`, `EXISTS`/`IN (subquery)` in `WHERE`, and most literal forms |
| ⛔ Silently wrong | 5 | `arr[-1]`, `0b101`, `0x1F`, `'\x…'::bytea`, and one division-by-zero row (`1.0::float8 / 0` — the integer and decimal forms now raise on both paths) |
| 🟡 Partial | 8 | `<=>`, `\|\|` on arrays, `COLLATE`, `X'…'`, `1_000`, `TIMESTAMP '…'`, `TIMESTAMPTZ '…'`, `'{1,2,3}'::INT[]` |
| ❌ Rejected | 20 | `**`, `//`, `DIV`, `-> ->> #> #>> @?`, `@@`, `GLOB`, string subscript, `struct.field`, `LIKE ANY`, subquery operators in the SELECT list, `U&'…'`, `N'…'`, `B'…'`, `INTERVAL '1-2' YEAR TO MONTH`, `{a: 1}`, `MAP {…}`, `MAP(…)`, `::json`, `::uuid`, `VARCHAR(n)` cast length |

The ✅ column is genuinely broad, and it is where v0.2's work went: the ordinary
comparison, logical, string-match, quantified-comparison and array surface now agrees
with PostgreSQL **on both paths**. What is left in ⛔ is no longer a read/write
disagreement — it is four rows where DataFusion and DuckDB agree with each other and
differ from PostgreSQL, plus one where division by zero is not an error: `float8`,
where Arrow follows IEEE 754 and there is no error to classify.

Two notes on the counts, because they are not comparable to the ones this doc carried
before. First, they are now **one row = one count**, tallied mechanically from the two
master tables below, replacing a hand-maintained "77 probes" figure that had drifted
out of agreement with those tables — the old Summary listed `~ !~` and `LIKE` in ✅
while the tables rated both ⛔, omitted `1.0 / 0` from ⛔ while the table gave it
that verdict, and counted `\|\|` once as ✅ where the tables rate the string form ✅
and the array form 🟡. Second, one row is new: `1.0::float8 / 0` split out from `1.0 / 0`,
because since split #3 an unsuffixed decimal is exact `numeric` and the two are now
typed differently and fail differently.

Counts reflect the state after the parser unification, the read path's PostgreSQL
expression layer, and v0.2's write-path mirror of it. See
[what unification changed](#what-the-parser-unification-changed),
[what the expression layer changed](#what-the-postgresql-expression-layer-changed) and
[what v0.2 changed](#what-v02-changed).

## The split-brain defect

Each of these was reproduced against the running cluster. In each, a `SELECT` and
an `UPDATE` carrying the **byte-identical predicate** select different rows, or an
`INSERT` stores a value that the same expression cannot find on read.

| # | SQL fragment | Read path (DataFusion) | Write path (DuckDB) | PostgreSQL | |
|---|---|---|---|---|---|
| 1 | `2 ^ 10` | `1024` — rewritten to `power()` | `1024` — `^` is **exponentiation** | `1024` | ✅ closed |
| 2 | `7 / 2` | `3` — integer division truncates | `3` — `integer_division` set on the shard | `3` | ✅ closed |
| 3 | `0.1 + 0.2` | `0.3` — literals are exact `numeric` | `0.3` — literals are `DECIMAL` | `0.3` | ✅ closed |
| 4 | `s ~ '^a'` | matches — **partial** match | matches — rewritten to `regexp_matches` | matches | ✅ closed |
| 5 | `s LIKE 'a\_b'` | matches — `\` is the default escape | matches — explicit `ESCAPE '\'` appended | matches | ✅ closed |
| 6 | `s ^@ 'a'` | matches — rewritten to `starts_with()` | matches — DuckDB supports `^@` | matches | ✅ closed |
| 7 | `s SIMILAR TO 'a%'` | matches — SQL wildcards | matches — same anchored translation | matches | ✅ closed |
| 8 | `s COLLATE "en_US" < 'a'` | `0A000` naming `COLLATE` | `0A000`, same message | locale order | ✅ closed |
| 9 | `CAST(s AS VARCHAR(3))` | `0A000` naming the cast | `0A000`, same message | truncates | ✅ closed |
| 10 | `n / 0` | `22012` | `22012` — the divisor is guarded in the re-render | `22012` | ✅ closed |

**All ten are closed.** Cases 1, 3 and 6
closed first, because DuckDB already agreed with PostgreSQL and only the read path
was wrong. Cases 2, 4, 5 and 7 were the expensive half — the read path was already
PostgreSQL-correct and DuckDB was the outlier, so closing them meant giving the
write path the PG→DuckDB expression layer it never had, in
`write_sql_cl/dialect.rs`. Cases 8 and 9 closed by **refusing on both paths rather
than answering on both**: they stay 🟡 and ❌ in the tables above, because agreeing
to reject is agreement, not support.

Case 10 is the one v0.2 opened, and it came directly out of closing case 2. Making
`/` truncate on the write path means setting DuckDB's `integer_division`, and that
setting also changes what division by zero does: DuckDB yields NULL rather than
raising. So `UPDATE t SET n = n / 0` reported `UPDATE 1` and stored NULL where it
used to store `inf` — both wrong against PostgreSQL, which raises `22012`, and the
new one worse in one specific way: NULL is a value a client is likely to read back
without noticing, and `inf` is not.

It closed on different terms from the other nine, because the setting cannot be asked
to fix it — one flag decides both behaviours, and DuckDB 1.5.5 has no second one
(`7/0`, `7.0/0` and `7 % 0` are all NULL with it on; nothing in `duckdb_settings()`
turns any of them into an error). So the divisor is checked where the statement is
built: every write-path `/` and `%` is re-rendered wrapped in a guard that raises
PostgreSQL's own message when the divisor is zero, and the core node classifies that
message back into `22012`. The guard does not change the result type, is evaluated per
row rather than folded at bind time, and binds a repeated placeholder once — all three
measured. It has to name the divisor twice, since `error()` inside a `CASE` is DuckDB's
only way to raise from an expression, so a divisor that is *itself* a division is
refused `0A000` rather than guarded; a division in the dividend is unaffected.

That is the honest arithmetic of fixing one path at a time. The direction is right —
nine closed for one opened, and the one opened is a narrower case than any it
replaced — but the pattern is worth naming: **each divergence closed by configuring
DuckDB rather than by translating the expression carries the rest of that setting's
semantics with it.** Case 10's fix is to translate `/` explicitly, the way `~` and
`SIMILAR TO` were translated, instead of delegating to a session flag.

### The two consequences, both reproduced

**A row you cannot `SELECT` can still be `UPDATE`d.** With one row where
`num = 0.3`, written by `INSERT … VALUES (0.1 + 0.2)`:

```sql
SELECT count(*) FROM litx WHERE id = 1 AND num = 0.1 + 0.2;          -- 0
UPDATE litx SET txt = 'matched' WHERE id = 1 AND num = 0.1 + 0.2;    -- UPDATE 1
```

Both consequences are **historical as of v0.2** — none of the nine still reproduces.
They are kept here because they are the clearest statement of why this class of
defect was ranked above every missing feature in the doc, and because case 10 is a
weaker version of the second one.

The example above is case 3. It defeats the universal safety practice of previewing a
mutation with a `SELECT` before running it: the preview is not the same query. Cases
8 and 9 produced the same shape with the `SELECT` erroring instead of returning zero
rows.

**A value written by an expression cannot be found by that expression.**
Case 2 made the write path compute a *different value* than the read path, with no
error on either side:

```sql
INSERT INTO opx (id, n) VALUES (4, 7 / 2);   -- DuckDB stored 3.5
SELECT 7 / 2;                                -- DataFusion returns 3
```

Cases 4, 5 and 7 ran the other way — the `SELECT` matched rows the `UPDATE`/`DELETE`
could not reach, so a mutation silently under-applied and reported `UPDATE 0`.

Case 10 is the surviving trace of the second shape, narrowed: the write path no
longer computes a different *number*, it computes NULL where the read path raises.

### Why cases 1, 3 and 6 were the cheap ones

Those three were the only ones where **PostgreSQL and DuckDB agreed with each other**
and VaireDB's read path satisfied neither, so a read-path fix closed the split
outright with nothing left to reconcile. Case 1 shows the general shape: VaireDB
already held the correct reading and merely failed to act on it — the single parser
produces `BinaryOperator::PGExp`, exponentiation, exactly what PostgreSQL and DuckDB
mean, and DataFusion's planner rejected that node only because it maps `^` to
`Operator::BitwiseXor` (interchangeable with `#`, per its own operators page).
Rewriting the node to `power()` at E2 reinterprets nobody's semantics.

Cases 2, 4, 5 and 7 were the inverse and the expensive shape: DataFusion — or
VaireDB's own read-path expression layer above it — is **PostgreSQL-correct** and
DuckDB is the outlier, so the read path was right and the write path was the one
standing on DuckDB's semantics. Closing them needed a write-path expression layer,
which is what `write_sql_cl/dialect.rs` now is: a mirror of E2 that rewrites the AST
on its way to the shard rather than on its way to the planner. Cases 8 and 9 needed
no translation at all, only for the read path's existing refusal to also run on write
statements — the seam was a single `continue` in `parser.rs` that skipped verbatim-AST
statements before the check.

## Master operator table

Read-path results are what the coordinator returned; write-path results are what
DuckDB did with the same fragment in an `UPDATE … WHERE`. `—` means not
separately probed because the read path already rejects it at parse (E1), so it
cannot reach DuckDB either.

### Numerical operators

| Operator | Read path | Write path | Verdict | Notes |
|---|---|---|---|---|
| `+` `-` `*` | ✅ | ✅ | ✅ | Incl. unary `-`/`+`. |
| `/` | `7/2` → `3` | `7/2` → `3` | ✅ | **Split #2, closed.** DataFusion truncates, which is PG-correct; DuckDB divides as floats. Closed not by a rewrite but by `SET GLOBAL integer_division = true` on every shard connection (`vairedb-core/src/engine/engine.rs`), which makes DuckDB's `/` integer division when both operands are integers. Verified by `UPDATE … SET n = 7/2`, which now stores `3`. The same setting is what opened split #10, since it also made a zero divisor answer NULL — closed separately, and the guard that closed it leaves this truncation intact; see the row below. |
| `%` | `-7 % 3` → `-1`; `7 % 0` → `22012` | ✅ | ✅ | Sign follows dividend, PG-aligned. Accepts floats (`7.5 % 2` → `1.5`). `7 % 0` raises `22012` on both paths, as in PG — DuckDB returns `NULL` for it, so a write's `%` is wrapped in the same zero-divisor guard as `/`; see `1 / 0` below. |
| `^` | `2^10` → `1024` | `2^10` → `1024` | ✅ | **Split #1, closed.** The parser yields `PGExp` (exponentiation, PG- and DuckDB-correct) and E2 rewrites it to `power()`; DataFusion's planner would otherwise map `^` to `BitwiseXor` and reject the node. Nests bottom-up, and stays **left**-associative as PostgreSQL specifies (`2^3^2` → 64, not 512). Before the parser unification this silently returned `8`. |
| `**` | `42601` | — | ❌ | Dies at E1. **No sqlparser 0.62 dialect parses `**`** (PG, Generic, DuckDB, SQLite all reject it), so this needs upstream work, not a dialect change. DuckDB itself supports it. |
| `//` | `42601` | — | ❌ | Dies at E1 because `PostgreSqlDialect` rejects it; `Generic` and `DuckDbDialect` both accept it. **Dialect-gated** — the single parser uses PG's dialect, so this stays unreachable. DataFusion's `Operator::IntegerDivide` is "not yet supported" anyway. |
| `DIV` | `42601` | — | ❌ | Dies at E1. No sqlparser 0.62 dialect parses it. |

### Comparison operators

| Operator | Read path | Write path | Verdict | Notes |
|---|---|---|---|---|
| `=` `<>` `!=` `<` `<=` `>` `>=` | ✅ | ✅ | ✅ | DuckDB implicitly casts across types PG would reject (`'1.1' = 1` → true), so a write predicate can succeed where a read predicate errors. |
| `<=>` | ✅ `NULL <=> NULL` → true | not probed | 🟡 | MySQL null-safe equality; **no PostgreSQL equivalent**. In DuckDB `<=>` is *vector distance* on `FLOAT[]`, so on a float-array column the two paths would diverge. |
| `IS [NOT] DISTINCT FROM` | ✅ | ✅ | ✅ | PG-aligned on both. |
| `~` `!~` | ✅ **partial** match | ✅ **partial** match | ✅ | **Split #4, closed.** DuckDB's `~` is `regexp_full_match`, so `'abcd' ~ '^ab'` was false there and a write predicate silently matched nothing. The write path now rewrites all four operators to `regexp_matches(…)`, DuckDB's partial-match function, which is what PostgreSQL's `~` means. Verified by `UPDATE … WHERE s ~ '^ab'`, which now reaches the row. |
| `~*` `!~*` | ✅ | ✅ | ✅ | DuckDB has no `~*` operator, so a write predicate used to fail loudly in the shard. Now rewritten to `regexp_matches(…, 'i')`; `UPDATE … WHERE s ~* '^ABCD$'` matches case-insensitively. |
| `~~` `~~*` `!~~` `!~~*` | ✅ | ✅ | ✅ | LIKE/ILIKE aliases; identical mapping in DataFusion and DuckDB. |
| `LIKE` `ILIKE` `NOT LIKE` `NOT ILIKE` | ✅ | ✅ | ✅ | **Split #5, closed.** Two escape gaps, one per path. The *default* escape differed — `\` in DataFusion/PG, none in DuckDB — which is the shape every ORM emits when it escapes `_`/`%` in user input. The write path now appends an explicit `ESCAPE '\'` whenever the pattern contains a backslash and no escape was given, so DuckDB is told what PostgreSQL assumes. `UPDATE … WHERE s LIKE 'x\_y'` now matches the literal underscore. A **client-chosen** escape is the mirror image: DuckDB honors any escape character, DataFusion honors only `\` and fails at execution on anything else (*"LIKE does not support escape_char other than the backslash"*), so the read path re-spells a literal pattern with backslash escaping — `'a!_%' ESCAPE '!'` becomes `'a\_%' ESCAPE '\'`, which matches the same strings. `SELECT … WHERE name LIKE 'a!_%' ESCAPE '!'` now answers. A computed pattern cannot be re-spelled before it is evaluated and is refused `0A000`. |
| `SIMILAR TO` | `'abc' SIMILAR TO 'a%'` → **true**; `'a.*'` → **false** | ✅ same | ✅ | **Split #7, closed.** `SIMILAR TO` is not a regex: `%` and `_` are its wildcards and every other regex metacharacter is literal. Both engines used to hand the pattern to a regex engine unchanged, which was wrong in **both** directions at once. E2 compiles the pattern to an anchored regex by PostgreSQL's own algorithm and calls `regexp_like`; the write path now reuses that same translation (`similar_to_regex_from_ast`) and emits `regexp_matches` with the anchored pattern, so both paths run PostgreSQL's rules rather than either engine's. A pattern that is not a literal is refused (`0A000`) on both paths rather than passed through under the wrong rules. |
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
| unary `~` (bitwise NOT) | ✅ `~5` → `-6` | — | ✅ | DataFusion handles only `NOT`, `+`, `-` as unary, so E2 rewrites `~x` to `x # -1` — exact for every two's-complement width. The one visible difference from PostgreSQL is that the result widens to `bigint`, which does not change the number. |

### Other operators

| Operator | Read path | Write path | Verdict | Notes |
|---|---|---|---|---|
| `\|\|` (string) | ✅ NULL-propagating | ✅ | ✅ | `'a' \|\| NULL` is NULL on both; `concat('a', NULL)` is `'a'`. PG-aligned. |
| `\|\|` (array) | ✅ `array_concat` | 🟡 | 🟡 | DataFusion also overloads it as append/prepend by dimension; DuckDB rejects element-to-list `\|\|` that PG accepts. |
| `@>` `<@` | ✅ arrays | ✅ | ✅ | Arrays only. Neither engine supports the `jsonb`/range/`inet` overloads PG has. |
| `&&` (overlap) | ✅ `ARRAY[1,2] && ARRAY[2,3]` → `t` | — | ✅ | E2 rewrites to `array_has_any`. PostgreSQL also defines `&&` for ranges and geometric types; arrays are the only one of the three VaireDB stores, so array overlap is the whole of what it can mean here. |
| `^@` (starts with) | ✅ matches | ✅ matches | ✅ | **Split #6, closed.** E2 rewrites to `starts_with()`, which is what DuckDB already did with `^@`. |
| `->` `->>` `#>` `#>>` `@?` | `0A000 Operator -> is not yet supported` | not reachable — `::json` fails at planning | ❌ | Present in DataFusion's `Operator` enum but unimplemented in type coercion. DuckDB *does* implement `->`/`->>`. **The whole `jsonb` operator family is absent.** The class was `XX000`; the typed classifier now returns `0A000`, so a client can distinguish "not supported" from "we broke". |
| `@@` | `0A000 Invalid function 'to_tsvector'. Did you mean 'to_timestamp'?` | — | ❌ | The raw `Status { code: InvalidArgument … }` gRPC payload that used to leak here is gone, but **not because it is sanitized** — nothing strips a `Status { … }` anywhere. The probe now fails one step *earlier*, at function resolution in the coordinator, because full-text search has no functions at all, so the plan never reaches the scheduler. Still absent, but absent with a PostgreSQL-shaped error. A `Status { … }` still leaks verbatim for any plan that *does* reach serialization and fails there — see the `format_type` row below. |
| `::` / `CAST` / `TRY_CAST` | ✅ | ✅ | ✅ | `TRY_CAST` returns NULL on failure. `CAST(x AS T ARRAY)` → `42601`. |
| `AT TIME ZONE` | ✅ | not probed | ✅ | Semantics agree between the engines. |
| `COLLATE` | `0A000` naming the collation, except for byte order | `0A000`, same message | 🟡 | **Split #8, closed.** The write path used to drop the clause silently; it now runs the same refusal, so both paths answer identically. Still 🟡 rather than ✅ because a non-byte-order collation is *refused* on both paths, not *applied*. `C`, `POSIX`, `ucs_basic` and `default` name byte order, which is what VaireDB does, so they are dropped without changing an answer — that is also what keeps driver introspection queries working, including `psql`'s `\d`, which sends `COLLATE pg_catalog.default`. Any other collation is refused: `SELECT 'B' COLLATE "en_US" < 'a'` used to answer **true** where PostgreSQL under `en_US` says false, with nothing to tell the client the collation never applied. Refused at **E1**, not E2: `StripCollate` deletes the clause during the compat parse, so the check runs against a parse of the client's own text. `ORDER BY` on text is still byte order, which is now what the accepted spellings ask for. |
| `arr[n]` | ✅ 1-based | — | ✅ | Base is PG-aligned. |
| `arr[-1]` | → last element | — | ⛔ | PostgreSQL returns `NULL`; DataFusion and DuckDB both return the last element. Consistent read/write, silently non-PG. |
| `arr[a:b]` | ✅ 1-based inclusive | — | ✅ | |
| `str[n]`, `str[a:b]` | `0A000 array_element does not support type Utf8` | — | ❌ | DuckDB supports string subscripting; PG does not. |
| `struct['field']` | ✅ | — | ✅ | `ROW(1,2)['c0']` → 1. |
| `struct.field` | `0A000 Dot access not supported for non-string expr` | — | ❌ | Both PG (with parens) and DuckDB support it. Was `XX000`; the class is now right even though the feature is still missing. |
| `ANY (array)` | ✅ | — | ✅ | `2 = ANY(ARRAY[1,2,3])` → true. |
| `ANY (subquery)` | ✅ | — | ✅ | `= ANY (SELECT …)` — ordinary PostgreSQL, and the single most commonly written form of the three — used to be misrouted to `array_has` and rejected `0A000 array_has does not support type Int64`. E2 now rewrites `x = ANY (subquery)` to `x IN (subquery)`, which the optimizer decorrelates into a semi join like any other `IN`, and the inequality forms to their `EXISTS` equivalents. In the **SELECT list** it still fails for the reason the subquery rows below give, not for this one. |
| `ALL (array)` | ✅ `1 < ALL (ARRAY[2,3])` → `t` | — | ✅ | Was `XX000 ALL only supports subquery comparison currently`. E2 expands the array form to the `CASE` PostgreSQL's semantics require — empty array is true, a NULL element with no false element is NULL — rather than to a simple `array_has`, which would get the NULL cases wrong. The expansion is what the column *label* shows, which is the cosmetic residue noted below. |
| `LIKE ANY (…)` | `0A000 ANY in LIKE expression` | — | ❌ | Was `XX000`. |
| `EXISTS` / `IN (subquery)` **in `WHERE`** | ✅ | — | ✅ | The optimizer decorrelates these into joins before plan serialization, so the proto limitation below never bites. Correlated `EXISTS` works. |
| `EXISTS` / `IN (subquery)` **in the SELECT list** | `XX000 failed to serialize logical plan: … Expr::Exists { .. } not supported` | — | ❌ | Not decorrelated, so it reaches `datafusion-proto`, which cannot encode subquery expressions. `SELECT EXISTS (SELECT 1 FROM t)` and `SELECT 1 IN (SELECT 1)` both fail. **Scalar** subqueries in the SELECT list *do* work (`scalar_subquery_to_join` decorrelates them) — though until recently a correlated one was folded to `NULL` before the planner ever saw it; see *Neutralizing `RemoveSubqueryFromProjection`* in [gap-analysis-aggregate-function.md](gap-analysis-aggregate-function.md). The error also advises the user to file a bug with the DataFusion project. |
| `GLOB` / `~~~` | `42601` | — | ❌ | DuckDB-only, dies at E1. |
| `1 / 0` | `22012 division_by_zero` | `22012 division_by_zero`, and no row written | ✅ | **Split #10, closed.** The read path is PG-exact, class and all — it used to land `XX000` because the error crosses the Ballista boundary as text; see the error-boundary note below. The write path was the half that kept this ⛔: closing split #2 meant setting DuckDB's `integer_division`, and that setting also makes a zero divisor yield NULL rather than raise, so `UPDATE t SET n = n / 0` **succeeded and stored NULL**. The setting cannot be asked to fix it — one flag decides both behaviours — so `write_sql_cl/dialect.rs` wraps every write-path `/` and `%` in a guard that raises PostgreSQL's own message on a zero divisor, which the core node classifies back into `22012`. The guard preserves the result type, is evaluated per row, and binds a repeated placeholder once. A divisor that is itself a division is refused `0A000` instead, because the guard names the divisor twice and nesting would grow the rendered statement exponentially. |
| `1.0 / 0` | `22012 division_by_zero` | `22012 division_by_zero`, and no row written | ✅ | Same shape, same guard. The read path used to return `inf`; now that unsuffixed decimals are exact `numeric` (split #3), `1.0 / 0` is decimal division and raises `22012` as PostgreSQL does. |
| `1.0::float8 / 0` | `inf` | `22012` — guarded like any other write-path division | ⛔ | Counted ⛔ on the read path, which is where it answers: PostgreSQL raises `division_by_zero` for `float8` too, and Arrow's float division follows IEEE 754 instead, so a poison value flows into aggregates rather than an error reaching the client. The **write** path is loud — the guard does not consult the operand types — so this is now the mirror image of the two rows above, and the only remaining division row where the two paths disagree. |

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
| `0x1F` | → **`Binary`** (bytes `1f`) | ⛔ | Counted ⛔ on the read path's answer, which is the verdict column's subject; the write path is loud, and the row says so. PG 16 gives the integer `31`. DuckDB silently parses `0x1F` as `0 AS x1F`. On the **write** path the W4 render rewrites `0x1F` to `X'1F'`, which fails in the shard: `42804 Could not convert string 'x1F' to DOUBLE`. Loud on write, silently a byte string on read. That render is irreducible (DuckDB is handed SQL text), so unification did not change this. |
| `0b101` | → **`0`** | ⛔ | Not a parse error: sqlparser tokenizes it as `0` with the alias `b101`, so the AST handed to the planner is `SELECT 0 AS b101`. PG 16 returns `5`. DuckDB mangles it the same way. Confirmed in all four sqlparser dialects and both versions — a single-parser tokenizer defect, unaffected by unification. |
| `1_000` (underscores) | → `1000`, typed `Decimal128(4, 0)` | 🟡 | The **value** is now right, the **type** is not. PG's dialect keeps `1_000` as one `Number` token; DataFusion's expression planner parsed it with `parse::<f64>()` and rejected the underscore (`XX000 ParserError("Cannot parse 1_000 as f64")`). Nothing in v0.2 targeted this row — it was fixed as a side effect of enabling `parse_float_as_decimal` for split #3, which routes the token down the decimal parser, and that one tolerates the separator. So `arrow_typeof(1_000)` is `Decimal128(4, 0)` — advertised as `numeric` — where plain `1000` is `Int64` and PostgreSQL says `integer`. Before the parser unification this **silently returned `1`**, because `Dialect::Generic` re-parsed the render as `1` aliased `_000`: this row has been ⛔, then ❌, and is now 🟡. The column *label* is the plan's own rendering (`Decimal128(Some(1000),4,0)`) where PG says `?column?`; see the cosmetic residue below. |
| `1e3`, `.5` | ✅ | ✅ | |
| `1.5` (decimal literal) | → **`Decimal128`** | ✅ | **Split #3, closed.** PostgreSQL types unsuffixed decimals as exact `numeric`; DuckDB as `DECIMAL`. DataFusion's `parse_float_as_decimal` defaults to false, which made `0.1 + 0.2 = 0.3` **false** on read and **true** on write; VaireDB now sets it on every read-path context. An explicitly-typed `double` is still binary floating point, as in PostgreSQL. |
| `123456789012345678901234567890` | → `Decimal128(30, 0)`, reads back exactly | ✅ | Fixed by the same setting: past `i64`/`u64`, DataFusion now parses the literal as an exact decimal rather than an `f64` that silently dropped the low digits. Holds to `Decimal128`'s 38 digits; beyond that the literal becomes `Decimal256`, which arrow-pg has no PostgreSQL OID for, so it is refused (`XX000 Unsupported Datatype`) rather than rounded. PG uses `numeric` throughout, DuckDB `HUGEINT`. |
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
| `'abcde'::VARCHAR(2)` | `0A000` naming the cast | ❌ | **Split #9, closed.** The write path now runs the same refusal with the same message, verified against an `UPDATE`, so the two paths no longer disagree. The length used to be silently discarded — no truncation, no error, where PG truncates on an explicit cast. It is not enforced anywhere on the read path, so E2 refuses the length rather than returning the value whole, and names `substr()` as what to write instead. All four spellings (`VARCHAR(n)`, `CHAR(n)`, `CHARACTER(n)`, `CHARACTER VARYING(n)`) are covered; an unbounded character cast has nothing to enforce and stays ✅. `NUMERIC(p, s)` is unaffected — that precision *is* applied. |
| `$1` placeholders | ✅ | ✅ | Well covered by `extended_protocol.rs`. |

### The cosmetic residue: an unaliased expression is labelled with the plan

Cutting across both tables and counted in neither, because it changes no value: an
expression column with no `AS` alias is labelled with DataFusion's **plan rendering**
of the expression rather than PostgreSQL's `?column?`.

| Query | VaireDB's column label | PostgreSQL's |
|---|---|---|
| `SELECT 1_000` | `Decimal128(Some(1000),4,0)` | `?column?` |
| `SELECT 5 # 3` | `Int64(5) BIT_XOR Int64(3)` | `?column?` |
| `SELECT X'DEADBEEF'` | `Binary("222,173,190,239")` | `?column?` |
| `SELECT 1 < ALL (ARRAY[2,3])` | a ~400-character `CASE WHEN make_array(…)` | `?column?` |

It is left out of the verdict columns deliberately, on the same principle the rest of
the doc uses: a verdict describes the *answer*, and every one of these answers is
correct. But it is not purely cosmetic in one case — the rewrites v0.2 added make the
labels *worse*, because a rewritten expression renders as its expansion rather than as
what the client wrote, and `ALL (array)` is the clearest example. A client that keys on
column names, or a `psql` user reading a 400-character header, sees the difference.
The fix belongs where the labels are assigned (`pgwire_handler/column_labels.rs`,
which today labels function-call columns and leaves the rest to the plan), and it is
tracked as a single item in [`gap-analysis.md`](gap-analysis.md) § 6.2 rather than
duplicated across the twenty-odd rows it touches.

## Root causes

Two defects account for the remaining ⛔ and 🟡 rows:

1. **The surviving W4 render is lossy.** `write_router.rs:94` rewrites the client's
   literal text on its way to DuckDB. Caught empirically: `0x1F` becomes `X'1F'`
   and then fails in the shard. Irreducible as long as shards are handed SQL text
   rather than a plan.
2. **Two engines' native semantics still show through where neither was
   translated** — `0b101`, `arr[-1]`, `'\x…'::bytea` and float division by zero on
   the **read** path are all cases where DataFusion and DuckDB *agree with each other*
   and disagree with PostgreSQL (the write path's zero-divisor guard has since made the
   last of those loud on writes, which is why it now reads as a one-path row). Consistency is why they are easy to miss and why no split-brain probe
   catches them; each needs its own read-path rewrite, not a shared fix point.

**✅ Resolved: `transform_to_duckdb` never translated operators.** This was root
cause #1 and the single fix point named for every surviving split. `dialect.rs`
skipped `UPDATE`/`DELETE` `WHERE` clauses entirely and rewrote only `Expr::Function`,
so the write path had no PG→DuckDB expression layer at all — the thing the read path
gained at E2. It now has one: the four regex operators become `regexp_matches`,
`SIMILAR TO` reuses the read path's own pattern translation, `LIKE` with a backslash
gains an explicit `ESCAPE '\'`, and `COLLATE` and `CAST(… AS VARCHAR(n))` are refused
rather than dropped; integer `/` was settled separately, by `integer_division` on the
shard connection. That closed splits #2, #4, #5, #7, #8 and #9 in one pass, and
opened #10.

**Resolved by the parser unification** (kept here because the analysis above and
several roadmap items were written against it): the read path used *two* sqlparser
dialects — VaireDB's own `PostgreSqlDialect` and DataFusion's default
`Dialect::Generic` — so the reachable surface was their intersection, and two of
the three lossy `.to_string()` round-trips sat between them. That cost `#`
outright, silently truncated `1_000`, and silently converted `^` from
exponentiation to XOR. There is now one parser, one dialect, and no read-path
render.

**Resolved by the PostgreSQL expression layer** — three former root causes, all
read-path:

- *DataFusion's planner maps `^` to `Operator::BitwiseXor`* and does not implement
  the `PGExp` node its own parser's PG dialect produces, while PostgreSQL and DuckDB
  both define `^` as exponentiation. E2 rewrites the node; the same stage supplies
  the other three operators DataFusion has no equivalent for.
- *Decimal and large-integer literals were typed `Float64`* by DataFusion's default
  `parse_float_as_decimal = false`, while DuckDB uses `DECIMAL`/`HUGEINT`. The option
  is now set on every read-path context, which closed split #3 and the precision
  loss together.
- *`COLLATE` and cast lengths were parsed and discarded* with no diagnostic. Both
  paths now refuse what they would otherwise ignore — on read, the cast length at E2
  and the collation at E1, because the compat parser strips `COLLATE` before E2 can
  see it; on write, both at the same pre-translation seam. Splits #8 and #9 are
  closed, though as mutual refusal rather than mutual support.

### ✅ Closed: SQLSTATE mapping was inconsistent

Probing surfaced a systematic error-code problem, independent of any operator gap:
the same *class* of failure reached the client under different SQLSTATEs depending on
which pipeline stage raised it. `XX000` is `internal_error`, so a client that retries
on `XX000` and reports `42601` to the user did the wrong thing for half these cases,
and three of the messages leaked DataFusion or gRPC internals — including one that
asked the user to file a bug against DataFusion.

The cause was concrete rather than architectural. The classifier matched error
*substrings* (`"not yet implemented"`, `"unsupported"`) that DataFusion 54.1 does not
emit — its `error_prefix()` says `"This feature is not implemented: "` and
`"not supported"` — so those arms were dead code and everything fell through to
`XX000`. It now matches on `DataFusionError` **variants**, which a version bump
cannot rot, and the sanitizer's strip list is derived from that same `error_prefix()`.

| Failure | Observed | Correct PG code | |
|---|---|---|---|
| Syntax error at the parse (`**`, `DIV`, `GLOB`, `CAST(… AS T ARRAY)`) | `42601` | `42601` | ✅ |
| Literal the planner cannot convert (`1_000`) | answers `1000` | — | ✅ no longer an error |
| Unsupported operator at logical planning (`^`, `^@`, `&&`) | `0A000` | `0A000` | ✅ |
| Unsupported operator at type coercion (`->`, `@?`) | `0A000` | `0A000` | ✅ was `XX000` |
| Cast failure (`'x'::int`) | `22P02` | `22P02` | ✅ was `42804` |
| Division by zero, raised in the coordinator | `22012` | `22012` | ✅ |
| Division by zero, raised in an executor | `22012` | `22012` | ✅ was `XX000` |
| `@@` | `0A000`, no transport detail in the message | `0A000` | ✅ |

**The executor-raised division row was a boundary rather than a mapping, and it is
worth recording because the variant-based rewrite is what exposed it.** Anything raised inside a
Ballista executor is serialized to a string by the scheduler before the coordinator
sees it, arriving as
`Job <id> failed: Job failed due to stage N failed: … DataFusionError(Execution("ArrowError(DivideByZero)"))`.
The typed classifier never gets a variant to match. It fell through to the text
fallback — which *did* have a divide-by-zero rule — and missed anyway, because the
rule looked for the prose spelling `"divide by zero"` and the wire carries Rust's
`Debug` spelling `DivideByZero`, with no spaces. One `contains("dividebyzero")` in
`is_divide_by_zero` closed it.

That is a small fix with a general lesson: **a text fallback behind a typed
classifier is only exercised by errors that crossed a process boundary, so it must be
written against the `Debug` spellings, not the `Display` ones.** The unit test for
this arm passed on `"Divide by zero error"` — a string no distributed query ever
produces. The regression test now pins the verbatim message copied off a live
five-node cluster, and `test_division_by_zero_is_a_data_error_not_an_internal_error`
in `tests/e2e/tests/sql_expression_gaps.rs` asserts the SQLSTATE where it can only be
observed. Carrying a structured error code through Ballista's `FailedTask` proto
remains the right long-term answer, and remains upstream work.

**Residue found while confirming the above: a plan that failed at *serialization*
leaked a raw gRPC `Status { … }`.** **Both halves are now closed** — see
[`gap-analysis.md`](gap-analysis.md) § 6.2 item 5. Nothing in `sanitize.rs` stripped that
shape at the time; the `@@` row above stopped leaking one only because its probe now fails
earlier, in the coordinator, not because it was sanitized. The reproducer was `psql`'s
`\gdesc`, which falls back to a `VALUES`-based query calling `pg_catalog.format_type`:

```
ERROR:  XX000: [VDB-2002] Status { code: InvalidArgument, message: "Could not parse plan:
  … NotImplemented(\"LogicalExtensionCodec is not provided for scalar function format_type\")",
  metadata: MetadataMap { headers: {…} }, source: None }
```

Two distinct defects arrived in that one message, both outside this axis. The first was
misread as a missing arm in `VaireLogicalCodec`: a scalar function crosses the Ballista wire
as a **name** and is looked up in the decoding node's own registry, so the real defect was a
registry that the coordinator had and the scheduler and executor did not.
`vairedb_common::pg_udf` now registers the `pg_catalog` scalar set on every context that plans
or executes. The second, the verbatim transport error, is stripped by `sanitize.rs` on every
path rather than at this call site. `SELECT format_type(23, NULL)` on its own always answered
`integer`, and `\d` always worked, which is why this went unnoticed — it only bit when the
plan crossed the scheduler boundary.

### ✅ Closed: filter pushdown, which would have broken the moment it was enabled

This was filed as latent, and the diagnosis held up. `SchedulerTableProvider` did not
implement `supports_filters_pushdown`, so DataFusion's default returned `Unsupported`
for every predicate and `filters` arrived empty — **no predicate was ever pushed to
DuckDB**, which is why the documentation claiming otherwise had gone unchallenged and
why the dormant rendering was never exercised. `_limit` was discarded too, so
`LIMIT 1` full-scanned every shard.

The dormant rendering was indeed wrong. `filters.iter().map(|f| f.to_string())` is
`Display for Expr` — DataFusion's *plan-display* format, whose literals go through
`ScalarValue`'s `Debug` — so `col("id").gt(lit(10))` rendered as `id > Int32(10)` and
`col("s").eq(lit("a"))` as `s = Utf8("a")`. Neither is valid DuckDB SQL, and columns
rendered fully-qualified against a table name that does not exist on the shard. The
only test asserted `contains("id")` and `contains("10")`, so it passed on the broken
output.

Both are now closed together, in `scheduler/filter_pushdown.rs`:

- **The rendering** is `datafusion::sql::unparser` with its `DuckDBDialect` — the engine
  that owns the expression tree writes it back out, quoting identifiers and escaping
  literals. Column qualifiers are stripped, because the predicate was planned against the
  logical table and executes against the shard's physical one.
- **The allow-list** admits comparisons, `AND`/`OR`/`NOT`, `IS [NOT] NULL`,
  `IS [NOT] TRUE/FALSE`, `IN`, `BETWEEN` and `LIKE`/`ILIKE` over columns and scalar
  literals. Nothing else: no function calls, no casts, no arithmetic, no nested-type or
  interval literals. Every ⛔ operator in the table above is therefore excluded by
  construction rather than by an exclusion list that could fall out of date — `/`, `~`,
  `SIMILAR TO`, the JSON operators and `COLLATE` are all function-or-operator shapes the
  list never admits.
- **`LIKE` is the exception that needed naming.** It is in the ⛔ column for split #5, and
  the split is a row-*dropping* one, so it could not simply be admitted. It is pushed only
  when the pattern is a literal containing no backslash, which is precisely the case where
  the missing default escape cannot matter. Verified against DuckDB 1.5:
  `SELECT 'a_b' LIKE 'a\_b'` answers `false` there and `true` in PostgreSQL.
- **Nothing is reported `Exact`.** `Inexact` keeps DataFusion's own `FilterExec` as the
  authority, so a shard that returns too many rows costs bandwidth only. That covers one
  direction of disagreement and not the other, which is why the allow-list is a whitelist
  of shapes with identical meaning rather than "whatever the unparser will render".
- **Nothing at all is pushed onto a column that is text in name only.** This is the
  narrowing that matters most, because it is the only one where pushing does not merely
  give a wrong answer but *breaks the query*. `parse_data_type` advertises `UUID`, `JSON`,
  `ENUM`, `STRUCT` and every unrecognized type as `Utf8` — faithful for reading a value,
  not for comparing one, because the shard evaluates the pushed copy against the type it
  really stored. Measured against DuckDB 1.5:

  | Column | Pushed predicate | DuckDB's answer |
  |--------|------------------|-----------------|
  | `UUID` | `u = 'notauuid'` | `Conversion Error: Could not convert string 'notauuid' to INT128` |
  | `STRUCT(a INTEGER)` | `s = 'text'` | `Conversion Error: ... can't be cast to the destination type STRUCT` |
  | `JSON` | `j = 'x'` | `Conversion Error: Malformed JSON at byte 0 of input` |
  | `ENUM('a','b')` | `e = 'notamember'` | no rows |
  | `CHAR(3)` | `c = 'zz'` | no rows |

  An error is the one outcome `Inexact` cannot repair — there is no result set left to
  re-filter. So the rule has no exceptions: not equality, not `IN`, not `IS NULL`. The
  distinction is drawn from the *declared* type string (`column_types::is_declared_text`),
  which only the catalog has, so the set of such columns is computed where the schema is
  built and **carried across the distributed query boundary** in the logical codec. A
  decoding that finds no such list reads it as "not known" and suppresses push-down on
  every text column, so a mixed-version cluster loses an optimization instead of gaining
  an error.
- **An ordering comparison (`< <= > >=`, `BETWEEN`) on a text column is not pushed either**,
  even a genuine `VARCHAR`. Both engines compare text by byte value today — checked, not
  assumed — so they agree; but DuckDB has a `default_collation` setting that VaireDB
  neither pins nor inspects, so the agreement is a property of the configuration rather
  than of the code. Equality is unaffected, and equality is the shape predicates
  overwhelmingly take.
- **A literal is only admitted if it renders as the value it stands for.** Membership in the
  list was audited by rendering one of every kind, not by reading the match arms, and one
  was wrong: an Arrow `Date64` unparses as `CAST('2022-01-01 01:20:00' AS DATETIME)` — a
  timestamp with a time component, not a date — so a value carrying a time of day would be
  truncated to its day by the coordinator and compared exactly by the shard, dropping rows.
  It is excluded; `Date32` renders as a real `CAST('2022-01-08' AS DATE)` and is what a
  PostgreSQL `date` parameter actually arrives as. Decimals were the other thing worth
  checking, and they are correct: scale and sign both survive (`15.00`, `-12.345`, `42`).
- **The limit** is passed to every shard as a per-shard cap. The coordinator applies the
  query's real `LIMIT` over the union, so up-to-`limit` rows per shard is always a
  superset. DataFusion does not offer a limit to `scan` when a `FilterExec` sits between
  the limit and the scan, which is exactly the case where truncating early would drop rows.

A wide float literal was checked rather than assumed: `1e300` unparses as a 300-digit
decimal (Rust's `f64` `Display` never uses exponent form), and DuckDB widens a literal
that overflows `DECIMAL` to `DOUBLE`, so it compares as the same value.

## Prioritized remediation

1. ~~**Fix `^`**~~ — **done.** The `PGExp` node is rewritten to `power()` at E2
   (`pgwire_handler/pg_operators.rs`), the hook that already runs on every SELECT.
   The parser unification made it strictly simpler: the AST carries `PGExp`, so the
   rewrite *preserves* the parsed meaning instead of overriding DataFusion's XOR
   reading. Split #1 is closed in both directions.
2. ~~**Add a PG→DuckDB expression layer to the write path**~~ — **done**, and it
   closed splits #2, #4, #5, #7, #8 and #9. `transform_exprs_in_statement` now visits
   `UPDATE`/`DELETE` `WHERE` clauses and nested expressions and rewrites
   `Expr::BinaryOp` as well as `Expr::Function`: `~`/`!~`/`~*`/`!~*` →
   `regexp_matches`, `LIKE` with a backslash → an explicit `ESCAPE '\'`,
   `SIMILAR TO` → the same anchored-regex translation E2 does. `COLLATE` and the
   character cast length are **refused** at the pre-translation seam in `parser.rs`
   rather than translated, since the read path refuses both and a predicate a `SELECT`
   rejects must not silently succeed in an `UPDATE`. One caveat, which is now item 2b:
   integer `/` was closed by a DuckDB *setting* rather than a rewrite.
2b. ~~**Translate integer `/` on the write path instead of setting `integer_division`.**~~
   — **done, on different terms than proposed.** The setting closed split #2 and
   opened split #10 in the same move: DuckDB's `integer_division` also makes a zero
   divisor return NULL rather than raise, so `UPDATE t SET n = n / 0` reported success
   and stored NULL where PostgreSQL raises `22012`. The proposal was to translate the
   `BinaryOp` and drop the setting, on the assumption that DuckDB's default divide-by-zero
   behaviour was the one to keep. Measured on 1.5.5, it is not: with the setting *off*,
   `7/0` is `inf` and `7 % 0` is already NULL — neither is an error, and nothing in
   `duckdb_settings()` makes either one raise.
   So the setting stays — it is what makes `7/2` truncate — and the divisor is checked
   instead. `write_sql_cl/dialect.rs` wraps every write-path `/` and `%` in a guard that
   raises PostgreSQL's own message when the divisor is zero, which the core node maps
   back to `22012`. A divisor that is itself a division is refused rather than guarded,
   since the guard names the divisor twice.
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
4. ~~**Decide the decimal-literal policy**~~ — **decided: `parse_float_as_decimal`
   is on.** PostgreSQL is the contract and PostgreSQL types an unsuffixed decimal as
   exact `numeric`, so the alternative — keeping the read path on `Float64` — meant
   holding two answers to `0.1 + 0.2 = 0.3` in one database. The cost was accepted
   knowingly: decimal-vs-float coercion now applies throughout the read path, and a
   literal past 38 digits becomes a `Decimal256` that has no PostgreSQL OID and is
   refused. The remaining `Decimal128`/`Decimal256` work in
   `gap-analysis-data-type.md` is unaffected.
5. ~~**Fix the SQLSTATE mapping**~~ — **done.** `0A000` for every unsupported
   operator, `22012` for division by zero wherever it is raised, `22P02` for cast
   failures, and no gRPC `Status` or "file a bug with DataFusion" advice in a
   client-visible message. The classifier now matches on `DataFusionError` **variants**
   rather than substrings, which is what made the arms fire at all — the substrings it
   was looking for (`"not yet implemented"`, `"unsupported"`) are not phrasings
   DataFusion 54.1 emits, so they were dead code and everything fell through to
   `XX000`. See [the closed section above](#-closed-sqlstate-mapping-was-inconsistent)
   for the one row that needed a second pass, and why.
6. ~~**Reject `COLLATE` instead of ignoring it**~~ — **done on the read path**, with
   one exception that makes it usable: `C`, `POSIX`, `ucs_basic` and `default` name
   byte order, which is what VaireDB does, so those are accepted and dropped and every other
   collation is refused `0A000`. The refusal had to move to E1: the compat parser's
   own `StripCollate` rule deletes the clause before E2 runs, so the check reads the
   client's text rather than the AST the read path plans. **The write path now runs the
   same refusal**, so split #8 is closed too.
7. **Make `CAST(… AS JSON/UUID)` work** — both types already exist as column
   types, so this is a planner type-name gap that currently makes the entire JSON
   operator surface unreachable.
8. **Close the remaining cheap read-path rejections** — `struct.field` is what is
   left. Unary `~`, `&&` and `^@` were done at E2; **`= ANY (subquery)` and
   `ALL (array)` are done** in v0.2. `= ANY (subquery)` was the one that mattered — it
   is ordinary PostgreSQL a client would not expect to fail, and it was *misrouted* to
   `array_has` rather than unimplemented, so the fix was to rewrite it to
   `IN (subquery)` and let the optimizer decorrelate it into the semi join it always
   was. `ALL (array)` needed more care than it looks: expanding it to `array_has`
   would have got PostgreSQL's NULL and empty-array cases wrong, so it expands to an
   explicit `CASE`.
9. **Reject or implement `arr[-1]`.** The discarded cast lengths half of this item is
   **done** — `VARCHAR(n)` and its three spellings are refused at E2 and name
   `substr()` as the alternative. `arr[-1]` is still the DuckDB reading (last
   element) where PostgreSQL returns `NULL`, consistently on both paths; it needs
   either a rewrite or a decision to document it as intentional.
10. ~~**Before implementing filter pushdown**, replace `Expr::to_string()` with
    `datafusion::sql::unparser` plus an explicit allow-list, and exclude every
    operator in the ⛔ table.~~ — **done**, and the pushdown is now live rather than
    dormant. The allow-list excludes the ⛔ operators by construction, and `LIKE` — the
    one ⛔ shape it does admit — is admitted only for patterns with no backslash. See
    [the closed section above](#-closed-filter-pushdown-which-would-have-broken-the-moment-it-was-enabled).

## Executable counterpart

`tests/e2e/tests/sql_expression_gaps.rs` holds the rows the expression layer closed
— every rewrite, every refusal, and the two seams a unit test cannot reach: the same
functions over a **sharded** column, because a name the coordinator's planner
resolves still has to exist on the executor that runs the stage, and a Describe/Execute
pair on the ranking functions, because both halves have to agree on the type.
`pg_operators.rs`'s own unit tests cover the pattern translation exhaustively, which
is where that belongs — it is a pure function from a pattern to a regex.

Everything else in the two master tables is still uncovered, beyond four incidental
tests (`||` and `ILIKE` in `data_types_dialect_gaps.rs:121`/`:101`, `BETWEEN` in
`sql_command_select.rs:310`, `CAST` as an error probe in `errors.rs:148`).

Proposed remaining suite, following the `<doc-topic>_<facet>.rs` convention
established by `sql_command_*` and `data_types_*`:

| File | Contents |
|---|---|
| `sql_expression_operators.rs` | The master operator table, one test per row, FROM-less where possible. |
| `sql_expression_literals.rs` | The master literal table. |
| `sql_expression_split_brain.rs` | Largely **absorbed into `sql_expression_gaps.rs`**, which is where the paired read/write tests for splits #2, #4, #5, #7, #8 and #9 now live (`both_paths_select`, `test_the_untranslatable_write_expressions_are_refused_like_the_read_ones`). The pairing idea was right and is worth keeping as the house pattern for this class: the same predicate through `SELECT` and through `UPDATE`, asserting they agree — including when they agree by both refusing. Split #10's pair is
`test_a_zero_divisor_raises_on_the_write_path_too`, beside the read path's
`test_division_by_zero_is_a_data_error_not_an_internal_error`. |

Follow the existing convention: a **passing** test pinning today's honest
rejection (`assert_unsupported` for `0A000`, `assert_rejected` where the stage is
uncertain), plus an `#[ignore = "gap (<section>): …"]` test asserting the
PostgreSQL-correct behavior, which fails by construction and is the definition of
done. The ⛔ rows need the ignored test to assert the **correct value**, not merely
that no error occurred — a silent-wrong-answer gap is invisible to an
error-shape assertion.

```sh
cd tests/e2e && cargo test --test sql_expression_gaps -- --test-threads=1
```

## How this was measured

Empirically, against the live 5-node e2e cluster (`make e2e-up`, DuckDB v1.5.5 on
the core nodes), over `psql` on the coordinator's PostgreSQL wire port — not by
reading capability tables. Every row in the two master tables was probed:

1. **Read path, FROM-less** — `SELECT <expr>`, which `extract_select_table_name`
   (`pgwire_handler/query_router.rs`) resolves to no table, so it plans on
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

Every ⛔ row and every split was reproduced at least twice, and the same protocol
was re-run in full for v0.2 — the verdicts above are that second pass, not the first
one amended.

After the parser unification the affected rows were re-probed the same way, and
the dialect-sensitive cases were additionally reduced to a standalone harness that
parses one fragment under `PostgreSqlDialect` and `Dialect::Generic` and compares
**both** the rendered text and the AST. That last part matters: `^` renders to
byte-identical text under both dialects while producing different AST nodes, so a
render-only comparison reports "no difference" on the very case that was silently
wrong.

After the PostgreSQL expression layer the nine affected rows were re-probed the same
way and then **pinned as tests**, which is the difference that matters for this
round: `tests/e2e/tests/sql_expression_gaps.rs` asserts each of them against the live
cluster, so the ✅ rows above are checked on every `make e2e` rather than measured
once. The write-path halves of splits #7–#9 were probed with the same `UPDATE … WHERE`
method as the original six.
