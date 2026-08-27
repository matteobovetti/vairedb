# DataFusion vs. VaireDB (PostgreSQL wire protocol) — Window Function Gap

Gap analysis of the **window function** surface DataFusion documents against what
VaireDB's coordinator actually returns over the PostgreSQL wire protocol, measured
against a live 5-node cluster and byte-diffed against PostgreSQL 16.

- **Reference (DataFusion):** [Window Functions](https://datafusion.apache.org/user-guide/sql/window_functions.html) — 11 built-in window functions plus the `OVER` clause surface.
- **Reference (PostgreSQL):** [Window Function Calls](https://www.postgresql.org/docs/16/sql-expressions.html#SYNTAX-WINDOW-FUNCTIONS) — the contract VaireDB advertises by speaking the PG wire protocol.
- **Sibling analyses:** [aggregate functions](gap-analysis-aggregate-function.md) (which explicitly defers this axis), [operators and literals](gap-analysis-operator-literal.md), [commands](gap-analysis-command.md), [data types](gap-analysis-data-type.md).

## How VaireDB decides

Window functions have **no VaireDB-specific code path at all**. A `grep` for window
machinery (`window`, `WindowSpec`, `WindowFrame`, `WindowFunction`) across the whole
`crates/` tree returns exactly one hit, in `node_service/failure_detector.rs`, and it
is an unrelated time-window. There is no hand-rolled partition recombination, no
special-case routing, and no validation.

Consequently **every behavior in this document is inherited**, from one of four layers:

| Layer | Version | What it decides |
|---|---|---|
| sqlparser (via `datafusion::sql::sqlparser`) | **0.61.0** | Whether the `OVER` clause parses at all (`42601`). |
| datafusion-sql / datafusion-expr | 53.1.0 | Whether the `WindowSpec` AST is honored, and logical validation. |
| datafusion-functions-window | 53.1.0 | The 11 UDWF implementations and their return types. |
| ballista-core + datafusion-proto | 53.0.0 | Whether the physical window expression survives serialization to the executors. |

A window query therefore fails at one of four points:

| Rejection point | Error | SQLSTATE | When |
|---|---|---|---|
| Parse | `SqlSyntaxError` | `42601` | sqlparser 0.61 cannot represent the clause. Only `EXCLUDE` (row 21). |
| Logical planning | `Error during planning` | `XX000` | Window function in an illegal position (rows 29, 31). |
| Physical planning | `Physical plan does not support logical expression …` | `XX000` | Same, surfaced later; emits a ~1.5 KB `Signature { … }` debug dump. |
| Distributed execution | `[VDB-5001] Job … failed` | `XX000` | Runtime failure inside a Ballista stage (row 38). |

**Nothing is rejected at classification.** Window functions ride the ordinary `Select`
path (`classify_statement` → `handle_select`), so unlike the command axis there is no
`0A000` surface here. That has a consequence worth stating plainly: **this axis fails
by returning wrong numbers far more often than by refusing** — 10 of 42 rows are
silently wrong, against 3 loud rejections.

## Verdict legend

Inherited from the aggregate analysis:

- ✅ works, and agrees with PostgreSQL
- ⛔ **silently wrong value** — returns a plausible answer that is not the PG answer
- 🟡 partial, degraded, wrong result type, or wrong SQLSTATE
- ❌ rejected loudly

## Summary

| Verdict | Count | Rows |
|---|---:|---|
| ✅ Correct | 21 | 5–10, 12, 14, 16–20, 23, 24, 28, 30, 32, 36, 37, 42 |
| 🟡 Partial / wrong type / wrong SQLSTATE | 8 | 1–3, 15, 31, 33–35 |
| ⛔ **Silently wrong** | 10 | 4, 11, 13, 22, 25–27, 39–41 |
| ❌ Rejected | 3 | 21, 29, 38 |

**There is no missing-function gap.** All 11 PostgreSQL window functions are
registered in DataFusion 53.1.0 (`datafusion-functions-window-53.1.0/src/lib.rs:69-83`),
Ballista re-installs exactly that set
(`ballista-core-53.0.0/src/extension.rs:341` → `with_window_functions(ballista_window_functions())`),
and PostgreSQL 16's `pg_proc WHERE prokind = 'w'` returns the same 11 names. The
frame engine is likewise healthy: all five frame units, peer-group semantics, and
integer/float/`INTERVAL` offsets are PG-correct.

**The gap is not coverage — it is silent wrongness in the clause surface around the
functions**, plus two systemic issues (anonymized columns, result-type OIDs).

### The two entries that outrank everything else

1. **Row 26 — `OVER (w …)` is nondeterministic.** Referencing a named window inside
   parentheses *and* adding a frame discards the named window's spec and sums rows in
   shard-arrival order. Five consecutive runs of one query on unchanged data returned
   **five different answers**:

   ```
   30,50,90,100   20,60,90,100   10,30,70,100   10,40,60,100   20,60,70,100
   ```

   PostgreSQL returns `10,30,30,70` every time. This is the only row in any VaireDB
   gap analysis that is not merely wrong but *unstable* — it cannot be caught by a
   golden-file test that runs once, and it will not reproduce for a user reporting it.

2. **Row 38 — `PARTITION BY <col>` + `WHERE <col> = <value>` fails hard.** The
   single most common shape in dashboard and reporting SQL ("this partition's ranking,
   for one category") errors out. Legal, ubiquitous PostgreSQL.

## § 1 — The 11 window functions

Values verified row-by-row against PostgreSQL 16.15 on identical fixtures.

| # | Function | Verdict | Notes |
|---|---|---|---|
| 1 | `row_number()` | 🟡 | Values correct. Advertises `numeric` (OID 1700); PG promises `int8` (20). Row 33. |
| 2 | `rank()` | 🟡 | Values correct. Same OID mismatch. |
| 3 | `dense_rank()` | 🟡 | Values correct. Same OID mismatch. |
| 4 | `ntile(n)` | ⛔ | **Wrong bucket assignment.** See below. Also the OID mismatch (PG: `int4`). |
| 5 | `percent_rank()` | ✅ | Values and `float8` OID both correct. |
| 6 | `cume_dist()` | ✅ | Values and `float8` OID both correct. |
| 7 | `lag(x[, off[, default]])` | ✅ | Values correct; input type preserved. `IGNORE NULLS` is row 22. |
| 8 | `lead(x[, off[, default]])` | ✅ | As `lag`. |
| 9 | `first_value(x)` | ✅ | Correct, including the default-frame interaction. |
| 10 | `last_value(x)` | ✅ | Correct — returns the current row under the default `RANGE` frame, as PG does. |
| 11 | `nth_value(x, n)` | ⛔ | Correct for `n ≥ 1`. **`n = 0` returns NULL for every row**; PG raises `22016 argument of nth_value must be greater than zero`. Negative `n` is a DataFusion superset (PG rejects). |

### Row 4 — `ntile` remainder distribution

The two engines use different formulas, not merely a different tie-break:

- **PostgreSQL** front-loads the remainder: with `q, r = divmod(rows, n)`, the first
  `r` buckets get `q+1` rows and the rest get `q`.
- **DataFusion** assigns by proportion: `bucket(i) = ⌊i·n / rows⌋ + 1`
  (`datafusion-functions-window-53.1.0/src/ntile.rs:174-181`), which spreads the
  oversized buckets evenly instead of packing them at the front.

They agree whenever `rows` is a multiple of `n`, and coincidentally in many other
cases. Over all `(rows, n)` pairs with `rows ≤ 20`, **83 of 210 diverge (39 %)**.
Measured, with the predicted formula matching VaireDB exactly in every case:

| rows | n | PostgreSQL | VaireDB | |
|---:|---:|---|---|---|
| 7 | 5 | `1122345` | `1123345` | ⛔ |
| 6 | 4 | `112234` | `112334` | ⛔ |
| 12 | 5 | `111222334455` | `111223334455` | ⛔ |
| 5 | 3 | `11223` | `11223` | ✅ coincide |
| 7 | 4 | `1122334` | `1122334` | ✅ coincide |
| 10 | 3 | `1111222333` | `1111222333` | ✅ coincide |

Identical at 1, 3 and 5 shards — this is a function-semantics divergence, not a
distribution artifact. It is dangerous precisely because it agrees on the small
round-numbered cases a developer tries by hand.

## § 2 — Aggregates used as window functions

| # | Construct | Verdict | Notes |
|---|---|---|---|
| 12 | `sum` / `count` / `min` / `max` / `avg` `OVER (…)` | ✅ | Values correct, including `avg(bigint) OVER ()`. |
| 13 | `count(*) FILTER (WHERE …) OVER (…)` | ⛔ | **`FILTER` is silently discarded.** Measured: `count(*) FILTER (WHERE m > 30) OVER (ORDER BY id)` returned `1,2,3,4,5,6,7` — a plain unfiltered `count(*)`; PG returns `0,0,0,0,1,2,3`. |
| 14 | `count(DISTINCT x) OVER (…)` | ✅ | Works and is correct. A **superset** — PG rejects `DISTINCT` in a window aggregate (`42P20`). Must not be "fixed". |
| 15 | Result type of `sum(bigint)` / `avg(bigint)` `OVER ()` | 🟡 | `int8` / `float8` where PG promises `numeric`. **Inherited from the aggregate axis, not window-specific** — identical without `OVER`. See [aggregate analysis](gap-analysis-aggregate-function.md). The >2⁵³ precision loss recorded there is likewise inherited; this analysis did not reproduce it independently. |

### Row 13 is distribution-specific

`FILTER` is not lost by DataFusion — it is lost by Ballista's plan serialization.
`PhysicalWindowExprNode` has no filter field (`datafusion.proto:924-938`), and the
deserializer hardcodes the absence (`from_proto.rs:203` → `None`). Single-node
DataFusion computes this correctly. **VaireDB is wrong here specifically because it
is distributed**, which makes this the one row that cannot be closed by a dependency
bump alone.

## § 3 — Frame and clause surface

| # | Construct | Verdict | Notes |
|---|---|---|---|
| 16 | `ROWS BETWEEN … AND …` | ✅ | All bound combinations correct. |
| 17 | `RANGE BETWEEN … AND …` | ✅ | Peer groups correct. **`RANGE` is *not* collapsed into `ROWS` on ties** — verified against duplicate ordering values that straddle shard boundaries. |
| 18 | `GROUPS BETWEEN … AND …` | ✅ | Correct. Notable: PG has supported `GROUPS` only since 11, and it works here. |
| 19 | Default frame (`RANGE UNBOUNDED PRECEDING TO CURRENT ROW`) | ✅ | Correct, both with and without `ORDER BY`. |
| 20 | Frame offsets: integer, float, `INTERVAL` | ✅ | Correct, including `RANGE BETWEEN INTERVAL '1 day' PRECEDING …`. |
| 21 | `EXCLUDE {CURRENT ROW \| GROUP \| TIES \| NO OTHERS}` | ❌ | **Unparseable.** `42601 … Expected: ), found: EXCLUDE`. sqlparser 0.61 cannot represent it: `WindowFrame` has `start_bound`/`end_bound` and a literal `// TBD: EXCLUDE` (`sqlparser-0.61.0/src/ast/mod.rs:2264-2270`). All 4 variants. Needs the parser taught first. |
| 22 | `IGNORE NULLS` | ⛔ | **Silent no-op.** Measured on `lag(m)` with NULLs: `IGNORE NULLS` and `RESPECT NULLS` returned byte-identical columns. Root cause is a single hardcoded `false` in `to_proto.rs:150-156` with a stale comment claiming the field is unused. Affects rows 7–11. **One line to fix.** |
| 23 | `RESPECT NULLS` | ✅ | Correct — but only because it is the default, and row 22 means it is the *only* behavior. |

## § 4 — Named windows (`WINDOW` clause)

The defect has a crisp boundary that is worth stating precisely, because it decides
whether a query is safe: **a bare reference works; a parenthesized reference discards
the referenced spec entirely.** Root cause: `WindowSpec::window_name` is parsed by
sqlparser but never read by datafusion-sql.

Measured on `(id, cat, m) = (1,a,10) (2,a,20) (3,b,30) (4,b,40)`, PG answer `10,30,30,70`:

| # | Construct | Verdict | VaireDB returns | Notes |
|---|---|---|---|---|
| 24 | `sum(m) OVER w` | ✅ | `10,30,30,70` | Bare reference is honored, including multiple references to one window. |
| 25 | `sum(m) OVER (w)` | ⛔ | `100,100,100,100` | Redundant parens **drop `PARTITION BY` *and* `ORDER BY`**; frame widens to the whole table. |
| 26 | `sum(m) OVER (w <extra clauses>)` | ⛔ | **unstable** | Adding `ORDER BY` → `10,30,60,100` (partition lost). Adding a **frame** → nondeterministic, 5 distinct answers in 5 runs. See summary. |
| 27 | `WINDOW w2 AS (w1 ORDER BY m)`, `OVER w2` | ⛔ | `10,30,60,100` | Chained inheritance: `w1`'s `PARTITION BY` is lost. |

Rows 25–27 are all **silent** — no warning, no error, a plausible-looking column.
The aggregate analysis recorded this as one ⛔ row; it is four distinguishable shapes,
one of which is nondeterministic.

## § 5 — Where a window function may appear

| # | Position | Verdict | Notes |
|---|---|---|---|
| 28 | `SELECT` list | ✅ | The supported case. |
| 29 | Outer `ORDER BY`, written inline | ❌ | **Legal PostgreSQL, rejected.** `SELECT id FROM t ORDER BY row_number() OVER (ORDER BY m DESC)` → `XX000 … Physical plan does not support logical expression WindowFunction(…)` plus a ~1.5 KB `Signature { … }` dump. |
| 30 | Outer `ORDER BY` by output alias or position | ✅ | The workaround for row 29, and fully general. |
| 31 | `WHERE`, `GROUP BY`, `HAVING`, nested in another window fn | 🟡 | **Correctly rejected** — PG rejects these too — but with `XX000` instead of PG's `42P20` / `42803`, and with the same 1.5 KB dump leaked to the client. Wrong class (internal error, not syntax error) breaks client error handling. |
| 32 | `QUALIFY` | ✅ | Works. A **superset** — DuckDB/Snowflake syntax that PG does not have. Must not be "fixed". |

The wrong SQLSTATEs in row 31 come from the confirmed DataFusion-53 message-text rot
in `error_enrichment.rs:112-116` and `sanitize.rs:9-10` — the matchers look for
message strings DataFusion 53 no longer emits, so classification falls through to
`XX000`. Same root cause as the misclassifications recorded on the operator axis.

## § 6 — Result types and column labels

| # | Construct | Verdict | Notes |
|---|---|---|---|
| 33 | Ranking function result OIDs | 🟡 | `row_number` / `rank` / `dense_rank` / `ntile` all advertise **`numeric` (1700)**; PG promises `int8` (20) for the first three and `int4` (23) for `ntile`. One line: `arrow-pg-0.14.0/src/datatypes.rs:31` maps `UInt64 => NUMERIC`. |
| 34 | Column label of an unaliased window column | 🟡 | The rendered plan expression, **up to 412 bytes** observed (114 bytes for a plain `sum … ROWS BETWEEN 3 PRECEDING AND 1 FOLLOWING`), against PG's 63-byte `NAMEDATALEN` guarantee. Varies with every clause **and with the FROM alias**. `AS` is a full workaround. |
| 35 | Bind-parameter offsets — `ntile($1)`, `lag(x, $1)`, `nth_value(x, $1)` | 🟡 | Correct **provided the client declares a parameter OID**. Values byte-identical to literal controls, verified with a discriminating offset-10 case. |

Row 33 is the most likely thing to break a real client, and it breaks it *hard*: a
driver that trusts the PG contract attempts an `int8` decode of a `numeric` payload
and raises a decode error, so the query fails rather than returning an odd type.
`psql` and `tokio-postgres` are unaffected (both read the declared OID). Workaround:
`row_number() OVER (…)::bigint` — verified to restore OID 20.

Row 34's protocol divergence is confirmed, but **no client breakage was reproduced**:
psql 18.3 and tokio-postgres 0.7.18 both tolerated a 412-byte label. Treat the
severity as unquantified.

## § 7 — Distribution and sharding

This section is almost entirely negative results, and they matter: the obvious
hypotheses about a sharded window engine are **wrong**, and the one real failure is
not the one you would guess.

| # | Scenario | Verdict | Notes |
|---|---|---|---|
| 36 | Global window, no `PARTITION BY` (`sum(x) OVER ()`, `row_number() OVER (ORDER BY id)`) | ✅ | Correct and **stable**: 10 consecutive runs identical, and byte-identical at 1, 3 and 5 shards. Ballista cuts stages at `CoalescePartitionsExec` / `SortPreservingMergeExec` (`planner.rs:194`, `:214`), so the window executes on one gathered partition. |
| 37 | `PARTITION BY <col>` | ✅ | Correct. `required_input_distribution` requests `HashPartitioned` on the partition keys, and `EnforceDistribution` inserts the repartition. Verified correct with skewed groups, empty tables, and — by copying `core.duckdb` out of a core container and reading it with the DuckDB CLI — with every `cat` group physically spanning all 3 shards. Shard-count-invariant. |
| 38 | `PARTITION BY <col>` **+ an equality predicate on that same `<col>`** | ❌ | **Hard failure.** `Execution("Expects PARTITION BY expression to be ordered")`, or a leaked `Internal("Assertion failed: … All partition by columns should have an ordering")`. |

### Row 38 — scope and escapes

Reproduced in 13 of 14 probes, **including on a single shard**, so it is a DataFusion
optimizer defect that distribution merely exposes. The trigger is specifically an
**equality** predicate on a `PARTITION BY` column: once the optimizer knows the column
is single-valued it drops its ordering, and `WindowAggExec` then asserts the ordering
it no longer has.

| Shape | Result |
|---|---|
| `WHERE cat = 'b'`, `PARTITION BY cat` | ❌ fails |
| `… ` wrapped in a subquery, CTE, or `MATERIALIZED` CTE | ❌ fails |
| Bind parameter instead of a literal | ❌ fails |
| Expression partition key (`PARTITION BY upper(cat)`) | ❌ fails |
| `WHERE cat > 'a'` (inequality) — control | ✅ works |
| `WHERE rn <= 1` in the outer query only (top-N-per-group) | ✅ works |
| `WHERE cat = (SELECT 'b')` — opaque scalar subquery | ✅ works |
| Partitioning a *different* column than the one filtered | ✅ works |

The last two are the practical escapes. Note the third-from-last row: the classic
top-N-per-group idiom is **fine** as long as the filter is on the window's output and
not on the partition key — two probes appeared to contradict each other on this until
the equality predicate was isolated as the trigger.

The precise mechanism is **INCONCLUSIVE** because `EXPLAIN` is unsupported
(`0A000 [VDB-1004] EXPLAIN is not supported by VaireDB`) — the behavior is certain and
reproducible, the cause is a hypothesis. This is a concrete cost of that gap; see
[command analysis](gap-analysis-command.md) priority 5.

## § 8 — Window functions over anonymized columns

VaireDB's pseudonymization is **write-path only**: `anonymize_statement` rewrites
`INSERT`/`UPDATE` values and falls through for everything else
(`anonymization/rewrite.rs:89` → `_ => Ok(())`). Reads therefore see HMAC-SHA256
digests. For aggregates that is mostly harmless; for window functions, which are
defined *by ordering*, it silently inverts results.

Measured with `email ∈ {aaa@x.com, bbb@x.com, ccc@x.com}` at `id` 1, 2, 3 — whose
digests sort `id2 < id3 < id1`, i.e. **not** the plaintext order:

| # | Construct | Verdict | VaireDB | PostgreSQL |
|---|---|---|---|---|
| 39 | `rank() OVER (ORDER BY <anon col>)` (and `row_number`, `dense_rank`, `percent_rank`, `cume_dist`) | ⛔ | `3, 1, 2` | `1, 2, 3` |
| 40 | `first_value(<anon col>) OVER (ORDER BY <anon col>)` (and `last_value`, `nth_value`, `lag`, `lead`) | ⛔ | the digest of `bbb@x.com` | `aaa@x.com` |
| 41 | Plaintext predicate + window (`WHERE email = 'aaa@x.com'`) | ⛔ | 0 rows | 1 row |
| 42 | `PARTITION BY <anon col>` | ✅ | correct | correct |

Row 42 is the one **sound** use, and it is sound for a real reason: HMAC is
deterministic and injective, so digest equality is plaintext equality. Grouping is
preserved exactly; only *order* and *value* are destroyed.

This is arguably by design — the digest is the stored value — but it is undocumented,
and rows 39–40 produce confidently wrong analytics on exactly the columns a compliance
feature marks as sensitive. At minimum it needs documenting; ideally an
order-sensitive window over an anonymized column should be rejected rather than
answered.

## Root causes

Ten ⛔ rows reduce to six causes, four of them one-liners:

| Cause | Rows | Where | Cost |
|---|---|---|---|
| `WindowSpec::window_name` never read | 25, 26, 27 | datafusion-sql 53.1.0 | Upstream, or a pre-plan AST expansion in VaireDB. |
| `IGNORE NULLS` hardcoded `false` | 22 | `to_proto.rs:150-156` | **One line** upstream. |
| `PhysicalWindowExprNode` has no filter field | 13 | `datafusion.proto:924-938`, `from_proto.rs:203` | Proto change + both codecs. Distribution-specific. |
| `ntile` uses proportional, not front-loaded, buckets | 4 | `ntile.rs:174-181` | **One function** upstream. |
| `nth_value` accepts `n = 0` | 11 | datafusion-functions-window | **One guard.** |
| Anonymization is write-path only | 39, 40, 41 | `anonymization/rewrite.rs:89` | Design decision — document, or reject. |

And the 🟡 rows:

| Cause | Rows | Where | Cost |
|---|---|---|---|
| `UInt64 => NUMERIC` OID mapping | 1, 2, 3, 33 | `arrow-pg-0.14.0/src/datatypes.rs:31` | **One line.** |
| Labels are rendered plan expressions | 34 | DataFusion field naming | Truncate/hash at the RowDescription boundary. |
| DataFusion-53 message-text rot | 29, 31 | `error_enrichment.rs:112-116`, `sanitize.rs:9-10` | Re-derive matchers from DF 53 message text. |

## Prioritized gaps

Ranked by consequence, then by cost. The parenthetical marks where the fix lives.

0. **Row 26 — nondeterministic `OVER (w …)`** *(upstream / AST rewrite)*. Not a
   feature gap but a correctness bug, and it outranks everything: the same query on
   unchanged data returns different numbers. Unreproducible bug reports, and no
   single-run test can catch it. If the named-window handling cannot be fixed quickly,
   **reject** `OVER (<name> …)` rather than answer it.
1. **Row 38 — `PARTITION BY col` + equality on `col`** *(upstream optimizer)*. The
   most common real-world window shape, failing loudly. Loud beats silent, so it ranks
   below row 26, but it is the row users will hit first. Cheap mitigation: rewrite the
   equality to an opaque scalar subquery in the coordinator, which is a verified escape.
2. **Rows 25, 27 — named-window inheritance silently drops clauses** *(as row 0)*.
   Same root cause; fixing row 0 fixes these.
3. **Rows 39–41 — anonymized-column windows** *(VaireDB)*. Wrong analytics on
   sensitive columns. The cheap, honest fix is to reject an order-sensitive window over
   an anonymized column; documenting row 42 as the one sound use is the minimum.
4. **Row 22 — `IGNORE NULLS` no-op** *(one line upstream)*. Best
   consequence-to-cost ratio in the document: a silently wrong answer for a one-line fix.
5. **Row 33 — ranking function OIDs** *(one line)*. The only row that makes a
   conforming driver fail outright rather than return something odd.
6. **Row 13 — `FILTER` on a window aggregate** *(proto + codecs)*. Silently wrong, and
   the only defect VaireDB owns *because* it is distributed.
7. **Row 4 — `ntile` buckets** *(one function upstream)*. Silently wrong in 39 % of
   `(rows, n)` pairs, and it agrees on the cases people check by hand.
8. **Rows 29, 31 — placement rejections** *(VaireDB)*. Row 29 rejects legal PG; row 31
   rejects correctly with the wrong SQLSTATE. Both leak a 1.5 KB `Signature { … }`
   dump. Row 30 is a full workaround for row 29, so this is mostly hygiene — but the
   leaked dump is a poor first impression and cheap to sanitize.
9. **Row 11 — `nth_value(x, 0)`** *(one guard upstream)*. Narrow.
10. **Row 34 — column labels** *(VaireDB)*. Protocol divergence confirmed, client
    breakage not reproduced. `AS` is a full workaround. Lowest priority of the real rows.
11. **Row 21 — `EXCLUDE`** *(parser + planner)*. The only ❌ needing parser work first,
    and the only genuinely *missing feature* in the whole document. Rare in practice.

**Supersets that must NOT be "fixed"** — VaireDB accepts these and PostgreSQL does
not; narrowing to PG parity would be a regression against DataFusion and DuckDB:
`DISTINCT` inside a window aggregate (row 14), `QUALIFY` (row 32), negative
`nth_value` offsets (row 11).

## Executable counterpart

No test file exists for this axis yet. The repository currently contains **exactly two
window tests** — `sql_command_select.rs:833-871`
(`test_window_row_number`, `test_window_sum_partition`) — and neither asserts a result
type or a column label, because both go through `simple_query_rows`, which discards
`row.columns()`. Every gap above is therefore unguarded.

Proposed, following the `sql_function_*` convention the aggregate analysis established:

| File | Rows | Contents |
|---|---|---|
| `sql_function_window.rs` | 1–11, 36, 37 | The 11 UDWFs' values and the distribution negative results. |
| `sql_function_window_clause.rs` | 12–32 | Aggregates in `OVER`, frames, named windows, placement. |
| `sql_function_window_types.rs` | 33–35 | Result OIDs and labels — **must** use extended-protocol `Describe`. |
| `anonymization.rs` (extend) | 39–42 | Order-sensitive windows over anonymized columns. |

Two structural prerequisites, both already identified by the aggregate analysis:

1. **Promote `describe_result_types`** from its private home at
   `data_types_round_trips.rs:52` into `tests/e2e/src/lib.rs`. Rows 33–35 cannot be
   tested through `simple_query_rows` at all.
2. **Add a label-returning helper** alongside it. Row 34 needs `row.columns()`.

Following the house pattern, each gap gets up to two tests: a **passing**
`*_currently_wrong` / `*_currently_rejected` test pinning today's actual behavior, and
an `#[ignore = "gap (row N): …"]` test asserting the PG-correct target. Row 26 needs a
third shape — a **loop** asserting stability across N runs, since a single run cannot
observe nondeterminism.

```sh
cd tests/e2e && cargo test --test sql_function_window -- --ignored --test-threads=1
```

`make e2e` runs only the passing set, so the gap map never blocks CI.

## How this was measured

Against the running 5-node e2e cluster (`make e2e-up`; 1 coordinator, 1 scheduler,
3 cores), matching the aggregate analysis's methodology:

- **A real PostgreSQL 16.15 oracle**, in a throwaway container, seeded with an
  identical fixture and driven through the same `psql` script, byte-diffed construct
  by construct. Result-type OIDs read from both engines. A **DuckDB 1.5.5** CLI served
  as a second oracle for `IGNORE NULLS`, which PG 16 cannot parse.
- **Extended-protocol `Describe`** for every result type and column label, via a
  `tokio-postgres` probe — `psql`'s own `\gdesc` cannot be used here, because it hits
  an unrelated `pg_catalog` gap (`format_type(Utf8, Int64)` coercion failure). That
  is a separate finding for the command axis.
- **`VERBOSITY verbose`** for every SQLSTATE quoted.
- **Physical shard placement verified**, by copying `core.duckdb` (+ `.wal`) out of a
  core container and reading it with the DuckDB CLI, to confirm partition groups
  genuinely spanned all 3 shards rather than coincidentally colocating.
- **Shard-count invariance** checked by re-running at 1, 3 and 5 shards; stability by
  repeating nondeterminism-suspect queries 5–10 times.
- **Registered surface derived from source**, not from documentation:
  `datafusion-functions-window-53.1.0/src/lib.rs`, `ballista-core-53.0.0/src/extension.rs`,
  cross-checked against PG's `pg_proc WHERE prokind = 'w'`.

### Claims investigated and *not* confirmed

Recorded so they are not re-litigated:

- **"`upgrade_for_ballista` drops window functions."** Refuted:
  `ballista-core-53.0.0/src/extension.rs:341` installs `ballista_window_functions()`.
- **"The global, no-`PARTITION BY` case is the top sharding risk."** Refuted
  structurally and empirically (row 36). Ballista's stage cuts make it safe.
- **"A constant window `ORDER BY` silently drops the outer `ORDER BY`."** **Not
  reproduced.** Probed at top level with a literal, a string constant, a constant
  `PARTITION BY`, and alongside a second correctly-ordered window: the outer
  `ORDER BY … DESC` was honored in every case. The only shapes where order changed were
  ones where SQL guarantees no order anyway (an `ORDER BY` inside a subquery feeding an
  aggregate). Excluded from the gap table.
- **`avg(bigint) OVER ()` precision loss** — the fixture yielded an exact answer, so
  the >2⁵³ concern is inherited from the aggregate analysis, not reproduced here.
- **Row 34 client breakage** — protocol divergence confirmed (412 bytes), but no client
  actually broke.

## Corrections to the sibling analyses

Verified while measuring this axis:

- **The parser is sqlparser 0.61, not 0.58.** [gap-analysis-command.md](gap-analysis-command.md)
  ("How VaireDB decides", and row 25's note on `MERGE INTO`) and the aggregate analysis
  both cite 0.58. `Cargo.lock` on this branch contains only 0.61.0; `main` had both,
  and the branch diff removes 0.58.0. The deployed coordinator image matches the
  current lock file. The 0.58-era conclusions were re-verified against 0.61 and still
  hold — only the version citations are stale.
- **The aggregate analysis's ⛔ "named-window `PARTITION BY` lost" is four shapes, not
  one** (rows 24–27), and one of them is nondeterministic. It correctly deferred them
  here.
- **The aggregate analysis's prediction that an untyped `$1` degrades comparisons to
  lexicographic does not apply to window arguments** (row 35). Window offsets are
  casts, not comparisons; values are byte-identical to literal controls once the client
  declares an OID. Reclassified ⛔ → 🟡; two probes converged on this independently.

### For the command axis

Found incidentally, both belonging to [gap-analysis-command.md](gap-analysis-command.md):

- **`DELETE FROM vairedb_catalog.anonymization_secret` is rejected** —
  `[VDB-1004] only INSERT is supported on vairedb_catalog.anonymization_secret`. Secrets
  are append-only with no documented retraction path, which also means test fixtures
  cannot clean up after themselves.
- **`\gdesc` fails** on `format_type(Utf8, Int64)` coercion, so psql's own
  type-description command does not work against VaireDB.

## Open questions

1. **Should an order-sensitive window over an anonymized column be rejected?** Rows
   39–40 return confidently wrong analytics on exactly the columns marked sensitive.
   Rejecting is a behavior change; documenting is cheaper but leaves the footgun armed.
   Row 42 (`PARTITION BY`) is provably sound and should stay allowed either way.
2. **Fix the named-window handling, or reject the syntax?** Row 26's nondeterminism
   argues for rejecting `OVER (<name> …)` immediately and fixing properly later —
   rejection is strictly safer than an unstable answer, but it breaks queries that
   "work" today.
3. **How much of this is worth patching locally vs. upstreaming?** Seven of the ten ⛔
   rows are pure DataFusion defects, four of them one-liners. A DataFusion bump may
   close them for free, so local workarounds risk being throwaway — except rows 13 and
   38, where VaireDB's distribution is the trigger and a local fix is unavoidable.
