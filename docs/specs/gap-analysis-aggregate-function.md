# Aggregate Function Gap — DataFusion ↔ VaireDB ↔ PostgreSQL Wire Protocol

Gap analysis of the **aggregate functions** VaireDB can evaluate: from a
PostgreSQL wire-protocol client, through the coordinator's parse/plan chain, into
DataFusion's two-phase distributed aggregation on the Ballista executors.

This document is the executable-spec counterpart of the roadmap's only
`IN PROGRESS` item — *"Close the GAP with Datafusion [Operation and Literals,
Aggregate, Window, Scalar, Special]"*. It covers the **Aggregate** half of that
item. Operators and literals are covered by
[`gap-analysis-operator-literal.md`](gap-analysis-operator-literal.md); the
window-function surface beyond *aggregates used as window functions* (§ "Modifier
and clause surface", rows M9–M11), and the scalar/special families, remain
separate axes.

### References

- **DataFusion:** [Aggregate Functions](https://datafusion.apache.org/user-guide/sql/aggregate_functions.html) — General, Statistical and Approximate sections. DataFusion **53.1.0** (`Cargo.lock`), `datafusion-functions-aggregate 53.1.0`.
- **Ballista:** `ballista-core 53.0.0` — supplies the distributed execution of the aggregate plan.
- **PostgreSQL:** [Aggregate Functions](https://www.postgresql.org/docs/current/functions-aggregate.html) — the result-type and spelling contract clients actually expect.
- **DuckDB:** [Aggregate functions](https://duckdb.org/docs/current/sql/functions/aggregates.html) — listed for completeness only; see the framing below, DuckDB never evaluates an aggregate in VaireDB.
- **Sibling analyses:** [`gap-analysis-data-type.md`](gap-analysis-data-type.md), [`gap-analysis-operator-literal.md`](gap-analysis-operator-literal.md), [`gap-analysis-command.md`](gap-analysis-command.md).

## Scope and framing

The two sibling analyses had to track expressions through two engines, because
`SELECT` evaluates on DataFusion while `INSERT`/`UPDATE`/`DELETE` evaluate on
DuckDB. **Aggregates have only one engine.** An aggregate can only appear in a
`SELECT`, and on the read path the core node runs nothing but

```sql
SELECT <projected cols> FROM <shard_table>
```

(`vairedb-core/src/table_provider/scan_exec.rs:93`). No `WHERE`, no `GROUP BY`,
no `LIMIT`, and no aggregate is ever pushed into DuckDB. **Every aggregate in
VaireDB is computed by DataFusion**, on the Ballista executors, over raw rows
streamed up from the shards. DuckDB's aggregate catalog is therefore irrelevant
to VaireDB's aggregate surface, and DuckDB's own PostgreSQL divergences cannot
reach an aggregate result.

That single-engine property is what makes this axis much healthier than the
operator axis: there are **no split-brain rows**, because there is no second
evaluator to disagree with.

### Where the aggregate work actually happens

VaireDB registers **zero custom UDAFs**. The scheduler builds its session state
with `SessionStateBuilder::new().with_default_features()`
(`scheduler/scheduler.rs:68`), and `upgrade_for_ballista` (`:79`) re-installs
`ballista_aggregate_functions()`, which is the same DataFusion default set. So
the available aggregate surface is *exactly* DataFusion 53's 38 default
`AggregateUDF`s (`datafusion-functions-aggregate-53.1.0/src/lib.rs`,
`all_default_aggregate_functions`) plus their 7 aliases — no more, no less.
Nothing in VaireDB inspects an aggregate's name: `classify_statement`
(`query_router/query_router.rs:24`) branches on the `Statement` variant only.

### The distributed merge is correct, and that is not an accident

The load-bearing property is in `scheduler/remote_scan_exec.rs:46-51`: the shard
scan declares `Partitioning::UnknownPartitioning(1)` and an empty
`EquivalenceProperties`. Because DataFusion cannot prove the shard streams are
already hash-partitioned on the grouping key, it is *forced* to insert a hash
shuffle, producing the standard two-phase plan:

```
AggregateExec{mode=Partial}          ← per shard stream, on the executor
  → RepartitionExec(Hash([keys]))    ← Ballista cuts the stage here
    → AggregateExec{mode=FinalPartitioned}
```

VaireDB contains **no hand-rolled aggregate recombination** — the merge is
DataFusion's, and it is the same code path a single-node DataFusion uses. This
structurally rules out the classic sharded-database aggregate bugs: there is no
place for an average-of-averages, a summed-per-shard `COUNT(DISTINCT)`, or a
per-shard `MEDIAN`, because the partial aggregate never emits a finished value.

This was confirmed empirically as well as structurally — see *How this was
measured*. Every aggregate's value matched hand-computed ground truth, whole-table
and per-group, across a 3-shard table, including NULLs, values duplicated across
shard boundaries, and a skewed table with one populated and two empty shards.

> **Documentation note.** `distributed-query-processing.md:70` and
> `docs/vairedb.io/docs/concepts/query-processing.md:85` claim Ballista/DataFusion
> handle "aggregation (two-phase, multi-level)". That claim is **accurate**. The
> adjacent claim on the same lines about "push-down (filters, partial
> aggregations)" is not: nothing is pushed into DuckDB SQL. The two must not be
> conflated — the aggregation is genuinely two-phase, it just happens above the
> shard boundary rather than inside it.

**So the gaps in this document are not about distribution.** They are about the
PostgreSQL *type* contract, four defects in the param-typing, AST-rewrite and
anonymization layers that sit either side of DataFusion, two window-clause
defects, and PostgreSQL spellings DataFusion simply does not have.

## Verdict legend

| Status | Meaning |
|---|---|
| ✅ | **Works, and agrees with PostgreSQL.** |
| ⛔ | **Silently wrong.** Parses, executes, returns a value — a *different* value than PostgreSQL would. No error, no warning. The most dangerous class. |
| 🟡 | **Works, but partially** — one path only, degraded semantics, a wrong advertised type, or an honest error in an unexpected SQLSTATE. |
| ❌ | **Rejected.** Fails loudly with a SQLSTATE. |

## Summary

Measured against DataFusion 53's 38 default aggregates (45 spellings including
aliases), the 12 modifier/clause combinations PostgreSQL clients use with them,
the 12 PostgreSQL aggregate spellings DataFusion lacks, and the 4 cross-cutting
defects that aggregates expose:

| Axis | Rows | ✅ | ⛔ | 🟡 | ❌ |
|---|---:|---:|---:|---:|---:|
| DataFusion's 38 default aggregates | 38 | 30 | 2 | 6 | — |
| Modifier & clause combinations (`DISTINCT`, `FILTER`, `WITHIN GROUP`, `OVER`, grouping sets, …) | 12 | 7 | 2 | 3 | — |
| PostgreSQL spellings absent from DataFusion | 12 | — | — | — | 12 |
| Cross-cutting defects reached *through* aggregates | 4 | — | 4 | — | — |
| **Total** | **66** | **37** | **8** | **9** | **12** |

**Every one of the 38 aggregates is reachable and computes a correct value over
its own Arrow type.** No aggregate is missing, mis-merged, or mis-distributed —
the 2 ⛔ aggregate rows are `sum(bigint)` and `avg(bigint)`, both caused by the
advertised *result type* rather than by the computation, and the 6 🟡 rows are
type divergences with correct values. Separately, **all 38 are labelled with
DataFusion's rendered plan expression instead of PostgreSQL's bare function
name** — not counted above because it is one defect affecting every row; see
*Result column labels*.

### The ⛔ rows, ranked

| # | Construct | Returns | PostgreSQL returns | Root cause |
|---:|---|---|---|---|
| 0 | `HAVING <agg> <op> $1` (extended protocol, untyped param) | wrong row set — `$1='60'` over sums `50/75/185` yields only the `75` group | all groups with `sum > 60`, i.e. `75` **and** `185` | `pgwire_handler/handler.rs:341` — no fallback to the client-declared OID, so `$1` stays `Utf8` and the comparison is **lexicographic** |
| 1 | `sum(bigint)` | `-1` for `2^63-1 + 2^63-1 + 1` | `18446744073709551615` | typed `int8` (oid 20), not `numeric`; wraps in two's complement |
| 2 | bare correlated scalar subquery in the select list | column literally named `NULL`, all rows empty | the subquery's value | `datafusion-pg-catalog` `RemoveSubqueryFromProjection` (`sql/rules.rs:1100`) folds it to `Expr::Value(Null)`; an **unaliased** table counts as correlated (`:1052`) |
| 3 | `<agg>(…) FILTER (WHERE …) OVER (…)` | the `FILTER` is **discarded** — `count(*) FILTER (WHERE m > 100000) OVER (PARTITION BY cat)` returns `4`, not `0` | `0` | `FILTER` is dropped when the aggregate is used as a window function. Works correctly on a plain aggregate (see M2) |
| 4 | `OVER (<named_window> ORDER BY …)` | `PARTITION BY` from the named window is dropped — returns the unpartitioned running sum (`80` where the partitioned answer is `30`) | the partitioned running sum | named-window inheritance loses clauses when the `OVER` adds its own; inline `OVER (PARTITION BY … ORDER BY …)` is correct |
| 5 | `avg(bigint)` | `6148914691236517000` | `6148914691236517205` | computed in `float8`; loses integer precision above 2⁵³ |
| 6 | `min`/`max`/`ORDER BY` on an **anonymized** column | lexicographic extreme of the HMAC **digest** | the plaintext extreme | `anonymization/rewrite.rs:89` — `_ => Ok(())`, a structural no-op for `SELECT` |
| 7 | `<agg>` filtered on an anonymized column by plaintext | the empty-set answer (`0` / `NULL`) | the real answer | same as row 6 — the predicate literal is never hashed, so it matches no digest |

Mapping these onto the summary axes: only **rows 1 and 5** are aggregate defects.
**Rows 3 and 4** are window-clause defects (`M10`, `M11`). **Rows 0, 2, 6 and 7**
are the cross-cutting ones — param typing, an AST rewrite, and anonymization
twice — and they are most visible through aggregates rather than caused by them,
so fixing them fixes far more than this document's surface.

Rows 6 and 7 are the consequence of anonymization being write-path-only. They are
listed because an analyst aggregating an anonymized column gets a plausible number
with no indication it is meaningless. Note that `count`, `count(DISTINCT)` and
`GROUP BY` on an anonymized column *are* semantically correct — the HMAC is
deterministic, so equality, and therefore cardinality, survive it.

## Master table: DataFusion's 38 default aggregates

Grouped as the DataFusion reference page groups them. `Result type` is the OID
VaireDB advertises in `RowDescription`, captured over the **extended** protocol
(`Parse`/`Describe`, no execution). `PG` is PostgreSQL 16's type for the same
input.

### General functions

| # | Aggregate | Result type (VaireDB) | PG | Status | Notes |
|---:|---|---|---|---|---|
| 1 | `count(*)`, `count(x)`, `count(DISTINCT x)` | `int8` (20) | `int8` | ✅ | Exact across shards; the hash shuffle makes `DISTINCT` globally correct. Empty table → `0`, matching PG. |
| 2 | `sum(int4)` | `int8` (20) | `int8` | ✅ | |
| 3 | `sum(int8)` | `int8` (20) | **`numeric`** | ⛔ | Wraps. See ⛔ row 1. Workaround: `sum(CAST(x AS NUMERIC))`. |
| 4 | `sum(float8)` | `float8` (701) | `float8` | ✅ | |
| 5 | `sum(numeric)` | `numeric` (1700) | `numeric` | ✅ | Scale follows the `Decimal128(38,10)` the coordinator assigns every NUMERIC — see the data-type analysis, `:171`. |
| 6 | `avg(int4)` | `float8` (701) | **`numeric`** | 🟡 | Value exact (int4 fits in the float8 mantissa), type wrong. A client binding the column into a decimal gets a float. |
| 7 | `avg(int8)` | `float8` (701) | **`numeric`** | ⛔ | Value **inexact** above 2⁵³. See ⛔ row 5. |
| 8 | `avg(float8)`, `avg(numeric)` | `float8` / `numeric` | same | ✅ | |
| 9 | `min`/`max` | input type (`int4`→23, `int8`→20, `float8`→701, `numeric`→1700, text→25, `bool`→16, `date`→1082, `timestamp`→1114) | same | ✅ | Type-preserving and correct for every type probed. text→25 rather than `varchar`→1043 is inherited from how the coordinator advertises VARCHAR, not an aggregate issue. |
| 10 | `median` | input type | *(none)* | ✅ | DataFusion extension. Exact, not approximate, and globally correct — the shuffle collects all values for a group onto one executor. |
| 11 | `array_agg` | `_int4` (1007) / `_text` (1009) | `anyarray` | ✅ | `ORDER BY` inside the call works (`array_agg(m ORDER BY m DESC)`), with PG's NULLS-FIRST-on-DESC default. Element OID follows the base column's advertised OID, so it is self-consistent. |
| 12 | `string_agg` | `text` (25) | `text` | ✅ | Separator and inner `ORDER BY` both work. `string_agg(DISTINCT …)` without `ORDER BY` returns shuffle order — unspecified in PG too, so not a gap, but do not rely on it. |
| 13 | `bit_and`, `bit_or`, `bit_xor` | `int4` (23) | `int4` | ✅ | |
| 14 | `bool_and`, `bool_or` | `bool` (16) | `bool` | ✅ | PG's `every()` alias is missing — see the ❌ table. |
| 15 | `first_value`, `last_value` | input type | *(none as aggregates)* | ✅ | Aggregate form with inner `ORDER BY` works. PostgreSQL has these only as window functions. |
| 16 | `grouping` | `int4` (23) | `int4` | ✅ | Correct bitmask under `ROLLUP`, `CUBE` and `GROUPING SETS`. The `not_impl_err!` in `grouping.rs:110` is unreachable from a grouping-set context — the planner resolves the call before an accumulator is ever built. |

### Statistical functions

| # | Aggregate | Result type (VaireDB) | PG | Status | Notes |
|---:|---|---|---|---|---|
| 17 | `corr` | `float8` (701) | `float8` | ✅ | |
| 18 | `covar_samp` (alias `covar`), `covar_pop` | `float8` (701) | `float8` | ✅ | |
| 19 | `stddev` (alias `stddev_samp`), `stddev_pop` | `float8` (701) | **`numeric`** for int/numeric input | 🟡 | Correct to float8 precision. Over a `numeric` column PG stays exact and VaireDB does not. |
| 20 | `var` (aliases `var_samp`, `var_sample`), `var_pop` (alias `var_population`) | `float8` (701) | **`numeric`** for int/numeric input | 🟡 | Same as row 19. PG's `variance` spelling is missing — see the ❌ table. |
| 21 | `regr_slope`, `regr_intercept`, `regr_r2`, `regr_avgx`, `regr_avgy`, `regr_sxx`, `regr_syy`, `regr_sxy` | `float8` (701) | `float8` | ✅ | All 8 correct, whole-table and per-group. |
| 22 | `regr_count` | **`numeric`** (1700) | `int8` | 🟡 | DataFusion returns `UInt64`; `arrow-pg` has no unsigned PG type so it maps to `numeric`. Value correct (`0` on an empty table, matching PG). |
| 23 | `nth_value` | input type | *(none as aggregate)* | ✅ | Aggregate form with inner `ORDER BY`. |

### Approximate functions

| # | Aggregate | Result type (VaireDB) | PG | Status | Notes |
|---:|---|---|---|---|---|
| 24 | `approx_distinct` | **`numeric`** (1700) | *(none)* | ✅ | Same `UInt64`→`numeric` mapping as row 22. No PG counterpart, so no divergence to record — but clients get a decimal where a count is expected. |
| 25 | `approx_median` | input type | *(none)* | ✅ | |
| 26 | `approx_percentile_cont`, `approx_percentile_cont_with_weight` | input type / `float8` | *(none)* | ✅ | |
| 27 | `percentile_cont` (alias `quantile_cont`) | `float8` (701) | `float8` | 🟡 | Type correct, **value truncated to 5 decimal places**: `percentile_cont(0.9) WITHIN GROUP (ORDER BY i_col)` returns `90.99999` where the exact answer is `91`. Interpolation precision, not a distribution error. |

## Modifier and clause surface

Where aggregates meet the rest of the language. Two of the 8 ⛔ rows live here,
and neither is in an aggregate itself — both are window-clause defects.

| # | Construct | Status | Behavior |
|---:|---|---|---|
| M1 | `DISTINCT` inside any aggregate | ✅ | `sum(DISTINCT m)`, `avg(DISTINCT m)`, `max(DISTINCT m)`, `string_agg(DISTINCT …)` all globally correct. |
| M2 | `FILTER (WHERE …)` on a plain aggregate | ✅ | `count(*) FILTER (…)`, `sum(m) FILTER (…)` correct. |
| M3 | `ORDER BY` inside an aggregate | ✅ | `array_agg`, `string_agg`, `first_value`, `last_value`, `nth_value`. |
| M4 | `GROUP BY` / `HAVING` | ✅ | Including `HAVING` with no `GROUP BY` (returns 1 row or 0, matching PG) and an aggregate in `ORDER BY`. |
| M5 | `ROLLUP`, `CUBE`, `GROUPING SETS ((a),())` | ✅ | Correct grand totals and correct `grouping()` bitmasks. |
| M6 | `GROUPING SETS (())` — empty set alone | 🟡 | Returns **0 rows**; PG returns 1 grand-total row. Narrow: the same set works when combined with a non-empty one (M5). |
| M7 | `DISTINCT ON (col)` | ✅ | PostgreSQL-specific extension, works. |
| M8 | `count(DISTINCT a, b)` — comma form | 🟡 | `XX000` `NotImplemented("COUNT DISTINCT with multiple arguments")`. Should be `0A000`. Workaround: `count(DISTINCT (a, b))`, which plans as `count(DISTINCT struct(a,b))` and is **correct**. |
| M9 | Aggregate as a window function, `OVER (PARTITION BY … ORDER BY …)` | ✅ | Inline `OVER` clauses are correct, including the `rank`/`dense_rank` family. |
| M10 | `FILTER` on a **window** aggregate | ⛔ | Silently discarded. ⛔ row 3. |
| M11 | `OVER (<named_window> ORDER BY …)` | ⛔ | Named-window clauses dropped. ⛔ row 4. |
| M12 | Nested aggregate, e.g. `max(sum(m))` | 🟡 | `XX000` with a **~1 KB internal `Signature { … }` debug dump** in the client-visible message. PG rejects at parse with `42803 aggregate function calls cannot be nested`. Honest failure, but the wrong SQLSTATE and an unacceptable message. |

### Result column labels

Every aggregate is labelled with DataFusion's plan-rendered expression rather than
PostgreSQL's bare function name:

| Query | VaireDB label | PG label |
|---|---|---|
| `SELECT count(*) FROM t` | `count(*)` | `count` |
| `SELECT sum(b) FROM t` | `sum(t.b)` | `sum` |
| `SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY i) FROM t` | `percentile_cont(Float64(0.5)) WITHIN GROUP [t.i ASC NULLS LAST]` | `percentile_cont` |
| `SELECT rank() OVER (ORDER BY m) FROM t` | `rank() ORDER BY [t.m ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` | `rank` |

This affects **every aggregate**, and it breaks any client that addresses result
columns by name (`row["count"]`, most ORMs' default aggregate mapping, BI tools
that infer measure names). It is not silently wrong — the data is right — but it
is the single highest-frequency compatibility issue on this axis. `AS <alias>` is
a complete workaround, which is why it has gone unnoticed.

## PostgreSQL aggregates that are absent

All 12 reject honestly: `0A000` `[VDB-1004] Error during planning: Invalid
function '<name>'`, with DataFusion's did-you-mean suggestion appended (often
comically unhelpful — `variance` suggests `range`, `xmlagg` suggests `log2`).

| # | PostgreSQL aggregate | Closest available | Cost to close |
|---:|---|---|---|
| 28 | `variance(x)` | `var(x)` / `var_samp(x)` | **Alias only** — one line in DataFusion, or a coordinator-side rewrite. |
| 29 | `every(x)` | `bool_and(x)` | **Alias only.** |
| 30 | `any_value(x)` | `first_value(x)` | Near-alias (PG's is order-indifferent). |
| 31 | `percentile_disc(f) WITHIN GROUP (ORDER BY x)` | `percentile_cont` | New UDAF — discrete percentile, no interpolation. |
| 32 | `mode() WITHIN GROUP (ORDER BY x)` | — | New UDAF. |
| 33–36 | `rank()`, `dense_rank()`, `percent_rank()`, `cume_dist()` as **hypothetical-set** aggregates (`WITHIN GROUP`) | the window forms, which work | New UDAFs. The window spellings of all four are supported (M9), so this is only the `WITHIN GROUP` form. |
| 37–38 | `json_agg`, `jsonb_agg` | — | New UDAFs, and blocked behind the JSON type gap — see the operator analysis, which finds the whole JSON operator surface unreachable. |
| 39 | `xmlagg` | — | No XML type. Out of scope. |

Rows 28–30 are aliases or near-aliases, and remove a quarter of this table for a
handful of lines.

## Root causes

Five root causes explain everything above. Only the first is aggregate-specific.

1. **No PostgreSQL result-type mapping layer for aggregates.** VaireDB advertises
   whatever Arrow type DataFusion's UDAF declares. PostgreSQL's aggregate result
   types are deliberately *wider* than their inputs — `sum(bigint)→numeric`,
   `avg(int)→numeric`, `stddev/variance→numeric` — precisely to prevent the
   overflow in ⛔ row 1 and the precision loss in ⛔ row 5. This single mismatch
   produces both aggregate ⛔ rows (1, 5) and 5 of the 6 🟡 rows — `stddev`,
   `stddev_pop`, `var`, `var_pop` (master rows 19–20) and `regr_count` (row 22),
   plus the type-only half of `avg` at master row 6. The sixth 🟡,
   `percentile_cont`, is an interpolation-precision bug and unrelated.

2. **`decode_param_values` has no fallback to the client-declared parameter OID**
   (`pgwire_handler/handler.rs:341`). It consults only
   `plan.get_parameter_types()`; when DataFusion cannot infer a placeholder's
   type — which is exactly what happens for `$1` on the right of a `HAVING`
   comparison — the parameter stays `Utf8` and the comparison silently becomes a
   *string* comparison. This is ⛔ row 0, and the same root cause is already
   recorded for `LIMIT $1` elsewhere. The `Bind` message carried the correct OID
   the whole time.

3. **`datafusion-pg-catalog`'s `RemoveSubqueryFromProjection` rewrite**
   (`sql/rules.rs:1019-1143`) replaces a correlated scalar subquery in a
   projection with a `NULL` literal (`:1100`, `:1115`), and its correlation check
   treats an **unaliased** table as correlated (`:1052`). This rule exists to make
   `pg_catalog` emulation tractable but applies to user queries too — ⛔ row 2.
   Adding any expression around the subquery (`(SELECT …) + 0`) defeats the rule
   and returns the correct answer, which is a reliable diagnostic signature.

4. **Anonymization is structurally write-path-only.**
   `anonymization/rewrite.rs:20-91` handles `Statement::Insert` and
   `Statement::Update` and falls through `_ => Ok(())` at `:89`. Aggregates
   therefore see HMAC digests, and plaintext predicates match nothing — ⛔ rows 6
   and 7. Equality-preserving aggregates (`count`, `count(DISTINCT)`, `GROUP BY`)
   remain correct because the HMAC is deterministic.

5. **The error classifier and sanitizer are pinned to pre-53 DataFusion message
   text.** `classify_generic_error_code`
   (`pgwire_handler/error_enrichment.rs:112`) matches `"not yet implemented"` and
   `"unsupported"`; DataFusion 53 emits `"This feature is not implemented: "` and
   `"not supported"`, so neither matches and honest feature gaps surface as
   `XX000` instead of `0A000` (M8, M12). Symmetrically,
   `vairedb-common/src/error/sanitize.rs:9-12` strips `"Plan error: "`,
   `"Not Implemented: "` and `"Configuration error: "`, none of which DataFusion 53
   emits — so `"Error during planning: "` leaks verbatim into every ❌ row's
   client-visible message, as does the `Signature { … }` dump in M12 and
   DataFusion's "file a bug report in our issue tracker" boilerplate.

## Prioritized remediation

Ranked by client impact per unit of work. Items 1–4 and 8 change a returned
*value* — they are the correctness work. The rest change types, labels or error
shapes.

1. **Fix the parameter-type fallback** (⛔ row 0) — in `decode_param_values`, fall
   back to the OID the client declared in `Parse`/`Bind` when
   `get_parameter_types()` yields none. Smallest change with the largest
   correctness win: a wrong-row-set bug in a construct (`HAVING sum(x) > ?`) that
   every reporting client emits, currently invisible because it returns *plausible*
   rows. Fixes `LIMIT $1` at the same time.
2. **Introduce a PostgreSQL aggregate result-type mapping** (⛔ rows 1 and 5, plus
   the 5 🟡 type rows) — promote `sum(int8)`→`numeric`, `avg(int*)`→`numeric`,
   `stddev`/`var` over exact inputs→`numeric`, `regr_count`→`int8`. Implementable
   either as thin wrapper UDAFs registered ahead of the defaults, or as a
   coordinator-side cast injected into the logical plan. Until then `sum` over a
   `bigint` column is a silent data-corruption risk.
3. **Neutralize `RemoveSubqueryFromProjection` for user queries** (⛔ row 2) —
   scope the rewrite to `pg_catalog`/`information_schema` plans (the `local_ctx`
   path, `scheduler.rs:99`) so a user `SELECT` never has a subquery folded to
   `NULL`. Alternatively fix the correlation check at `:1052` to stop treating
   unaliased tables as correlated.
4. **Fix the window-clause defects** (⛔ rows 3 and 4) — carry `FILTER` through
   window-aggregate planning, and make named-window inheritance additive. Both are
   silent, both have correct inline equivalents to differential-test against.
   Belongs to whoever picks up the window-function axis.
5. **Refresh the error classifier and sanitizer to DataFusion 53 text** (M8, M12,
   all ❌ rows) — match on `DataFusionError` variants rather than message
   substrings, so the mapping cannot silently rot on the next upgrade. Then
   truncate or suppress `Signature { … }` dumps. Cheap, and it makes every honest
   failure on this axis report the right SQLSTATE.
6. **Add the PostgreSQL aliases** — `variance`→`var_samp` and `every`→`bool_and`
   are exact synonyms; `any_value`→`first_value` is a near-synonym (PostgreSQL's is
   explicitly order-indifferent, so aliasing it is defensible but should be a
   deliberate decision). Removes 3 of the 12 ❌ rows for a handful of lines.
7. **Label aggregate result columns PostgreSQL-style** — emit the bare function
   name instead of the rendered plan expression. Affects every aggregate query and
   every by-name client mapping; needs care not to disturb the `pg_catalog`
   emulation path, which relies on `RemoveQualifier` (`sql/rules.rs:780`) for its
   own labelling.
8. **Reject or annotate aggregates over anonymized columns** (⛔ rows 6 and 7) —
   at minimum reject `min`/`max`/`ORDER BY` on an anonymized column with a clear
   error, and hash the literal in a `SELECT … WHERE anon_col = <literal>`
   predicate. A number that is silently computed over digests is worse than an
   error.
9. **`percentile_cont` precision** (master row 27) and **`GROUPING SETS (())`**
   (M6) — narrow, upstream, low frequency. File upstream rather than working
   around locally.
10. **Consider `percentile_disc` and `mode()`** — genuinely useful and genuinely
    absent; the only two ❌ rows that need real UDAFs and are worth writing.

Explicitly **not** a gap and not worth work: DuckDB aggregate parity. DuckDB never
evaluates an aggregate in VaireDB, so its catalog is not a target.

## Executable counterpart

Aggregate coverage today is incidental rather than systematic — 9 tests that use
an aggregate while testing something else (`sql_command_select.rs:374` `test_count`,
`:407` `test_scalar_aggregates`, `:443` `test_group_by_multi_agg`, `:501`
`test_group_by_having`, `:325` `test_distinct`, `:838`/`:856` the two window tests;
`errors.rs:242` `test_aggregates_empty_table`; and the `COUNT` trio at
`data_types_round_trips.rs:103` plus `SUM(amount)` at `:518`, both there to verify a
*type*, not the aggregate). All pass. **None asserts an aggregate's result type,
and none of the 8 ⛔ rows is covered** — which is why they were all live.

Proposed suite, following the `<doc-topic>_<facet>.rs` convention established by
`sql_command_*` and `data_types_*`:

| File | Rows | Contents |
|---|---|---|
| `sql_function_aggregate.rs` | 1–27, M1–M12, 28–39 | One test per master-table row: value correctness against hand-computed ground truth, plus the modifier surface and the ❌ rejections. |
| `sql_function_aggregate_types.rs` | 1–27, labels | The result-type contract. `Parse`/`Describe` only, **no execution** — reuse the private `describe_result_types` helper at `data_types_round_trips.rs:52`, promoting it into `tests/e2e/src/lib.rs`. Also pins the column-label rows. |
| `sql_function_aggregate_distributed.rs` | merge invariants | The properties that must not regress if `remote_scan_exec.rs:46` ever gains a partitioning claim: `count(DISTINCT)` across shards, `median` across shards, `avg` ≠ average-of-averages, aggregates over a skewed table (one populated shard, two empty), and over NULL-heavy and cross-shard-duplicate data. These are the tests that would catch a wrong "optimization". |

Follow the existing convention: a **passing** test pinning today's honest
rejection (`assert_unsupported` for `0A000`, `assert_rejected` where the stage is
uncertain), plus an `#[ignore = "gap (row N): …"]` test asserting the
PostgreSQL-correct behavior, which fails by construction and is the definition of
done. For the 8 ⛔ rows the ignored test **must assert the correct value**, not
merely that no error occurred — every one of these gaps returns a successful
`CommandComplete`, so an error-shape assertion passes while the answer is wrong.

```sh
cd tests/e2e && cargo test --test sql_function_aggregate -- --ignored --test-threads=1
```

`make e2e` runs only the passing set, so the gap map never blocks CI.

## How this was measured

Empirically, against the live 5-node e2e cluster (`make e2e-up`, DuckDB v1.5.5 on
the core nodes) over the coordinator's PostgreSQL wire port — not by reading
capability tables. Before any probe, the running images were confirmed to postdate
the newest source change on the branch, so the measurements describe current code.

1. **Value correctness, whole-table and per-group** — all 38 aggregates over a
   3-shard table seeded so that ground truth is hand-computable (12 rows, 3 groups,
   2 NULLs, and values deliberately duplicated across shard boundaries so a
   per-shard-then-sum bug would show). Per-group sums `50/75/185`, total `310`,
   `count(*)=12`, `count(m)=10`.
2. **Distribution stress** — the same aggregates over a skewed table (1 row, 2
   empty shards) and an empty table, which is what exercises the
   partial-aggregate-with-no-input path.
3. **Result types over the extended protocol** — 70 `Parse`/`Describe` round trips
   through a standalone `tokio-postgres` client, reading OIDs straight from
   `RowDescription` **without executing**. This was necessary because `psql \gdesc`
   does not work against VaireDB: it fails `0A000` inside `format_type(Utf8,
   Int64)`. Base-column OIDs were captured alongside the aggregate OIDs, which is
   what separates a real type gap from one merely inherited from how the
   coordinator advertises the column (this reclassified `array_agg(varchar)` and
   `min(varchar)` from gaps to self-consistent rows).
4. **`VERBOSITY verbose` on every failing probe**, to capture the SQLSTATE and the
   `[VDB-NNNN]` enrichment code and attribute each failure to a pipeline stage.
5. **Registered-surface ground truth from source** — the 38 aggregates and their 7
   aliases were read out of `all_default_aggregate_functions` in
   `datafusion-functions-aggregate-53.1.0/src/lib.rs` and the per-UDAF `aliases()`
   implementations, rather than from the documentation page, which does not
   distinguish primary names from aliases.
6. **Plan-shape audit** — `remote_scan_exec.rs`, `scheduler.rs` and
   `scan_exec.rs` were read to establish *why* the merge is correct, so that the
   empirical result is explained by a structural property rather than by luck.

Two source-derived conclusions were **overturned by measurement** and the
empirical result kept:

- `grouping()`'s `accumulator()` is `not_impl_err!` at
  `datafusion-functions-aggregate-53.1.0/src/grouping.rs:110`, which reads like a
  failure at execution. `grouping()` in fact works with `ROLLUP`, `CUBE` and
  `GROUPING SETS`, with correct bitmasks — the planner resolves the call before an
  accumulator is built, so that path is unreachable from a grouping-set context.
- Partial aggregation was initially assumed to be an overclaim in
  `distributed-query-processing.md:70` by analogy with filter push-down. Tracing
  the physical plan showed aggregation *is* genuinely two-phase; only push-down
  **into DuckDB SQL** is absent. The two claims share a line of documentation but
  not a fate.

Every ⛔ row was reproduced at least twice, and each was paired with a *control*
probe that isolates the cause: `sum(CAST(x AS NUMERIC))` for row 1, `(SELECT …) + 0`
for row 2, an inline `OVER (PARTITION BY … ORDER BY …)` for rows 3 and 4, and
`$1::int` for row 0. Each control returns the correct answer, which is what
localizes the defect to the rewrite/typing layer rather than to the aggregate or
its distribution.
