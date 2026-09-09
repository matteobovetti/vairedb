# DataFusion vs. VaireDB (PostgreSQL wire protocol) — Window Function Gap

Gap analysis of the **window function** surface DataFusion documents against what
VaireDB's coordinator actually returns over the PostgreSQL wire protocol, measured
against a live 5-node cluster and byte-diffed against PostgreSQL 16.

- **Reference (DataFusion):** [Window Functions](https://datafusion.apache.org/user-guide/sql/window_functions.html) — 11 built-in window functions plus the `OVER` clause surface.
- **Reference (PostgreSQL):** [Window Function Calls](https://www.postgresql.org/docs/16/sql-expressions.html#SYNTAX-WINDOW-FUNCTIONS) — the contract VaireDB advertises by speaking the PG wire protocol.
- **Sibling analyses:** [aggregate functions](gap-analysis-aggregate-function.md) (which explicitly defers this axis), [operators and literals](gap-analysis-operator-literal.md), [commands](gap-analysis-command.md), [data types](gap-analysis-data-type.md). All five are consolidated in [`gap-analysis.md`](gap-analysis.md).

## How VaireDB decides

When this was first measured, window functions had **no VaireDB-specific code path at
all**: a `grep` for window machinery across the whole `crates/` tree returned exactly one
hit, in `node_service/failure_detector.rs`, and it was an unrelated time-window. Every
behavior was inherited.

That is no longer true. The coordinator now owns five window-specific pieces, and every one
of them exists because an inherited behavior was wrong:

| VaireDB layer | What it decides | Rows |
|---|---|---|
| `pgwire_handler/pg_operators.rs` | Refuses the clauses DataFusion parses and then drops, with `0A000`. | 13, 22, 25, 26, 27 |
| `pgwire_handler/anonymized_reads.rs` | Refuses a window ordered by a pseudonymized column, whose plaintext order is not on the server. | 39, 40, 41 |
| `scheduler/window_partition_sort.rs` | Restores the partition ordering Ballista's shuffle boundary erases. | 38 |
| `pgwire_handler/pg_aggregate_widening.rs` | Widens `sum(bigint) OVER (…)` and `avg(int*) OVER (…)` to a decimal accumulator, as for the grouped form. | 15 |
| `pgwire_handler/column_labels.rs` | Labels an unaliased result column after its function, not after the rendered plan expression. | 34 |

Everything else is still inherited, from one of four layers:

| Layer | Version | What it decides |
|---|---|---|
| sqlparser (via `datafusion::sql::sqlparser`) | **0.62.0** | Whether the `OVER` clause parses at all (`42601`). |
| datafusion-sql / datafusion-expr | 54.1.0 | Whether the `WindowSpec` AST is honored, and logical validation. |
| datafusion-functions-window | 54.1.0 | The 11 UDWF implementations and their return types. |
| ballista-core + datafusion-proto | 54.1.0 | Whether the physical window expression survives serialization to the executors. |

Versions are the ones in `Cargo.lock` today. The row-by-row behavior below was first
measured on sqlparser 0.61 / DataFusion 53.1, before the upgrade. Every row that changed
verdict was re-probed on the current versions and says so; treat an unannotated verdict as
"last measured on 53.1", and note that one such row — `ntile` (row 4) — turned out to have
been fixed by the bump alone, so the unannotated verdicts are the ones most worth
re-probing.

A window query therefore fails at one of five points:

| Rejection point | Error | SQLSTATE | When |
|---|---|---|---|
| Parse | `SqlSyntaxError` | `42601` | sqlparser cannot represent the clause. Only `EXCLUDE` (row 21). |
| Coordinator rewrite | `[VDB-1004] … is not supported: …` | `0A000` | A clause DataFusion would parse and drop (rows 13, 22, 25–27). |
| Logical planning | `Error during planning` | `XX000` | Window function in an illegal position (rows 29, 31). |
| Physical planning | `Physical plan does not support logical expression …` | `XX000` | Same, surfaced later; emits a ~1.5 KB `Signature { … }` debug dump. |
| Distributed execution | `[VDB-5001] Job … failed` | `XX000` | Runtime failure inside a Ballista stage. Was row 38, now closed. |

**The second row is new, and it is the shape of the whole update to this document.**
Window functions ride the ordinary `Select` path (`classify_statement` →
`handle_select`), so originally there was no `0A000` surface here at all, and the axis
failed by returning wrong numbers far more often than by refusing: 10 of 42 rows silently
wrong against 3 loud rejections. Five of those ten were clauses DataFusion accepted and
then ignored, and each is now refused by name before planning; three more were the
anonymized-column rows, refused in `pgwire_handler/anonymized_reads.rs` for the same
reason. The count is **1 silently wrong against 10 rejections** — the same defects,
converted from a plausible answer into a statement that VaireDB will not answer that
question.

## Verdict legend

Inherited from the aggregate analysis:

- ✅ works, and agrees with PostgreSQL
- ⛔ **silently wrong value** — returns a plausible answer that is not the PG answer
- 🟡 partial, degraded, wrong result type, or wrong SQLSTATE
- ❌ rejected loudly

## Summary

| Verdict | Count | Rows |
|---|---:|---|
| ✅ Correct | 27 | 1–10, 12, 14–20, 23, 24, 28, 30, 32, 36–38, 42 |
| 🟡 Partial / wrong type / wrong SQLSTATE | 4 | 31, 33–35 |
| ⛔ **Silently wrong** | 1 | 11 |
| ❌ Rejected | 10 | 13, 21, 22, 25–27, 29, 39–41 |

Thirteen rows moved after this was first measured, and the one that remains silently wrong is
the narrowest in the document. In order of how they closed:

- **Rows 1–3 (⛔ → ✅), by the coordinator.** The wire schema widens a `UInt64` result
  column to `Int64` and casts the payload with it (`pgwire_handler/encoding.rs`), so
  `row_number` / `rank` / `dense_rank` advertise the `int8` PostgreSQL promises.
- **Rows 4 (⛔ → ✅) and 38 (❌ → ✅), by a version bump and a physical rule.** `ntile`'s
  bucket formula was corrected upstream between 53.1 and 54.1; row 38 was VaireDB's own
  and is fixed in `scheduler/window_partition_sort.rs`.
- **Rows 13, 22, 25, 26, 27 (⛔ → ❌), by refusing.** Five clauses DataFusion parses and
  then discards. None of them can be answered correctly here, so each is refused by name
  with `0A000` and a suggested rewrite, rather than answered as though the clause had
  not been written.
- **Rows 39–41 (⛔ → ❌), by refusing.** A window *is* an ordering, and a pseudonymized
  column has no plaintext order on the server to order by, so the coordinator refuses the
  order-sensitive reads instead of ranking digests (§ 8).
- **Row 15 (🟡 → ✅), on the aggregate axis rather than this one.** `avg(bigint) OVER ()` is
  now the exact `numeric` PostgreSQL promises, because `pg_aggregate_widening` gained an `avg`
  arm and — as with `sum` — matches the window node beside the aggregate one. That invariant
  is what this row exists to hold: a client must not get two types for one average depending
  on whether it wrote `OVER`.
- **Row 34 narrowed but stays 🟡**, and row 33 keeps its 🟡 for `ntile` alone,
  which moved from `numeric` to `int8` and still is not PG's `int4`.

The one remaining ⛔ row is `nth_value(x, 0)` (row 11). **The clause surface no longer
lies.**

**There is no missing-function gap.** All 11 PostgreSQL window functions are
registered in DataFusion (`datafusion-functions-window/src/lib.rs`), Ballista re-installs
exactly that set (`ballista-core/src/extension.rs` →
`with_window_functions(ballista_window_functions())`), and PostgreSQL 16's
`pg_proc WHERE prokind = 'w'` returns the same 11 names. The frame engine is likewise
healthy: all five frame units, peer-group semantics, and integer/float/`INTERVAL` offsets
are PG-correct.

**The gap was not coverage — it was silent wrongness in the clause surface around the
functions**, plus two systemic issues (anonymized columns, and result-type OIDs). Both are
now closed the same way — answered where the right answer was reachable, refused where it
was not — and what is left is `ntile`'s `int4` and one upstream guard.

### The two entries that outranked everything else — both closed

1. ~~**Row 26 — `OVER (w …)` is nondeterministic.**~~ Referencing a named window inside
   parentheses *and* adding a frame discarded the named window's spec and summed rows in
   shard-arrival order. Five consecutive runs of one query on unchanged data returned
   **five different answers**:

   ```
   30,50,90,100   20,60,90,100   10,30,70,100   10,40,60,100   20,60,70,100
   ```

   PostgreSQL returns `10,30,30,70` every time. This was the only row in any VaireDB gap
   analysis that was not merely wrong but *unstable* — uncatchable by a golden-file test
   that runs once, and it would not reproduce for a user reporting it. **That is exactly
   why it is refused rather than fixed**: the open question below asked whether to reject
   the syntax or fix the handling, and an unstable answer is worse than no answer, so it
   is rejected now and can be answered later without breaking anything that works. The
   spelling that keeps the clauses, `OVER w` without parentheses, was always correct and
   is what the refusal points at.

2. ~~**Row 38 — `PARTITION BY <col>` + `WHERE <col> = <value>` fails hard.**~~ The single
   most common shape in dashboard and reporting SQL ("this partition's ranking, for one
   category") errored out. **Fixed** — see § 7, and note the root cause, which was not the
   one this document hypothesized: the coordinator's optimizer was right to drop the sort,
   and Ballista's shuffle boundary is what invalidated its reasoning.

## § 1 — The 11 window functions

Values verified row-by-row against PostgreSQL 16.15 on identical fixtures.

| # | Function | Verdict | Notes |
|---|---|---|---|
| 1 | `row_number()` | ✅ | Values correct, and `int8` (20) since the wire schema widens the `UInt64` column. Row 33. |
| 2 | `rank()` | ✅ | As `row_number`. |
| 3 | `dense_rank()` | ✅ | As `row_number`. |
| 4 | `ntile(n)` | ✅ | Buckets correct since 54.1 — see below. The **type** is still `int8` where PG promises `int4`, which is row 33. |
| 5 | `percent_rank()` | ✅ | Values and `float8` OID both correct. |
| 6 | `cume_dist()` | ✅ | Values and `float8` OID both correct. |
| 7 | `lag(x[, off[, default]])` | ✅ | Values correct; input type preserved. `IGNORE NULLS` is row 22. |
| 8 | `lead(x[, off[, default]])` | ✅ | As `lag`. |
| 9 | `first_value(x)` | ✅ | Correct, including the default-frame interaction. |
| 10 | `last_value(x)` | ✅ | Correct — returns the current row under the default `RANGE` frame, as PG does. |
| 11 | `nth_value(x, n)` | ⛔ | Correct for `n ≥ 1`. **`n = 0` returns NULL for every row**; PG raises `22016 argument of nth_value must be greater than zero`. Negative `n` is a DataFusion superset (PG rejects). |

### Row 4 — `ntile` remainder distribution, closed upstream

**Closed by the DataFusion 53.1 → 54.1 bump, with nothing written locally.** Recorded in
full because it is the clearest example in these documents of a defect worth *not*
patching around: the whole divergence was one upstream formula.

The two engines used different formulas, not merely a different tie-break:

- **PostgreSQL** front-loads the remainder: with `q, r = divmod(rows, n)`, the first
  `r` buckets get `q+1` rows and the rest get `q`.
- **DataFusion 53.1** assigned by proportion: `bucket(i) = ⌊i·n / rows⌋ + 1`
  (`ntile.rs:174-181`), which spread the oversized buckets evenly instead of packing them
  at the front.

They agreed whenever `rows` was a multiple of `n`, and coincidentally in many other cases.
Over all `(rows, n)` pairs with `rows ≤ 20`, **83 of 210 diverged (39 %)** — dangerous
precisely because the small round-numbered cases a developer tries by hand were among the
ones that agreed:

| rows | n | PostgreSQL | VaireDB on 53.1 | |
|---:|---:|---|---|---|
| 7 | 5 | `1122345` | `1123345` | ⛔ |
| 6 | 4 | `112234` | `112334` | ⛔ |
| 12 | 5 | `111222334455` | `111223334455` | ⛔ |
| 5 | 3 | `11223` | `11223` | ✅ coincided |
| 7 | 4 | `1122334` | `1122334` | ✅ coincided |
| 10 | 3 | `1111222333` | `1111222333` | ✅ coincided |

It was identical at 1, 3 and 5 shards, which is what identified it as a function-semantics
divergence rather than a distribution artifact — and therefore as something a version bump
could close, which is what happened. Re-measured on 54.1: 5 rows into 3 buckets is
`1,1,2,2,3` and 5 into 2 is `1,1,1,2,2`, both front-loaded, both PG's answer. Pinned in
`sql_expression_gaps.rs::test_ntile_fills_the_larger_buckets_first`, so a regression is a
test failure rather than a rediscovery.

## § 2 — Aggregates used as window functions

| # | Construct | Verdict | Notes |
|---|---|---|---|
| 12 | `sum` / `count` / `min` / `max` / `avg` `OVER (…)` | ✅ | Values correct, including `avg(bigint) OVER ()`. |
| 13 | `count(*) FILTER (WHERE …) OVER (…)` | ❌ | **Refused** (`0A000`), because the `FILTER` was silently discarded: `count(*) FILTER (WHERE m > 30) OVER (ORDER BY id)` returned `1,2,3,4,5,6,7` — a plain unfiltered `count(*)`; PG returns `0,0,0,0,1,2,3`. See below. |
| 14 | `count(DISTINCT x) OVER (…)` | ✅ | Works and is correct. A **superset** — PG rejects `DISTINCT` in a window aggregate (`42P20`). Must not be "fixed". |
| 15 | Result type of `sum(bigint)` / `avg(bigint)` `OVER ()` | ✅ | Both are now `numeric` and exact past `i64::MAX`, because `pg_aggregate_widening` matches the window spelling of the aggregate as well as the grouped one — for `avg` as for `sum`, and for `avg(integer) OVER ()` too, which PG also promises as `numeric`. Closed on the aggregate axis; this row's contribution was the invariant that the two spellings must not report different types. `avg` carries ten decimal places where PG prints sixteen — recorded as 🟡 there, on master rows 6 and 7. See [aggregate analysis](gap-analysis-aggregate-function.md). |

### Row 13 is distribution-specific, which is why it is refused and not fixed

`FILTER` is not lost by DataFusion — it is lost by Ballista's plan serialization.
`PhysicalWindowExprNode` has no filter field (`datafusion.proto:924-938`), and the
deserializer hardcodes the absence (`from_proto.rs:203` → `None`). Single-node
DataFusion computes this correctly. **VaireDB is wrong here specifically because it
is distributed**, which makes this the one row that could not be closed by a dependency
bump alone: it needs a proto field and both codecs, upstream.

Until that exists the coordinator refuses the combination —`FILTER` *and* `OVER` on the
same call — and names the two rewrites that do work: move the condition into a `CASE`
inside the aggregate, or aggregate a subquery that applies it in its `WHERE`. A plain
aggregate `FILTER` never travelled through `PhysicalWindowExprNode` and is applied
correctly, so it stays accepted; the refusal is scoped to the pair, not to the keyword.

## § 3 — Frame and clause surface

| # | Construct | Verdict | Notes |
|---|---|---|---|
| 16 | `ROWS BETWEEN … AND …` | ✅ | All bound combinations correct. |
| 17 | `RANGE BETWEEN … AND …` | ✅ | Peer groups correct. **`RANGE` is *not* collapsed into `ROWS` on ties** — verified against duplicate ordering values that straddle shard boundaries. |
| 18 | `GROUPS BETWEEN … AND …` | ✅ | Correct. Notable: PG has supported `GROUPS` only since 11, and it works here. |
| 19 | Default frame (`RANGE UNBOUNDED PRECEDING TO CURRENT ROW`) | ✅ | Correct, both with and without `ORDER BY`. |
| 20 | Frame offsets: integer, float, `INTERVAL` | ✅ | Correct, including `RANGE BETWEEN INTERVAL '1 day' PRECEDING …`. |
| 21 | `EXCLUDE {CURRENT ROW \| GROUP \| TIES \| NO OTHERS}` | ❌ | **Unparseable.** `42601 … Expected: ), found: EXCLUDE`. sqlparser cannot represent it: `WindowFrame` has `start_bound`/`end_bound` and a literal `// TBD: EXCLUDE` (`sqlparser/src/ast/mod.rs`). All 4 variants. Needs the parser taught first. |
| 22 | `IGNORE NULLS` | ❌ | **Refused** (`0A000`), because it was a silent no-op: measured on `lag(m)` with NULLs, `IGNORE NULLS` and `RESPECT NULLS` returned byte-identical columns. Root cause is a single hardcoded `false` in `to_proto.rs:150-156` with a stale comment claiming the field is unused. Affected rows 7–11 — the worst of the discarded clauses, because the NULL it asked to skip comes back looking like data. |
| 23 | `RESPECT NULLS` | ✅ | Correct, and **still accepted** — it asks for the behavior DataFusion already has, so dropping it loses nothing. That asymmetry is the point of scoping the refusal to the form rather than the keyword. |

## § 4 — Named windows (`WINDOW` clause)

The defect has a crisp boundary that is worth stating precisely, because it is what the
refusal is scoped to: **a bare reference works; a reference inside parentheses discards
the referenced spec entirely.** Root cause: `WindowSpec::window_name` is parsed by
sqlparser but never read by datafusion-sql.

Measured on `(id, cat, m) = (1,a,10) (2,a,20) (3,b,30) (4,b,40)`, PG answer `10,30,30,70`:

| # | Construct | Verdict | Returned before the refusal | Notes |
|---|---|---|---|---|
| 24 | `sum(m) OVER w` | ✅ | `10,30,30,70` | Bare reference is honored, including multiple references to one window. **Still accepted** — and it is what the three refusals below tell the client to write. |
| 25 | `sum(m) OVER (w)` | ❌ | `100,100,100,100` | Redundant parens **dropped `PARTITION BY` *and* `ORDER BY`**; the frame widened to the whole table. |
| 26 | `sum(m) OVER (w <extra clauses>)` | ❌ | **unstable** | Adding `ORDER BY` → `10,30,60,100` (partition lost). Adding a **frame** → nondeterministic, 5 distinct answers in 5 runs. See summary. |
| 27 | `WINDOW w2 AS (w1 …)`, `OVER w2` | ❌ | `100,100,100,100` / `10,30,60,100` | Chained inheritance, declared in the `WINDOW` list rather than in the `OVER`. `w2 AS (w1)` lost everything; `w2 AS (w1 ORDER BY id)` lost `w1`'s `PARTITION BY`. Re-measured on 54.1. |

Rows 25–27 were all **silent** — no warning, no error, a plausible-looking column. The
aggregate analysis recorded this as one ⛔ row; it is four distinguishable shapes, one of
which is nondeterministic.

**All three are now `0A000`**, and the refusal is deliberately narrow. Rows 25 and 26 are
caught on the function's own `over` clause (`WindowType::WindowSpec` with a
`window_name`); row 27 is caught on the select's `WINDOW` list, which the first check
cannot see because a `NamedWindowDefinition` is a property of the select and not of any
expression. Three things in this neighbourhood are left alone on purpose:

- `OVER w`, the correct spelling (row 24).
- A `WINDOW` list whose definitions inherit nothing from each other — two independent
  definitions in one clause lose nothing and are answered.
- `w2 AS w1` **without** parentheses, which is BigQuery's spelling rather than
  PostgreSQL's and which DataFusion already rejects loudly by name
  (`The window w1 is not defined!`). A second refusal on top of a working one buys
  nothing.

## § 5 — Where a window function may appear

| # | Position | Verdict | Notes |
|---|---|---|---|
| 28 | `SELECT` list | ✅ | The supported case. |
| 29 | Outer `ORDER BY`, written inline | ❌ | **Legal PostgreSQL, rejected.** `SELECT id FROM t ORDER BY row_number() OVER (ORDER BY m DESC)` → `XX000 … Physical plan does not support logical expression WindowFunction(…)` plus a ~1.5 KB `Signature { … }` dump. |
| 30 | Outer `ORDER BY` by output alias or position | ✅ | The workaround for row 29, and fully general. |
| 31 | `WHERE`, `GROUP BY`, `HAVING`, nested in another window fn | 🟡 | **Correctly rejected** — PG rejects these too — but with `XX000` instead of PG's `42P20` / `42803`, and with the same 1.5 KB dump leaked to the client. Wrong class (internal error, not syntax error) breaks client error handling. |
| 32 | `QUALIFY` | ✅ | Works. A **superset** — DuckDB/Snowflake syntax that PG does not have. Must not be "fixed". |

The wrong SQLSTATEs in row 31 come from the confirmed message-text rot in
`error_enrichment.rs:112-116` and `sanitize.rs:9-10` — the matchers look for message
strings DataFusion stopped emitting before 53, so classification falls through to
`XX000`. Same root cause as the misclassifications recorded on the operator axis.

## § 6 — Result types and column labels

| # | Construct | Verdict | Notes |
|---|---|---|---|
| 33 | Ranking function result OIDs | 🟡 | ~~all four advertise **`numeric` (1700)**~~ — the coordinator now widens a top-level `UInt64` result column to `Int64` and checked-casts the payload to match (`pgwire_handler/encoding.rs`), so `row_number` / `rank` / `dense_rank` report `int8` (20) as PG promises. **`ntile` remains 🟡**: it reports `int8` where PG promises `int4` (23). The upstream cause is unchanged — arrow-pg maps `UInt64 => NUMERIC` — the widening sits above it. |
| 34 | Column label of an unaliased window column | 🟡 | Mostly closed: `pgwire_handler/column_labels.rs` gives the column the name PostgreSQL gives it, so `row_number() OVER (…)` is labelled `row_number` rather than a 99-byte rendering of a frame the client never wrote. The **residue** is the case DataFusion will not allow — two unaliased calls to the same function in one select list, where PG returns two columns both called `sum` and DataFusion refuses duplicate projection names outright. Those keep the verbose label; `AS` remains a full workaround. |
| 35 | Bind-parameter offsets — `ntile($1)`, `lag(x, $1)`, `nth_value(x, $1)` | 🟡 | Correct **provided the client declares a parameter OID**. Values byte-identical to literal controls, verified with a discriminating offset-10 case. |

Row 33 *was* the most likely thing to break a real client, and it broke it *hard*: a
driver that trusts the PG contract attempted an `int8` decode of a `numeric` payload
and raised a decode error, so the query failed rather than returning an odd type.
That is now answered for the three ranking functions, and the workaround
(`row_number() OVER (…)::bigint`) is no longer needed. What is left is narrower and
softer: `ntile` reports a wider integer than PG's `int4`, which a driver decodes
successfully but into the wrong width. Its buckets are correct now (row 4), so this is the
whole of what remains of `ntile` — a one-function widening in the same place the `UInt64`
widening already lives, and the last 🟡 on this axis that VaireDB can close alone.

Row 34's protocol divergence was confirmed, but **no client breakage was reproduced**:
psql 18.3 and tokio-postgres 0.7.18 both tolerated a 412-byte label. It was closed anyway,
because a label is a name clients read *by* — the fix is an alias the coordinator adds, not
a truncation, so nothing is lost. Treat the severity of the remaining residue as
unquantified.

## § 7 — Distribution and sharding

This section is almost entirely negative results, and they matter: the obvious
hypotheses about a sharded window engine are **wrong**, and the one real failure is
not the one you would guess.

| # | Scenario | Verdict | Notes |
|---|---|---|---|
| 36 | Global window, no `PARTITION BY` (`sum(x) OVER ()`, `row_number() OVER (ORDER BY id)`) | ✅ | Correct and **stable**: 10 consecutive runs identical, and byte-identical at 1, 3 and 5 shards. Ballista cuts stages at `CoalescePartitionsExec` / `SortPreservingMergeExec` (`planner.rs:194`, `:214`), so the window executes on one gathered partition. |
| 37 | `PARTITION BY <col>` | ✅ | Correct. `required_input_distribution` requests `HashPartitioned` on the partition keys, and `EnforceDistribution` inserts the repartition. Verified correct with skewed groups, empty tables, and — by copying `core.duckdb` out of a core container and reading it with the DuckDB CLI — with every `cat` group physically spanning all 3 shards. Shard-count-invariant. |
| 38 | `PARTITION BY <col>` **+ an equality predicate on that same `<col>`** | ✅ | **Was a hard failure** — `Execution("Expects PARTITION BY expression to be ordered")`, or a leaked `Internal("Assertion failed: … All partition by columns should have an ordering")`. Fixed in `scheduler/window_partition_sort.rs`; see below. |

### Row 38 — the mechanism, and the fix

The behavior was certain and reproducible; the *cause* recorded here was a hypothesis, and
it was **wrong in an instructive way**. This document concluded "a DataFusion optimizer
defect that distribution merely exposes", on the strength of reproducing it on a single
shard. Both halves of that sentence were wrong: the optimizer's reasoning was correct at
every step, and distribution was not exposing the defect but *causing* it — a single-shard
VaireDB is still a distributed plan cut into Ballista stages, which is why the one-shard
probe misled rather than exonerated.

What actually happens:

1. `FilterExec: g = 1` tells the plan above it that `g` is constant, and a constant column
   is trivially ordered.
2. `EnforceSorting` therefore concludes — **correctly** — that `PARTITION BY g` needs no
   sort of its own, and emits `SortExec: expr=[x ASC]` alone.
3. `BoundedWindowAggExec` is built in `Sorted` mode on the strength of that conclusion.
4. Ballista then cuts the plan into stages at the shuffle. The window and its sort land in
   a stage whose input is a **shuffle reader, which reports no equivalences at all** — the
   knowledge that `g` is constant stayed behind in the stage that held the filter.
5. The window re-derives its ordering against that input, finds nothing ordering `g`, and
   fails.

The plan was valid as one piece and invalid once split. That is a hazard of distributing a
plan rather than a mistake in either half, and it is the general lesson worth carrying
into the rest of this analysis: **an equivalence property is a fact about a plan, not a
fact about the data, and it does not cross a stage boundary.**

The repair puts the partition columns into the window's own sort, so the requirement is
met by an ordering physically present inside that stage. Where the plan believed *every*
ordering key was constant there is no sort to widen, so one is inserted instead. The rule
fires only on a window in `Sorted` mode whose input does not already carry every partition
column in its ordering — a plan that was going to work is untouched, and a sort is only
ever added, so no result changes.

The escapes below are recorded because they explain the shape of the defect, and because
they remain valid rewrites for anyone on an older build:

| Shape | Before the fix |
|---|---|
| `WHERE cat = 'b'`, `PARTITION BY cat` | ❌ failed |
| `…` wrapped in a subquery, CTE, or `MATERIALIZED` CTE | ❌ failed |
| Bind parameter instead of a literal | ❌ failed |
| Expression partition key (`PARTITION BY upper(cat)`) | ❌ failed |
| `WHERE cat > 'a'` (inequality) — control | ✅ worked |
| `WHERE rn <= 1` in the outer query only (top-N-per-group) | ✅ worked |
| `WHERE cat = (SELECT 'b')` — opaque scalar subquery | ✅ worked |
| Partitioning a *different* column than the one filtered | ✅ worked |

Every row of that table is consistent with the mechanism above: the four failures are the
four shapes where the optimizer could still prove the partition column constant, and the
opaque scalar subquery worked precisely because it defeated that proof. The classic
top-N-per-group idiom was always fine, because its filter is on the window's *output*.

## § 8 — Window functions over anonymized columns

VaireDB's pseudonymization is **write-path only**: `anonymize_statement` rewrites
`INSERT`/`UPDATE` values and falls through for everything else
(`anonymization/rewrite.rs:89` → `_ => Ok(())`). Reads therefore see HMAC-SHA256
digests. For aggregates that is mostly harmless; for window functions, which are
defined *by ordering*, it inverted results — silently, until these rows were refused.

Measured with `email ∈ {aaa@x.com, bbb@x.com, ccc@x.com}` at `id` 1, 2, 3 — whose
digests sort `id2 < id3 < id1`, i.e. **not** the plaintext order:

| # | Construct | Verdict | VaireDB | PostgreSQL |
|---|---|---|---|---|
| 39 | ~~`rank() OVER (ORDER BY <anon col>)`~~ (and `row_number`, `dense_rank`, `percent_rank`, `cume_dist`) **now refused** | ❌ | was `3, 1, 2` | `1, 2, 3` |
| 40 | ~~`first_value(<anon col>) OVER (ORDER BY <anon col>)`~~ (and `last_value`, `nth_value`, `lag`, `lead`) **now refused** | ❌ | was the digest of `bbb@x.com` | `aaa@x.com` |
| 41 | ~~Plaintext predicate + window (`WHERE email = 'aaa@x.com'`)~~ **now refused** | ❌ | was 0 rows | 1 row |
| 42 | `PARTITION BY <anon col>` | ✅ | correct | correct |

Row 42 is the one **sound** use, and it is sound for a real reason: HMAC is
deterministic and injective, so digest equality is plaintext equality. Grouping is
preserved exactly; only *order* and *value* are destroyed.

That asymmetry is what the refusal is cut along. `pgwire_handler/anonymized_reads.rs`
rejects, with `0A000`, an ordering over a pseudonymized column — a window's `ORDER BY`,
inline or through a named `WINDOW` definition, alongside `ORDER BY`, `min`/`max`, range
comparisons and pattern matches — and an equality against a literal that cannot be a
digest, which is row 41. Row 42 stays allowed, as this section argued it must, and so does
everything else the hash preserves: `count`, `count(DISTINCT)`, `GROUP BY`, a join, and the
digest lookup `WHERE email = '<digest>'`, which is the documented way to find a row.

The plaintext is genuinely not on the server, so there was no third option here: the digest
order is the only order a read can see, and a window is *defined* by its ordering. The one
shape still unguarded is `WHERE email = $1` bound to plaintext — the value never appears in
the AST, so catching it needs a check at `Bind`. The aggregate analysis carries the full
argument, in *Refusing the reads a digest cannot answer*.

## Root causes

The original ten ⛔ rows reduced to six causes. Five of the six are now answered, and the
table is kept in full because *which* answer each one got is the useful part:

| Cause | Rows | Where | Status |
|---|---|---|---|
| `WindowSpec::window_name` never read | 25, 26, 27 | datafusion-sql | **Refused.** Still true upstream, so the coordinator rejects the forms that lose the clause rather than expanding them — an AST expansion is a correct fix and remains open, but it is not the cheap one it looks like: `OVER (w ORDER BY x)` has to merge two specs with PostgreSQL's own rules about which clauses may be added. |
| `IGNORE NULLS` hardcoded `false` | 22 | `to_proto.rs:150-156` | **Refused.** One line upstream, and until it lands a discarded clause is a wrong answer. |
| `PhysicalWindowExprNode` has no filter field | 13 | `datafusion.proto:924-938`, `from_proto.rs:203` | **Refused.** Proto change plus both codecs, and distribution-specific — the only cause here VaireDB cannot wait out. |
| `ntile` uses proportional, not front-loaded, buckets | 4 | `ntile.rs` | **Fixed upstream** in 54.1. Nothing written locally. |
| `nth_value` accepts `n = 0` | 11 | datafusion-functions-window | Open. One guard upstream; the narrowest row in the document. |
| Anonymization is write-path only | 39, 40, 41 | `anonymization/rewrite.rs:89` | **Refused,** which was the choice this table left open. Still true upstream of the read path — and unfixable there, since the plaintext is not stored — so `pgwire_handler/anonymized_reads.rs` rejects the reads the digest cannot answer and leaves the ones it can (row 42 among them). |

Plus the one cause this document attributed to DataFusion and that turned out to be
VaireDB's own:

| Cause | Rows | Where | Status |
|---|---|---|---|
| A stage boundary erases the equivalence property a sort decision rested on | 38 | Ballista's shuffle reader vs. `EnforceSorting` | **Fixed** in `scheduler/window_partition_sort.rs`. Not an upstream defect at all — see § 7. |

And the 🟡 rows:

| Cause | Rows | Where | Status |
|---|---|---|---|
| ~~`UInt64 => NUMERIC` OID mapping~~ | ~~1, 2, 3~~, 33 | still `UInt64 => NUMERIC` in arrow-pg (0.15.0 today) | **Done** for rows 1–3: `pgwire_handler/encoding.rs` widens the column above arrow-pg rather than patching it. `ntile`'s `int8`-vs-`int4` remains, in the same place. |
| ~~Labels are rendered plan expressions~~ | 34 | DataFusion field naming | **Done**, and by aliasing rather than truncating: `pgwire_handler/column_labels.rs`. The residue is select lists where PG's own answer has duplicate names, which DataFusion will not plan. |
| ~~`avg(bigint)` returns `float8`~~ | ~~15~~ | the aggregate axis, not this one | **Done** there, with `sum`'s own patch plus an `avg` arm. Both window spellings report `numeric`, because `pg_aggregate_widening` matches the window node as well as the aggregate one. |
| ~~DataFusion message-text rot~~ | 29, ~~31~~ | `error_enrichment.rs`, `sanitize.rs` | **Done, and re-derived from types rather than text**: the classifier matches `DataFusionError` variants, so no message change can rot it, and the prefix list comes from 54.1's own `error_prefix()`. Row 31 keeps its 🟡 for a different reason than this one — an illegal window placement is rejected *past the Ballista scheduler*, which re-textualizes it as `Job <id> failed: …`, so no typed error reaches the classifier. The 1.5 KB dump is truncated. |

## Prioritized gaps

Ranked by consequence, then by cost. The parenthetical marks where the fix lives.
Everything struck through has been closed since this was written; the ordering is left
intact, because the order the priorities were assigned in is what the closures were
chosen by.

0. ~~**Row 26 — nondeterministic `OVER (w …)`**~~ **Closed by rejecting it**, which is
   what this entry recommended as the fallback. It was not a feature gap but a
   correctness bug, and it outranked everything: the same query on unchanged data
   returned different numbers, giving unreproducible bug reports that no single-run test
   could catch. A refusal removes the nondeterminism outright and leaves the real fix —
   an AST expansion of the named window — available later.
1. ~~**Row 38 — `PARTITION BY col` + equality on `col`**~~ **Fixed**, and not by the
   suggested mitigation. This entry proposed rewriting the equality into an opaque scalar
   subquery, which would have worked by defeating an optimization; the actual repair keeps
   the optimization and restores the ordering inside the stage that needs it
   (`scheduler/window_partition_sort.rs`), so no plan gets worse.
2. ~~**Rows 25, 27 — named-window inheritance silently drops clauses**~~ **Closed as
   row 0.** Row 27 needed a second check of its own: it is declared in the `WINDOW` list,
   which the `OVER`-clause check structurally cannot see.
3. ~~**Rows 39–41 — anonymized-column windows**~~ *(VaireDB)*. **Closed by rejecting
   it** — the fix this entry called the cheap, honest one, and here it is also the only
   one: the plaintext order a correct answer would need is not stored anywhere.
   `pgwire_handler/anonymized_reads.rs` refuses an order-sensitive read over a
   pseudonymized column, and row 42 stays allowed as the one sound use, now documented as
   such in § 8.
4. ~~**Row 22 — `IGNORE NULLS` no-op**~~ **Closed by rejecting it.** Still one line
   upstream, and still worth taking when it lands.
5. ~~**Row 33 — ranking function OIDs**~~ **Done** for `row_number` / `rank` /
   `dense_rank`, which was the only row that made a conforming driver fail outright
   rather than return something odd. `ntile`'s `int8`-for-`int4` is what remains, and it
   decodes — so it is now the highest-ranked purely-cosmetic entry rather than one hiding
   behind a wrong value.
6. ~~**Row 13 — `FILTER` on a window aggregate**~~ **Closed by rejecting it**, and it is
   the row where rejecting is most clearly the right call rather than a stopgap: the fix
   needs a proto field and both codecs, and it is the only defect VaireDB owns *because*
   it is distributed, so there is no upstream release to wait for.
7. ~~**Row 4 — `ntile` buckets**~~ **Fixed upstream** in DataFusion 54.1. It was silently
   wrong in 39 % of `(rows, n)` pairs while agreeing on the cases people check by hand.
8. **Rows 29, 31 — placement rejections** *(VaireDB)*. Row 29 rejects legal PG; row 31
   rejects correctly with the wrong SQLSTATE. The **dump half is closed** — the 1.5 KB
   `Signature { … }` is truncated, and the classifier now reads `DataFusionError` variants
   instead of message substrings — but row 31's class is still `XX000` rather than `42P20`,
   and the reason is not the classifier: an illegal window placement is caught past the
   Ballista scheduler, which hands the coordinator its own text with no typed error inside.
   Division by zero is the one class carved out of that boundary by name, and deliberately
   not generalized — see [`gap-analysis.md`](gap-analysis.md) § 6.2 item 2.
   Row 30 is a full workaround for row 29, so this stays mostly hygiene. **Still the top
   entry that is purely VaireDB's to fix and still open.**
9. **Row 11 — `nth_value(x, 0)`** *(one guard upstream)*. Narrow.
10. ~~**Row 34 — column labels**~~ **Done** (`pgwire_handler/column_labels.rs`), despite
    being ranked here as the lowest priority of the real rows — it came for free alongside
    the same fix on the aggregate axis, which is the only reason it jumped the queue.
11. **Row 21 — `EXCLUDE`** *(parser + planner)*. The only ❌ needing parser work first,
    and the only genuinely *missing feature* in the whole document. Rare in practice.

**Supersets that must NOT be "fixed"** — VaireDB accepts these and PostgreSQL does
not; narrowing to PG parity would be a regression against DataFusion and DuckDB:
`DISTINCT` inside a window aggregate (row 14), `QUALIFY` (row 32), negative
`nth_value` offsets (row 11).

## Executable counterpart

When this was written the repository contained **exactly two** window tests
(`sql_command_select.rs`: `test_window_row_number`, `test_window_sum_partition`), and
neither asserted a result type or a column label, because both go through
`simple_query_rows`, which discards `row.columns()`. Every gap above was unguarded.

The rows that have since closed are now covered in **`tests/e2e/tests/sql_expression_gaps.rs`**,
which is organized by the seam a gap sits on rather than by function — a refusal, a
rewrite, and a distributed case read differently, and the file says which each test is:

| Test | Rows |
|---|---|
| `test_window_clauses_datafusion_discards_are_refused` | 13, 22, 24, 25, 26, 27 — each refusal beside the neighbouring form that still answers, which is what keeps the refusals honestly scoped |
| `test_a_window_partitioned_by_a_filtered_column_answers` | 38, plus the every-key-constant variant and the two shapes that always worked |
| `test_ntile_fills_the_larger_buckets_first` | 4 |
| `test_ranking_functions_are_bigint` | 1, 2, 3, 33 — via `Describe` *and* the rows, since the two have to agree |
| `test_sum_of_bigint_does_not_wrap`, `test_avg_of_an_integer_is_exact_numeric` | 15, both the grouped and the windowed spelling, for `sum` and `avg` — and `avg(x::float8) OVER ()` staying `float8`, which is what fails if the widening ever reaches past the integer types |
| `test_a_function_column_is_labelled_the_way_postgres_labels_it` | 34 |

Both structural prerequisites the aggregate analysis identified are done:
`describe_result_types` and a label-returning helper now live in `tests/e2e/src/lib.rs`
rather than privately in `data_types_round_trips.rs`, so a result OID is assertable from
any suite.

Rows 39–41 are covered where this section said they belonged, in
**`tests/e2e/tests/anonymization.rs`** —
`test_reads_that_would_report_digest_order_are_refused` pins the `0A000` for
`row_number() OVER (ORDER BY email)` beside the reads that must keep answering.

Still uncovered, and in priority order: row 42 (`PARTITION BY` over a pseudonymized
column, the sound use, unguarded against a future over-broad refusal), rows 16–20 and 23
(the frame surface, which is correct and therefore unguarded against regression), rows 29
and 31 (placement, which need the SQLSTATE decided before they can be pinned), and row 21
(`EXCLUDE`, which cannot be written until the parser accepts it).

Where a gap is still open, the house pattern applies: a **passing** test pinning today's
actual behavior, plus an `#[ignore = "gap (row N): …"]` test asserting the PG-correct
target.

```sh
cd tests/e2e && cargo test --test sql_expression_gaps -- --test-threads=1
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
  `tokio-postgres` probe — `psql`'s own `\gdesc` could not be used here, because it hit
  an unrelated `pg_catalog` gap, diagnosed at the time as a `format_type(Utf8, Int64)`
  coercion failure. The diagnosis was half right and the finding is recorded below; the
  probe remains the right instrument either way, because it describes without executing.
- **`VERBOSITY verbose`** for every SQLSTATE quoted.
- **Physical shard placement verified**, by copying `core.duckdb` (+ `.wal`) out of a
  core container and reading it with the DuckDB CLI, to confirm partition groups
  genuinely spanned all 3 shards rather than coincidentally colocating.
- **Shard-count invariance** checked by re-running at 1, 3 and 5 shards; stability by
  repeating nondeterminism-suspect queries 5–10 times.
- **Registered surface derived from source**, not from documentation:
  `datafusion-functions-window/src/lib.rs`, `ballista-core/src/extension.rs`,
  cross-checked against PG's `pg_proc WHERE prokind = 'w'`.

Each row that changed verdict was **re-measured against the same cluster** and then pinned
as an e2e assertion, so the closures are not inferred from reading the fix. The refusals
were additionally probed for over-reach: for each one, the neighbouring form that loses no
clause is asserted to still answer, in the same test.

### Claims investigated and *not* confirmed

Recorded so they are not re-litigated:

- **"`upgrade_for_ballista` drops window functions."** Refuted:
  `ballista-core/src/extension.rs` installs `ballista_window_functions()`.
- **"The global, no-`PARTITION BY` case is the top sharding risk."** Refuted
  structurally and empirically (row 36). Ballista's stage cuts make it safe.
- **"Row 38 is an upstream optimizer defect that distribution merely exposes."**
  **Refuted, by fixing it.** Reproducing the failure on a single shard was read as
  exonerating distribution; it does not, because a one-shard VaireDB still cuts the plan
  into stages. See § 7 — the hypothesis was recorded as INCONCLUSIVE and was wrong.
- **"A constant window `ORDER BY` silently drops the outer `ORDER BY`."** **Not
  reproduced.** Probed at top level with a literal, a string constant, a constant
  `PARTITION BY`, and alongside a second correctly-ordered window: the outer
  `ORDER BY … DESC` was honored in every case. The only shapes where order changed were
  ones where SQL guarantees no order anyway (an `ORDER BY` inside a subquery feeding an
  aggregate). Excluded from the gap table.
- **`avg(bigint) OVER ()` precision loss** — the fixture yielded an exact answer, so
  the >2⁵³ concern was inherited from the aggregate analysis rather than reproduced here.
  It was real: the aggregate axis reproduced it with a purpose-built fixture and closed it.
  The lesson is about the fixture rather than the row — a value chosen for convenience sat
  inside the float mantissa, and an inexactness that only appears past 2⁵³ cannot be found
  by a test whose data does not go there.
- **Row 34 client breakage** — protocol divergence confirmed (412 bytes), but no client
  actually broke.

## Corrections to the sibling analyses

Verified while measuring this axis:

- **Version citations across all five gap analyses are stale by construction.** This
  document was written against sqlparser 0.61 / DataFusion 53.1 and its siblings against
  0.58; `Cargo.lock` is now sqlparser 0.62 / DataFusion 54.1. The conclusions were
  re-verified across each bump and hold; only the citations move. The lesson for all five
  documents is to **cite a file and a symbol rather than a versioned path** —
  `ntile.rs:174-181` under a `-53.1.0` prefix is a dead link one bump later, and row 4
  shows a versioned citation can outlive the defect it describes.
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
  type-description command does not work against VaireDB. **Half closed, and the halves were
  not the ones this bullet assumed** ([`gap-analysis.md`](gap-analysis.md) § 6.2 item 5):
  `\gdesc` failed because the plan it builds — a `VALUES` list plus `pg_catalog.format_type` —
  could not be *serialized* to the scheduler, which had never been given the function's name;
  it works now, measured with `psql` 18.6 over both a literal projection and a sharded table.
  The coercion failure is real but separate and still open: `format_type('23', 0)` is `0A000`
  because the signature has no `(Utf8, …)` arm, where PostgreSQL coerces the literal to `oid`.

## Open questions

1. ~~**Should an order-sensitive window over an anonymized column be rejected?**~~
   **Answered: yes, reject.** It was indeed the same question the clause rows were answered
   with, and the extra difficulty this entry noted — that the wrong answer is arguably the
   intended one — resolved once the two halves were separated: the digest is the intended
   *stored value*, which is why projecting it stays allowed, but nobody intends a ranking
   over digests to be reported as a ranking over addresses. Row 42 stays allowed, as this
   entry required. The behavior change is real and is the point.
2. ~~**Fix the named-window handling, or reject the syntax?**~~ **Answered: reject.** The
   argument in the original entry held — rejection is strictly safer than an unstable
   answer — and the cost it worried about, breaking queries that "work" today, is smaller
   than it looks, because the refusal is scoped to the forms that were silently wrong and
   names the spelling that works. The AST expansion remains a legitimate future fix.
3. **How much of this is worth patching locally vs. upstreaming?** *(Answered in practice,
   and worth recording as a pattern.)* The bump closed row 4 for free, which vindicates the
   caution about throwaway local workarounds. But rows 13, 22 and 25–27 show the third
   option this question missed: **a refusal is neither a local fix nor an upstream wait.**
   It costs little, it is not thrown away when upstream lands — deleting a refusal is
   trivial — and it converts the silent wrongness that made the row urgent into something a
   client can see. Rows 13 and 38 remain the two where distribution is the trigger; 38 is
   now fixed locally because there was no upstream defect to wait for.
