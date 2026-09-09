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

- **DataFusion:** [Aggregate Functions](https://datafusion.apache.org/user-guide/sql/aggregate_functions.html) — General, Statistical and Approximate sections. Measured on DataFusion **53.1.0**; `Cargo.lock` now pins **54.1.0**.
- **Ballista:** `ballista-core` (53.0.0 when measured, 54.1.0 today) — supplies the distributed execution of the aggregate plan.
- **PostgreSQL:** [Aggregate Functions](https://www.postgresql.org/docs/current/functions-aggregate.html) — the result-type and spelling contract clients actually expect.
- **DuckDB:** [Aggregate functions](https://duckdb.org/docs/current/sql/functions/aggregates.html) — listed for completeness only; see the framing below, DuckDB never evaluates an aggregate in VaireDB.
- **Sibling analyses:** [`gap-analysis-data-type.md`](gap-analysis-data-type.md), [`gap-analysis-operator-literal.md`](gap-analysis-operator-literal.md), [`gap-analysis-command.md`](gap-analysis-command.md). All five are consolidated in [`gap-analysis.md`](gap-analysis.md).

## Scope and framing

The two sibling analyses had to track expressions through two engines, because
`SELECT` evaluates on DataFusion while `INSERT`/`UPDATE`/`DELETE` evaluate on
DuckDB. **Aggregates have only one engine.** An aggregate can only appear in a
`SELECT`, and on the read path the core node runs nothing but

```sql
SELECT <projected cols> FROM <shard_table> [WHERE <pushed predicate>] [LIMIT n]
```

(`vairedb-core/src/table_provider/scan_exec.rs`, `build_query`). **No `GROUP BY`, and
no aggregate is ever pushed into DuckDB. Every aggregate in VaireDB is computed by
DataFusion**, on the Ballista executors, over rows streamed up from the shards.
DuckDB's aggregate catalog is therefore irrelevant to VaireDB's aggregate surface, and
DuckDB's own PostgreSQL divergences cannot reach an aggregate result.

The `WHERE` and `LIMIT` in that statement are a row-count optimization only: they are
copies of a filter and a limit DataFusion still applies itself, and their allow-list
excludes every shape the two engines read differently. They cannot change which rows an
aggregate sees. What they *do* change is how many rows cross the network to reach it,
which is why the shape of the statement above is no longer `SELECT … FROM …` alone.

That single-engine property is what makes this axis much healthier than the
operator axis: there are **no split-brain rows**, because there is no second
evaluator to disagree with.

### Where the aggregate work actually happens

VaireDB registers **zero custom UDAFs**. The scheduler builds its session state
with `SessionStateBuilder::new().with_default_features()`
(`scheduler/scheduler.rs:68`), and `upgrade_for_ballista` (`:79`) re-installs
`ballista_aggregate_functions()`, which is the same DataFusion default set. So
the available aggregate surface is *exactly* DataFusion's 38 default
`AggregateUDF`s (`datafusion-functions-aggregate/src/lib.rs`,
`all_default_aggregate_functions`) plus their 7 aliases — no more, no less. The 38
and the 7 were counted on 53.1, which is what every row below was measured against;
`Cargo.lock` has since moved to 54.1 and the count has not been re-taken.

Statement *routing* still ignores aggregate names — `classify_statement`
(`pgwire_handler/query_router.rs`) branches on the `Statement` variant only — but three
places in the read path now do look at one, and they are what several rows below turn on:

| Where | Names it acts on | What it does |
|---|---|---|
| `pgwire_handler/pg_operators.rs` | `variance`, `every`, `any_value` | rewrites the AST call to the DataFusion aggregate that means the same thing (rows 28–30) |
| `pgwire_handler/pg_aggregate_widening.rs` | `sum` | widens a `bigint` argument so the accumulator is `numeric`, grouped or windowed (master row 3) |
| `pgwire_handler/column_labels.rs` | any function call | replaces DataFusion's rendered plan expression with PostgreSQL's bare function name |

None of the three registers a UDAF, and that is deliberate — see the note under
*PostgreSQL aggregates that are absent* for why a coordinator-only registration fails after
the query has already been accepted.

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

> **Documentation note — resolved.** `distributed-query-processing.md` and
> `docs/vairedb.io/docs/concepts/query-processing.md` claimed Ballista/DataFusion
> handle "aggregation (two-phase, multi-level)" *and* "push-down (filters, partial
> aggregations)". The first was accurate; the second was not, because nothing was
> pushed into DuckDB SQL at all. Both pages have since been rewritten to separate the
> two, and filter and limit push-down is now real — but **partial aggregation still is
> not pushed**, and the aggregation remains two-phase above the shard boundary rather
> than inside it. Everything in this document about which engine computes an aggregate
> is unchanged.

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

Measured against DataFusion's 38 default aggregates (45 spellings including
aliases), the 12 modifier/clause combinations PostgreSQL clients use with them,
the 12 PostgreSQL aggregate spellings DataFusion lacks, and the 4 cross-cutting
defects that aggregates expose:

Counts as they stand today; the *Prioritized remediation* section records which entry
each closure came from.

| Axis | Rows | ✅ | ⛔ | 🟡 | ❌ |
|---|---:|---:|---:|---:|---:|
| DataFusion's 38 default aggregates | 38 | 33 | — | 5 | — |
| Modifier & clause combinations (`DISTINCT`, `FILTER`, `WITHIN GROUP`, `OVER`, grouping sets, …) | 12 | 7 | — | 3 | 2 |
| PostgreSQL spellings absent from DataFusion | 12 | 4 | — | — | 8 |
| Cross-cutting defects reached *through* aggregates | 4 | 2 | — | — | 2 |
| **Total** | **66** | **46** | **—** | **8** | **12** |

As first measured this was **40 ✅ / 8 ⛔ / 9 🟡 / 9 ❌**. **The ⛔ column is now empty**:
every one of the eight silently-wrong rows is closed, and it took two passes to get there.

The first pass moved seven, in two different directions:

- **Answered.** ⛔ row 2, the bare correlated scalar subquery in a select list, returns the
  subquery's value instead of being folded to `NULL` (see *Neutralizing
  `RemoveSubqueryFromProjection`*). ⛔ row 1, `sum(bigint)`, is now `numeric` and exact
  (see *Widening `sum`*). ⛔ row 0, `HAVING <agg> <op> $1`, compares as a number rather
  than as text (see *Typing an untyped `$N`*).
- **Refused.** The two window-clause ⛔ rows, M10 and M11, cannot be answered correctly
  here, so they are rejected with `0A000` instead of returning a plausible number. The
  window analysis owns the detail — it holds five such rows, of which these are two. The
  two anonymization rows, 6 and 7, are refused for a stronger reason: the plaintext they
  ask about is not on the server at all (see *Refusing the reads a digest cannot answer*).
The second pass closed the eighth, `avg(bigint)` (⛔ row 5), and one 🟡 row, and it did **not**
reduce the 🟡 count, because closing `avg` moved it from ⛔ to 🟡 rather than to ✅:

- **`avg` now widens like `sum`.** Rows 6 and 7. The argument is cast to `Decimal128(38, 0)`
  on the logical plan before `TypeCoercion`, so `avg(bigint)` and `avg(integer)` are both the
  `numeric` PostgreSQL promises, and an integral average is exact at any `bigint` magnitude —
  `avg(x) OVER (…)` agrees with `avg(x)`, which is the invariant that made row 3's patch worth
  generalizing rather than copying. Both stay 🟡 for a **scale** reason that has nothing to do
  with the original defect: the accumulator carries 10 decimal places, so a non-terminating
  average has 10 correct digits where PostgreSQL prints 16.
- **`percentile_cont` is exact and `percentile_disc` exists.** Rows 27 and 31, both as VaireDB
  UDAFs, and 27 is the pass's only 🟡 → ✅. This is where the *Still open* line above used to
  point at a type-mapping layer as a prerequisite; it turned out not to be one, because the
  type these two need is the type they already return. See *A UDAF that crosses a stage
  boundary* for why a new UDAF stopped being expensive.

**Every one of the 38 aggregates is reachable, computes a correct value, and returns it under
the type PostgreSQL promises, with five exceptions — all of them narrowings rather than wrong
answers.** No aggregate is missing, mis-merged or mis-distributed. The 5 remaining function 🟡
rows: `avg` over integers loses digits past the 10th decimal place (rows 6, 7), the `stddev`
and `var` families answer `float8` where PG stays exact over `numeric` (rows 19, 20), and
`regr_count` answers `numeric` where PG answers `int8` (row 22). All five close in the
result-type layer rather than here, and none of them returns a value a client would call wrong
without comparing digit for digit.

**Result column labels are no longer a blanket defect.** All 38 used to be labelled with
DataFusion's rendered plan expression rather than PostgreSQL's bare function name;
`pgwire_handler/column_labels.rs` now supplies the PostgreSQL label. The residue is the
select list where PostgreSQL's own answer contains a duplicate name — `SELECT sum(a),
sum(b)` is two columns both called `sum`, which DataFusion refuses to plan — so those keep
the verbose label. See *Result column labels*.

### The ⛔ rows, ranked

| # | Construct | Returns | PostgreSQL returns | Root cause |
|---:|---|---|---|---|
| 0 | ~~`HAVING <agg> <op> $1`~~ (extended protocol, untyped param) **fixed** | wrong row set — `$1='60'` over sums `50/75/185` yielded only the `75` group | all groups with `sum > 60`, i.e. `75` **and** `185` | was: `$1` stayed `Utf8`, so the comparison was **lexicographic**. The placeholder now takes its type from the aggregate beside it — see *Typing an untyped `$N`* |
| 1 | ~~`sum(bigint)`~~ **fixed** | `-1` for `2^63-1 + 2^63-1 + 1` | `18446744073709551615` | was: typed `int8` (oid 20), not `numeric`, and wrapped in two's complement. The read path now accumulates in `Decimal128(38, 0)`, which is the `numeric` PostgreSQL promises — see *Widening `sum`* |
| 2 | ~~bare correlated scalar subquery in the select list~~ **fixed** | the subquery's value | the subquery's value | was: `datafusion-pg-catalog` `RemoveSubqueryFromProjection` (`sql/rules.rs:1100`) folded it to `Expr::Value(Null)`, with an **unaliased** table counting as correlated (`:1052`). VaireDB now re-applies the same rule chain minus that one rule to the statements it would have damaged — see *Neutralizing `RemoveSubqueryFromProjection`* |
| 3 | ~~`<agg>(…) FILTER (WHERE …) OVER (…)`~~ **now refused** | the `FILTER` was **discarded** — `count(*) FILTER (WHERE m > 100000) OVER (PARTITION BY cat)` returned `4`, not `0` | `0` | `FILTER` is dropped when the aggregate is used as a window function, because the serialized window expression has no field to carry it. Rejected with `0A000` rather than answered; works correctly on a plain aggregate (see M2) |
| 4 | ~~`OVER (<named_window> ORDER BY …)`~~ **now refused** | `PARTITION BY` from the named window was dropped — the unpartitioned running sum (`80` where the partitioned answer is `30`) | the partitioned running sum | named-window inheritance loses clauses. Rejected with `0A000`; inline `OVER (PARTITION BY … ORDER BY …)`, and `OVER <name>` without parentheses, are correct and stay accepted |
| 5 | ~~`avg(bigint)`~~ **fixed** | `6148914691236517000` | `6148914691236517205` | was: computed in `float8`, losing integer precision above 2⁵³. Closed by row 1's own patch with one arm added — `avg` follows its argument's type after all, so casting the argument to a decimal is enough. See *Widening `sum`*, where the prediction that it would not be is recorded |
| 6 | ~~`min`/`max`/`ORDER BY` on an **anonymized** column~~ **now refused** | lexicographic extreme of the HMAC **digest** | the plaintext extreme | anonymization is write-path-only (`anonymization/rewrite.rs:89` — `_ => Ok(())`), so a `SELECT` reads digests. The read path now rejects the orderings with `0A000` — see *Refusing the reads a digest cannot answer* |
| 7 | ~~`<agg>` filtered on an anonymized column by plaintext~~ **now refused** | the empty-set answer (`0` / `NULL`) | the real answer | same cause as row 6 — the predicate literal is never hashed, so it matches no digest. A plaintext equality and a pattern match are now rejected, and the equality's message names the digest to send instead |

Mapping these onto the summary axes: only **rows 1 and 5** are aggregate defects.
**Rows 3 and 4** are window-clause defects (`M10`, `M11`). **Rows 0, 2, 6 and 7**
are the cross-cutting ones — param typing, an AST rewrite, and anonymization
twice — and they are most visible through aggregates rather than caused by them,
so fixing them fixes far more than this document's surface.

**All eight are closed, and the ranking held up.** Rows 0, 1 and 2 were the three that
returned a wrong *value* — or a wrong row set — through a construct with no workaround a
client would find on its own, and they were fixed first. Rows 3, 4, 6 and 7 are refused.
Row 5, `avg(bigint)`, went last, and it is the one this document mispredicted: it was held
back on the argument that `avg` needs a result-type mapping layer `sum` did not, and it
closed with `sum`'s own patch generalized by one arm. The prediction that was right is the
one that mattered — that the fix belongs on the *logical plan before coercion*, not in an
`AnalyzerRule`.

### Typing an untyped `$N`

Row 0 is closed, and the interesting part is that the remedy this document originally
proposed was the wrong one.

The defect: a client that sends `Parse` without declaring parameter OIDs — which is what
`tokio-postgres`, and so a large share of drivers, does — left `$1` untyped, and an untyped
parameter is decoded as text. `HAVING sum(n) > $1` with `$1 = '60'` therefore became a
**string** comparison. `'185' > '60'` is false, so the group totalling 185 dropped out of
the answer: wrong rows, no error, and a result set that looks entirely plausible.

The originally documented fix was to fall back to the client-declared OID in
`decode_param_values`. Measurement killed it twice over. `tokio-postgres` declares no OID at
all, so there is nothing to fall back *to*; and `arrow-pg` already prefers a declared OID
over an inferred type when one is present (`datatypes/df.rs`, `pg_type_hint` before the
inferred type before `UNKNOWN`), which is PostgreSQL's own precedence. The decoder was
never the layer at fault — it was faithfully decoding a type the plan had failed to state.

The type is available, one layer up and one step too late. DataFusion's
`Expr::infer_placeholder_types` does exactly this inference — for a binary comparison,
`BETWEEN`, `IN` and `LIKE` it takes the type of the expression on the other side — but
DataFusion only calls it from `replace_params_with_values`, on the way to substituting
values in. By then the value has already been decoded, using the type the plan reported at
`Describe` time. So `pgwire_handler/pg_param_types.rs` runs **the same inference one step
earlier**, over the logical plan in `parser::plan_select`, so that `get_parameter_types`
carries the type: `Describe` advertises `int8`, `arrow-pg` decodes an `i64`, and the
comparison DataFusion plans is the arithmetic one. Describe and Execute cannot disagree,
because it is the same plan and the same inference.

This is the same lesson as *Widening `sum`* arriving from the other direction. A type the
client is told is read off the logical plan, so a type-changing fix has to be a plan
rewrite in `plan_select` — not a rule in the analyzer, and not a special case in the
decoder.

Two shapes need saying explicitly:

- **`LIMIT $1` / `OFFSET $1`** have no comparand — the placeholder stands alone — so
  inference cannot reach them, and the pass types them `bigint` outright. Not a guess:
  PostgreSQL's grammar admits only a row count there and declares the parameter `bigint`
  for that reason. This is the extra row the remediation item predicted would close with
  row 0, and it did.
- **`SELECT $1`** with no context stays untyped and is still decoded as text. PostgreSQL
  refuses the shape (`42P18`, *could not determine data type of parameter*) when no OID is
  declared, so text is a divergence — but a client that declares the OID, which is the only
  way the shape is useful, already gets the right answer. It belongs to the operator and
  literal surface, not here.

The pass is infallible by construction: where a neighbouring type cannot be derived it
returns the original plan rather than an error. Nothing is lost by that.
`replace_params_with_values` runs the same inference when the values arrive, so an
uninferable shape fails there exactly as it does today, and an inferable one is only ever
improved — which is what makes it safe to apply to every read-path plan rather than to a
matched subset.

### Widening `sum`

Row 1 is closed, and where the fix had to live is the transferable part.

`sum(bigint)` accumulates in `Int64` in DataFusion and wraps silently. PostgreSQL's answer
is reachable rather than merely refusable — accumulate in `Decimal128(38, 0)`, which
`arrow-pg` advertises as exactly the `numeric` PostgreSQL promises, and which `bigint`
inputs cannot exhaust at any row count a cluster will hold. So this row is answered, not
rejected.

Two attempts failed before the third worked, both for the same reason, and it is a reason
that constrains every future type-changing rewrite on the read path:

**The type a client is told is read off the *unanalyzed* logical plan.**
`create_logical_plan` returns the plan `statement_to_plan` produced; the analyzer does not
run until `create_physical_plan`. VaireDB turns `df.schema()` into the result-column type
OIDs, for `Describe` and for the rows both. A rewrite registered as a DataFusion
`AnalyzerRule` therefore changes what *executes* while leaving the advertised type behind
— the plan promised `bigint`, execution produced a decimal, and the encoder's checked cast
into the promised type met the client as `[VDB-1019] column "sum" holds a value
PostgreSQL's Int64 cannot represent`. A silent wrong answer traded for a confusing error
rather than for a right one.

The rewrite lives in `pgwire_handler/pg_aggregate_widening.rs` and is applied to the
logical plan in `parser::plan_select`, which is the single funnel every read-path entry
goes through — `handle_select`, `collect_query_rows`, the view expansion, and the Describe
path — so Describe and Execute cannot disagree, because they are the same plan.

Running **before** the analyzer is also what makes it correct rather than merely visible.
PostgreSQL resolves between `sum(integer)` and `sum(bigint)` on the argument's *written*
type and never widens to pick an overload, so `sum(n)` over an `integer` column is `bigint`
while `sum(n::bigint)` is `numeric`. DataFusion's `TypeCoercion` erases exactly that
distinction — it rewrites `sum(n)` into `sum(CAST(n AS Int64))` to match the signature,
after which the two are the same expression. Anything running later cannot tell them apart
and would widen both, getting `sum(integer)` wrong in the other direction.

The rewrite matches both plan nodes the aggregate appears under: a grouped `sum(x)` is an
`AggregateFunction`, and `sum(x) OVER (…)` is a `WindowFunction` wrapping the same UDF.
Covering only the first would have left the two spellings reporting different types for the
same total, and the window form is the easier overflow to reach, since each row of a
partition carries the running sum of the rows before it.

Row 5, `avg(bigint)`, was predicted here to need a **different** patch — the argument was
that `sum` had a target accumulator both wider than the input and a type PostgreSQL agrees
with, whereas `avg` returns `float8` from a UDAF whose return type is not a function of an
argument cast, so getting `numeric` out would need a wrapper UDAF or a division expressed in
the plan.

That was wrong, and worth recording as wrong. DataFusion's `avg` return type **is** a function
of its argument type: `avg(Decimal128(38, s))` returns `Decimal128(38, s + 4)`, and it answers
`float8` only because the argument arrives as an integer. So row 5 closed by adding one arm to
the same rewrite, and the module became `widen_bigint_aggregates`. One arm differs
deliberately: `avg` widens from **every** integer width because PostgreSQL's `avg(integer)` is
`numeric`, while `sum` widens only from `bigint` because `sum(integer)` is `bigint` — the two
functions agree about the large case and disagree about the small one.

Where `avg` does need its own decision is the accumulator's **scale**, and this is the part
that has no free answer. `sum` needs none: an integer total has no fractional part. An average
does, so accumulating at scale 0 would answer `1.6666` for PostgreSQL's `1.6666666666666667` —
right type, right magnitude, visibly rounded off. The accumulator is `Decimal128(38, 6)`, whose
result is therefore `Decimal128(38, 10)`: ten correct decimal places against PostgreSQL's
sixteen. That is why rows 6 and 7 are 🟡 rather than ✅.

Scale is bought out of the same 38 digits the accumulation spends, which is why it is six and
not twelve. `avg` sums into `Decimal128(38, s)`, so `N` rows of magnitude up to `i64::MAX`
need `N · 9.22 × 10¹⁸ · 10ˢ < 10³⁸`: at scale 6 that ceiling is ~10¹³ rows, past any cluster;
at 12 it is ~10⁷, which an analytical table reaches. And the ceiling is loud rather than
silent — DataFusion's `AvgAccumulator` raises `Arithmetic Overflow` where its `sum` wraps — so
the trade costs an error, not a wrong number. Matching PostgreSQL's sixteen digits means a
wider accumulator than 128 bits, not a different constant.

### Refusing the reads a digest cannot answer

Rows 6 and 7 are closed by refusal, and the line drawn is the interesting part, because a
pseudonymized column is not uniformly unreadable — a deterministic HMAC keeps some
questions exactly answerable and destroys others completely.

**What the hash keeps.** Equality, and everything built on it: `GROUP BY`,
`DISTINCT`, `count`, `count(DISTINCT)`, a join on the column, `IS NULL`. Equal
plaintexts have equal digests and unequal plaintexts have unequal digests, so cardinality
and grouping are not approximations — they are the same answer PostgreSQL gives over the
plaintext. All of it stays accepted.

**What the hash destroys.** *Order*, and *structure*. `ORDER BY email` sorts by digest;
`min(email)` and `max(email)` return the digest's extreme; `email > 'b'` compares hex
against a letter; `email LIKE '%@x.com'` matches nothing however many rows end that way.
Each of those returns a well-formed answer with nothing in it to say the answer is about
something other than what was asked, which is the ⛔ class in its purest form.

So the read path refuses them by name, in
`pgwire_handler/anonymized_reads.rs`, wired into `prepare_select_for_planning`
immediately after view expansion — so a view's body is checked too, and so the AST is
still spelled the way the client wrote it. The check costs one catalog lookup per relation
read and short-circuits on the ordinary case, where no column in scope is
pseudonymized. What is rejected with `0A000`:

- **Ordering** — `ORDER BY`, `min`, `max`, `<`/`<=`/`>`/`>=`, `BETWEEN`, a window's
  `ORDER BY` (inline or via a named `WINDOW`), and an aggregate's own
  `ORDER BY` (`array_agg(id ORDER BY email)`).
- **Structure** — `LIKE`, `ILIKE`, `SIMILAR TO`, `~` and their negations.
- **Equality against a literal that cannot be a digest** — and only that. Equality is
  answerable, by the documented route: hash client-side and send the 64-hex digest. So
  refusing `WHERE email = '<digest>'` would remove the contract, while refusing
  `WHERE email = 'alice@x.com'` removes a guaranteed-empty answer; the message says which
  to send. A literal is taken for a digest by shape alone — 64 ASCII hex characters —
  which over-refuses a 64-hex plaintext, the harmless direction.

Two things this deliberately does not do. It does **not** hash the read-path literal:
the digest lookup is the tested contract (`test_anonymized_insert_stores_digest_not_plaintext`
and its neighbours), and silently hashing would mean `WHERE email = '<digest>'` stopped
finding the row. And it does not yet catch `WHERE email = $1` bound to plaintext — the
literal is not in the AST at all, so that needs a check at `Bind`, against the parameter's
value rather than the statement. Until then that one shape still answers empty.

Column matching is by name, unqualified, case-insensitively, and it counts a mention
inside a function call, so `ORDER BY lower(email)` is refused too. Over-refusing is the
safe direction here: the cost is an error a client can work around, and the alternative is
the wrong number this row was opened for.

### Neutralizing `RemoveSubqueryFromProjection`

Row 2 is closed, and the shape of the fix is worth recording because the same
tension will recur with every upstream compatibility rule.

The two obvious remedies were both wrong. Scoping the rule to the local
introspection context does not work — routing is decided *after* the parse, and the
parse is where the rule runs. Patching the correlation check upstream fixes the
false positives but not the genuinely correlated case, which is the common one.

What VaireDB does instead is take the statement over. `datafusion-pg-catalog`
exposes its rules publicly, so the coordinator assembles **the same chain in the
same order, minus that one rule**, and applies it to a fresh parse of the client's
own text. That path is entered only for the statements the rule would have damaged,
which is decided by running upstream's own rule and seeing whether a subquery
disappeared — so the definition of "damaged" cannot drift from the version in
`Cargo.lock`. Statements that read metadata are deliberately left on the upstream
path, because for a driver probe the `NULL` fallback is what makes the query
answerable at all; that is the distinction the rule needed and did not have.

What the client gets afterwards is DataFusion's own answer to the subquery, or
DataFusion's own error where it cannot plan one. Both are acceptable; a column of
NULLs was not.

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
| 3 | `sum(int8)` | `numeric` (1700) | `numeric` | ✅ | Was `int8` and wrapped. The read path widens the accumulator to `Decimal128(38, 0)` — see *Widening `sum`*. Holds for `sum(x) OVER (…)` too. |
| 4 | `sum(float8)` | `float8` (701) | `float8` | ✅ | |
| 5 | `sum(numeric)` | `numeric` (1700) | `numeric` | ✅ | Scale follows the `Decimal128(38,10)` the coordinator assigns every NUMERIC — see the data-type analysis, `:171`. |
| 6 | `avg(int4)` | `numeric` (1700) | `numeric` | 🟡 | Type was `float8` and is now right; `avg` widens from `integer` too, because PG promises `numeric` there while `sum(integer)` stays `int8`. What remains is **scale**: a non-terminating average is carried to 10 decimal places, so `avg` of `1,2,2` is `1.6666666666` where PG answers `1.6666666666666667`. |
| 7 | `avg(int8)` | `numeric` (1700) | `numeric` | 🟡 | Was `float8` and **inexact** above 2⁵³ — the average of `6148914691236517205` came back `6148914691236517000`. The argument is now cast to `Decimal128(38, 0)` before `TypeCoercion`, the same accumulator row 3 uses, so an integral average is exact at any magnitude `bigint` holds; `avg(x) OVER (…)` agrees. Row 6's scale narrowing applies here too. See *Widening `sum`*. |
| 8 | `avg(float8)`, `avg(numeric)` | `float8` / `numeric` | same | ✅ | |
| 9 | `min`/`max` | input type (`int4`→23, `int8`→20, `float8`→701, `numeric`→1700, text→25, `bool`→16, `date`→1082, `timestamp`→1114) | same | ✅ | Type-preserving and correct for every type probed. text→25 rather than `varchar`→1043 is inherited from how the coordinator advertises VARCHAR, not an aggregate issue. |
| 10 | `median` | input type | *(none)* | ✅ | DataFusion extension. Exact, not approximate, and globally correct — the shuffle collects all values for a group onto one executor. |
| 11 | `array_agg` | `_int4` (1007) / `_text` (1009) | `anyarray` | ✅ | `ORDER BY` inside the call works (`array_agg(m ORDER BY m DESC)`), with PG's NULLS-FIRST-on-DESC default. Element OID follows the base column's advertised OID, so it is self-consistent. |
| 12 | `string_agg` | `text` (25) | `text` | ✅ | Separator and inner `ORDER BY` both work. `string_agg(DISTINCT …)` without `ORDER BY` returns shuffle order — unspecified in PG too, so not a gap, but do not rely on it. |
| 13 | `bit_and`, `bit_or`, `bit_xor` | `int4` (23) | `int4` | ✅ | |
| 14 | `bool_and`, `bool_or` | `bool` (16) | `bool` | ✅ | PG's `every()` spelling is answered too, rewritten to `bool_and` — row 29. |
| 15 | `first_value`, `last_value` | input type | *(none as aggregates)* | ✅ | Aggregate form with inner `ORDER BY` works. PostgreSQL has these only as window functions. |
| 16 | `grouping` | `int4` (23) | `int4` | ✅ | Correct bitmask under `ROLLUP`, `CUBE` and `GROUPING SETS`. The `not_impl_err!` in `grouping.rs:110` is unreachable from a grouping-set context — the planner resolves the call before an accumulator is ever built. |

### Statistical functions

| # | Aggregate | Result type (VaireDB) | PG | Status | Notes |
|---:|---|---|---|---|---|
| 17 | `corr` | `float8` (701) | `float8` | ✅ | |
| 18 | `covar_samp` (alias `covar`), `covar_pop` | `float8` (701) | `float8` | ✅ | |
| 19 | `stddev` (alias `stddev_samp`), `stddev_pop` | `float8` (701) | **`numeric`** for int/numeric input | 🟡 | Correct to float8 precision. Over a `numeric` column PG stays exact and VaireDB does not. |
| 20 | `var` (aliases `var_samp`, `var_sample`), `var_pop` (alias `var_population`) | `float8` (701) | **`numeric`** for int/numeric input | 🟡 | Same as row 19. PG's `variance` spelling is answered too, rewritten to `var_samp` — row 28, so it inherits this row's result-type gap. |
| 21 | `regr_slope`, `regr_intercept`, `regr_r2`, `regr_avgx`, `regr_avgy`, `regr_sxx`, `regr_syy`, `regr_sxy` | `float8` (701) | `float8` | ✅ | All 8 correct, whole-table and per-group. |
| 22 | `regr_count` | **`numeric`** (1700) | `int8` | 🟡 | DataFusion returns `UInt64`; `arrow-pg` has no unsigned PG type so it maps to `numeric`. Value correct (`0` on an empty table, matching PG). |
| 23 | `nth_value` | input type | *(none as aggregate)* | ✅ | Aggregate form with inner `ORDER BY`. |

### Approximate functions

| # | Aggregate | Result type (VaireDB) | PG | Status | Notes |
|---:|---|---|---|---|---|
| 24 | `approx_distinct` | **`numeric`** (1700) | *(none)* | ✅ | Same `UInt64`→`numeric` mapping as row 22. No PG counterpart, so no divergence to record — but clients get a decimal where a count is expected. |
| 25 | `approx_median` | input type | *(none)* | ✅ | |
| 26 | `approx_percentile_cont`, `approx_percentile_cont_with_weight` | input type / `float8` | *(none)* | ✅ | |
| 27 | `percentile_cont` (alias `quantile_cont`) | `float8` (701) | `float8` | ✅ | Was **truncated to 5 decimal places** — `percentile_cont(0.9) WITHIN GROUP (ORDER BY i_col)` answered `90.99999` for an exact `91`, because DataFusion quantizes the interpolation weight. A VaireDB UDAF registered under the same name now shadows it and interpolates in `f64` throughout; the value is exact over `integer`, `double precision` and `numeric` sort columns, ascending or descending, and a fraction outside 0..1 is an error rather than a clamp. A **literal** fraction is checked on the coordinator, before a plan is shipped, so `percentile_cont(1.5)` gets PostgreSQL's own message under `22023` instead of a failed Ballista job wrapping it; the UDAF keeps its own check for the fractions the coordinator cannot see. `float8` is the right type: PG's only signatures are over `double precision` and `interval`. The **array-of-fractions** form is refused `0A000` — see row 31. |

## Modifier and clause surface

Where aggregates meet the rest of the language. Both of this section's ⛔ rows were
window-clause defects rather than aggregate ones, and both are now ❌ — refused with
`0A000` instead of answered with a plausible wrong number.

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
| M10 | `FILTER` on a **window** aggregate | ❌ | Was silently discarded; now `0A000`. ⛔ row 3. `FILTER` on a plain aggregate (M2) is unaffected. |
| M11 | `OVER (<named_window> ORDER BY …)` | ❌ | Was dropping the named window's clauses; now `0A000`. ⛔ row 4. `OVER <name>` without parentheses inherits everything and stays ✅. |
| M12 | Nested aggregate, e.g. `max(sum(m))` | 🟡 | `XX000` with a **~1 KB internal `Signature { … }` debug dump** in the client-visible message. PG rejects at parse with `42803 aggregate function calls cannot be nested`. Honest failure, but the wrong SQLSTATE and an unacceptable message. |

### Result column labels — closed, with a residue

Every aggregate used to be labelled with DataFusion's plan-rendered expression rather than
PostgreSQL's bare function name. `pgwire_handler/column_labels.rs` now supplies
PostgreSQL's label, so the right-hand column below is what a client sees:

| Query | Was | Is (= PG) |
|---|---|---|
| `SELECT count(*) FROM t` | `count(*)` | `count` |
| `SELECT sum(b) FROM t` | `sum(t.b)` | `sum` |
| `SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY i) FROM t` | `percentile_cont(Float64(0.5)) WITHIN GROUP [t.i ASC NULLS LAST]` | `percentile_cont` |
| `SELECT rank() OVER (ORDER BY m) FROM t` | `rank() ORDER BY [t.m ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` | `rank` |

This was the highest-frequency compatibility issue on the axis — it affected all 38
aggregates and broke any client addressing result columns by name (`row["count"]`, most
ORMs' default aggregate mapping, BI tools that infer measure names) — and it never returned
a wrong value, which is why `AS <alias>` hid it for so long.

**The residue is duplicate names, and it is PostgreSQL's own answer that causes it.**
`SELECT sum(a), sum(b) FROM t` is two columns both called `sum` in PostgreSQL. DataFusion
refuses to plan a projection with two identically-named expressions — *"Projections require
unique expression names"* — so the label is applied only where it is unique within its own
select list, and a select list like that keeps the verbose labels on both columns. Closing
the residue means labelling at the encoding boundary rather than in the plan, which is a
larger change than the one it would buy.

## PostgreSQL aggregates that are absent

Four of the twelve are now answered — see rows 28–31 — and the remaining eight
reject honestly: `0A000` `[VDB-1004] Error during planning: Invalid function
'<name>'`, with DataFusion's did-you-mean suggestion appended (often comically
unhelpful — `xmlagg` suggests `log2`).

| # | PostgreSQL aggregate | Closest available | Cost to close |
|---:|---|---|---|
| 28 | `variance(x)` | ✅ **answered** — rewritten to `var_samp(x)` | *Closed.* Exact synonym. |
| 29 | `every(x)` | ✅ **answered** — rewritten to `bool_and(x)` | *Closed.* Exact synonym. |
| 30 | `any_value(x)` | ✅ **answered** — rewritten to `min(x)` | *Closed.* PostgreSQL defines `any_value` as an arbitrary value **among the non-null inputs**; `min` satisfies that and is deterministic besides. DataFusion's `first_value` is the closer-looking choice and the wrong one — with no `ORDER BY` it can return the `NULL` PostgreSQL promises to skip. |
| 31 | `percentile_disc(f) WITHIN GROUP (ORDER BY x)` | ✅ **answered** — a VaireDB UDAF | *Closed.* Discrete percentile with no interpolation: sorts the group, takes PostgreSQL's `ceil(fraction × n)` row and never row 0, so it returns one of the **input** values and keeps the sort column's own type — `integer` in, `integer` out; `text` in, `text` out, which no interpolating percentile can do. Unlike rows 28–30 this could not be a rewrite, since there is no equivalent to rewrite *to*; the reason a new UDAF was affordable anyway is in *A UDAF that crosses a stage boundary*. The **array-of-fractions** overload PostgreSQL also defines is not implemented on either form, and the two refuse differently: `percentile_cont(ARRAY[…])` fails at coordinator signature resolution with `0A000` and the candidate list, while `percentile_disc(ARRAY[…])` gets past resolution and is caught by the UDAF's own fraction check *on an executor*, so it arrives as `XX000` wrapping `the percentile fraction … must be a number between 0 and 1, not [0.25, 0.50]` — a good message in the wrong class, for the boundary reason in [`gap-analysis.md`](gap-analysis.md) § 6.2 item 2. Adding the overload would close both; refusing it in the coordinator would at least make the class consistent. |
| 32 | `mode() WITHIN GROUP (ORDER BY x)` | — | New UDAF. |
| 33–36 | `rank()`, `dense_rank()`, `percent_rank()`, `cume_dist()` as **hypothetical-set** aggregates (`WITHIN GROUP`) | the window forms, which work | New UDAFs. The window spellings of all four are supported (M9), so this is only the `WITHIN GROUP` form. |
| 37–38 | `json_agg`, `jsonb_agg` | — | New UDAFs, and blocked behind the JSON type gap — see the operator analysis, which finds the whole JSON operator surface unreachable. |
| 39 | `xmlagg` | — | No XML type. Out of scope. |

Rows 28–30 are closed, which is where the read path's expression layer reaches: the
three names are rewritten in the AST (`pgwire_handler/pg_operators.rs`) rather than
registered as alias UDAFs. That is the distributed constraint, not a shortcut — a
function registered on the coordinator's planner but not on every Ballista executor
resolves at planning time and then fails when the stage is deserialized, *after* the
client's query was accepted. Rewriting means only names both sides already know cross
the wire. The cost used to be that the result column carried the DataFusion name — a client
asking for `variance(x)` got a column called `var_samp(t.x)` — and `column_labels.rs`
removes it: the label is taken from what the client wrote, not from what the plan resolved
to.

### A UDAF that crosses a stage boundary

The paragraph above is why rows 28–30 were rewrites rather than registrations, and it reads
like a general prohibition on VaireDB aggregates. Rows 27 and 31 are the exception, and the
distinction is worth stating because it decides the cost of every future aggregate on this
axis.

The obstacle was never registration — it was **serialization**. A physical plan crossing the
scheduler's gRPC surface is encoded by a codec, and an aggregate the codec does not know
about is a plan that cannot be decoded on the executor. That is what makes a new UDAF look
expensive: three codecs to teach, on both the logical and physical side.

It turns out none of that is needed. `datafusion-proto`'s physical-plan decoder resolves an
aggregate whose codec payload is **empty** by looking the name up in *the executor's own*
function registry first, falling back to the codec only if that misses — and the default
encoder writes an empty payload. So a UDAF registered under the same name on every context
round-trips a stage boundary untouched, with no codec change at all. The requirement is only
that the registration happen everywhere the plan is built or run: the client context, the
scheduler's state, and every executor. `register_postgres_functions` is already called at all
three, which is why both percentile UDAFs went in at one seam.

Two consequences. First, **shadowing** works the same way: registering `percentile_cont`
under DataFusion's own name replaces it on every node at once, which is how row 27's
truncation was fixed without patching DataFusion. Second, the constraint that killed rows
28–30 as registrations is unchanged in kind but not in cost — a function registered on the
coordinator **only** still fails after the query was accepted. What matters is that "register
on every executor" turned out to be a one-line-per-context change rather than a codec
project. Anything on this list that genuinely has no equivalent to rewrite to — row 32's
`mode()`, the hypothetical-set forms at 33–36 — is now a UDAF-shaped problem, not a
serialization-shaped one.

## Root causes

Five root causes explain everything above. Only the first is aggregate-specific.

1. **No PostgreSQL result-type mapping layer for aggregates** — *partly answered.*
   VaireDB advertises whatever Arrow type DataFusion's UDAF declares. PostgreSQL's
   aggregate result types are deliberately *wider* than their inputs —
   `sum(bigint)→numeric`, `avg(int)→numeric`, `stddev/variance→numeric` — precisely
   to prevent the overflow in ⛔ row 1 and the precision loss in ⛔ row 5. This
   single mismatch produced both aggregate ⛔ rows (1, 5) and 5 of the 6 🟡 rows —
   `stddev`, `stddev_pop`, `var`, `var_pop` (master rows 19–20) and `regr_count`
   (row 22), plus the type-only half of `avg` at master row 6. The sixth 🟡,
   `percentile_cont`, was an interpolation-precision bug and unrelated; it is closed.

   `sum` and `avg` are now mapped (*Widening `sum`*), which settles both rows where the
   wrong type also produced a wrong *value*. The 5 that remain are type-only: `stddev`,
   `var` and `regr_count` are unmapped, and `avg` is mapped but to a decimal whose scale
   is short of PostgreSQL's. What the fix established is **where** such a mapping has to
   live: on the logical plan in `plan_select`, before the analyzer, because the
   analyzer never runs on the plan whose schema a client is told about, and because
   `TypeCoercion` erases the argument type PostgreSQL resolves the overload on.
   Anything built later — an `AnalyzerRule`, a physical-plan pass, an encoder-side
   cast — changes the value without changing the promise, or changes both without
   being able to tell `sum(integer)` from `sum(bigint)`.

   The generalization this row was waiting for did not turn out to be needed. Both closures
   are the *same* rewrite with a per-function target type, and a table of
   `(name, argument type) → accumulator` is all the "mapping layer" this axis ever
   required — `stddev` and `var` are two more entries in it. That is worth stating because
   the layer was, for two passes, the reason to defer.

2. **An untyped `$N` was decoded as text** — *fixed, and the diagnosis first recorded
   here was wrong.* The parameter decode consults `plan.get_parameter_types()`; when
   DataFusion could not infer a placeholder's type — which is exactly what happens for
   `$1` on the right of a `HAVING` comparison — the parameter stayed `Utf8` and the
   comparison silently became a *string* comparison. That is ⛔ row 0, and `LIMIT $1`
   shared it.

   What was wrong was the claim that the `Bind` message carried the correct OID the whole
   time and that the decoder merely lacked a fallback to it. It did not: `tokio-postgres`
   declares no OID at all, and `arrow-pg` already prefers a declared OID over an inferred
   type when one is present. The fault was upstream of the decoder — the *plan* never
   stated the type — and so is the fix: DataFusion's own inference, run over the logical
   plan before the values are decoded. See *Typing an untyped `$N`*.

3. **`datafusion-pg-catalog`'s `RemoveSubqueryFromProjection` rewrite** — *fixed;
   recorded because the shape of the fix generalizes.* The rule
   (`sql/rules.rs:1019-1143`) replaces a correlated scalar subquery in a
   projection with a `NULL` literal (`:1100`, `:1115`), and its correlation check
   treats an **unaliased** table as correlated (`:1052`) and counts an `$N`
   placeholder as a reference to the outer row. It exists to make `pg_catalog`
   emulation tractable, and applied to user queries too — ⛔ row 2. The lesson is
   that an upstream compatibility layer optimizes for *answering* driver probes,
   which is the opposite of this codebase's invariant for user data, so a rule that
   is right for one is a silent wrong answer for the other. See *Neutralizing
   `RemoveSubqueryFromProjection`* for how the two were separated.

4. **Anonymization is structurally write-path-only** — *and that is now enforced rather
   than merely true.* `anonymization/rewrite.rs:20-91` handles `Statement::Insert` and
   `Statement::Update` and falls through `_ => Ok(())` at `:89`. Aggregates therefore see
   HMAC digests, and plaintext predicates match nothing — ⛔ rows 6 and 7. That cannot be
   fixed by moving the rewrite to the read path, because the plaintext is not on the server
   to be recovered; the remedy is to refuse the questions the digest cannot answer, which
   is what `pgwire_handler/anonymized_reads.rs` now does. Equality-preserving reads
   (`count`, `count(DISTINCT)`, `GROUP BY`, a digest lookup) remain correct, and remain
   accepted, because the HMAC is deterministic. See *Refusing the reads a digest cannot
   answer*.

5. **The error classifier and sanitizer were pinned to pre-53 DataFusion message
   text** — *fixed, and the residue is somewhere else entirely.*
   `classify_generic_error_code` matched `"not yet implemented"` and `"unsupported"`;
   DataFusion emits `"This feature is not implemented: "` and `"not supported"`, so
   neither matched and honest feature gaps surfaced as `XX000` instead of `0A000`.
   Symmetrically, `vairedb-common/src/error/sanitize.rs` stripped `"Plan error: "`,
   `"Not Implemented: "` and `"Configuration error: "`, none of which DataFusion emits,
   so `"Error during planning: "` leaked verbatim into every ❌ row's message.

   Both are closed the way the mis-match itself suggests: the classifier now matches on
   **`DataFusionError` variants** rather than on substrings of their display text, which a
   version bump cannot rot, and the prefix list is re-derived from DataFusion 54.1's own
   `error_prefix()`. The `Signature { … }` dump is truncated. What that did **not** fix is
   M8 and M12, and the reason is worth recording on this axis: those two errors are raised
   on an **executor**, and everything past the Ballista scheduler arrives at the coordinator
   as the scheduler's own text (`Job <id> failed: …`) with no typed error left to classify.
   So a nested aggregate is still `XX000` where PostgreSQL says `42803` — not because the
   classifier is wrong, but because it is never shown the error. This was diagnosed as a
   message-text problem and is really a boundary problem. **One class did get carved out by
   name**: division by zero, matched on the transported text only after the typed classifier
   has already given up, so all its forms report `22012` (see
   [`gap-analysis.md`](gap-analysis.md) § 6.2 item 2). That is a scoped concession for the
   class an analytical client hits daily, not a template — extending it per class is the
   substring matching the typed classifier was written to remove.

## Prioritized remediation

Ranked by client impact per unit of work. Items 1–4 and 8 change a returned
*value* — they are the correctness work. The rest change types, labels or error
shapes.

Items 1, 3, 4, 6, 7, 8 and 10 are closed, 2 and 5 in part, and 9 in half. **No item on this
list still returns a wrong number.** All five value-changing items are settled: 1 and 3 were
answered, 4 and 8 refused, and 2's value half — `avg(bigint)` — closed with the cast this item
predicted would not work for it. What remains across the whole list is types, scales, labels
and error classes.

1. ~~**Fix the parameter typing**~~ (⛔ row 0) — **done**, though not where this item
   said to do it: the fix is DataFusion's own placeholder inference run over the logical
   plan in `plan_select`, not a client-OID fallback in the decoder (the client declares no
   OID, and the decoder already prefers one when it exists). It was the smallest change
   with the largest correctness win, as ranked — a wrong-row-set bug in a construct
   (`HAVING sum(x) > ?`) that every reporting client emits, invisible because it returned
   *plausible* rows — and it closed `LIMIT $1` at the same time, as predicted. See *Typing
   an untyped `$N`*.
2. **Introduce a PostgreSQL aggregate result-type mapping** (⛔ row 5, plus the 5 🟡
   type rows) — *`sum(int8)` and `avg(int*)`→`numeric` are done*; still open are
   `stddev`/`var` over exact inputs→`numeric`, `regr_count`→`int8`, and `avg`'s scale.
   Both done ones took the "cast injected into the logical plan" route, and the second is
   where this item was wrong: it asserted that only `sum`'s return type follows its
   argument's and that `avg` returns `float8` however it is cast. `avg` does follow —
   `avg(Decimal128(38, s))` is `Decimal128(38, s + 4)` — so a wrapper UDAF was never
   needed, and the "mapping layer" this item asks for is a two-column table of
   `(name, argument type) → accumulator`, which now exists and has two entries.
   `stddev`/`var` are two more entries to measure; `regr_count` returns `UInt64` and
   genuinely does need something else, since no argument cast changes a count's type.
   The urgency dropped with rows 1 and 5: what is left returns the right number, or the
   right number to ten decimal places, under a divergent type.
3. ~~**Neutralize `RemoveSubqueryFromProjection` for user queries**~~ (⛔ row 2) —
   **done**; see *Neutralizing `RemoveSubqueryFromProjection`* below.
4. ~~**Fix the window-clause defects**~~ (⛔ rows 3 and 4) — **answered by refusal,**
   not by a fix. Carrying `FILTER` through window-aggregate planning and making named-
   window inheritance additive both need the serialized window expression to gain a
   field it does not have, which is upstream work. So the coordinator rejects the two
   shapes with `0A000` (`pgwire_handler/pg_operators.rs`) and leaves the inline
   equivalents — which were the differential-test controls — accepted and correct.
   A refusal is the third option next to "fix locally" and "wait upstream", and it is
   the one that stops a wrong number reaching a client today.
5. **Refresh the error classifier and sanitizer to current DataFusion text** (M8, M12,
   all ❌ rows) — **done, and it did not close M8 or M12.** The classifier matches on
   `DataFusionError` variants rather than message substrings, the prefix list is
   re-derived from `error_prefix()`, and the `Signature { … }` dump is truncated, so those
   messages are readable now. But both of these rows raise their error on an *executor*,
   and the scheduler re-textualizes anything past its boundary as `Job <id> failed: …`, so
   there is no typed error to classify and they stay `XX000`. The cheap half was cheap and
   the remaining half is a boundary, not a mapping — see root cause 5.
6. ~~**Add the PostgreSQL aliases**~~ — **done.** `variance`→`var_samp`,
   `every`→`bool_and` and `any_value`→`min` are rewritten in the AST on the read path
   (`pgwire_handler/pg_operators.rs`), not registered as aliases, so the set of
   function names the coordinator plans with stays identical to the set every executor
   holds. `any_value`→`min` rather than `first_value` was the deliberate part: PG's
   contract is *an arbitrary non-null input*, which `min` keeps and `first_value`
   breaks. 3 of the 12 ❌ rows closed.
7. ~~**Label aggregate result columns PostgreSQL-style**~~ — **done.**
   `pgwire_handler/column_labels.rs` aliases an unaliased function-call column with
   PostgreSQL's bare function name, on the AST and before the `variance`/`every`/
   `any_value` rewrites, so the label is the one the client wrote. The `pg_catalog`
   emulation path is skipped, as this item warned it had to be: those statements come
   from upstream's own rewrites, tuned to the column names particular drivers look
   for. The residue is a select list whose PostgreSQL labels collide — see *Result
   column labels*.
8. ~~**Reject or annotate aggregates over anonymized columns**~~ (⛔ rows 6 and 7) —
   **done by rejection, and the second half of this item was withdrawn.** `min`/`max`/
   `ORDER BY`, range comparisons and pattern matches on a pseudonymized column are now
   `0A000` (`pgwire_handler/anonymized_reads.rs`). Hashing the literal in
   `WHERE anon_col = <literal>`, which this item also asked for, would have reversed a
   tested contract — the documented lookup *is* `WHERE anon_col = '<digest>'` — so the
   plaintext equality is refused with a message naming the digest instead. A number
   silently computed over digests is worse than an error; so is a lookup that silently
   stops matching. See *Refusing the reads a digest cannot answer*.
9. ~~**`percentile_cont` precision**~~ (master row 27) — **done, and locally, against this
   item's own advice.** "Narrow, upstream, low frequency; file upstream rather than working
   around locally" was the wrong call twice over: an analytical client asks for a percentile
   constantly, and the local workaround turned out to cost about as much as filing the bug,
   because shadowing DataFusion's UDAF by name replaces it everywhere at once (see *A UDAF
   that crosses a stage boundary*). **`GROUPING SETS (())`** (M6) stands as written — that
   one really is narrow, and the same set works combined with a non-empty one.
10. ~~**Consider `percentile_disc` and `mode()`**~~ — **`percentile_disc` done** (row 31);
    `mode()` still absent. Both were ranked last on the assumption that a real UDAF is
    expensive to distribute, and that assumption was false: an aggregate registered under
    the same name on the client context, the scheduler and every executor round-trips a
    stage boundary with no codec change, because the decoder resolves an empty payload by
    name from the executor's own registry. `mode()` is now the cheapest ❌ row left on this
    axis, not the most expensive.

Explicitly **not** a gap and not worth work: DuckDB aggregate parity. DuckDB never
evaluates an aggregate in VaireDB, so its catalog is not a target.

## Executable counterpart

Aggregate coverage was incidental rather than systematic — 9 tests that use an aggregate
while testing something else (`sql_command_select.rs` `test_count`,
`test_scalar_aggregates`, `test_group_by_multi_agg`, `test_group_by_having`,
`test_distinct` and the two window tests; `errors.rs` `test_aggregates_empty_table`; and
the `COUNT` trio plus `SUM(amount)` in `data_types_round_trips.rs`, both there to verify a
*type*, not the aggregate). All passed, **none asserted an aggregate's result type, and
none of the 8 ⛔ rows was covered** — which is why they were all live.

All eight closed rows are covered — in `tests/e2e/tests/sql_expression_gaps.rs`, except the
two anonymization rows, which belong beside the write-path half of the same contract in
`tests/e2e/tests/anonymization.rs`:

| Test | Rows | What it pins |
|---|---|---|
| `test_an_untyped_parameter_beside_an_aggregate_compares_as_a_number` | ⛔ 0 | `Type::INT8` on `Describe` for `$1` in `HAVING sum(n) > $1`, and three thresholds chosen so that a lexicographic compare answers differently at each one — the assertion is the row set, not the type |
| `test_a_row_count_parameter_is_a_bigint` | ⛔ 0 (`LIMIT`/`OFFSET`) | `Type::INT8` for both, and that a bound `i64` actually limits and skips |
| `test_a_parameter_typed_from_a_column_keeps_the_columns_type` | ⛔ 0 (regression) | the pass does not override what the planner already inferred: `TEXT` from a `VARCHAR` column, `INT4` from an `INTEGER` one |
| `test_sum_of_bigint_does_not_wrap` | ⛔ 1, master 3 | the exact total past `i64::MAX`, `Type::NUMERIC` on Describe, and the same pair for the `sum(big) OVER (ORDER BY id)` spelling — plus `sum(small)` and `sum(small) OVER ()` staying `INT8`, which is the assertion that fails if the rewrite ever moves after `TypeCoercion` |
| `test_correlated_projection_subquery_is_answered`, `test_uncorrelated_projection_subquery_shapes_are_answered` | ⛔ 2 | the subquery's value instead of `NULL`, and that the shapes upstream would *not* have damaged still work |
| `test_window_clauses_datafusion_discards_are_refused` | ⛔ 3, 4 / M10, M11 | the two `0A000` refusals, each beside a positive control that must keep answering — an inline `OVER (PARTITION BY …)`, `OVER <name>` without parentheses, and two independent `WINDOW` definitions |
| `test_reads_that_would_report_digest_order_are_refused` (in `anonymization.rs`, where the HMAC helper and the secret-registration fixture already live) | ⛔ 6, 7 | the `0A000` refusals for `ORDER BY`, `min`, `max`, a window `ORDER BY`, `>`, `BETWEEN`, `LIKE`, `ILIKE` and `~` over a pseudonymized column, that the plaintext-equality message names the digest — and the positive controls that must keep answering: the `WHERE email = '<digest>'` lookup, `count(DISTINCT email)`, and projecting the digests under `ORDER BY id` |
| `test_a_function_column_is_labelled_the_way_postgres_labels_it` | labels | `count`, `sum`, `rank` rather than the rendered plan expression |
| `test_postgres_aggregate_spellings` | 28–30 | `variance`, `every`, `any_value` answered, and `any_value` skipping NULLs as PG promises |
| `test_avg_of_an_integer_is_exact_numeric` | ⛔ 5, master 6, 7 | the exact average past 2⁵³ and `Type::NUMERIC` on Describe, for `avg(bigint)` **and** `avg(integer)`, and for both the grouped and the `OVER ()` spelling — plus `avg(x::float8)` staying `FLOAT8`, which is the assertion that fails if the rewrite ever widens what PostgreSQL leaves alone. The ten-decimal-place narrowing is pinned here as what it is, so a future scale change has to change a test |
| `test_percentile_cont_interpolates_exactly_across_shards`, `test_percentile_disc_returns_an_input_value`, `test_a_percentile_of_nothing_and_of_a_bad_fraction` | master 27, 31 | The values, over a fixture whose ten rows are spread across all three shards so a percentile has to be gathered rather than answered from one — which is the whole point of testing a UDAF end to end rather than in-process. Ascending and descending, `integer` and `double precision` sort columns, every `ceil(f × n)` boundary for the discrete form, the sort column's type on Describe for both, a fraction outside 0..1 erroring, and an empty group answering NULL |

`describe_result_types` and `describe_param_types` are now public helpers in
`tests/e2e/src/lib.rs`, so a type assertion — on a result column or on a parameter — no
longer needs a bespoke client.

Still uncovered, and the shape the remaining suite should take — following the
`<doc-topic>_<facet>.rs` convention established by `sql_command_*` and `data_types_*`:

| File | Rows | Contents |
|---|---|---|
| `sql_function_aggregate.rs` | 1–27, M1–M12, 28–39 | One test per master-table row: value correctness against hand-computed ground truth, plus the modifier surface and the ❌ rejections. |
| `sql_function_aggregate_types.rs` | 1–27, labels | The result-type contract. `Parse`/`Describe` only, **no execution** — `describe_result_types` in `tests/e2e/src/lib.rs` is already promoted and public. Also pins the column-label rows, including the duplicate-name residue. |
| `sql_function_aggregate_distributed.rs` | merge invariants | The properties that must not regress if `remote_scan_exec.rs:46` ever gains a partitioning claim: `count(DISTINCT)` across shards, `median` across shards, `avg` ≠ average-of-averages, aggregates over a skewed table (one populated shard, two empty), and over NULL-heavy and cross-shard-duplicate data. These are the tests that would catch a wrong "optimization". |

Follow the existing convention: a **passing** test pinning today's honest
rejection (`assert_unsupported` for `0A000`, `assert_rejected` where the stage is
uncertain), plus an `#[ignore = "gap (row N): …"]` test asserting the
PostgreSQL-correct behavior, which fails by construction and is the definition of
done. With the ⛔ column empty there is no row left on this axis whose ignored test has to
assert a *value*; the rule still stands for whatever lands here next, because a silently-wrong
row returns a successful `CommandComplete`, so an error-shape assertion passes while the answer
is wrong.

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
   did not work against VaireDB at the time, diagnosed as a `0A000` inside
   `format_type(Utf8, Int64)` — half of that has since closed and the half that
   closed was the serialization of the plan `\gdesc` builds, not the coercion
   ([`gap-analysis.md`](gap-analysis.md) § 6.2 item 5). The probe is still the right
   instrument, because describing without executing is what these rows need.
   Base-column OIDs were captured alongside the aggregate OIDs, which is
   what separates a real type gap from one merely inherited from how the
   coordinator advertises the column (this reclassified `array_agg(varchar)` and
   `min(varchar)` from gaps to self-consistent rows).
4. **`VERBOSITY verbose` on every failing probe**, to capture the SQLSTATE and the
   `[VDB-NNNN]` enrichment code and attribute each failure to a pipeline stage.
5. **Registered-surface ground truth from source** — the 38 aggregates and their 7
   aliases were read out of `all_default_aggregate_functions` in
   `datafusion-functions-aggregate/src/lib.rs` and the per-UDAF `aliases()`
   implementations, rather than from the documentation page, which does not
   distinguish primary names from aliases. Cite a crate and a symbol rather than a
   versioned path: the version in a vendored path is stale the day the lockfile moves,
   and the symbol is what survives the upgrade.
6. **Plan-shape audit** — `remote_scan_exec.rs`, `scheduler.rs` and
   `scan_exec.rs` were read to establish *why* the merge is correct, so that the
   empirical result is explained by a structural property rather than by luck.

Two source-derived conclusions were **overturned by measurement** and the
empirical result kept:

- `grouping()`'s `accumulator()` is `not_impl_err!` in
  `datafusion-functions-aggregate`'s `grouping.rs`, which reads like a
  failure at execution. `grouping()` in fact works with `ROLLUP`, `CUBE` and
  `GROUPING SETS`, with correct bitmasks — the planner resolves the call before an
  accumulator is built, so that path is unreachable from a grouping-set context.
- Partial aggregation was initially assumed to be an overclaim in
  `distributed-query-processing.md` by analogy with filter push-down. Tracing
  the physical plan showed aggregation *is* genuinely two-phase; only push-down
  **into DuckDB SQL** was absent. The two claims shared a line of documentation but
  not a fate — and they have since diverged further: filter and limit push-down into
  DuckDB SQL now exists, and partial-aggregate push-down still does not.

Every ⛔ row was reproduced at least twice, and each was paired with a *control*
probe that isolates the cause: `sum(CAST(x AS NUMERIC))` for row 1, `(SELECT …) + 0`
for row 2, an inline `OVER (PARTITION BY … ORDER BY …)` for rows 3 and 4, and
`$1::int` for row 0. Each control returns the correct answer, which is what
localizes the defect to the rewrite/typing layer rather than to the aggregate or
its distribution.

The controls then did a second job the pairing did not anticipate: each became the
regression test for the row's fix. A control is by construction the query that must keep
working after the defective one changes behavior, so the inline `OVER` clauses now guard
the two refusals, and `sum(small)` — the control that says the rewrite must *not* fire —
is what would catch the widening being moved to a stage where it can no longer tell
`sum(integer)` from `sum(bigint)`. **Pair every silent-wrong row with a control that
returns the right answer, and the fix arrives with its test already written.**

One earlier conclusion was reached from source rather than measurement and turned out to
matter: `sum(x) OVER (…)` was still wrapping after the grouped form was fixed. No probe
found it — reading `Expr` while writing this document did, because the rewrite matched one
enum variant and DataFusion plans the same aggregate under two. Measurement is what
establishes a gap; it is not what enumerates the spellings of one.
