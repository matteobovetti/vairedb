# Join and Set Operation Gap — DataFusion ↔ Ballista ↔ VaireDB ↔ PostgreSQL Wire Protocol

Gap analysis of the **relational combinators**: every way a PostgreSQL client can put two
row sources together — a join, a semi/anti subquery, or a set operation — measured from the
wire, through the coordinator's parse/plan chain, into Ballista's distributed execution over
the per-shard DuckDB backends.

This is the sixth axis, added last and for a specific reason: it was the only surface in the
product with **no measurement at all**. Its five siblings each enumerate a catalog — the
types, the aggregates, the window functions, the operators, the statements — so their risk
was bounded before a single row was written. A join has no catalog. What it has is a
combinatorial surface, and it is the one construct whose *correctness depends on how the
data is laid out*, which in VaireDB means it depends on the shard map. So the size of the
risk here was not merely unknown, it was the only unknown of that kind left. Measuring it
found seven wrong answers, and closing one of those found an eighth — the widest of all of
them, and the one no row had asked about: on a cluster, a join **with no equijoin key whose
result is decided on its build side** silently lost exactly those rows. That made `EXISTS`
and `NOT EXISTS` answer constant false for every uncorrelated or inequality-correlated
subquery, and made a keyless `LEFT`/`FULL JOIN` drop its unmatched rows, which is an inner
join wearing an outer join's spelling. **All eight are now closed**, that one included; three
of them left a narrower corner behind, each recorded as a row of its own — and one of those
corners (row 42) was recorded as unclosable and then closed, because the reason given for it
turned out to be a guess about a plan shape rather than a measurement of one.

### References

- **PostgreSQL:** [Joined Tables](https://www.postgresql.org/docs/current/queries-table-expressions.html#QUERIES-JOIN), [Subquery Expressions](https://www.postgresql.org/docs/current/functions-subquery.html), [Combining Queries](https://www.postgresql.org/docs/current/queries-union.html) — the contract every row is measured against.
- **DataFusion 54.1:** `HashJoinExec`, `SortMergeJoinExec`, `NestedLoopJoinExec`, `SymmetricHashJoinExec`; the `decorrelate_predicate_subquery` and `scalar_subquery_to_join` optimizer rules that turn a subquery into one of them.
- **Ballista 54.1:** supplies the stage boundaries a join plan is cut at, and — see rows 13, 20 and 22 — a session default that changed a join's *answer*, a plan rewrite that made the obvious fix for it unshippable, and a task model that runs each partition of a stage in its own process, which voided a join operator's cross-partition bookkeeping and answered a whole class of correct plans with no rows at all.
- **DuckDB 1.5.5:** listed for completeness only. DuckDB never evaluates a join in VaireDB; see the framing below.
- **Sibling analyses:** [`gap-analysis-command.md`](gap-analysis-command.md), [`gap-analysis-data-type.md`](gap-analysis-data-type.md), [`gap-analysis-aggregate-function.md`](gap-analysis-aggregate-function.md), [`gap-analysis-window-function.md`](gap-analysis-window-function.md), [`gap-analysis-operator-literal.md`](gap-analysis-operator-literal.md). All six are consolidated in [`gap-analysis.md`](gap-analysis.md).

## Scope and framing

**One engine, as with aggregates.** A join can only appear in a `SELECT`, and on the read
path a core node runs nothing but

```sql
SELECT <projected cols> FROM <shard_table> [WHERE <pushed predicate>] [LIMIT n]
```

(`vairedb-core/src/table_provider/scan_exec.rs`, `build_query`). **No join is ever pushed
into DuckDB.** Every join, every semi/anti subquery and every set operation in VaireDB is
executed by DataFusion on the Ballista executors, over rows streamed up from the shards. So
this axis has **no split-brain rows** — there is no second evaluator to disagree with — and
DuckDB's own join divergences cannot reach a result.

What replaces that hazard is a different one, and it is the whole reason this axis exists.

### The two layouts, and why every row says which one it is

Every table in the test fixture is created with `shards = 3, shard_by = 'id'`, so `id`'s hash
decides where a row lives. That splits joins into two physically different plans:

| Layout | When | What the cluster does |
|---|---|---|
| **Co-located** | the join key is the shard key on both sides | equal keys hash to the same shard, so each shard can be joined where it lives; no data crosses the network to make a match |
| **Shuffle** | the join key is anything else | rows have to be repartitioned across the cluster before they can meet, so the join's inputs are rewritten by a network exchange first |

Both must give PostgreSQL's answer, and they are different code paths, so **each row is
measured in both layouts** where the distinction applies. The rows table marks which.

A third case sits underneath both: an operator whose semantics differ *once its input is
repartitioned*, a plan that cannot be serialized to reach an executor at all, one the
distributed planner rewrites into a shape its own operators reject, or one that ships and
runs and simply returns nothing. Those are the failures that exist only in a distributed
engine, and all four showed up — rows 13, 20, 22, 23 and 24. They are also, between them,
the reason this axis found more than it expected: the widest of them (rows 13 and 20, which
are one defect in two spellings) is invisible to any single-process test, because in one
process the same plan answers correctly.

### What "correct" means for a join, and why it is mostly about NULL

Excluding the plumbing, almost every real join divergence is a three-valued-logic
divergence, because that is the one place two operators can compute *different rows* while
both believe they implemented the same join:

- a **join predicate** treats two NULLs as unknown, so `NULL = NULL` never matches;
- a **set operation** treats two NULLs as *equal*, so `EXCEPT` and `INTERSECT` compare them
  like any other value;
- `NOT IN (subquery)` is neither: it is NULL — and so not true — as soon as the left value
  is NULL **or any candidate is**, and only an empty subquery makes it unconditionally true.

Three different rules, one query language. Every row below that carries a NULL in its
fixture is there because those three rules are where a join engine and PostgreSQL part
company without either of them raising an error.

## Legend

| Status | Meaning |
|---|---|
| ✅ | Correct: the same rows PostgreSQL returns, in both layouts. |
| 🟡 | Correct within a restriction that follows from the architecture. The restriction is in the row, and it is enforced rather than assumed. |
| ⛔ | **Silently wrong** — answers, and the answer differs from PostgreSQL's with nothing to tell a client. The worst status on this axis, and the only one that is a bug rather than a gap. |
| ❌ | Refused with `0A000`, no target behaviour agreed. Honest, but unavailable. |

A refused combinator fails at one of two points, the same two as the command axis:

| Rejection point | Error | SQLSTATE | When |
|---|---|---|---|
| Parse | `SqlSyntaxError` | `42601` | Does not parse, or does not type-check structurally, under sqlparser's `PostgreSqlDialect` — row 33's branch-width mismatch is here. |
| Classification | `FeatureNotSupported` | `0A000` | Parses, but VaireDB refuses it by name on the verbatim AST, before planning. Rows 24 and 27 are here, and both name a working alternative in the message. |

Every SQLSTATE below was **observed against the 5-node e2e cluster**, not inferred.

## Summary

| Status | Count | Rows |
|---|---:|---|
| ✅ | 36 | 1–22, 25, 26, 28–38, 42 (except 27) |
| 🟡 | 2 | 39 — a join on a pseudonymized column: equality only; 43 — an untyped literal branch of a set operation resolves to `text`, not the typed branch's type |
| ⛔ | 2 | 23 — `NOT IN` over an aggregate left side or a correlated subquery; 40 — a `USING` key reached by an explicit qualifier reports the merged value |
| ❌ | 3 | 24 — `<op> ANY/ALL (subquery)`; 27 — `INTERSECT ALL` / `EXCEPT ALL`; 41 — an unqualified `USING` key in `WHERE` |

Eight wrong answers were found by writing this axis. **All eight are closed** in the same pass
that measured them — rows 7, 10, 20 (with its second spelling, row 13), 22, 31, 32 and 42 — and
every original ❌ row is a wrong answer that was converted into a refusal rather than left to
lie. The widest of them, row 20, was found only because closing row 22 ran into it, and closing
*it* in turn exposed the keyless outer joins in row 13. Rows 32 and 42 were the last, and they
closed the way two of the ❌ rows did: by refusing rather than answering, because a common type
Arrow can always find is not a common type PostgreSQL has.

Row 42 is worth naming separately because it was recorded as unclosable and then closed. It was
written down as indistinguishable from a client's own `IN (subquery)` once planned — and the
claim was wrong, which measuring it rather than reasoning about it is what showed. At the point
the check runs a subquery is still an expression inside a `Filter` and has not become a join at
all, so the two shapes never overlap. The measurement also corrected what the defect *was*: not
a wrong answer but an `XX000` failure part-way through distributed execution, leaking a raw
DataFusion cast error to the client.

What is left ⛔ is row 23, the two corners of row 22's fix that cannot be entered without making
the result worse, and row 40, which is row 7's fix trading a common wrong answer for a rare one
it cannot avoid. Row 43 is the same kind of thing one step milder — row 32's fix recognizing an
untyped branch without retyping it, so the rows are right and the advertised column type is not.
**All three remaining non-✅ residues are residues of a closure rather than untouched defects**,
which is the shape this axis has settled into: each fix is narrower than the rule it implements,
and the difference is written down.

## Rules that apply to every row

- **A join is answered by DataFusion or not at all.** Nothing about a join is delegated to
  DuckDB, so no row can be correct on one shard and wrong on another for join reasons.
- **The shard map may change the plan; it must never change the answer.** Co-located and
  shuffle plans are measured separately for exactly this, and no row differs between them.
- **A join whose result is decided on its *build* side must not have its probe side split
  across tasks.** Rows 13 and 20: DataFusion's nested-loop join emits the semi, anti and
  outer-padding rows only after every probe partition has reported in, and it counts those
  reports in shared memory, which Ballista's one-process-per-partition task model does not
  provide — so nothing ever reported completion and the rows were never emitted. The
  scheduler now carries a physical rule that coalesces the probe side of exactly those join
  types, so the count is always one. Row 22's respelling still reduces to an anti join on a
  bare equality, which since this pass is a cost preference rather than the only shape that
  answers at all.
- **A plan that cannot be shipped is refused before it is planned**, not after it fails.
  Row 24 is the worked example: the mark-join plan failed *inside* Ballista's serializer and
  surfaced as `XX000` carrying a raw gRPC `Status { … }`; it is now `0A000` naming the
  rewrite that works.
- **A refusal names a working alternative, and does not silently substitute it.** Row 24's
  message points at `max()`/`min()` over the same subquery *and states that the two answer
  differently for an empty subquery and for one containing NULLs*. Row 27's points at the
  `DISTINCT` form. Choosing between them is the client's, because the choice changes the
  answer.
- **`USING (c)` names a merged column, not just a join predicate.** The result has one `c`
  and its value is `COALESCE(left.c, right.c)`. Rows 7, 40 and 41 are the three things that
  follow: the merge itself, the raw per-side values PostgreSQL still exposes by qualifier, and
  the one clause that cannot see the merged column. `NATURAL` is the same rule over every
  shared column.
- **A set operation's column *type* is resolved over every branch; its column *label* comes
  from the first.** Both are PostgreSQL's rules, they are different rules, and reading the
  type off the first branch was row 31's defect.
- **Type metadata is a contract too.** Rows 6, 8 and 31 assert at Describe — before a row is
  fetched — because that is what a client sizes its buffers from. A `USING` join is
  *narrower* than its inputs, so getting it wrong shifts every subsequent column index.

## Join surface — rows 1–16

| # | Form | Layout | Verdict | Notes |
|---:|---|---|:---:|---|
| 1 | `a JOIN b ON a.id = b.id` | co-located | ✅ | The baseline. Join key = shard key on both sides, so no exchange is planned. |
| 2 | `a JOIN b ON a.k = b.k` | shuffle | ✅ | Non-shard key, so both inputs are repartitioned first. Same answer as row 1's shape over the same data. |
| 3 | `LEFT [OUTER] JOIN` | both | ✅ | The padding rows are produced, in both layouts. Measured explicitly because a join that quietly degraded to an inner join would pass row 1 and fail only here. In the shuffle layout the NULL-keyed left row survives padded, which is the three-valued case: `NULL = NULL` does not match, but a left join keeps the row. |
| 4 | `RIGHT [OUTER] JOIN` | both | ✅ | The mirror of row 3, measured separately — the two are distinct operators, not one with its inputs swapped, once partitioning is involved. |
| 5 | `FULL [OUTER] JOIN … ON` | both | ✅ | The union of rows 3 and 4: five ids from a 4-row and a 3-row side overlapping on two. `COALESCE(l.id, r.id)` is the client's to write, and it answers correctly. |
| 6 | `JOIN`/`LEFT JOIN … USING (c)` | both | ✅ | The named column is **merged**: one `id` in the output, not two, so `SELECT *` over the fixture describes as `id, k, v, k, w` — `k` is common to both tables but not named in `USING`, so it is *not* merged. Asserted at Describe. |
| 7 | `FULL JOIN`/`RIGHT JOIN … USING (c)`, `NATURAL FULL JOIN` | both | ✅ | **Was silently wrong; closed in this pass.** PostgreSQL's merged column is `COALESCE(left.c, right.c)`, so a row only one side has reports *its own* key. DataFusion keeps both sides' `c` and fakes the merge by *picking* one of them: name resolution takes the field that comes first in the schema, wildcard expansion the one whose qualifier sorts first. For inner and left joins picking the left is right by construction (row 6); for a full join it answered `1, 2, 3, 4, NULL` where PostgreSQL answers `1, 2, 3, 4, 5`, and a right join was wrong the same way — a key indistinguishable from a genuine NULL. The coordinator now projects the coalesce over **both** key columns after planning, which is what makes `SELECT c` and `SELECT *` agree whatever the two tables are called. Rows 40 and 41 are what that leaves. |
| 8 | `NATURAL JOIN`, `NATURAL LEFT JOIN` | both | ✅ | Equates **every** common column, so over a fixture sharing `id` and `k` it returns only the row agreeing on both, and `SELECT *` describes as `id, k, v, w` — each common column once. Both the row set and the shape are asserted. |
| 9 | `CROSS JOIN`, `a, b`, `a, b WHERE …` | n/a | ✅ | The three spellings agree. The comma-plus-`WHERE` form — an inner join as older SQL writes it — is measured too, since it reaches the planner as a cross join plus a filter. |
| 10 | An aggregate over a join that projects **no** column | both | ✅ | **Closed this pass.** `SELECT COUNT(*) FROM a CROSS JOIN b` is the first query an analytical client writes and it used to fail: the aggregate needs no column from either side, so the per-shard scan was planned with an *empty projection*, and a scan that answered `SELECT *` for an empty projection returned three columns where the plan promised none — `XX000 … number of columns(3) must match number of fields(0) in schema`. A scan now answers an empty projection with a row count (`SELECT COUNT(*) FROM (SELECT 1 FROM t …)`) and builds a column-less batch carrying that count, and a width mismatch is now an `Internal` error naming both widths instead of a panic-shaped message. Every shape is pinned: `COUNT(*)` over a cross join, the comma spelling, a self cross join, three relations, a cross join with a one-sided `WHERE`, `COUNT(<column>)` (the same defect one field wider), and an equi self join under a count. |
| 11 | Self join, `a JOIN a ON …` | both | ✅ | The same table scanned twice in one plan, on the shard key and on a non-shard key. |
| 12 | Three-way join | both | ✅ | `a ⋈ b ⋈ c` on the shard key. Reassociation is the planner's, and the answer does not depend on which order it picks. |
| 13 | Non-equi `ON`, `a.k < b.k` — inner, and the `LEFT`/`FULL` spellings of it | shuffle | ✅ | No equality to partition on, so this is a nested-loop join. The **inner** form was correct from the start: counted rather than listed (7 pairs over the fixture) because the pairing is the contract, not the order, and the NULL key participates in nothing. The **outer** forms were not, and closing row 20 is what exposed them — `l LEFT JOIN r ON r.k > l.k` returned the 7 matched pairs and dropped the unmatched left row, and `FULL JOIN` dropped the same one, so a keyless outer join was silently an inner join in outer-join spelling. One root cause with row 20, one fix; see that row for the mechanism. Now measured explicitly: the left join returns 8 pairs ending `(4, NULL)`, the full join 8, and a full join that pads on *both* sides returns its right-only rows too. `RIGHT` and `CROSS` over the identical shape are pinned alongside as controls — they are decided as the probe streams, so they were never affected, and a fix that broke them would be invisible otherwise. |
| 14 | Disjunctive `ON`, `… OR …` | shuffle | ✅ | Cannot be a hash join on either half alone. Both halves contribute, and the row matching both is returned once. |
| 15 | Join key of different types | both | ✅ | `int4` against `int8`, and a `text` key. Both join. Widening the narrower side is the planner's, and it does not lose the match. |
| 16 | `ON` vs `WHERE` on an outer join | both | ✅ | The distinction that decides whether an outer join *stays* outer. An extra condition in `ON` filters the match, so all four left rows survive and one is matched; the same condition in `WHERE` filters the result, which is an inner join. Both spellings measured, because collapsing the first into the second is a classic optimizer bug and it is silent. |

## Semi, anti and correlated subqueries — rows 17–24

These are the spellings that ask *is there a match?* rather than *join me to it*.
PostgreSQL has five, and they decorrelate to two joins — a semi and an anti — so what
matters is that all five agree.

| # | Form | Verdict | Notes |
|---:|---|:---:|---|
| 17 | `x IN (subquery)`, `x = ANY (subquery)` | ✅ | The semi join. `= ANY (subquery)` is normalized to `IN` on the verbatim AST (`compat_rewrite::normalize_any_all_subqueries`) *before* the compatibility rule that owns `ANY` over an **array** can see it — otherwise the subquery would have been handed to `array_contains` and answered wrongly. `IN` needs no NULL care of its own: a NULL candidate can only turn a false into a NULL, and neither is returned. |
| 18 | `EXISTS (subquery correlated by **equality**)` | ✅ | The same semi join reached from the other spelling. The equality correlation used to be the *only* one the cluster answered, which is what row 20 was; it is now simply the one that plans as a hash join. |
| 19 | `NOT EXISTS (subquery correlated by **equality**)` | ✅ | The anti join, measured separately from row 18. Unlike row 22 this one is *not* null-sensitive in PostgreSQL — an unmatched row is returned whatever its key — so it agrees under either join operator. |
| 20 | `EXISTS`/`NOT EXISTS` over an **uncorrelated** subquery, or one correlated by anything other than equality | ✅ | **Closed this pass, and it was the widest defect the axis found.** A semi or anti join with no equijoin key is planned as a `NestedLoopJoinExec`, and the plan was built, shipped and run — and returned **no rows**. So every such `EXISTS` evaluated to false and every such `NOT EXISTS` did too, which is not even a consistent lie: one of the two is always wrong, and for the uncorrelated case both are. Measured before the fix, over the standard fixture: `EXISTS (SELECT 1 FROM r)` → **none** where PostgreSQL answers `1,2,3,4`; `EXISTS (… WHERE k = 20)` → **none**, PostgreSQL `1,2,3,4`; `EXISTS (… WHERE r.k > l.k)` → **none**, PostgreSQL `1,2,3`; `NOT EXISTS (… WHERE r.k > l.k)` → **none**, PostgreSQL `4`; `NOT EXISTS (… WHERE k = 777)` → **none**, PostgreSQL `1,2,3,4`. **The cause is a counter that cannot count across processes.** `NestedLoopJoinExec` collects its left input and streams its right, and for the join types whose output is decided on the *build* side — DataFusion's own `need_produce_result_in_final` set: `Left`, `Full`, `LeftSemi`, `LeftAnti`, `LeftMark` — the result comes from a match bitmap over the collected side that may only be emitted once **every** probe partition has finished. DataFusion coordinates that with one shared `probe_threads_counter` seeded with the probe's partition count, and emits when a decrement reaches zero. Ballista runs each partition of a stage as a **separate task in a separate process**, so every task seeded its own counter with 3 and decremented it once; none ever reached zero, and the emission happened nowhere. For semi and anti that emission *is* the whole result, hence zero rows; for `Left`/`Full` the matched rows still streamed and only the unmatched ones vanished, which is row 13. `Inner`, `Right`, `RightSemi`, `RightAnti`, `RightMark` and `CrossJoinExec` are decided as the probe streams, need no final pass, and were never affected — measured as controls, not assumed. Nothing can make that counter work across processes without changing Ballista, so the repair removes the need for it: `scheduler::nested_loop_join_one_task` is a physical optimizer rule appended after the built-in ones that coalesces the probe side of exactly those five join types to a single partition. The cost is the probe's parallelism, paid only by the shapes that were broken — any join carrying an equijoin key plans as a hash or sort-merge join, is partitioned by key, shares no counter, and is left untouched. **Invisible without a cluster**: the identical plan answers correctly in one process, which is why five axes of single-process measurement never saw it, and the rule's unit tests therefore build the operator by hand rather than through SQL. Two spellings escaped even before the fix, and neither by being handled — `NOT EXISTS (… WHERE 1=0)`, folded away by the simplifier before a join is planned, and `NOT EXISTS (SELECT 1 FROM r)`, right by accident, false being the answer; both are pinned so the fix cannot regress them into a different kind of right. |
| 21 | Correlated scalar subquery; uncorrelated aggregate subquery | ✅ | The same question asked as a *value* rather than a predicate (`(SELECT COUNT(*) FROM r WHERE r.id = l.id)` per row), and the uncorrelated form the planner may evaluate once (`x = (SELECT MAX(…) …)`). |
| 22 | `x NOT IN (subquery)`, `x <> ALL (subquery)`, in a `WHERE`/`HAVING`/`QUALIFY`/`ON` clause over an uncorrelated subquery, with a non-aggregate `x` | ✅ | **Closed this pass**, and the interesting part is what did *not* close it. `NOT IN (subquery)` decorrelates to an anti join, and only DataFusion's `HashJoinExec` carries the `null_aware` flag that reproduces PostgreSQL's rule; `SortMergeJoinExec` reads a NULL as merely unequal. Ballista turns `prefer_hash_join` **off** (`ballista-core/src/extension.rs`) for a resource reason — its hash join cannot spill — so the cluster planned the sort-merge one and answered `1, 3, 4` where PostgreSQL answers `1, 3`, and one row where PostgreSQL returns none. Turning the option on looked like the fix and is **unshippable**, which was measured, not reasoned: a null-aware anti join is only correct as a broadcast (one NULL on the right suppresses *every* left row), so `JoinSelection` stamps it `CollectLeft`; Ballista's planner then refuses to broadcast a join driven by its build side, demotes it to a shuffle and swaps the sides on the way; and `HashJoinExec` will not build a null-aware `RightAnti`. The job died inside the scheduler with `null_aware can only be true for LeftAnti joins, got RightAnti` — **and the client hung**, because the failed stage reported no status. So the predicate is respelled in the AST instead, before planning, by `compat_rewrite::rewrite_not_in_subqueries` — and the shape of that respelling was dictated by **row 20**, which is what makes this row worth reading twice. The obvious rewrite, one anti join whose filter carries the three ways PostgreSQL declines to say true (`NOT EXISTS (SELECT 1 FROM (q) AS v (key) WHERE (x) IS NULL OR v.key IS NULL OR v.key = (x))`), was correct in one process and returned **no rows at all** on a cluster: a disjunctive filter gives the planner no equijoin key, and at the time a keyless join answered nothing. Only a bare equality survived. So the two NULL questions are asked as *uncorrelated aggregates* — constant for the statement, hence evaluated once — and only the third is a subquery predicate:<br><br>`(SELECT count(*) FROM (q) AS v_all (key)) = 0` `OR (` `(x) IS NOT NULL` `AND (SELECT count(*) FROM (q) AS v_null (key) WHERE key IS NULL) = 0` `AND NOT EXISTS (SELECT 1 FROM (q) AS v (key) WHERE v.key = (x)) )`<br><br>The leading disjunct is the empty-`q` case, which PostgreSQL answers true whatever `x` is, including NULL. `q` is named three times, but two of those are scalar aggregates the planner hoists out of the row loop, so the per-row cost is still one anti join — and it is an anti join on an equality, which was then the only kind the cluster answered and is now the cheaper of the two. The rewrite is kept as it stands: row 20's fix would make the disjunctive form answer correctly, but it would answer as a coalesced nested-loop join, where this one is a partitioned hash join. Measured on the cluster in all three NULL cases, with an empty `q`, in a conjunction, under `COUNT(*)`, with a non-column left side, in each of `WHERE`/`HAVING`/`QUALIFY`/`ON`, and via the `<> ALL` spelling. |
| 23 | `x NOT IN (subquery)` where `x` is an **aggregate** of the group, or where the subquery is **correlated** | ⛔ | **Silently wrong** — the two shapes row 22's rewrite declines to enter, each for a reason that makes entering it worse, and each left with the sort-merge anti join's two-valued answer. **(a)** In `HAVING`/`QUALIFY` the left side may be an aggregate, and no respelling of the rewrite plans there: DataFusion cannot resolve a correlated subquery whose outer reference is an aggregate of the group, and every attempt failed with `Aggregate functions are not allowed in the WHERE clause`, which would turn a wrong answer into a broken query. So the rewrite is skipped when the left side holds **any function call** — a guard on the shape rather than on a list of aggregate names, which would rot as functions are added — and `HAVING MAX(k) NOT IN (SELECT k FROM r)` measured `10, 30, NULL` where PostgreSQL answers `10, 30`: the group whose `MAX(k)` is NULL is kept, when `NULL NOT IN (…)` is never true. **(b)** A derived table may not see the outer row without `LATERAL`, so a **correlated** `q` cannot be wrapped: `WHERE k NOT IN (SELECT k FROM r WHERE r.id > l.id)` measured `1, 2, 3, 4` where PostgreSQL answers `1, 2, 3`. Closing (b) needs `LATERAL` in the derived table, or a decorrelation that keeps the outer reference. It used to need one thing more — the wrapped predicate has no bare equality, so it landed on **row 20** and would have answered no rows at all — but row 20 is closed this pass, so `LATERAL` is now the only obstacle left in front of it. Two things are explicitly *not* in this row. The rewrite fires only in truth-valued positions — `NOT EXISTS` is two-valued, so substituting it is invisible only where NULL and false are indistinguishable — but the negated case is correct anyway: `WHERE NOT (k NOT IN (SELECT k FROM r))` measured `2`, PostgreSQL's answer, because the simplifier pushes the negation into the `InSubquery` and plans a **semi** join, which needs no null-awareness. And a select list is not in this row either: DataFusion plans neither spelling there, and `Physical plan does not support logical expression InSubquery` is loud. |
| 24 | `x <op> ANY \| SOME \| ALL (subquery)` for any `<op>` other than row 17's `=` and row 22's `<>` | ❌ | Every other spelling plans to a **mark** join, whose output column is named `mark` on both sides of the join above it. Serializing that plan for an executor fails — `Schema contains duplicate unqualified field name mark` — so the query used to reach the client as `XX000` carrying a raw gRPC `Status { … }`. Now `0A000`, naming the aggregate rewrite (`max()`/`min()` over the same subquery for an ordering operator, or `EXISTS`) **and stating that the aggregate form answers differently for an empty subquery and for one containing NULLs**, which is why it is named and not applied: substituting it would answer a different question quietly. The two spellings that *are* supported are named in the message. |

## Set operations — rows 25–35

| # | Form | Verdict | Notes |
|---:|---|:---:|---|
| 25 | `UNION`, `UNION DISTINCT`, `UNION ALL` | ✅ | `ALL` concatenates, the other two deduplicate; all three spellings measured, since `UNION DISTINCT` is the one a parser is most likely to mis-map. |
| 26 | `INTERSECT`, `EXCEPT` | ✅ | The set forms. `MINUS` is `EXCEPT` under another name and is treated as such throughout. |
| 27 | `INTERSECT ALL`, `EXCEPT ALL` | ❌ | These count **multiplicity**: `INTERSECT ALL` keeps a row as many times as it appears on both sides, and `EXCEPT ALL` removes one left row per matching right row. Both were answered as the plain semi/anti join the `DISTINCT` forms are built from — measured with left `1,1,1,2` and right `1,1,3`: `INTERSECT ALL` answered `1,1,1` where PostgreSQL answers `1,1`, and `EXCEPT ALL` answered `2` where PostgreSQL answers `1,2`. A row *count* is exactly what an analytical client goes on to aggregate, so both are now refused `0A000` naming the `DISTINCT` form, rather than answered. `UNION ALL` is untouched — concatenating is all its `ALL` asks for. |
| 28 | NULL identity across a set operation | ✅ | The second of the three rules: a set operation compares whole rows and treats two NULLs as **equal**. So the NULL-keyed row is one value among the others — `EXCEPT` keeps it when the right side has no NULL, `INTERSECT` returns it when both sides do and drops it when only one does, and `UNION` folds two of them into one row. All four measured, and all four agree with PostgreSQL. |
| 29 | `ORDER BY` over a set operation | ✅ | By **ordinal** (the only portable way to name a set operation's column) and by the **first branch's** output label, which is where a set operation's labels come from. |
| 30 | `LIMIT` over a set operation; a parenthesized per-branch `LIMIT` | ✅ | Two different scopes, and getting one wrong is not an error but a different answer, so each is pinned: a `LIMIT` after the operation applies to the result, while `(SELECT … LIMIT 1) UNION ALL (SELECT … LIMIT 1)` applies to each branch and returns two rows. Left-to-right chaining is measured too — a `UNION` after a `UNION ALL` deduplicates what the `ALL` produced. |
| 31 | Branch type resolution | ✅ | **Closed this pass.** The advertised type followed the *first* branch, which is not a wrong type in the abstract but a wrong **value**: `int4 ∪ int8` described as `int4` and raised `22003` on `5000000000`, and `int4 ∪ float8` described as `int4` and truncated `2.5` to `2` — silently. `plan_select` now coerces over every branch, so both resolve to `int8`/`float8` **in either branch order**, and the wide value and the fraction both survive. Asserted at Describe as well as by value, since a client binds its buffer from the former. |
| 32 | `UNION` branches whose types are **incompatible** | ✅ | **Was silently wrong; closed in this pass.** `int ∪ text` used to render the integers as text and **return rows PostgreSQL never would** — measured `1, 2, 3, 4, x, y, z` in either branch order, with an `ORDER BY` over them sorting `10` before `9`. The two engines resolve the branch type by different rules and DataFusion's is *wider*: it asks for a common type and Arrow always has one, because everything casts to a string, while PostgreSQL asks for one reachable by **implicit** coercion and between two type categories there is none. So the mismatch is now refused `42804` before coercion (`pgwire_handler/pg_set_op_types.rs`), naming both types and the column, in either order and for `ALL` as well as `DISTINCT`, and inside a derived table or CTE too. The subtlety is PostgreSQL's `UNKNOWN`, which had to be modelled or the refusal would have caught queries PostgreSQL answers: a bare string literal or `NULL` in a branch's select list has no type yet and takes the other branches', so `… UNION ALL SELECT '9'` is an integer union — while `… UNION ALL VALUES ('9')` is *not*, because a `VALUES` clause resolves its own columns to text first. All three spellings byte-diffed against the PostgreSQL 16.15 oracle. Row 43 is what this leaves — the untyped branch is recognized, but not retyped. |
| 33 | Branches of different **width** | ✅ | Refused `42601` — by the parser, before planning, as in PostgreSQL. |
| 34 | A set operation nested in a derived table, a CTE, or one side of a join | ✅ | Each puts a stage *above* the operation, which is where a plan that only works at the top level breaks. All three measured, plus branches that are themselves aggregates, so each side is a two-stage plan. |
| 35 | A `VALUES` branch; a bare-`NULL` branch | ✅ | A `VALUES` branch reaches no shard at all, and a bare `NULL` has to be typed from the other branch. Both are the degenerate inputs a set operation's type resolution is most likely to trip on. |

## Composition — rows 36–39

| # | Form | Verdict | Notes |
|---:|---|:---:|---|
| 36 | `LATERAL`, `LEFT JOIN LATERAL` | ✅ | A subquery on the right of a join that references the left row — a *correlated* join, so it cannot be planned as one exchange. Both spellings answer identically here, because the aggregate inside always returns a row and there is nothing to pad. |
| 37 | A join feeding `GROUP BY`, `HAVING`, `ORDER BY`, `LIMIT` | ✅ | Each adds a stage above the join, and the join's output partitioning has to satisfy what that stage needs. |
| 38 | A join over a derived table or a CTE; a shard-key predicate on one side | ✅ | The derived-table and CTE forms are where a view would land too. The one-sided shard-key predicate is the case where the planner may prune shards on that side while still reading the other in full. |
| 39 | A join on a **pseudonymized** column | 🟡 | A column in `anonymized_columns` stores the HMAC-SHA256 digest of its plaintext, not the plaintext. **Equality survives that** — equal values have equal digests — so an equi-join on such a column relates two tables by it correctly, including under a `GROUP BY`/`SUM` above the join. Every read that would report a property the digest does *not* preserve is refused `0A000` rather than answered: an ordering join (`u.email < g.email`), an `ORDER BY` on the column, and a comparison against **plaintext**, which would match nothing and say so nowhere. That is the restriction, and it is enforced, which is what makes this 🟡 rather than ⛔. See [`gap-analysis.md` § 2.6](gap-analysis.md). |

## What the closures leave — rows 40–43

Rows 40 and 41 were opened by closing row 7, rows 42 and 43 by closing row 32. Each is recorded
as its own row rather than folded into the one it came from, because a client meets them as
different things — a wrong answer, a refusal, and a wrong column type — and because a residue
that is written down is one somebody can close later. Row 42 is the demonstration of that: it
was recorded here as unclosable, and closing it was a matter of measuring the claim.

| # | Form | Verdict | Notes |
|---:|---|:---:|---|
| 40 | `SELECT l.c, r.c FROM l FULL JOIN r USING (c)` | ⛔ | **Silently wrong, and unavoidable in this plan shape.** PostgreSQL keeps the raw per-side values reachable by qualifier — `4` beside a NULL, and a NULL beside `5` — while merging only the unqualified `c`. Its join output therefore has *three* addressable names (`c`, `l.c`, `r.c`) where a DataFusion schema has two fields, so the merged value has to live in whichever fields every other consumer reads, and that is both of them. VaireDB answers the merged value under either qualifier. Rare and expert — it is the query that asks *which side matched* — and the `ON` spelling answers it exactly, which is what the fix for row 7 deliberately leaves untouched. |
| 41 | `… USING (c) WHERE c > 2` | ❌ | Refused `42703` (observed; PostgreSQL's own code for an ambiguous column is `42702`, and PostgreSQL does not refuse this at all) as `Ambiguous reference to unqualified field c`: DataFusion plans a `WHERE` predicate without the join's `USING` set in hand, so two fields named `c` are ambiguous to it where PostgreSQL sees one merged column. Not a cost of row 7's fix — it refuses on **every** `USING` and `NATURAL` join, inner included — and not silent. `GROUP BY`, `HAVING`, `ORDER BY`, an aggregate argument and the select list all resolve the merged column correctly; `WHERE` is the only clause that does not, and the qualified spelling `WHERE l.c > 2` works and carries the merged value. |
| 42 | `SELECT id FROM l INTERSECT SELECT w FROM r`, and the `EXCEPT` spelling | ✅ | **Was a mid-execution failure; closed in this pass, after being recorded here as unclosable.** PostgreSQL refuses both with the same `42804` as row 32 and the operator's own name in the message. VaireDB did not answer these wrongly — it failed *after distributing the query*, with `XX000` and a raw DataFusion debug string (`CastError("Cannot cast string 'x' to value of Int32 type")`), which is a worse outcome than either a refusal or a wrong answer: work is done, the client learns nothing usable, and the message leaks internals. This row previously claimed the fix was impossible, because neither operator survives planning as a set operation — DataFusion lowers `INTERSECT` to a `LeftSemi` join and `EXCEPT` to a `LeftAnti` join — and that shape was assumed identical to a client's own `IN (subquery)`, which PostgreSQL refuses with a *different* code (`42883`). Measuring the two plans disproved it: at the point the check runs, `IN`/`NOT IN`/`EXISTS` are still expressions inside a `Filter` and have not become joins at all, and an explicit `LEFT SEMI JOIN` (not PostgreSQL syntax) carries its equality in the join's filter rather than its keys, where a lowered set operation carries it in the keys. So the check now covers all three operators under one rule, naming the client's own operator, and the `UNKNOWN` handling reaches them too — `… INTERSECT SELECT '2'` is an integer intersection and answers `2`. |
| 43 | `SELECT id FROM l UNION ALL SELECT '9'` — the *type* of the untyped branch's result | 🟡 | **The boundary of row 32's fix, and mild.** PostgreSQL's `UNKNOWN` takes the other branch's type, so this is an integer union and `pg_typeof` reports `integer` — measured against the 16.15 oracle for the bare literal and the bare `NULL`. VaireDB returns the correct rows (`1, 2, 3, 4, 9`) but advertises the column as `text`: Arrow has no `UNKNOWN`, the planner has already made the literal `Utf8`, and coercion then picks the common type of `Int32 ∪ Utf8`. 🟡 rather than ⛔ because no value is wrong — what differs is the OID a driver binds its buffer from, and an `ORDER BY` over the result therefore sorts lexicographically. Closing it means rewriting the untyped branch's projection to cast to the resolved type before coercion runs; the refusal in row 32 only has to *recognize* an untyped branch, which is strictly less. |

## Open gaps, ranked

By what a client loses, and what closing it costs.

1. **Row 23 — the two shapes `NOT IN` is still wrong in.** Both are blocked on something
   other than the rewrite. The aggregate left side needs DataFusion to plan a correlated
   subquery whose outer reference is an aggregate of the group, which it does not — every
   respelling was measured and every one failed to plan — so the alternative is not a better
   rewrite but pushing the whole comparison below the aggregation. The correlated `q` needs
   `LATERAL` in a derived table, or a decorrelation that keeps the outer reference; that is
   now the *only* thing in front of it, since the keyless-join defect it would also have
   landed on (row 20) is closed. Narrow — `NOT EXISTS` (row 19) expresses both correctly and
   is available today — but silent.
2. **Row 41 — an unqualified `USING` key in `WHERE`.** Loud rather than wrong, and the only
   clause the merged column does not reach. Closing it means qualifying the reference before
   the planner sees it, which is now *safe* to do: since row 7's fix, `l.c` carries the merged
   value, so rewriting `WHERE c` to `WHERE l.c` over a `USING` join answers PostgreSQL's
   question. What it costs is scope tracking in the AST — knowing which `c` in which clause
   belongs to which join — and doing it partially would be worse than not doing it at all,
   because a missed reference turns a refusal into a wrong answer.
3. **Row 27 — `INTERSECT ALL` / `EXCEPT ALL`.** Refused today rather than wrong, which is
   the right interim state. Closing it means implementing multiplicity-counting set
   operations, which DataFusion does not provide: the `DISTINCT` forms are built on a semi
   and an anti join, and the `ALL` forms need the *count* of matches per distinct row.
4. **Row 24 — `<op> ANY/ALL (subquery)`.** Refused today. Closing it means either making the
   mark-join plan serializable — the duplicate unqualified `mark` field is a DataFusion
   naming issue, not a semantic one — or rewriting the predicate into a form that preserves
   its empty-subquery and NULL behaviour, which the aggregate form does not.
5. **Row 43 — an untyped literal branch resolves to `text`.** Every value is correct; what a
   client sees wrong is the column's advertised type, and the visible consequence is that an
   `ORDER BY` over the result sorts lexicographically. Closing it means casting the untyped
   branch's projection to the resolved type before coercion runs, at the same seam row 32's
   refusal already identifies the branch from — so it is cheap, and it is ranked here rather
   than higher only because nothing is silently wrong.
6. **Row 40 — a qualified `USING` key reports the merged value.** Last because it is the one
   row here with no known fix short of changing DataFusion: PostgreSQL's join output has one
   more addressable name than a `DFSchema` has fields to hold. A client that needs the raw
   sides has the `ON` spelling, which answers exactly and is the query this shape is really
   asking for.

**Closed by writing this axis:** row 7 (`USING` on a full or right join, which reported a key
indistinguishable from a NULL), row 10 (`COUNT(*)` over a join, which failed outright),
row 22 (`NOT IN` NULL-awareness, which was silently wrong, and whose residue is row 23),
row 31 (set-operation branch types, which were silently wrong in two different ways),
row 32 (incompatible `UNION` branches, which answered rows PostgreSQL refuses, and whose
residue is row 43), row 42 (the same mismatch under `INTERSECT` and `EXCEPT`, which failed
mid-execution with a leaked internal error, and which this document had recorded as unclosable
until the claim was measured),
rows 24 and 27, which were converted from a leaked internal error and two wrong row counts
into refusals that name a working alternative, and rows 20 and 13 — one defect, five join
types, `EXISTS`/`NOT EXISTS` and every keyless outer join between them.

**Opened by writing it:** row 20, which no row asked about directly. It was found because the
first fix for row 22 returned nothing on the cluster while passing every unit test; running
that discrepancy down turned a one-query puzzle into the axis's largest defect, and closing
*that* exposed row 13's outer joins, which no row had asked about either. Two rows this axis
now records were reached only by pulling on a thread it found by accident, which is the
argument for the axis existing. Rows 40 and 41 were opened the same way, by closing row 7:
one is what its fix cannot represent, the other a refusal the fix made visible by making
every other clause work. Row 42 was opened by closing row 32, and is the plainest of the
three — the rule is right and reaches one of the three set operations, because only one of
them is still a set operation by the time the plan exists.

**Not a gap:** nothing on this axis is 🚫. Every remaining row has a target behaviour, which
distinguishes it from its five siblings — there is no distributed reason to decline any join
form, only cost.

## Tests

| File (`tests/e2e/tests/`) | Rows |
|---|---|
| `sql_join_gaps.rs` | 1–43. Grouped so a distributed `CREATE TABLE` is amortized across the assertions that share a fixture, rather than one table per row. |
| `sql_command_select.rs` | Predates this axis: the read path's operator surface and push-down, including the three join tests and four set-operation tests this axis grew out of. |

The fixture is deliberately small enough that every expected answer is derivable by hand
rather than looked up:

```text
  l: (1, 10, 'a')  (2, 20, 'b')  (3, 30, 'c')  (4, NULL, 'd')
  r: (2, 20, 'x')  (3, 99, 'y')  (5, 50, 'z')
```

The `id` sets overlap on `2, 3`, so an `id` join returns **two** rows; the `k` sets overlap
only on `20`, so a `k` join returns **one**; and `l` holds a row whose key is NULL, which is
what every three-valued row turns on. Rows 27 and 31 add the two fixtures that cannot be
expressed in that shape — a duplicate-bearing pair (`1,1,1,2` against `1,1,3`) and a
`bigint`/`double` table.

As on the other axes, each open row has up to two tests: a **passing** one pinning today's
behaviour, so a regression is visible, and an `#[ignore = "gap (row N): …"]` one asserting
PostgreSQL's answer, which is the definition of done.

```sh
cd tests/e2e && cargo test --test sql_join_gaps -- --ignored --test-threads=1
```

`make e2e` runs only the passing set, so this gap map never blocks CI.
