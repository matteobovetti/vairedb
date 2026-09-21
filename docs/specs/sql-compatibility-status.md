# VaireDB SQL Compatibility Status

The per-axis status of PostgreSQL compatibility: one line per axis, what each count means, and
what has to be true for a count to move.

This document is the **summary**, not the record. [`gap-analysis.md`](gap-analysis.md) is the
single gap record and the source of truth for every number below; this file is its *Totals*
section pulled out and given one section per axis, so the shape of the surface can be read
without reading the census. A count here changes only when a row changes there.

## What the statuses mean

| Status | Meaning |
|---|---|
| 🟡 | **Partial** — right value under a wrong advertised type, one path only, degraded semantics, or an honest error in the wrong SQLSTATE. |
| ⛔ | **Silently wrong** — parses, executes, returns a plausible value that is not PostgreSQL's. The dangerous class. |
| ❌ | **Rejected loudly** with a SQLSTATE, and no target behaviour agreed yet. An open gap. |
| 🚫 | **Not planned**, with a written rationale — no meaning in a shared-nothing cluster, or DuckDB-only. A decision, not backlog. |

**PostgreSQL is the contract.** DataFusion (read path, over Ballista) and DuckDB (write path,
one instance per shard) are implementation layers; a form only they have is out of scope.

## Totals

Counted as rows of the gap tables; a row can group several spellings (`+ - *` is one row,
`regr_*` is one row), so this compares axes rather than individual constructs.

| Axis | 🟡 | ⛔ | ❌ | 🚫 |
|---|---:|---:|---:|---:|
| [Statements](gap-analysis.md#21-statements) | 15 | — | 9 | 8 |
| [Data types](gap-analysis.md#22-data-types) | 6 | — | — | 8 |
| [Aggregate functions](gap-analysis.md#23-aggregate-functions) | 2 | — | 3 | — |
| [Window functions](gap-analysis.md#24-window-functions) | 1 | — | 5 | 1 |
| [Operators, literals, casts](gap-analysis.md#25-operators-literals-and-casts) | 12 | — | 19 | — |
| [Joins and set operations](gap-analysis.md#26-joins-and-set-operations) | 2 | — | 4 | — |
| **Total** | **38** | **—** | **40** | **17** |

**The ⛔ column is empty.** All eight historically silently-wrong rows are closed: nothing on
these six axes returns a plausible answer that is not PostgreSQL's. That is the single most
load-bearing fact in this table, and it is also the one that must be re-earned every time an
axis is extended — see [the two caveats](#two-caveats-that-qualify-every-count-above).

## Status by axis

### Statements — 🟡 15, ❌ 9, 🚫 8

Every statement a PostgreSQL client sends is either routed and correct, or refused **by name**
with a SQLSTATE. The 🟡 rows are not partial implementations of a statement: the statement
works, and a *decoration* of it that sharding cannot honour is refused rather than
under-applied — dropping the shard key, `CASCADE`, a `UNIQUE` off the shard key, `MERGE` from a
non-co-located source, DDL inside a transaction block. The 9 ❌ rows are single-node DuckDB
utilities and catalog features with no cluster meaning (`ATTACH`, `CHECKPOINT`, `INSTALL`,
`USE`, `CALL`, `COMMENT ON`, `CREATE MACRO`, whole-database dump/load, DuckDB secrets).

This axis gained a ❌ and lost it again in the same revision: a filtered read of a
schema-qualified relation failed `42703`, and now answers.

### Data types — 🟡 6, ❌ 0, 🚫 8

**No ❌ rows left: no value a shard can store is refused on the wire.** Every value on this axis
round-trips faithfully, so what the six 🟡 rows are about is the *advertised* type, not the
value — `UInt64` as `numeric`, `STRUCT` and `ENUM` as `text`, an enum sorted by decoded string
rather than declaration order, a `VARCHAR(n)` typmod that is not on the wire. Four of the six
are blocked in pgwire, sqlparser or `datafusion-pg-catalog` rather than here.

The exception is the last row, and it is the one divergence anywhere in the census that is
neither fixed nor settled: a **`VARCHAR(n)` / `CHAR(n)` length is reported but not enforced**.
The 🚫 8 are types refused by name at DDL, each naming what to write instead.

### Aggregate functions — 🟡 2, ❌ 3, 🚫 0

Structurally the strongest axis, for one reason: **DuckDB never computes an aggregate.** Every
aggregate is evaluated by DataFusion and the distributed merge is correct by construction — the
remote scan declares no partitioning and no equivalences, which forces a shuffle and guarantees
no partial aggregate ever emits a finished value. The ordered-set and hypothetical-set family,
both percentiles, `json_agg`, and the exact-numeric result types are all closed.

What is left: a fixed decimal scale where PostgreSQL picks one per value, `json_agg` of a bare
`JSONB` column (the `::json` cast is the spelling that embeds), the multi-column
hypothetical-set form (blocked in datafusion-sql), a repeated empty `GROUPING SETS`, and
`xmlagg` (no XML type).

### Window functions — 🟡 1, ❌ 5, 🚫 1

All 11 PostgreSQL window functions are present and the frame engine is correct: all five units,
peer groups, and integer, float and `INTERVAL` offsets. Named windows, chained inheritance, a
windowed `FILTER`, and a window function in the outer `ORDER BY` are all closed.

Three of the five ❌ rows are **anonymization** refusals rather than missing features — a
pseudonymized column has no plaintext order on the server, so a ranking or offset function
ordered by one is refused instead of answered wrongly. Of the remaining two, one is upstream
(`FILTER` over a *collecting* aggregate, because Ballista's window proto has no filter field)
and one needs parser work (`EXCLUDE`, which sqlparser cannot parse) — the only genuinely
missing feature on this axis. `PARTITION BY` over a pseudonymized column is sound and stays
allowed.

### Operators, literals and casts — 🟡 12, ❌ 19, 🚫 0

The widest axis, and the one where the counts are least comparable to the others: a row here is
one operator or one literal form, so 31 rows is a smaller share of the surface than it looks
beside 24 statement rows. The read path's expression surface and type layer are closed,
including live predicate and `LIMIT` push-down to the shards, with **one push-down narrowing
left** where there were four.

The ❌ rows split into three causes, none of which is "not implemented yet" for a form
PostgreSQL has: a spelling **no sqlparser dialect parses** (`**`, `//`, `CAST(x AS T ARRAY)`,
brace struct literals), a form only **DuckDB** has (`GLOB`, string subscripting), and a form
whose PostgreSQL meaning cannot be reproduced exactly here, so it is refused with the spelling
that answers named in the message (`@?` jsonpath, `LIKE ANY`, a computed `SIMILAR TO` pattern,
a `VARCHAR(n)` cast whose length nothing enforces, correlated subqueries outside the shapes
DataFusion decorrelates).

### Joins and set operations — 🟡 2, ❌ 4, 🚫 0

Every row on this axis was measured in **both** layouts the shard map produces, co-located and
shuffle, and no row differs between them: the shard map may change the plan, never the answer.

The two 🟡 rows are a join on a pseudonymized column (equality survives HMAC, order and
plaintext comparison are refused) and an untyped literal branch of a set operation. The four ❌
rows are all shape limits rather than feature gaps: `ANY`/`SOME`/`ALL` outside a predicate `AND`
chain, a correlated `NOT IN`, and the two `USING`/`NATURAL` shapes that need three addressable
names where a `DFSchema` holds two fields.

## Two caveats that qualify every count above

1. **Six axes is not every axis.** PostgreSQL's **scalar functions** — string, math, pattern,
   conditional, array, datetime — are not among the six and have never been measured against
   the oracle. The census's silence over them is currently being read as parity, which it has
   not earned: a function neither engine has answers `42883` and is loud, but a function both
   engines have under PostgreSQL's name and evaluate by a different rule would be a ⛔. Until
   that pass has run, the totals above are a census of six axes and should be read as one.
   Enumerating that catalogue and running it against the oracle the way the six axes were run is
   a measurement pass rather than a fix, and it has not run.
2. **One divergence is neither fixed nor settled.** The `VARCHAR(n)` / `CHAR(n)` length is
   advertised and not applied: a value longer than the declared length is stored and returned,
   where PostgreSQL raises `22001`. It is not upstream, not a correction and not a deliberate
   divergence — the check simply does not exist, and it got *worse* as the OID started
   advertising `varchar`, because the `n` is now the one part of the declaration a client
   cannot rely on.
   It is the last row of [§ 2.2](gap-analysis.md#22-data-types), pinned by an `#[ignore]`d test
   in `data_types_round_trips.rs`.

## How a count moves

A count is not a score, and the ways it moves are not symmetrical.

- A fix removes a census row — and any *residue* it leaves **enters** the census as a 🟡 or ❌ of
  its own. A residue a query can see belongs in the count exactly as much as the divergence it
  came from, so a repair can leave the total flat and still be a repair. Counts are therefore not
  comparable across revisions.
- **Settled is not fixed.** Some rows have nothing left to do on them and still do not agree with
  PostgreSQL: the item is blocked upstream (DuckDB, sqlparser, pgwire, `datafusion-pg-catalog`),
  or the row's own premise was wrong, or the divergence is held back deliberately because the
  mechanism that would repair it cannot. Such a row keeps its 🟡 rather than being deleted or
  rounded up to agreement — a silent closure is the one thing the census will not do.
- An empty ⛔ column is a property of **the axes that were measured**, and has to be re-earned
  whenever an axis is extended — see the two caveats above.
- Some rows must **not** move. [Supersets](gap-analysis.md#4--supersets-that-must-not-be-fixed)
  VaireDB accepts and PostgreSQL rejects — `QUALIFY`, `count(DISTINCT x) OVER (…)`, negative
  `nth_value` offsets — are not gaps to narrow.
- Ranking and scheduling live in [`intenal-roadmap.md`](intenal-roadmap.md), not here. This document says what
  diverges; it does not say what is done about it or in what order.

## Standing limitations, on every axis at once

Properties of the design rather than backlog, and the cause of most 🟡 statement rows: no
cross-shard atomicity (no 2PC), uniqueness only over the shard key, an immutable shard key, a
transaction block that buffers in the coordinator, no role model and therefore no privilege
checks, one-way write-path-only anonymization, and byte-order text comparison. Each is argued in
[§ 3.4](gap-analysis.md#34-standing-limitations-a-client-must-expect).

## How the numbers were measured

A live 5-node e2e cluster (1 coordinator, 1 scheduler, 3 cores), byte-diffed against a
**PostgreSQL 16.15** oracle, with a **DuckDB 1.5.5** CLI as a second oracle for the forms
PostgreSQL cannot parse, and result types and column labels read from extended-protocol
`Describe`. The executable counterpart is the e2e suite: a passing test pinning today's
behaviour beside an `#[ignore = "gap: …"]` test asserting the PostgreSQL-correct target, which
fails by construction and is the definition of done ([§ 5](gap-analysis.md#5--executable-counterpart)).

The public rendering of this status lives in the user documentation, at
[`docs/vairedb.io/docs/sql/compatibility.md`](../vairedb.io/docs/sql/compatibility.md).
