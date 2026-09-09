# VaireDB v0.2 Gap-Closing Plan

The ordered plan for turning what [`gap-analysis.md`](gap-analysis.md) measures into VaireDB
v0.2, a minimum viable product for an analytical workload. The gap analysis says what is
true; this says what to do next, in what order, and why that order and not another.

Scope is **the rows of that document** — §§ 3, 4.2, 4.3 and the open items of § 6.2. Two
things it leaves out stay out: cross-shard atomicity (a design decision, § 6.1.1) and
aggregate push-down into DuckDB (a performance property, not a gap row). What this plan
*adds* to the document is measurement: one axis of the surface has never been measured, and
an unmeasured axis cannot be prioritized — so measuring it is the first wave rather than a
footnote.

## The MVP bar: four client surfaces, all of them

"Minimum viable for an analytical workload" is defined here by four clients, each of which
fails on a different part of the surface. This is why the ordering below is not simply the
§ 6.2 ranking, which is ranked by consequence in the abstract:

| Surface | What it needs first | Which waves serve it |
|---|---|---|
| **psql and raw drivers** (JDBC, ODBC, psycopg) | Correct `Describe` types and a real SQLSTATE on every failure | W1, W2, W4 |
| **BI tools** (Metabase, Superset, Tableau) | `pg_catalog` introspection that answers, and broad scalar-function coverage — datetime above all | W0, W5, W7 |
| **dbt and SQL transformation tools** | Generated DDL that lands: `DROP … CASCADE`, schema moves, atomic swaps, and errors it can classify | W0, W2, W3 |
| **Arrow-native clients** (pandas, Polars) | Binary-format fidelity, exact decimals, `COPY` throughput, faithful OIDs | W1, W4 |

No single wave serves all four, and no surface is served by fewer than three waves. That is
the argument for the sequence rather than for cherry-picking.

## The ordering rule

Priority is assigned by four tests, applied in order:

1. **Consequence class.** A wrong answer outranks a wrong error class, which outranks a
   wrong advertised type, which outranks a loud refusal. § 4.3 is therefore first and § 4.2
   is last, regardless of how many rows each holds.
2. **How many surfaces a row blocks.** A row all four clients hit outranks one an expert
   reaches deliberately.
3. **Leverage.** One fix that closes several rows outranks several fixes that close one
   each. Two items in this document are leveraged: carrying a SQLSTATE across the Ballista
   boundary (W2) closes every executor-raised error class at once, and making `JSON`/`UUID`
   work as cast targets (W5) unblocks nine rows behind it.
4. **Cost**, last — and a **refusal counts as a closure** when it converts a wrong answer
   into something a client can see. That third option is the reason the silently-wrong class
   shrank from twenty-odd rows to five (§ 8), and it is available in every wave below.

Effort is relative, not calendar: **S** = one focused change and its tests, **M** = several
files or a new module, **L** = a proto or cross-crate change, **XL** = an upstream
dependency.

## Waves

| Wave | Theme | Effort | Rows it moves |
|---|---|---|---|
| **W0** | Measure the unmeasured axis; make the tally mean what it says | M | +1 axis; ❌ 55 → ~38 by reclassification |
| **W1** | No silently wrong answer a non-expert can reach | M | ⛔ 8 → 2 |
| **W2** | An error keeps its SQLSTATE across the Ballista boundary | L | 🟡 −2, retires one text-match concession |
| **W3** | The statement surface a transformation tool actually emits | M | ❌ −2, plus new command rows |
| **W4** | The result-type contract | M | 🟡 −6 |
| **W5** | Breadth that one fix unblocks | M | ❌ −11 |
| **W6** | Window residues, and the one defect distribution owns | L | ❌ −6 |
| **W7** | Function coverage, push-down narrowings, cosmetics | M–L | scaled by W0's measurement |

---

### W0 — Measure the unmeasured axis, and make the tally mean what it says

The gap analysis has six axes and no **scalar-function** axis. That is the largest unknown
on the surface, and it is where two of the four client surfaces fail first: a BI tool's
generated SQL is mostly datetime and string functions, and § 6.2 item 9 is the only open
item with no row count attached to it.

**A finding that changes what item 9 means.** `datafusion-pg-functions` 0.1.0 is registered
on every context that plans or executes (`ballista_exec/executor.rs:147`,
`scheduler/scheduler.rs:219`), and item 9 records that only its `math` category is
populated. Reading the crate: `datetime.rs`, `format.rs`, `string.rs`, `conditional.rs`,
`json.rs` and `uuid.rs` are each a 17-line stub whose `register` returns zero. **Enabling
those Cargo features would add nothing.** So item 9 is not a configuration change; it is a
choice between upstreaming into that crate and adding local UDFs on the
`vairedb_common::pg_udf` seam — and the choice cannot be made before knowing which functions
DataFusion's own defaults already answer, which is likely most of the common datetime and
string surface.

Three deliverables:

1. **`gap-analysis-scalar-function.md`**, measured the way every other axis was: live 5-node
   cluster, byte-diffed against the PostgreSQL 16.15 oracle, result types read off
   `Describe` rather than off source. Group by PostgreSQL's own function categories and
   record for each row *which* layer answers — a DataFusion default, a
   `datafusion-pg-functions` UDF, a `pg_udf` registration, or nothing. The last group is the
   only backlog.
2. **A client-conformance suite** (`tests/e2e/tests/client_conformance.rs`) that runs what
   the four surfaces actually send, captured rather than guessed: `psql`'s `\d \dt \df \l
   \gdesc`; the introspection queries Metabase and Superset issue on connect; the statements
   `dbt-postgres` emits for a table, view and incremental materialization; and
   `pandas.read_sql` / `to_sql` over both formats. Each failure becomes a row on the axis it
   belongs to. This is what tells W3 what to build instead of assuming.
3. **Reclassify ❌ rows that are already decisions.** The ❌ count is inflated: of the nine ❌
   statements, eight (`ATTACH`/`DETACH`, `CHECKPOINT`, `EXPORT`/`IMPORT DATABASE`,
   `INSTALL`/`LOAD`, `CREATE SECRET`, `USE`, `CALL`, `CREATE MACRO`) already carry a written
   rationale in § 4.2 and are 🚫 in everything but the symbol. The same holds for the
   DuckDB-only operator rows (`GLOB`/`~~~`, `MAP` literals, `str[n]`). Moving them costs
   nothing and makes the remaining ❌ count mean "open gap" as the legend promises. `COMMENT
   ON` is the one that stays ❌ pending W3.

**Exit:** the scalar axis exists with per-row verdicts; the conformance suite runs in `make
e2e` with its failures recorded as rows; ❌ means open.

---

### W1 — No silently wrong answer a non-expert can reach

§ 4.3 is the dangerous class and the shortest list in the document: five rows plus three
deliberately deferred, eight by the axis tallies. Six are reachable by ordinary SQL and all
six close here. **The target is ⛔ 8 → 2**, the two remainders being join residues that are
each narrower than the rule that produced them.

| Row | Fix | Effort |
|---|---|---|
| `1.0::float8 / 0` → `inf` | Two parts, deliberately split. **(a)** Refuse a divisor that is a literal zero at E2, `22012`, which catches the shape every demo and every generated `CASE`-guarded ratio produces. **(b)** The general case needs a guarded-division scalar UDF registered on every context through the `pg_udf` seam, and it is sequenced into W2 (below) rather than here, because a UDF that raises does so on an executor — where today the class is only correct via the divide-by-zero text carve-out. | S then M |
| `'\xDEADBEEF'::bytea` | Decode PostgreSQL's hex-escape input format at E2 into a `Binary` literal. The `Binary` type itself is clean; only this literal form is not. Arrow-native clients hit it. | S |
| `0b101` → `0` | A tokenizer defect in sqlparser: `0` aliased `b101`. Detect the token shape ahead of the parser and refuse `42601`. Making it loud is the whole fix — the value cannot be produced without upstream work, and answering `0` is worse than answering nothing. | S |
| `0x1F` → `Binary` | Same treatment, and the same argument. On the write path the re-render is irreducible and the shard fails `42804`; a coordinator-side refusal makes both paths agree and say so. | S |
| `nth_value(x, 0)` → NULL | One guard, `22016`. The narrowest row in the analysis. | S |
| `arr[-1]` → last element | Needs the call recorded in § 6.2 item 15. **Recommendation: match PostgreSQL and return NULL**, because PostgreSQL is the contract and DuckDB's behaviour is an extension, not a requirement. One constraint is load-bearing: this must land on **both** paths in the same change, or it becomes an eleventh split-brain row. | S |

Rows 2 and 3 of § 4.3 — a `USING` key under an explicit qualifier on a full or right join,
and `NOT IN` over an aggregate or a correlated subquery — stay open with `#[ignore = "gap"]`
targets. Row 2 is not representable in a plan whose schema has two fields for PostgreSQL's
three names; row 3 waits on DataFusion planning `LATERAL` in that position. Both have exact
documented alternatives (`ON` with the client's own `COALESCE`; `NOT EXISTS`). Neither is
reachable without writing the expert form deliberately.

**Exit:** the ⛔ column of § 4.3's totals reads 2, and each of the six closures is probed for
over-reach in the same test — the neighbouring form that loses no clause still answers.

---

### W2 — An error keeps its SQLSTATE across the Ballista boundary

§ 6.2 item 2, and the only remaining piece of Phase A. It is second rather than first
because a wrong answer outranks a wrong error class, and it is above everything else because
of leverage: **every** failure raised on an executor lands `XX000` today, so this is one fix
for an open-ended set of rows rather than a row of its own.

The classifier is already right where it can see a typed error: it matches `DataFusionError`
variants rather than message substrings, so `'x'::int` is `22P02` and `struct.field` is
`0A000`. What is left is structural. An error raised on an executor arrives as the
scheduler's own text — `Job <id> failed: … DataFusionError(Execution("ArrowError(DivideByZero)"))`
— and there is no typed error left to classify.

The work, in order:

1. **Carry a structured error over the scheduler's gRPC surface.** Add a code-plus-detail
   field to the stage-failure message in `proto/vairedb/v1/`, classify at the point of
   failure on the executor where the `DataFusionError` still exists, and thread it through
   stage-failure reporting into the coordinator's enrichment path. This is the fix item 2
   names; everything else in this wave falls out of it.
2. **Retire the divide-by-zero text carve-out** — `reclassify_transported_data_error` in
   `error_enrichment.rs` — once the structured path reports `22012` on its own. It was
   scoped as a concession for the one class an analytical client hits daily, explicitly not
   as a pattern to copy, and leaving it in place after the general fix would be a second
   competing classifier.
3. **Close the two 🟡 rows that are only wrong because of this**: a nested aggregate becomes
   `42803` instead of `XX000`, and a window function in `WHERE`/`GROUP BY`/`HAVING` becomes
   `42P20`/`42803`. Both are already *correctly rejected*; only the class is wrong, and a
   wrong class is what breaks a driver's and dbt's error handling.
4. **Land W1's guarded division (b).** With the SQLSTATE arriving typed, a
   guarded-division UDF raising on an executor reports `22012` on its own terms. Scope it to
   float division and to divisors that are not provably non-zero literals — the guard costs
   the Arrow kernel's vectorization and blocks constant folding, so it should not be paid
   where it cannot fire. Mirror the write path's guard, which already raises PostgreSQL's own
   message for the same expression.

Two properties from § 8 apply directly and are not optional. The text arm behind the typed
classifier is only ever met by errors that crossed a process boundary, so any assertion about
it must be written against **`Debug`** spellings, not `Display` ones. And the arriving
`DataFusionError` variant is not the one Ballista's source constructs, so nothing in this
wave may key on the wrapper variant.

**Exit:** an executor-raised error of each class reports its PostgreSQL SQLSTATE live on the
cluster, asserted as a property over the error corpus in the way
`test_no_error_reply_leaks_transport_internals` already asserts the sanitizer — not one test
per statement.

---

### W3 — The statement surface a transformation tool actually emits

Driven by what W0's captured statements prove, not by this list. Three items are near
certain, and the first is a genuine MVP blocker for the dbt surface that no row of the
command axis currently reads as one:

| Item | Why it is here | Effort |
|---|---|---|
| `DROP … CASCADE` / `RESTRICT` | Refused today (`ddl.rs:1216`, and the same for `TRUNCATE` and `ALTER TABLE … DROP CONSTRAINT`). A transformation tool's drop macro emits `cascade` unconditionally, so the refusal fails a run that has nothing cascading in it. Views are coordinator-local and their dependencies *are* knowable, so `CASCADE` can be implemented honestly over the coordinator's own catalog — drop dependent views with the table — and refused only where it would mean work the catalog cannot see. Confirm the exact spelling in W0 before building. | M |
| `ALTER SCHEMA`, `ALTER TABLE … SET SCHEMA` | § 6.2 item 11. Parser plus routing, both write-path. A schema qualifier folds into the physical name (`sales.t` → `sales_t_shard2`), so a schema move is a rename on every shard, which makes it a real operation rather than a catalog edit. | M |
| Several names in one `DROP` | Currently refused. Trivially decomposable into the per-name path that already exists. | S |
| `COMMENT ON` | The one ❌ statement that is a live candidate rather than a decision. It is refused because there is no catalog comment storage and nowhere to read one back, and "accepting it would be a fake OK" is the right instinct. **Recommendation: keep the refusal for v0.2** and document that `persist_docs` must be off, because a fake `OK` on a documentation statement is a lie that surfaces months later. Revisit with `pg_description` read-back, which is a catalog feature, not a statement fix. | — |

**Exit:** W0's captured statement set runs end to end for a table, a view and an incremental
materialization, or each remaining failure is a recorded row with a named alternative.

---

### W4 — The result-type contract

Every row here returns the **right value** under the **wrong advertised type**. That is
invisible to a client that reads text and load-bearing for one that binds a buffer from an
OID — which is both the raw-driver and the Arrow-native surface. § 6.2 items 3 and 12 plus
the type rows of § 3. Ordered by cost, because the cheap ones are cheap:

1. **`ntile` → `int4`** (item 3). A one-function widening in the same place the `UInt64`
   widening already lives. The last 🟡 on the window axis VaireDB can close alone.
2. **`percentile_cont` → `numeric`** over a `numeric` or integer sort column. It is VaireDB's
   own UDAF now, so the type is VaireDB's to fix, and `percentile_disc` already returns the
   sort column's own type and is the working precedent.
3. **The untyped set-operation branch** (§ 6.1 item 12's residue): `… UNION ALL SELECT '9'`
   comes back `text` where PostgreSQL says `integer`. The literal is already *recognized* in
   the plan; this is retyping it.
4. **`UInt64` → `bigint`**, and the `STRUCT` OID. arrow-pg has no unsigned PostgreSQL type,
   so this is a coordinator-side widening decision with a checked cast, exactly as the
   ranking functions already do.
5. **`Decimal256` → a clean refusal.** A `NUMERIC` with precision > 38 is `XX000` today
   because arrow-pg has no arm for it. `22003` or `0A000` naming `DECIMAL(38,s)` is the
   honest answer and is cheap; the arm itself is upstream (item 14).
6. **The `stddev` / `var` family → `numeric`.** Last because it is the expensive one:
   these are DataFusion's aggregates, so exactness over a `numeric` column means shadowing
   them with VaireDB UDAFs the way `percentile_cont` was shadowed. The precedent makes it
   affordable; it is still six functions.

**Exit:** `describe_result_types` in `tests/e2e/src/lib.rs` asserts PostgreSQL's OID for each
row above, in both text and binary format. Several rows of § 3 exist *only* because they are
visible over `Describe`, which is why the assertion belongs there and not in a value check.

---

### W5 — Breadth that one fix unblocks

The highest-leverage breadth item in the document, and the reason this wave exists as its own
step: **`CAST(… AS JSON)` and `CAST(… AS UUID)`** (§ 6.2 item 8). Both types work as
*column* types and neither works as a cast target, which is what makes nine other rows
unreachable rather than merely missing:

- The whole `jsonb` operator family — `->` `->>` `#>` `#>>` `@?` `@@` — six rows present in
  DataFusion's `Operator` enum and unimplemented in type coercion. Every probe of them is
  currently unreachable because `::json` fails at planning first.
- `json_agg` / `jsonb_agg`, two ❌ aggregates blocked entirely behind the type gap.
- `'…'::uuid`, which an ORM emits for every filter on a UUID key column — and UUID columns
  are on the `Utf8` fallback, so the column works and the predicate does not.

Three cheaper rows ride along in this wave because they are the same kind of work:

- **`mode() WITHIN GROUP`** — a new UDAF, and common enough in an analytical workload to
  outrank the four hypothetical-set forms beside it in § 4.2, whose window spellings all
  already work.
- **`GROUPING SETS (())` alone** returning 0 rows instead of the grand total. Narrow — the
  same set works combined with a non-empty one.
- **`count(DISTINCT a, b)`**'s comma form, `XX000` today with a correct working workaround.

Everything added here is subject to the standing constraint: **an AST rewrite, or a
registration on every context that plans or executes — never coordinator-only.** § 6.2 item 5
is what a coordinator-only registration costs, and `vairedb_common::pg_udf` is the seam that
prevents repeating it.

**Exit:** `::json` and `::uuid` plan and execute distributed; the six operator rows are
measured and either answer or are refused by name with a written reason; `json_agg` answers.

---

### W6 — Window residues, and the one defect distribution owns

Four rows, and the first is the only defect in the document VaireDB owns **because** it is
distributed:

1. **`FILTER` on a window aggregate.** `count(*) FILTER (WHERE …) OVER (…)` is refused
   `0A000` because `PhysicalWindowExprNode` has no filter field and Ballista's deserializer
   hardcodes `None`. Single-node DataFusion is correct and no upstream release closes it. The
   fix is a proto field plus both codecs — the same shape of change as W2, and worth
   sequencing after it so the proto surface is touched once by someone who has just learned
   it. The `CASE`-inside-the-aggregate workaround is real, which is what keeps this below W5.
2. **`IGNORE NULLS`.** One hardcoded `false` in `to_proto.rs` with a stale comment. It was a
   silent no-op and is now a refusal, which was the right call — the NULL it asked to skip
   came back looking like data. Closing it is small once the proto work of item 1 exists.
3. **The named-window forms** — `OVER (w)`, `OVER (w <extra clauses>)`, `WINDOW w2 AS (w1 …)`.
   Root cause is upstream and precise: `WindowSpec::window_name` is parsed by sqlparser and
   never read by datafusion-sql. One of these must stay refused even when the others land:
   `OVER (w <frame>)` returned five different answers on five consecutive runs over unchanged
   data, and an unstable answer is worse than no answer.
4. **`EXCLUDE {CURRENT ROW | GROUP | TIES | NO OTHERS}`** — `42601`, unparseable, a literal
   `// TBD` in sqlparser's `WindowFrame`. The only row in the analysis that needs parser work
   first and the only genuinely missing feature. **XL, and rare in practice** — this is the
   row to drop from v0.2 if anything is dropped.

---

### W7 — Function coverage, push-down narrowings, cosmetics

Sized by W0's measurement rather than in advance, which is the point of putting W0 first.

- **Function coverage** (§ 6.2 item 9, Phase D). Whatever the scalar axis records as
  answered by nothing. Expect datetime and format to dominate and expect the real list to be
  shorter than the empty crate categories suggest, because DataFusion's own defaults answer a
  large part of that surface already. `pg_typeof()` is the smallest visible example and a
  good first row.
- **Session-scoped `pg_settings`** (item 10). `SHOW` works because it reads the session
  registry; the catalog table has no per-session context to read from. Narrow, and it is
  BI-tool-facing.
- **The three push-down narrowings** (item 15). Performance, not correctness — but for an
  analytical workload performance is a feature, and the **text-ordering** one is cheapest:
  pin DuckDB's `default_collation`, inspect it, and a `WHERE name > 'M'` filter starts
  pruning at the shard instead of at the coordinator.
- **Cosmetics** (item 13): `JSON`/`UUID` OIDs, `ENUM` ordering, the duplicate-unaliased-label
  residue, `percentile_cont(0)`/`(1)`'s missing `simplify()` shortcut, and `format_type`'s
  missing `(Utf8, …)` signature arm — an untyped string literal for the OID is `0A000` where
  PostgreSQL coerces it to `oid`. That last one has no known client-tool reproducer, which is
  exactly why it sits here and not at item 5's rank.
- **Deferred upstream** (item 14), unchanged and not scheduled: `Decimal256`'s arrow-pg arm,
  `NUMERIC` above 29 digits in binary format, timestamp literals outside 1677–2262, `**`,
  `0b101`'s *value*.

---

## The executable counterpart

Every wave ships with tests, and § 7's convention holds throughout: a **passing** test
pinning today's behaviour plus an `#[ignore = "gap (row N): …"]` test asserting the
PostgreSQL-correct target, so the gap map never blocks CI. There are 16 such ignored targets
today; each closure deletes one.

| Suite | Status | Wave |
|---|---|---|
| `client_conformance.rs` | **new** — the four surfaces' captured statements | W0 |
| `sql_function_scalar.rs` | **new** — the scalar axis, by category | W0, W7 |
| `sql_expression_literals.rs` | proposed in § 7, unwritten — `0b101`, `0x1F`, `\x…`, `arr[-1]` | W1 |
| `sql_expression_split_brain.rs` | proposed in § 7, unwritten — the read/write verdict pairs, including `arr[-1]` | W1 |
| `errors.rs` | exists — extend to the executor-raised SQLSTATE property | W2 |
| `sql_command_ddl.rs` | exists — `CASCADE`, schema moves | W3 |
| `data_types_round_trips.rs` | exists — the OID assertions over `describe_result_types` | W4 |
| `sql_expression_operators.rs` | proposed in § 7, unwritten — the `jsonb` family | W5 |
| `sql_function_aggregate_distributed.rs` | proposed in § 7, unwritten — the merge invariants that would catch a wrong "optimization" if `remote_scan_exec.rs` ever gained a partitioning claim | W4, W5 |

That last one deserves its rank: the distributed merge is correct *structurally*, because
`remote_scan_exec.rs` declares `UnknownPartitioning(1)` and empty `EquivalenceProperties` and
so forces a shuffle. Nothing currently guards that declaration. A future performance change
that "helpfully" claims hash partitioning would silently produce average-of-averages, and no
existing test would fail.

## Standing rules for anyone doing this work

Four constraints from § 8 that have already cost a pass each, restated because every wave
above can violate one:

1. **A logically equivalent rewrite is not a correct rewrite until it has run on a cluster.**
   Reason about equivalence over relational algebra; verify over the physical plan the
   scheduler actually builds. The keyless-join defect was found only because one rewrite
   behaved differently on five nodes than on one.
2. **An unexplained cluster/unit-test divergence is a finding, not an obstacle.** Running one
   down turned up two silently wrong shapes no row of any axis had asked about.
3. **A claim about a plan is worth what printing the plan costs.** § 6.1 item 13 was recorded
   as impossible on the strength of a plan shape nobody had printed; printing it disproved
   the claim.
4. **AST rewrite, or register on every context that plans or executes — never
   coordinator-only.** § 6.2 item 5 is what the alternative costs, and
   `vairedb_common::pg_udf` is where the set now lives.

And one that follows from the read path's shape: **type-changing read-path rewrites must run
on the logical plan inside `plan_select`, before the analyzer** — the type a client is told
is read off the *unanalyzed* plan, and `TypeCoercion` erases the argument type PostgreSQL
resolves overloads on. W4 lives entirely inside this constraint.

## What v0.2 does not close, and why

Recorded so none of it reads as forgotten:

- **Cross-shard atomicity.** A design decision (§ 6.1.1). Changing it means adding 2PC.
- **Aggregate and `GROUP BY` push-down into DuckDB.** Each shard runs only `SELECT <cols>
  FROM <shard_table> [WHERE …] [LIMIT n]`, and no aggregate is ever pushed. This is a
  performance property rather than a gap row, and it is the thing to weigh for v0.3 if the
  analytical positioning is to mean throughput as well as correctness.
- **`EXCLUDE` window frames** (W6 item 4) — upstream parser work, rare in practice.
- **`FOREIGN KEY` enforcement, uniqueness off the shard key, shard-key mutation, DDL inside a
  transaction block.** Standing architectural limitations (§ 6.1), not backlog.
- **TLS, users, groups.** Its own roadmap line. Note that until it lands, `COPY` reads and
  writes any path the coordinator process can, with no privilege check — the one item on this
  list a user could mistake for a gap row.

## Open decisions that need a call

1. **`arr[-1]`** — recommended: match PostgreSQL, return NULL, on both paths in one change
   (W1). § 6.2 item 15.
2. **`COMMENT ON`** — recommended: keep the refusal for v0.2, document it, revisit as a
   `pg_description` catalog feature rather than a statement fix (W3).
3. **`DROP … CASCADE`** — recommended: implement over the coordinator's own view catalog,
   refuse where the catalog cannot see the dependency (W3). Confirm the emitted spelling in
   W0 first.
4. **The `stddev`/`var` family** — six DataFusion aggregates to shadow with VaireDB UDAFs for
   exactness over `numeric` (W4 item 6). It is the one item in W4 that could reasonably slip
   to v0.3, since the values are correct and only the OID diverges.
