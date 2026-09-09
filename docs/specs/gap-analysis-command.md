# SQL Command Gap — DuckDB vs. VaireDB (PostgreSQL wire protocol)

Which SQL **statements** VaireDB's coordinator accepts over the PostgreSQL wire
protocol, measured against the statements DuckDB documents.

- **Reference:** DuckDB's [SQL Statements overview](https://duckdb.org/docs/current/sql/statements/overview) — 35 documented entries. The row numbers below are that list's, and the tests cite them, so they stay fixed even where a table groups rows by theme rather than by number. A few combined entries are split into their sub-commands, hence 36 rows.
- **Siblings:** `gap-analysis-data-type.md`, `gap-analysis-operator-literal.md`, `gap-analysis-aggregate-function.md`, `gap-analysis-window-function.md`. All five are consolidated in [`gap-analysis.md`](gap-analysis.md); read that for the overall picture and this one for the statement rows.
- **Executable counterpart:** every row maps to an end-to-end test — see [Tests](#tests).

VaireDB is a sharded coordinator that speaks the PG wire protocol and executes on
per-shard DuckDB backends; it is not a drop-in DuckDB. It recognizes only the statement
kinds it can shard, route and replicate: `classify_statement`
([`pgwire_handler/query_router.rs`](../../crates/vairedb-coordinator/src/pgwire_handler/query_router.rs))
sorts a statement into one of 21 `QueryType`s, and anything else becomes
`QueryType::Other` and is refused.

## Legend

| Status | Meaning |
|---|---|
| ✅ | Supported. |
| 🟡 | Supported, with restrictions that follow from sharding. Each is listed in the row. |
| 🚫 | **Not planned** — a decided limitation. The deliverable is a rejection that explains itself. |
| ❌ | Refused today, with no target behavior specified. All of them sit in [Not planned, or out of scope](#not-planned-or-out-of-scope); what separates them from 🚫 is only that a 🚫 row carries a written rationale and, where one exists, a named alternative. |

A refused statement fails at one of two points, and which one decides the cost of closing
it:

| Rejection point | Error | SQLSTATE | When |
|---|---|---|---|
| Parse | `SqlSyntaxError` | `42601` | Does not parse under sqlparser's `PostgreSqlDialect` (most DuckDB-only syntax), **plus** `VACUUM`, `CHECKPOINT`, `ALTER SCHEMA`, `ALTER SEQUENCE` and `ALTER VIEW … RENAME TO`, which sqlparser omits although PostgreSQL has them. Closing such a row needs the parser taught first — `RESET` (row 28) is the worked example: it is recognized ahead of the parser and rewritten into a statement sqlparser does have. |
| Classification | `FeatureNotSupported` | `0A000` | Parses, but is not one of the routed kinds. Only routing/execution is missing. |

The `SQLSTATE` column in the tables records what was **observed against the 5-node e2e
cluster**, not a guess. Every classification refusal names the command it refused, so a
client sending a batch can tell which statement was rejected.

## Summary

| Status | Count | Statements |
|---|---:|---|
| ✅ | 5 | SELECT, INSERT, UPDATE, DELETE, CREATE TABLE |
| 🟡 | 14 | ALTER TABLE, DROP, ALTER VIEW, COPY, CREATE INDEX, CREATE SCHEMA, CREATE VIEW, MERGE INTO, transaction management, SET, SHOW, RESET, EXPLAIN, DESCRIBE |
| 🚫 | 8 | CREATE SEQUENCE, CREATE TYPE, ANALYZE, PIVOT, UNPIVOT, SUMMARIZE, SET VARIABLE, VACUUM |
| ❌ | 9 | all others |

Of the 9 ❌ rows: **4 fail at classification**, **3 fail at parse**, and **2 are split
across both points** (`ATTACH`/`DETACH`, `INSTALL`/`LOAD`).

## Rules that apply to every row

These are the invariants the write path is built on; the row notes below only record
where a statement deviates.

- **A statement is either correct on every shard or refused.** No statement is accepted
  and silently under-applied, and no refusal is a fake `OK` for work that never happened.
- **A write must have a determinable shard key before it leaves the coordinator**, because
  the coordinator hashes it to pick the shard. A key that is a computed expression is
  refused rather than guessed. `UPDATE`/`DELETE` predicates broadcast instead, since every
  shard re-evaluates the `WHERE` clause.
- **Uniqueness is only enforceable when it includes the shard key.** Equal shard keys share
  a shard, so a per-shard unique index or constraint then sees every row that could
  collide. Off the shard key it is refused, in every spelling (`UNIQUE` index, `UNIQUE` /
  `PRIMARY KEY` constraint, `ON CONFLICT` arbiter).
- **There is no cross-shard atomicity yet (no 2PC).** A transaction block on one node set
  is all-or-nothing; one spanning node sets is refused at `COMMIT` unless
  `allow_cross_shard_transactions` is set. A multi-shard write that fails part-way reports
  `40003` and how much was written — never a rollback that did not happen. Statements whose
  retry converges (`TRUNCATE`, index and constraint DDL) are shipped with
  `IF (NOT) EXISTS` so re-running finishes the job.
- **DDL is refused inside a transaction block**, because it reaches the catalog and the
  shards immediately and `ROLLBACK` could not undo it.
- **Every object kind resolves through its own namespace**, so no statement can destroy an
  object of another kind: the wrong kind is `42809` naming the statement to use instead, a
  name that resolves to nothing is `42P01`. Tables, views, indexes and index-backed
  constraints share **one relation namespace**, claimed atomically.
- **A schema qualifier is part of the catalog key** (`sales.t`; a relation in the default
  schema keys as the bare `t`) and is folded into the physical per-shard name
  (`sales_t_shard2`). There is [no `search_path`](#no-search-path): an unqualified name
  always means the default schema.
- **Decorations that only affect performance are stripped**; anything that would change
  results is refused by name rather than accepted and ignored.

## Write path and DDL — closed

The statements that route to shards, plus the DDL that is coordinator-local (views and
schemas reach no shard). This surface is complete; what stays refused in a 🟡 row is
refused by design, not waiting for a turn.

| # | Statement | `QueryType` | Status | SQLSTATE | Notes |
|---|---|---|---|:-:|---|
| 2 | `INSERT` | `Insert` | ✅ | — | Needs the shard key present with a non-NULL, determinable value per row; positional rows are resolved against the declared column order first. Multi-row inserts are split per shard. Any query source (`SELECT`, `UNION`, CTE) is materialized and re-emitted as literal `VALUES`, so it routes like a hand-written insert; `RETURNING` on that form is refused. `ON CONFLICT DO UPDATE` works when the arbiter includes the shard key and is backed by a `PRIMARY KEY`/`UNIQUE` index. |
| 3 | `UPDATE` | `Update` | ✅ | — | Except mutating the shard-key column: row relocation is refused (`0A000`). |
| 4 | `DELETE` | `Delete` | ✅ | — | Routed to the owning shards, or broadcast when the predicate's shard key is not a literal. |
| 5 | `CREATE TABLE` | `CreateTable` | ✅ | — | VaireDB-extended: `WITH (shards, replication_factor, shard_by, anonymized_columns)`. PG types mapped to DuckDB. The name is claimed atomically. Declared constraints are recorded and reach every shard: `CHECK` always, `UNIQUE`/`PRIMARY KEY` only over the shard key, `FOREIGN KEY` never (unenforceable across shards). A constraint is recorded from its **column list**, so `PRIMARY KEY`/`UNIQUE … USING INDEX <name>` names an index instead and is refused by name rather than stored column-less, which would let the shard-key rule pass a constraint it never checked; an `INDEX`/`KEY`/`FULLTEXT` clause inside the definition is refused too, since it declares an index (`CREATE INDEX`, row 15) and not a constraint. `AS SELECT` is supported when it states `WITH (shard_by = …)` **before** the query; `LIKE`/`CLONE` supply no column list and are refused. |
| 6 | `ALTER TABLE` | `AlterTable` | 🟡 | `0A000` | Column ops (ADD/DROP/RENAME COLUMN, ALTER COLUMN type/nullability/default), `RENAME TO`, and `ADD`/`DROP CONSTRAINT … UNIQUE (<shard key>)`. `RENAME TO` re-keys the catalog and renames every `{table}_shard{n}`; it stays inside the table's schema and must be the only action (`42601`). Cannot drop the shard key or touch anonymized columns. While the table carries an index or index-backed constraint, only adding a column and changing a default are possible — the engine's dependency is on the table. A column a declared constraint covers cannot be dropped or retyped. Every other `ADD`/`DROP CONSTRAINT` form is refused by name: the shards' engine implements neither `ADD … CHECK` nor `DROP CONSTRAINT`. |
| 7 | `DROP` | `DropTable` / `DropIndex` / `DropView` / `DropSchema` | 🟡 | `0A000` / `42P01` | The four kinds the catalog knows, each through its own namespace. `IF EXISTS` honored. Several names in one statement, and `CASCADE`/`RESTRICT`, are refused rather than partly applied. `DROP SEQUENCE` reaches the table handler and reports as a missing table. |
| 8 | `ALTER VIEW` | `AlterView` | 🟡 | `42601` / `42P01` | `AS <query>` redefines a view in place, validated like a `CREATE`, and refuses a name no view holds. `RENAME TO` fails at parse. |
| 14 | `COPY` | `Copy` | 🟡 | `0A000` | Bulk import/export in **both** directions and over **both** sources: a CSV file on the coordinator, and `FROM STDIN` / `TO STDOUT` over the wire protocol's streaming sub-protocol, so `psql`'s `\copy` works. `TO` gathers from every shard; `FROM` routes each row by its shard key through the same lane a client `INSERT … VALUES` uses, buffering partial records across `CopyData` chunk boundaries and flushing a bounded batch at a time. `FORMAT CSV` must be stated (PG's default is `TEXT`); `HEADER`/`DELIMITER`/`QUOTE` are honored and any other option is refused by name. Refused: `PROGRAM`, non-CSV formats. A failure part-way through a load leaves the batches already sent committed — the same guarantee a multi-row `INSERT` gives (see cross-shard atomicity below). ⚠️ No privilege check exists — VaireDB has no role model, so any client reads and writes any path the coordinator process can. Two things about the streaming half are the *protocol's* doing rather than the statement's, and both are only visible over the **extended** query protocol, which is what every driver's bulk-load API uses: a `FROM STDIN` that cannot work — unknown table, unknown column, no shard key — is refused at **Parse** rather than at Execute, because a driver that has already been told to start streaming aborts with `CopyFail` + `Sync` and the extra `ReadyForQuery` is then charged to the *next* statement, desynchronising the connection for good; and pgwire 0.40.4 never leaves copy mode after an `Execute`-driven copy (it waits for a `Sync` that its own dispatch loop discards while the copy state is set), so VaireDB clears that state itself when the copy completes. Without it a client's **second** `COPY … FROM STDIN` on one connection hangs forever. |
| 15 | `CREATE INDEX` / `DROP INDEX` | `CreateIndex` / `DropIndex` | 🟡 | `0A000` | One real index per shard, on every replica, recorded on its table. `UNIQUE` over the shard key doubles as an `ON CONFLICT` arbiter, so an existing table can gain upsert. An index is created in its table's schema, so `CREATE INDEX` takes a bare name and `DROP INDEX` the qualified one. Refused: `UNIQUE` off the shard key, a `UNIQUE` index narrowed by `WHERE` or `NULLS NOT DISTINCT`, an unnamed index (nothing for `DROP` to resolve), an index over an expression. |
| 17 | `CREATE SCHEMA` / `DROP SCHEMA` | `CreateSchema` / `DropSchema` | 🟡 | `0A000` | **Coordinator-local**: a namespace in the catalog, nothing broadcast. A taken name is `42P06`, a missing schema `3F000`, a non-empty `DROP SCHEMA` `2BP01` (RESTRICT-only). Because the physical name folds the qualifier in, `sales.t` and `sales_t` cannot both exist — the second is `42P07` naming the owner — and a `.` inside a quoted name still reads as a qualifier. Refused: `CASCADE` (it would drop tables as a side effect of a namespace statement), `AUTHORIZATION` (no role model), dropping `public` or a metadata schema. `ALTER SCHEMA` fails at parse. |
| 20 | `CREATE VIEW` | `CreateView` | 🟡 | `0A000` | **Coordinator-local**: the query text is stored and inlined as a CTE ahead of every query naming the view, so it is planned fresh on each read, never stale, and gets the shard fan-out the query would. The definition is validated at create time. `CREATE OR REPLACE`, `IF NOT EXISTS`, a column list and a view over a view are honored; a client's own CTE of the same name wins. A view is read-only (writes are `42809`). Refused: `MATERIALIZED` (nothing is stored here), the dialect decorations (`TEMPORARY`, `WITH (…)`, `SECURE`, `CLUSTER BY`, `TO`, `COMMENT`, a typed column), a view over a metadata schema or named after a `pg_catalog` table, and a cyclic definition. |
| 25 | `MERGE INTO` | `Merge` | 🟡 | `0A000` | Applied shard by shard, with all four `WHEN` clause kinds, their `AND` conditions and `UPDATE`/`INSERT`/`DELETE` actions honored as written. Accepted when `ON` equates the target's **shard key** with a source column and the source is either a co-located table sharded by that column (same shard count, same nodes) or an inline `VALUES` list, which is split per shard. Refused: any other source, an `ON` that does not pin the shard key (each shard would decide "not matched" on partial information and insert a duplicate), `UPDATE SET <shard key>`, an `INSERT` that omits or contradicts the shard key, `RETURNING`, `NOT MATCHED BY SOURCE` over a `VALUES` source, per-action `WHERE` predicates, a merge into a table with anonymized columns. |
| 33 | Transactions (`BEGIN`/`COMMIT`/`ROLLBACK`/`SAVEPOINT`) | `TransactionControl` | 🟡 | `0A000` | All of `BEGIN`/`START TRANSACTION`, `COMMIT`/`END`, `ROLLBACK`/`ABORT`, `SAVEPOINT`, `ROLLBACK TO SAVEPOINT`, `RELEASE`, on both protocols — so a driver that opens a transaction implicitly works. The writes are **buffered in the coordinator** and shipped at `COMMIT` as one atomic batch per node set, which makes `ROLLBACK` exact and savepoints positions in that buffer. Buffering is also the constraint: only statements the coordinator can answer truthfully **without running them** are allowed inside a block, so `UPDATE`/`DELETE` (unknowable row count), DDL, and reads of a table the block has written are refused, `READ ONLY` is honored (`25006`), and a failed block stays failed (`25P02`). Isolation levels are accepted and ignored — the node-local DuckDB transaction's level governs. |
| — | `TRUNCATE` | `TruncateTable` | 🟡 | `0A000` | Not in DuckDB's list, but standard PG that clients send. Every replica of every shard is emptied; the table, its shard layout and its schema survive. `ONLY` and a trailing `*` are accepted (nothing inherits here). Refused: several tables in one statement, `CASCADE`/`RESTRICT`, `RESTART IDENTITY`. |

## Read path and session state — closed

Planned on DataFusion, or held in the connection's session state. None of them touch
shard routing, which is why they were a separate body of work. At the **statement** level
that work is done, and the expression and type layers have since followed — see
[gap-analysis-operator-literal.md](gap-analysis-operator-literal.md) and
[gap-analysis-data-type.md](gap-analysis-data-type.md) for what each closed. What remains
on the read path is function-level coverage, the SQLSTATE mapping, and the items each
sibling doc lists as deferred with its reason.

One structural change came out of that work rather than out of any single row: a `SELECT`'s
predicates and its `LIMIT` now reach the shards' own scans instead of every shard streaming
its whole table. The coordinator re-applies both over the union, so this changes how much
data crosses the network and not what any row above answers.

| # | Statement | `QueryType` | Status | SQLSTATE | Notes |
|---|---|---|---|:-:|---|
| 1 | `SELECT` | `Select` | ✅ | — | Full read path, distributed via Ballista; `pg_catalog`/`vairedb_catalog` introspection is answered from a local context. Its operator, type and function coverage is the subject of the sibling gap docs. One class of read is refused rather than answered: over a column declared in `anonymized_columns` (row 5), which stores and returns an HMAC digest, an ordering (`ORDER BY`, `min`/`max`, `<`…`>=`, `BETWEEN`, a window's `ORDER BY`) or a pattern match would report a property of the digest under the name of the plaintext, so it is rejected `0A000`. Equality against a 64-hex digest — the documented lookup — and everything else the hash preserves (`count`, `count(DISTINCT)`, `GROUP BY`, joins, projecting the digest) stay accepted. |
| 29 | `SET` | `SessionParam` | 🟡 | `0A000` / `22023` / `42704` / `55P02` | The `SET`s drivers issue on connect (`client_encoding`, `application_name`, `extra_float_digits`, `DateStyle`, `TimeZone`, `IntervalStyle`, `standard_conforming_strings`, `statement_timeout`, `client_min_messages`) are accepted, so a client is no longer broken before its first query. Accepted is **not** "accepted and ignored": each parameter declares which values the coordinator's own behaviour already matches, and a value outside that set is refused by value (`22023`) naming what VaireDB does instead — `client_encoding` must be UTF-8 because the encoder emits nothing else, `TimeZone` must be UTC, `statement_timeout` must be 0 because nothing cancels a running statement. Recording a value nothing honors would make `SHOW` promise a rendering the read path never applies. `search_path` is refused **by name** (`0A000`): there is no search path to record. An unknown parameter is `42704` and a startup-fixed one `55P02`, so a driver can tell a typo from a refusal. Refused by name: `SET LOCAL` (a block holds no parameter snapshot to roll back), `SET ROLE`, `SET SESSION AUTHORIZATION`, `SET TRANSACTION`, `SET NAMES`. Allowed inside a transaction block, as in PostgreSQL. |
| 31 | `SHOW` | `SessionParam` | 🟡 | `42704` | The read half, over the same registry, so it always agrees with the `ParameterStatus` the client was sent at startup — the session is seeded from it. `SHOW <name>` returns one `text` column labelled with the parameter's canonical spelling, `SHOW ALL` the three PostgreSQL columns (`name`, `setting`, `description`). The spelled-out forms `SHOW TIME ZONE` and `SHOW TRANSACTION ISOLATION LEVEL` resolve too — JDBC calls the latter from `getTransactionIsolation()`, and it answers `read uncommitted`, the weakest level because a multi-shard commit is applied one node set at a time. `SHOW TABLES` / `DATABASES` / `SCHEMAS` and the rest of that family are a different statement and stay refused (`0A000`); PostgreSQL has no such command either. |
| 28 | `RESET` | `SessionParam` | 🟡 | `42704` | `RESET <name>` and `RESET ALL` restore what startup announced, which is what a connection pooler issues on checkout. Recognized before the parser (sqlparser has no `RESET`) and rewritten to the `SET … TO DEFAULT` PostgreSQL defines it to be equivalent to. A `RESET` of a refused or startup-fixed parameter **succeeds** while the matching `SET` does not: resetting asks for the default, and the default is the behaviour VaireDB already has. |
| 27 | `EXPLAIN` / `EXPLAIN ANALYZE` / `PRAGMA` | `Explain` | 🟡 | `0A000` / `22023` | The plan a read already builds, rendered instead of executed. The query inside goes through the **same preparation a bare `SELECT` gets** — views inlined, qualified names collapsed, compatibility rewrites applied — so what is reported is the plan VaireDB would run, down to the per-shard remote scans; explaining the unprepared statement would print a plan for a query that never runs. `EXPLAIN` renders in the coordinator and executes nothing; `EXPLAIN ANALYZE` runs the query distributed and reports its per-stage metrics. Output is PostgreSQL's shape — one `text` column named `QUERY PLAN`, one row per line — not DataFusion's `plan_type`/`plan` pair, because that column is what a client's plan display keys on. Inside a transaction block it inherits the `SELECT` rule: refused for a table the block has written, since the plan (and, for `ANALYZE`, the rows) would be for a state the client cannot see. Refused: `EXPLAIN` of a write — a write is rendered back to SQL and planned by each shard's engine, so there is no coordinator plan to show; every utility option other than `ANALYZE`/`VERBOSE` (`FORMAT`, `COSTS`, `BUFFERS`, `SETTINGS`, …), each by name, because each changes what a client parses; a non-boolean option value (`22023`); the dialect plan modes `EXPLAIN QUERY PLAN` and `EXPLAIN ESTIMATE`; and `EXPLAIN <relation>`, which neither PostgreSQL nor DuckDB has — `DESCRIBE` (row 22) is that. `PRAGMA` stays refused: it is DuckDB-only, and one accepted here would configure a single shard's engine while the client believed it had configured the database. |
| 22 | `DESCRIBE` / `DESC` | `Explain` | 🟡 | `0A000` | Answered by describing `SELECT * FROM <relation>`, which is the same shape by construction and reaches a **view** for free — a view is not a registered relation but a definition preparation inlines. One row per result column, with its name, type and nullability. `DESCRIBE <query>` resolves too, over any query the read path can plan. Allowed inside a transaction block: only DDL changes a relation's shape and DDL is refused in a block, so the shape reported cannot be stale. Refused: `DESCRIBE FORMATTED` / `EXTENDED`, dialect-only forms that promise storage detail the coordinator does not hold. |

### Observed while probing, outside any row above

Three things a live cluster showed that belong to no statement in the table:

- **`pg_typeof()` is not implemented.** It appears in no VaireDB source file and DataFusion
  does not provide it, so `SELECT pg_typeof(x)` fails as an unknown function. It is a
  one-argument function over the *advertised* Arrow type, which the coordinator already has
  in hand at plan time, so this is function-level coverage rather than a structural gap —
  filed with the rest of it. Worth more than its size suggests, because it is how a client
  asks what VaireDB thinks a column is, which is the question every type-layer surprise
  starts with.
- **`\d` reports a `VARCHAR(64)` column as `text`.** Correct as far as the wire goes — the
  advertised type is `Utf8`, whose OID *is* `text` — but PostgreSQL's `\d` prints
  `character varying(64)`, because its catalog keeps the length as a typmod and Arrow has
  nowhere to put one. The declared string is not actually lost: the catalog still holds it
  verbatim, so the length is recoverable for introspection even though it can never reach
  the Arrow schema `\d` is currently answered from. See
  [gap-analysis-data-type.md](gap-analysis-data-type.md).
- **`SET datafusion.optimizer.repartition_windows = false` is refused `[VDB-1016]`
  (`42704`), and that is correct.** `42704 undefined_object` is exactly what PostgreSQL
  reports for a configuration parameter it does not model, and VaireDB models the
  PostgreSQL parameter set, not DataFusion's. Accepting a `datafusion.*` name would either
  do nothing — the fake `OK` row 29 exists to avoid — or let a client reconfigure the
  planner underneath a contract written in PostgreSQL's terms. Recorded here because it
  looks like a gap and is not one.

## Not planned, or out of scope

| # | Statement | Status | SQLSTATE | Why |
|---|---|:-:|:-:|---|
| 19 | `CREATE SEQUENCE` / `nextval()` / `SERIAL` | 🚫 | `0A000` (`ALTER SEQUENCE`: `42601`) | See [no sequences](#no-sequences). |
| 21 | `CREATE TYPE` / `CREATE DOMAIN` / `ALTER TYPE` | 🚫 | `0A000` | See [no user-defined types](#no-user-defined-types). |
| 26 / 34 | `PIVOT` / `UNPIVOT` | 🚫 | `42601` | DuckDB-only reshaping. PostgreSQL says it with `CASE` inside aggregates plus `GROUP BY`, and with `UNION ALL` or a `LATERAL` over a `VALUES` list. See [no dialect-only statements](#no-dialect-only-statements). |
| 32 | `SUMMARIZE` | 🚫 | `42601` | DuckDB-only shorthand for aggregates the read path already answers when written out. |
| 30 | `SET VARIABLE` | 🚫 | `42601` | DuckDB variables. Nothing in the read path would read one back, so accepting it would be a fake `OK`. |
| 9 | `ANALYZE` | 🚫 | `0A000` | No planner-statistics surface for it to populate. |
| 36 | `VACUUM` | 🚫 | `42601` | Per-shard storage maintenance, like `CHECKPOINT` below. |
| 10 | `ATTACH` / `DETACH` | ❌ | `0A000` / `42601` | Single-node DuckDB attachment; meaningless for a sharded cluster. |
| 12 | `CHECKPOINT` | ❌ | `42601` | Per-shard storage concern, not coordinator-exposed. |
| 23 | `EXPORT` / `IMPORT DATABASE` | ❌ | `42601` | Whole-DB dump/load. Use `COPY` (row 14). |
| 24 | `INSTALL` / `LOAD` | ❌ | `42601` / `0A000` | Extension management is a per-node concern. |
| 18 | `CREATE SECRET` | ❌ | `0A000` | DuckDB's secrets manager; VaireDB has its own anonymization-secret path (`INSERT INTO vairedb_catalog.anonymization_secret`). |
| 35 | `USE` | ❌ | `0A000` | One database per cluster, and no `search_path` to switch — a relation elsewhere is named with its qualifier. |
| 11 | `CALL` | ❌ | `0A000` | No stored or table procedures. |
| 13 | `COMMENT ON` | ❌ | `0A000` | No catalog comment storage, and nowhere to read one back — accepting and discarding it would be a fake `OK`. |
| 16 | `CREATE MACRO` | ❌ | `42601` | DuckDB-only. |

Rows 10, 12, 18, 23, 24 and 35 are single-node DuckDB concerns: accepting one would mean it
silently ran on one arbitrary node. No row in this table has a target-state test, because
every one of them must **stay** rejected; two guards in `sql_command_unsupported.rs` pin
that none is ever quietly accepted —
`test_out_of_scope_statements_stay_rejected` for the single-node concerns and
`test_statements_with_a_postgres_rewrite_stay_rejected` for the dialect-only ones, which
also asserts that the PostgreSQL rewrite it names does work. Row 30 `SET VARIABLE` is the
exception, guarded alongside the other `SET` forms in
`test_unmodelled_set_forms_stay_rejected`.

## Decided limitations

### No sequences

A sequence is one monotonically increasing counter, which is the single thing a
shared-nothing cluster cannot provide cheaply. Broadcasting `CREATE SEQUENCE` gives every
shard its own counter, so the "unique id" collides across shards — and because replication
is statement shipping, a surviving `nextval()` would evaluate differently on each replica.
A coordinator-allocated counter is correct but turns every insert into a round trip through
one serialized allocator and makes the coordinator a hard point of failure for writes,
which is the opposite of what sharding buys.

So `CREATE`/`DROP SEQUENCE`, `nextval()` and `SERIAL`/`BIGSERIAL`/`SMALLSERIAL` are refused
with a message naming the alternative: generate ids in the application (UUID/ULID, or a
client-side snowflake), which needs no coordination and spreads evenly over the shards.
This also settles the `SERIAL` row of `gap-analysis-data-type.md`.

### No search path

A schema qualifier is not a lookup hint here, it is part of the catalog key and part of the
physical per-shard name: `sales.t` keys as `sales.t` and lands as `sales_t_shard2`, while a
relation in the default schema keys as the bare `t`. A search path would make one written
name resolve to different keys depending on session state, and the coordinator resolves a
name once, at planning time, for every shard at once — so the same statement replayed on a
replica, or re-planned after a reconnect, could reach a different table. That is the one
kind of ambiguity a statement-shipping cluster cannot carry.

So an unqualified name always means the default schema, and `SET search_path` is refused
**by name** (`0A000`) rather than accepted and ignored: there is no path to record, and
recording one would have `SHOW search_path` promise a resolution rule no query applies.
`USE` (row 35) is refused for the same reason. The way to reach a relation in another
schema is to qualify it.

### No dialect-only statements

PostgreSQL is VaireDB's contract; DataFusion and DuckDB are implementation layers under it.
A statement that exists only in a layer below has no client asking for it, and accepting one
would publish a second dialect that clients would then have to discover and depend on. So
the DuckDB-only reshaping and configuration statements are refused with the PostgreSQL form
named instead — `PIVOT` becomes `CASE` inside aggregates with `GROUP BY`, `UNPIVOT` becomes
`UNION ALL` or a `LATERAL` over a `VALUES` list, `SUMMARIZE` becomes the aggregates it
wraps, written out. `SET VARIABLE` and `PRAGMA` have no PostgreSQL form and no reader here:
a `PRAGMA` accepted at the coordinator would configure one shard's engine while the client
believed it had configured the database.

`ANALYZE` and `VACUUM` are refused for the other reason — there is nothing here for them to
act on. The coordinator keeps no planner statistics for `ANALYZE` to populate, and storage
maintenance is per-shard, like `CHECKPOINT`. Either one could only report an `OK` for work
that never happened, which is the failure mode this whole document exists to remove.

### No user-defined types

A user-defined type is cluster-wide state the coordinator has nowhere to keep. The catalog
models tables and has no replay path for non-table DDL, so a node that joins or is rebuilt
would come back without the type and refuse every write using it. It would not survive the
wire boundary either: the emulated `pg_catalog` has no `pg_type` row to give it an OID. And
what an enum or domain buys is a value check, which is shard-local — a `CHECK` constraint
declared at `CREATE TABLE` is the in-database version of the same guarantee.

So `CREATE TYPE` (enum, composite, range), `CREATE DOMAIN` and `ALTER TYPE` are refused
with the way out. `DROP TYPE` is left alone: it reports that the type does not exist, which
is exactly true. DuckDB's **inline** `ENUM(...)` column type is not a named type and still
reaches the shards — see `gap-analysis-data-type.md`.

## Open gaps, ranked

By how often real PG clients and ORMs need them. **(routing)** = parses today,
**(parser + routing)** = the parser has to be taught the statement first.

1. **`ALTER SCHEMA`** *(parser + routing)* and **`ALTER TABLE … SET SCHEMA`** — what a
   namespace alone does not give. `search_path` is no longer on this list: it is a
   [decided limitation](#no-search-path) rather than a gap.

The one remaining item is write-path DDL work: the read path's statement list is closed.

**Closed since the last pass:** the **`COPY` streaming sub-protocol** (row 14), which was
this list's top item and the reason `psql`'s `\copy` did not work — it turned out to be
smaller than "protocol work" suggested, because pgwire already ships the sub-protocol and the
file-based `COPY` already owned the shard-routing lane, so only the chunk buffering between
them was new. Before it: `SET` / `SHOW` / `RESET` (rows 29/31/28), the one gap that could
break a client on connect rather than on a query, and `EXPLAIN` / `DESCRIBE` (rows 27/22),
the query-inspection and schema-exploration pair tooling reaches for first.

**Left the list without being closed:** `PIVOT` / `UNPIVOT` (rows 26/34), `SUMMARIZE`
(32), `SET VARIABLE` (30), `ANALYZE` (9) and `VACUUM` (36) are now
[decided limitations](#no-dialect-only-statements) — dialect-only spellings of things
PostgreSQL says another way, or maintenance verbs with nothing to maintain. VACUUM's
"undecided" mark is settled: it belongs with `CHECKPOINT`.

Beyond the statement list, the standing limitation is **cross-shard atomicity**: a
transaction block spanning node sets is refused rather than half-applied, and multi-shard
writes report partial commits honestly. Closing that is 2PC, not a statement gap.

## Tests

| File (`tests/e2e/tests/`) | Rows |
|---|---|
| `sql_command_select.rs` | 1 — the read path's operator surface, plus the predicate and `LIMIT` push-down: the answer must not change when a filter moves to the shard, including a quoted literal, a top-level `OR`, the escaped-`LIKE` pattern the two engines read differently, and a predicate on a `UUID`/`JSON` column, where pushing would have turned an empty result into a shard-side conversion error. |
| `sql_expression_gaps.rs` | 1 — the expression-level rows of [gap-analysis-operator-literal.md](gap-analysis-operator-literal.md). |
| `sql_command_dml.rs` | 2–4, 25 — INSERT/UPDATE/DELETE, MERGE, upsert. |
| `sql_command_ddl.rs` | 5–7 — CREATE/ALTER/DROP TABLE, constraints, `TRUNCATE`. |
| `sql_command_transaction.rs` | 33 — what a buffered block honors and refuses. |
| `sql_command_unsupported.rs` | 8–32, 34–36 — every ❌ and 🚫 statement, plus the closed rows (14 `COPY`, 15 indexes, 17 schemas, 20 views, 22/27 `DESCRIBE`/`EXPLAIN`, 28–31 session state), which now pin what stays refused. Row 14's streaming half is here too: a `FROM STDIN` load, a `TO STDOUT` dump, a `STDOUT` → `STDIN` round-trip into an empty table, the option and column-list forms that stay refused **without** leaving the connection stuck in copy mode, and a payload large enough to span many `CopyData` messages. |
| `shard_key_hazards.rs`, `identifier_rewrite.rs`, `data_types_dialect_gaps.rs`, `concurrency.rs` | Write-path rules that cut across rows: which shard-key values route, how a name becomes one catalog key and one physical name, PG → DuckDB expression rewrites, concurrent DDL/DML. |

Each gap has up to two tests: a **passing** `*_currently_rejected` test pinning that
today's refusal is honest, and an `#[ignore = "gap (row N): …"]` test asserting the
PostgreSQL-correct target behavior. The ignored ones fail by construction and are the
definition of done — un-ignore one as its gap closes, and delete it if the row is decided
🚫 instead, since there is then no target state to assert:

```sh
cd tests/e2e && cargo test --test sql_command_unsupported -- --ignored --test-threads=1
```

`make e2e` runs only the passing set, so the gap map never blocks CI.
