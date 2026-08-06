# Compliance

## Overview

Compliance rules, especially in highly regulated regions (like Europe), are something that every company needs to implement by itself on top of the storage infrastructure.
VaireDB plans to have three core features for compliance:

1. Data Pseudonymization
2. Data Take Out
3. Data Deletion [Art. 17 GDPR — Right to erasure (‘right to be forgotten’)](https://gdpr-info.eu/art-17-gdpr/)

Defining a DAG (Directed Acyclic Graph) of connected tables, features 2. and 3. create a vectorized representation of the data takeout or deletion that needs to be performed. Feature 1. implements a set of hashing algorithms for pseudonymizing specified columns. Using this last feature you can retain data in your platform for several years while protecting it from malicious usage.

## Data Pseudonymization

> **Terminology note — this is pseudonymization, not anonymization.**
> A keyed hash is *deterministic*: the same input always produces the same output. Anyone who obtains the secret key can rebuild a lookup table over a known input space (e.g. all possible emails) and recover the original values. Under GDPR (Art. 4(5)) this makes the data **pseudonymized**, not anonymized — the column is still personal data and remains in scope for the "right to be forgotten" (feature 3.). This feature reduces exposure; it does not make re-identification impossible.

### Why hash the column

A hash is a **one-way** function: given the stored digest you cannot compute the original value — you can only take a candidate value, hash it, and check whether it matches. This is what keeps the data useful for equality and joins while making the plaintext unrecoverable from the table itself.

Because the hash is also **deterministic**, equality lookups and joins on the column still work (e.g. `WHERE customer_email = <hash>`) while the plaintext is never stored at rest. The trade-off is that determinism *leaks equality*: identical values produce identical hashes, so low-entropy columns (emails, names) are vulnerable to dictionary / rainbow-table attacks by anyone who obtains the secret. The secret key is therefore the critical asset to protect.

### Secret key (pepper), not a salt

We would like to implement a special SQL DDL command that declares which fields must be pseudonymized.
The hashing uses **HMAC-SHA256** keyed with a single secret value defined in a lookup table in the coordinator catalog. This secret is a **pepper** (a single, global, secret key shared across all rows) — not a *salt* (which is unique per record). A per-record salt would break equality and joins, so a global secret is the correct choice here; we just avoid calling it a "salt".

`vairedb_catalog.anonymization_secret` table definition:

```rust
// from crates/vairedb-coordinator/src/catalog/catalog.rs
// 
type RecordTable = TableDefinition<'static, &'static str, &'static [u8]>;

// other table definitions ...
const ANONYMIZATION_SECRET_TABLE: RecordTable = TableDefinition::new("anonymization_secret");
// key string
// values:
// - algo string -- always "SHA256" (the only supported algorithm)
// - secret_key string -- the secret pepper used as the HMAC key
```

CREATE TABLE syntax:

```sql
CREATE TABLE [name] (cols ...)
WITH (
   [OPTIONS] ...
   anonymized_columns = [
      -- Target column MUST be able to hold the digest (see length rule below).
      [COLUMN_NAME] -> '[vairedb_catalog.anonymization_secret.key]',
      ...
   ]
)
```

Example:
```sql

INSERT INTO vairedb_catalog.anonymization_secret (id, algo, secret_key)
VALUES ('my_sha256_secret_id', 'HMAC-SHA256', 'my_awesome_and_secret_key');

CREATE TABLE foo_table (
    id INT,
    person_name VARCHAR(64), -- hash compatible length
    article_id INT,
    price DOUBLE PRECISION,
    customer_email VARCHAR(64) -- hash compatible length
) WITH (
    shards = 3,
    replication_factor = 2,
    shard_by = 'HASH(id)',
    anonymized_columns = [
       person_name -> 'my_sha256_secret_id',
       customer_email -> 'my_sha256_secret_id'
    ]
);
```

When a DML statement (INSERT or UPDATE) touches any column listed in the anonymized map, the coordinator retrieves the algorithm and secret key from the system table `vairedb_catalog.anonymization_secret`, computes the HMAC-SHA256 digest in-process, and substitutes the plaintext in the SQL statement with the resulting **literal hex digest**. The rewritten statement contains no plaintext and no hash function call — just the finished 64-character string.

For example:

```sql
-- considering the CREATE TABLE above:
INSERT INTO foo_table (id, person_name, article_id, price, customer_email)
VALUES ('1', 'Antony McDonald', 12, 100.00, 'antony.mcdonald@gmail.com');

-- The coordinator computes HMAC-SHA256 for each anonymized column and inlines the
-- resulting 64-character hex digest. So the rewritten statement (ready to be
-- propagated to the core node / DuckDB) will be:

INSERT INTO foo_table (id, person_name, article_id, price, customer_email)
VALUES ('1', '3fb5449f3175e3aa8cf1fa31fe880e31681583d6b5977ab183f5fc274c277eea', 12, 100.00, '506af1c81168e28f474d9cfe457d2eb8d218bacf23c49caa8a320b5d9398e003');
```

The value physically persisted for `customer_email` is that 64-char string — the original email never touches disk.

> **Why HMAC, and why a literal.** Plain concatenation (`sha256(value + secret)`) is weak: SHA-2 is susceptible to length-extension attacks, and naive concatenation is ambiguous (`"ab" + "c"` collides with `"a" + "bc"`). The correct primitive is **HMAC-SHA256** (RFC 2104), which requires byte-level XOR of the key against the `ipad`/`opad` constants — something DuckDB's native `sha256(VARCHAR)` cannot express. Rather than approximate it with a nested-`sha256` SQL rewrite, the coordinator computes a real HMAC-SHA256 using a vetted Rust implementation and inlines the finished digest. No DB-side hash function is needed.

### Where the HMAC is computed (and why not the core node)

The digest is computed **once, in the coordinator** — not in the core nodes. The coordinator is the natural anonymization boundary: it already parses and rewrites the statement, already holds the plaintext, and already owns the secret key in its catalog. Pushing the hashing down to the core nodes was considered but rejected:

- **Keeps plaintext contained.** If the core hashed, the coordinator would have to ship the *raw* plaintext over the internal wire, where it could surface in core-node memory, logs, or crash dumps. Hashing in the coordinator means only the digest ever crosses to the core.
- **Limits secret-key exposure.** The secret stays in the coordinator catalog instead of being replicated to every core node, minimizing the places it can leak.
- **Guarantees replica consistency.** With `replication_factor > 1` a value lands on multiple core nodes; hashing once in the coordinator ships a single canonical digest to all replicas, so they cannot diverge due to a secret/algorithm mismatch.
- **Negligible cost.** HMAC-SHA256 of a short field is microseconds — trivial next to storage, query execution, and replication, which already run distributed on the core nodes. There is no meaningful load to offload.

Once rewritten, the statement is propagated to the VaireDB core node and executed there like any other write.

### Column length rule

The anonymized column MUST be a string/varchar large enough to hold the digest, validated during CREATE TABLE. The HMAC-SHA256 digest is **64 characters** when hex-encoded, so the column must be at least `VARCHAR(64)`. It is not possible to anonymize other data types directly; for those, create a string/varchar column holding the stringified value and anonymize that.

### Algorithm

The only supported algorithm is [HMAC-SHA256](https://it.wikipedia.org/wiki/HMAC). Weaker hashes (SHA1, MD5) are intentionally not offered, as they are cryptographically broken.

## Data Deletion

### Overview

Data Deletion (DDR) - Delete or anonymize all user data, is a distributed operation that deletes or anonymizes all user data across the VaireDB cluster.

With this feature we store a yaml based graph of connected tables, defining the data deletion or anonymization logic.

### Request Pipeline State Machine

Considering an ID is inserted in a request tables (see below how compliance tables are structured), below the state machine of every pipeline:

- `pending`: The deletion or anonymization operation has been requested but not yet started.
- `distributed`: The deletion or anonymization operation is distributed (from coordinator) across the VaireDB's core nodes. Every core node will execute the operation on its own data.
- `executing`: The deletion or anonymization operation is currently being executed on a core node. Every command is executed asynchronously on every shard (primary and replica).
- `completed`: The deletion or anonymization operation has been completed successfully inside the core node and the result has been propagated to the coordinator.
- `failed`: The deletion or anonymization operation has failed inside the core node and the result has not been propagated to the coordinator.

### Data Deletion Tables

The data deletion tables store the compliance requests and their state. They are used to track the progress of data deletion or anonymization operations across the VaireDB cluster.

- `vairedb_catalog.compliance_requests`: Stores the compliance requests submitted by the coordinator and tracked by the pipeline state.
- `vairedb_catalog.compliance_requests_pipelines`: Stores the pipeline definition in YAML format.
- `vairedb_catalog.compliance_requests_pipeline_state`: Stores the state of the pipeline execution for each request.

```sql
CREATE TABLE vairedb_catalog.compliance_requests (
    request_id UUID PRIMARY KEY,
    request_type VARCHAR(255) NOT NULL, -- `data_deletion` or `data_take_out`
    request_closed BOOLEAN NOT NULL, -- `true` if the request has been closed, `false` otherwise
    request_closed_at TIMESTAMP WITH TIME ZONE, -- the timestamp when the request was closed
    pipeline_to_execute VARCHAR(255) NOT NULL, -- the pipeline to execute for this request
);

CREATE TABLE vairedb_catalog.compliance_requests_pipelines (
    pipeline_id UUID PRIMARY KEY,
    pipeline_definition TEXT NOT NULL, -- the pipeline definition in YAML format
);

CREATE TABLE vairedb_catalog.compliance_requests_pipeline_state (
    request_id UUID NOT NULL, -- the ID of the request
    pipeline_state VARCHAR(255) NOT NULL, -- the state of the pipeline execution
    pipeline_execution_id UUID NOT NULL, -- the ID of the pipeline execution
    pipeline_execution_started_at TIMESTAMP WITH TIME ZONE NOT NULL, -- the timestamp when the pipeline execution started
    pipeline_execution_ended_at TIMESTAMP WITH TIME ZONE, -- the timestamp when the pipeline execution ended
    PRIMARY KEY (request_id, pipeline_execution_id),
    FOREIGN KEY (request_id) REFERENCES vairedb_catalog.compliance_requests(request_id)
);
```

### Data Deletion Graph

```yaml
pipeline-name: string
graph-edges:
  [
   { "from": "sql-1", "to": "sql-3" },
   { "from": "sql-2", "to": "sql-3" }
  ]
graph-nodes:
  sql-1:
    command: "DELETE FROM my_table WHERE id IN (1,2,3)"
  sql-2:
    command: "DELETE FROM my_other_table WHERE id IN (SELECT id FROM my_table WHERE id IN (1,2,3))"
  sql-3:
    command: "DELETE FROM my_final_table WHERE id IN (SELECT id FROM my_table WHERE id IN (1,2,3))"
```

### Operations details

`command` is the point where the user defines the SQL command to be executed. Inside this commmand we can define a deletetion of the rows/data or apply an anonymization function on top of the underling data.
The anonymization process is indipendent from the pseudonymization process described above [here](COMPLIANCE.md#data-pseudonymization).

## Data Take Out

I have some doubts that this feature need to handled inside VaireDB. Potencially can be done outside VaireDB using a custom data extraction tool.
