# Column Pseudonymization

VaireDB can **pseudonymize** declared columns for compliance: their plaintext
is replaced with a keyed **HMAC-SHA256** digest **in the coordinator**, before
the write is dispatched, so the original value never reaches a core node or
touches disk.

!!! info "This is pseudonymization, not anonymization"
    A keyed hash is *deterministic* — the same input always yields the same
    output. Anyone who obtains the secret key can rebuild a lookup table over a
    known input space (for example, all possible emails) and recover the
    originals. Under [GDPR Art. 4(5)](https://gdpr-info.eu/art-4-gdpr/) this
    makes the column **pseudonymized**, not anonymized: it is still personal
    data. The feature reduces exposure; it does not make re-identification
    impossible. The secret key is the critical asset to protect.

## Why hash the column

A hash is a **one-way** function: given the stored digest you cannot compute the
original value — you can only take a candidate, hash it, and check for a match.
Because HMAC-SHA256 is also **deterministic**, equality lookups and joins on the
column keep working while the plaintext is never stored at rest:

```sql
-- Matching a known value: hash the candidate the same way and compare digests.
SELECT * FROM foo_table WHERE customer_email = '<digest of the email>';
```

The trade-off is that determinism *leaks equality*: identical values produce
identical digests, so low-entropy columns (emails, names) are vulnerable to
dictionary attacks by anyone holding the secret.

## The secret is a pepper, not a salt

Hashing uses **HMAC-SHA256** keyed with a single secret value stored in the
coordinator catalog table `vairedb_catalog.anonymization_secret`. This secret is
a **pepper** — one global key shared across all rows — not a *salt* (which is
unique per record). A per-record salt would break equality and joins, so a
global secret is the correct choice here.

Declare a secret before you reference it from a table:

```sql
INSERT INTO vairedb_catalog.anonymization_secret (id, algo, secret_key)
VALUES ('my_secret_id', 'HMAC-SHA256', 'my_awesome_and_secret_key');
```

| Field | Meaning |
|-------|---------|
| `id` | The secret id you reference from `anonymized_columns`. |
| `algo` | Must be `HMAC-SHA256` — the only supported algorithm. |
| `secret_key` | The secret pepper used as the HMAC key. |

!!! warning "Only HMAC-SHA256 is supported"
    Weaker hashes (SHA1, MD5) are intentionally not offered — they are
    cryptographically broken. A secret declaring any other `algo` is rejected at
    write time.

## Declaring anonymized columns

List the columns to pseudonymize in the `WITH (...)` clause of `CREATE TABLE`,
mapping each column to a secret id with the `->` operator:

```sql
CREATE TABLE foo_table (
    id INTEGER,
    person_name VARCHAR(64),      -- (1)!
    article_id INTEGER,
    price DOUBLE PRECISION,
    customer_email VARCHAR(64)    -- (2)!
) WITH (
    shards = 3,
    replication_factor = 2,
    shard_by = 'HASH(id)',
    anonymized_columns = [
       person_name -> 'my_secret_id',
       customer_email -> 'my_secret_id'
    ]
);
```

1. Must hold the 64-character digest — see the length rule below.
2. Column names are matched case-insensitively at write time.

!!! danger "Columns must be able to hold the digest"
    An HMAC-SHA256 digest is **64 characters** hex-encoded, so an anonymized
    column must be a string type of at least 64 characters: `VARCHAR(64)` (or
    larger), or an unbounded `VARCHAR`/`TEXT`. A bounded length below 64, or a
    non-string type, is **rejected at `CREATE TABLE`**. To pseudonymize other
    data types, store the stringified value in a string column and anonymize
    that.

## What happens on writes

When an `INSERT` or `UPDATE` touches an anonymized column, the coordinator
resolves the algorithm and key from `vairedb_catalog.anonymization_secret`,
computes the HMAC-SHA256 digest in-process, and substitutes the plaintext with
the resulting **literal 64-character hex digest**. The statement that leaves the
coordinator contains no plaintext and no hash-function call.

```sql
-- What the client sends:
INSERT INTO foo_table (id, person_name, article_id, price, customer_email)
VALUES ('1', 'Antony McDonald', 12, 100.00, 'antony.mcdonald@gmail.com');

-- What the coordinator dispatches to the core node (DuckDB):
INSERT INTO foo_table (id, person_name, article_id, price, customer_email)
VALUES ('1', '3fb5449f3175e3aa8cf1fa31fe880e31681583d6b5977ab183f5fc274c277eea',
        12, 100.00,
        '506af1c81168e28f474d9cfe457d2eb8d218bacf23c49caa8a320b5d9398e003');
```

The value physically persisted for `customer_email` is that 64-char string — the
original email never touches disk.

### Behavior rules

| Case | Behavior |
|------|----------|
| Non-anonymized columns | Left untouched. |
| `NULL` value | Preserved as `NULL` — a hash of "nothing" would defeat nullability. |
| Column case mismatch (`EMAIL` vs `email`) | Still hashed — matching is case-insensitive. |
| Missing / unknown secret id | Statement rejected with a client-facing error. |
| Unsupported `algo` on the secret | Statement rejected. |
| Bind parameter (`$1`) in an anonymized column | Rejected — the value must be a literal, or it would reach the node unhashed. |
| Non-literal expression | Rejected for the same reason. |
| `INSERT ... SELECT` | Rejected — anonymized columns require `INSERT ... VALUES`. |

## Why the coordinator, and why a literal

The digest is computed **once, in the coordinator** — not in the core nodes. The
coordinator is the natural anonymization boundary: it already parses and
rewrites the statement, already holds the plaintext, and already owns the secret
key in its catalog. Computing it there:

- **Keeps plaintext contained.** Only the digest ever crosses the internal wire;
  the raw value never lands in core-node memory, logs, or crash dumps.
- **Limits secret-key exposure.** The secret stays in the coordinator catalog
  instead of being replicated to every core node.
- **Guarantees replica consistency.** With `replication_factor > 1`, hashing
  once ships a single canonical digest to all replicas, so they cannot diverge
  from a secret/algorithm mismatch.
- **Costs almost nothing.** HMAC-SHA256 of a short field is microseconds —
  trivial next to storage, execution, and replication.

Inlining a **literal** digest (rather than a SQL hash call) also matters
cryptographically: the correct primitive is **HMAC-SHA256**
([RFC 2104](https://www.rfc-editor.org/rfc/rfc2104)), which XORs the key against
the `ipad`/`opad` constants — something DuckDB's native `sha256(VARCHAR)` cannot
express, and which a naive `sha256(value + secret)` concatenation gets wrong
(SHA-2 is susceptible to length-extension attacks). The coordinator computes a
real HMAC-SHA256 with a vetted implementation and inlines the finished digest,
so no DB-side hash function is needed.

!!! note "Part of the compliance roadmap"
    Column pseudonymization is the first of VaireDB's planned compliance
    features, alongside data take-out (export) and deletion
    ([right to be forgotten](https://gdpr-info.eu/art-17-gdpr/)). See the
    [Roadmap](../roadmap.md).
