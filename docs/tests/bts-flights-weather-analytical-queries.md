# Analytical query suite — BTS flights + weather

A differential test of VaireDB's **read path** over a real dataset: 48 analytical queries built
around window functions, non-trivial aggregation, joins, subqueries and set operations, each run
on VaireDB and on one DuckDB instance, compared cell by cell, and then timed on both.

| | |
|---|---|
| Suite version | `1.1.0` — bump on any change to a query, a rule, or the runner's comparison |
| Dataset | `data/datasets/Arimancy/bts-flights-weather/flights_1m.parquet` — the first 1,000,000 rows of 2019, 211 columns |
| DDL | `data/datasets/Arimancy/bts-flights-weather/flights_weather.postgres.sql` (5 shards, RF 1, `shard_by = 'sched_dep_utc'`) |
| Runner | [`bts_flights_weather_suite.py`](bts_flights_weather_suite.py), beside this file |
| Performance reports | [`results/`](results/) — one file per run, `bts-flights-weather-perf-<UTC timestamp>.md` |

## The test pattern

```text
            ┌─► VaireDB ──────────────────────────────► result A ─┐
SQL block ──┼─► PostgreSQL 16: EXPLAIN (schema, no rows) ─► valid? │   compare A with B
            └─► DuckDB, the same text ───────────────────► result B ─┴─► MATCH · DIFF · ERR

then, for every Q and B block both engines answered:
  1 warm-up + N timed runs on VaireDB and on DuckDB ─► results/bts-flights-weather-perf-<timestamp>.md
```

1. **Results are compared between VaireDB and DuckDB only.** DuckDB computes each query as a
   single node, in one process. In VaireDB, DuckDB only scans the shards; every aggregate and
   window runs in DataFusion, distributed over Ballista. A MATCH says that the distributed plan
   reached the answer a single engine reaches.
2. **PostgreSQL checks syntax only; it is not a result oracle.** Each block is `EXPLAIN`ed on a
   PostgreSQL 16 server that holds the schema and no rows, so it is parsed, analyzed (names,
   types, function signatures) and planned, but never executed. PostgreSQL 16 is the contract
   (`AGENTS.md`). A block this check rejects is not a PostgreSQL statement and does not belong in
   the suite. That rule keeps DuckDB-only forms out, even though DuckDB is the result oracle.
3. **One pattern per block:** SQL → VaireDB, syntax-checked by PostgreSQL → the same text on
   DuckDB → the two results compared.
4. **Performance: one DuckDB instance against the distributed cluster,** on the same host and
   the same slice. A block is timed only when both engines answered it. Each run writes its own
   report, stamped with the UTC time, the suite version and the version of every engine (§ 6).

Why this dataset is a good stress for the distributed path: `flights_weather` is hash-sharded on
`sched_dep_utc`, and no query below groups or partitions by it. So every `GROUP BY` and every
`PARTITION BY` needs a shuffle, nothing can be answered shard-locally, and each shard only ever
runs `SELECT <cols> FROM flights_weather_shardN [WHERE …]`.

---

## 1 — Setup

### 1.1 VaireDB

```bash
make vdb-cluster-up      # 1 coordinator + 5 cores on 127.0.0.1:5432

# the coordinator reads COPY paths from its own filesystem
docker exec vairedb-e2e-coordinator mkdir -p /app/data/datasets/Arimancy/bts-flights-weather
docker cp data/datasets/Arimancy/bts-flights-weather/flights_1m.parquet \
  vairedb-e2e-coordinator:/app/data/datasets/Arimancy/bts-flights-weather/

# CREATE TABLE only (lines 47-298 of the DDL file); the constraints and indexes are not needed for reads
sed -n '47,298p' data/datasets/Arimancy/bts-flights-weather/flights_weather.postgres.sql \
  | psql -h 127.0.0.1 -U postgres -v ON_ERROR_STOP=1
psql -h 127.0.0.1 -U postgres -c "COPY flights_weather FROM '/app/data/datasets/Arimancy/bts-flights-weather/flights_1m.parquet' (FORMAT PARQUET)"
# COPY 1000000 — about 95 s
```

### 1.2 DuckDB

```bash
duckdb "$TMPDIR/flights_oracle.duckdb" -c "CREATE TABLE flights_weather AS
  SELECT * FROM read_parquet('data/datasets/Arimancy/bts-flights-weather/flights_1m.parquet')"
```

The runner opens every DuckDB session with two of PostgreSQL's conventions where DuckDB's
defaults differ. `SET TimeZone = 'UTC'` matches the cluster's `Etc/UTC` session; no query emits a
`timestamptz`, so that one is hygiene. `SET default_null_order =
'nulls_last_on_asc_first_on_desc'` makes NULLs sort first under `DESC`, as in PostgreSQL and
VaireDB, where DuckDB would put them last (§ 2.1). The runner also pins DuckDB's `threads` (§ 6).

### 1.3 PostgreSQL syntax checker

```bash
docker run -d --name vdb-flights-pg-oracle -e POSTGRES_HOST_AUTH_METHOD=trust -p 5433:5432 postgres:16.15
sed -n '47,298p' data/datasets/Arimancy/bts-flights-weather/flights_weather.postgres.sql \
  | sed 's/) WITH (shards = 5.*);/);/' | psql -h 127.0.0.1 -p 5433 -U postgres -v ON_ERROR_STOP=1
```

Only the schema is needed: `EXPLAIN` never reads a row. The `sed` strips VaireDB's `WITH (shards …)`
clause, which is the one part of the DDL that is not PostgreSQL.

### 1.4 Run

```bash
python3 -I docs/tests/bts_flights_weather_suite.py --duckdb "$TMPDIR/flights_oracle.duckdb"
python3 -I docs/tests/bts_flights_weather_suite.py --duckdb … --only Q24,Q25 --no-perf   # a subset, no timing
```

| Flag | Default | |
|---|---|---|
| `--runs N` | `5` | timed runs per engine and block, after one warm-up |
| `--duckdb-threads` | `docker` | `docker` gives DuckDB as many threads as the Docker VM has CPUs, the budget the whole cluster shares; `default` leaves DuckDB's own setting, or pass a number |
| `--no-perf` | off | results and syntax only, no report |
| `--report-dir` | `docs/tests/results` | where the timestamped report is written |
| `--vdb-port`, `--pg-port` | `5432`, `5433` | |

The runner prints one verdict per block, then the timings, then the report's path. It exits
non-zero while any Q or B block disagrees or fails the syntax check — today that is the findings
in § 7 — so read a run against § 5 rather than against exit code 0.

---

## 2 — Comparing answers

### 2.1 Why not `diff`

A text diff of the two outputs reports a difference on almost every query, and almost none of
them is a wrong answer. Each hazard below was measured on this slice. The suite avoids the ones
that change a **value**, and the runner absorbs the ones that change only the **spelling**.

| Hazard | DuckDB | VaireDB | Handled by |
|---|---|---|---|
| `7 / 2` on integers | `3.5` | `3` (PostgreSQL's integer division) | **the queries** — no integer is divided; every ratio casts to `double precision` first |
| bare `NUMERIC` | `DECIMAL(18,3)`, rounds to 3 places | arbitrary precision | **the queries** — no cast to a bare `numeric` |
| `round(x::numeric(18,6), 2)` | `1.23` | `1.230000` | **the queries** — no `round()`; the divergence is finding F5 |
| float `sum`/`avg` | one summation order | partial aggregates merged in shuffle order | **the runner** — relative tolerance `1e-9` |
| `avg(bigint)` | `double` | `numeric`, 16 places | **the runner** — numeric comparison |
| boolean | `true` / `false` | `t` / `f` | **the runner** |
| NULL in CSV | `NULL` | empty | **the runner** — `duckdb -nullvalue ''` |
| `timestamptz` | the session zone | the session zone (`Etc/UTC`) | **the queries** — none emits one; dates and local timestamps only |
| NULLs under `ORDER BY … DESC` | last | first (PostgreSQL's default) | **the runner** — PostgreSQL's NULL order is set on DuckDB (§ 1.2); a query that cares spells `NULLS FIRST\|LAST` |
| ties | engine-specific | partition-order specific | **the queries** — see the rule below |

A cell matches when the strings are equal, when both are the same boolean (`t` ≡ `true`), or
when both parse as numbers within a relative or absolute tolerance of `1e-9`. Column labels and
row order must match exactly.

**Ties.** Every ordering that feeds `row_number`, `ntile`, `lag`/`lead`, `first_value`/`nth_value`
or a `LIMIT`, and every final `ORDER BY`, ends on a **total** key. The natural key is
`(operating_carrier, flight_number, origin_iata, dest_iata, sched_dep_utc)`. Time alone is not
total even per tail: 100 `(tail_number, sched_dep_utc)` pairs repeat in this slice, so a per-tail
rotation ordered only by time would make `lag` pick an arbitrary predecessor, and the engines
would disagree without either being wrong.

### 2.2 Where DuckDB is not a valid oracle

Passing the syntax check does not make DuckDB's answer PostgreSQL's. A few valid PostgreSQL forms
have different semantics in DuckDB. A MATCH on one of them would only show agreement with
DuckDB, so they are kept out of the suite:

- integer `/` — avoided in every query (§ 2.1);
- `GROUP BY GROUPING SETS ((), ())` — DuckDB answers one grand-total row where PostgreSQL defines
  one per empty set; it is probe P04;
- hypothetical-set aggregates (`rank(30) WITHIN GROUP (ORDER BY x)`) — DuckDB has none, so
  there is nothing to compare; it is probe P03b;
- `~` with an unanchored pattern — DuckDB's `~` matches the *whole* string, PostgreSQL's
  any part of it (`'N123AB' ~ '[0-9]+'` is false in DuckDB, true in PostgreSQL and VaireDB), so
  the suite only writes anchored patterns (Q48).

### 2.3 Writing a new query here

1. It must pass the **PostgreSQL syntax check**. No DuckDB-only form (`QUALIFY`, `PIVOT`,
   `GROUP BY ALL`, `//`, `IGNORE NULLS`, unquoted `INTERVAL 1 DAY`), and none of § 2.2's forms.
2. Alias every computed output column — the runner compares labels too.
3. A total `ORDER BY`, and total orderings inside windows (§ 2.1); spell `NULLS FIRST|LAST` on a
   nullable sort key whenever the placement changes which rows a `LIMIT` keeps.
4. No integer division, no bare `numeric`, no `round()`, no `timestamptz` in the output, and
   no unanchored `~`.
5. Bounded output: aggregate, or `LIMIT` behind a total order.
6. Bump the suite version: the reports in `results/` are only comparable within one version.

---

## 3 — Query catalog

Every query here passes the PostgreSQL syntax check and is answered by DuckDB; § 5 has
VaireDB's verdict on each, § 6 the timings.

### A. Grouped aggregation

#### Q01 — Carrier scorecard

`FILTER` on aggregates, `percentile_cont`, `stddev_samp`, and two windows evaluated **over the
aggregates** (a share of the grand total and a rank).

```sql
-- Q01 carrier scorecard
SELECT operating_carrier,
       count(*)                                                    AS flights,
       count(*) FILTER (WHERE cancelled)                           AS cancelled,
       count(*) FILTER (WHERE arr_del15)                           AS delayed_15,
       count(*) FILTER (WHERE NOT arr_del15)::double precision
           / count(arr_del15)                                      AS on_time_rate,
       avg(arr_delay_min)                                          AS avg_arr_delay,
       percentile_cont(0.5)  WITHIN GROUP (ORDER BY arr_delay_min) AS p50_arr_delay,
       percentile_cont(0.95) WITHIN GROUP (ORDER BY arr_delay_min) AS p95_arr_delay,
       stddev_samp(arr_delay_min)                                  AS sd_arr_delay,
       count(*)::double precision / sum(count(*)) OVER ()          AS share_of_flights,
       rank() OVER (ORDER BY count(*) FILTER (WHERE NOT arr_del15)::double precision
                             / count(arr_del15) DESC)              AS on_time_rank
FROM flights_weather
GROUP BY operating_carrier
ORDER BY operating_carrier;
```

#### Q02 — Regression family: arrival delay on departure delay

All of `regr_*`, both covariances, `corr`, and the population/sample variance pair. These are
the aggregates whose partial states merge least trivially across shards.

```sql
-- Q02 regression family per carrier
SELECT operating_carrier,
       regr_count(arr_delay_min, dep_delay_min)     AS n,
       regr_slope(arr_delay_min, dep_delay_min)     AS slope,
       regr_intercept(arr_delay_min, dep_delay_min) AS intercept,
       regr_r2(arr_delay_min, dep_delay_min)        AS r2,
       regr_avgx(arr_delay_min, dep_delay_min)      AS avg_dep,
       regr_avgy(arr_delay_min, dep_delay_min)      AS avg_arr,
       regr_sxx(arr_delay_min, dep_delay_min)       AS sxx,
       regr_sxy(arr_delay_min, dep_delay_min)       AS sxy,
       regr_syy(arr_delay_min, dep_delay_min)       AS syy,
       covar_pop(arr_delay_min, dep_delay_min)      AS covar_pop,
       covar_samp(arr_delay_min, dep_delay_min)     AS covar_samp,
       corr(arr_delay_min, dep_delay_min)           AS corr,
       var_pop(taxi_out_min)                        AS var_pop_taxi_out,
       var_samp(taxi_out_min)                       AS var_samp_taxi_out,
       stddev_pop(taxi_out_min)                     AS sd_pop_taxi_out
FROM flights_weather
GROUP BY operating_carrier
ORDER BY operating_carrier;
```

#### Q03 — Hourly departure profile at ATL

Ordered-set aggregates (`mode`, `percentile_disc`) and the boolean aggregates.

```sql
-- Q03 hourly departure profile at ATL
SELECT extract(hour FROM sched_dep_local)::int                     AS dep_hour,
       count(*)                                                    AS departures,
       mode() WITHIN GROUP (ORDER BY operating_carrier)            AS dominant_carrier,
       percentile_disc(0.5) WITHIN GROUP (ORDER BY dep_delay_min)  AS median_dep_delay,
       percentile_disc(0.9) WITHIN GROUP (ORDER BY dep_delay_min)  AS p90_dep_delay,
       bool_or(wx_origin_thunderstorm)                             AS any_thunderstorm,
       bool_and(NOT cancelled)                                     AS none_cancelled,
       avg(origin_sched_departures_this_hour)                      AS avg_sched_this_hour
FROM flights_weather
WHERE origin_iata = 'ATL'
GROUP BY 1
ORDER BY 1;
```

#### Q04 — Delay-cause attribution

An unpivot written as `UNION ALL` (PostgreSQL has no `UNPIVOT`), a window share and
`row_number` per carrier, folded back by an **ordered** `string_agg`.

```sql
-- Q04 delay cause attribution
WITH s AS (
    SELECT operating_carrier,
           sum(reported_carrier_delay_min)       AS carrier,
           sum(reported_weather_delay_min)       AS weather,
           sum(reported_nas_delay_min)           AS nas,
           sum(reported_security_delay_min)      AS security,
           sum(reported_late_aircraft_delay_min) AS late_aircraft
    FROM flights_weather
    GROUP BY operating_carrier
),
causes AS (
              SELECT operating_carrier, 'carrier'       AS cause, carrier       AS minutes FROM s
    UNION ALL SELECT operating_carrier, 'weather',                weather                  FROM s
    UNION ALL SELECT operating_carrier, 'nas',                    nas                      FROM s
    UNION ALL SELECT operating_carrier, 'security',               security                 FROM s
    UNION ALL SELECT operating_carrier, 'late_aircraft',          late_aircraft            FROM s
),
ranked AS (
    SELECT operating_carrier, cause, minutes,
           minutes / sum(minutes) OVER (PARTITION BY operating_carrier)                    AS share,
           row_number() OVER (PARTITION BY operating_carrier ORDER BY minutes DESC, cause) AS rn
    FROM causes
)
SELECT operating_carrier,
       string_agg(cause, ' > ' ORDER BY rn)  AS cause_ranking,
       max(share) FILTER (WHERE rn = 1)      AS top_cause_share,
       sum(minutes)                          AS total_delay_minutes
FROM ranked
GROUP BY operating_carrier
ORDER BY operating_carrier;
```

#### Q05 — Origin × destination weather matrix

`FILTER` as the PostgreSQL spelling of `PIVOT`, with a rank over an aggregate and an `ORDER BY`
on an aggregate that is not projected.

```sql
-- Q05 origin x destination weather matrix
SELECT wx_origin_flight_category                                          AS origin_category,
       count(*)                                                           AS flights,
       avg(arr_delay_min) FILTER (WHERE wx_dest_flight_category = 'VFR')  AS dest_vfr,
       avg(arr_delay_min) FILTER (WHERE wx_dest_flight_category = 'MVFR') AS dest_mvfr,
       avg(arr_delay_min) FILTER (WHERE wx_dest_flight_category = 'IFR')  AS dest_ifr,
       avg(arr_delay_min) FILTER (WHERE wx_dest_flight_category = 'LIFR') AS dest_lifr,
       rank() OVER (ORDER BY avg(arr_delay_min) DESC)                     AS delay_rank
FROM flights_weather
WHERE wx_origin_flight_category IS NOT NULL
GROUP BY wx_origin_flight_category
ORDER BY min(wx_origin_flight_category_ordinal);
```

### B. Grouping sets

#### Q06 — `ROLLUP` over flight category × day part

Multi-argument `grouping()` as a bitmask, and `NULLS LAST` ordering of the subtotal rows.

```sql
-- Q06 ROLLUP with a grouping() bitmask
WITH f AS (
    SELECT wx_origin_flight_category AS category,
           CASE WHEN extract(hour FROM sched_dep_local) < 6  THEN '1-night'
                WHEN extract(hour FROM sched_dep_local) < 12 THEN '2-morning'
                WHEN extract(hour FROM sched_dep_local) < 18 THEN '3-afternoon'
                ELSE '4-evening' END AS day_part,
           dep_delay_min, cancelled
    FROM flights_weather
    WHERE wx_origin_flight_category IS NOT NULL
)
SELECT category,
       day_part,
       grouping(category, day_part)                                    AS grp,
       count(*)                                                        AS flights,
       avg(dep_delay_min)                                              AS avg_dep_delay,
       count(*) FILTER (WHERE cancelled)::double precision / count(*)  AS cancel_rate
FROM f
GROUP BY ROLLUP (category, day_part)
ORDER BY grp, category NULLS LAST, day_part NULLS LAST;
```

#### Q07 — `CUBE` over carrier × cancellation reason

A window **partitioned by `grouping()`**, so each grouping set's shares sum to 1 on their own.

```sql
-- Q07 CUBE with a window partitioned by grouping()
SELECT operating_carrier,
       cancellation_reason,
       grouping(operating_carrier)   AS g_carrier,
       grouping(cancellation_reason) AS g_reason,
       count(*)                      AS cancellations,
       count(*)::double precision
           / sum(count(*)) OVER (PARTITION BY grouping(operating_carrier), grouping(cancellation_reason))
                                     AS share_within_set
FROM flights_weather
WHERE cancelled
GROUP BY CUBE (operating_carrier, cancellation_reason)
ORDER BY g_carrier, g_reason, operating_carrier NULLS LAST, cancellation_reason NULLS LAST;
```

### C. Ranking and distribution windows

#### Q08 — Top-3 routes at the ten busiest origins

`dense_rank` per partition, a partition total beside it, and an `IN` subquery with
`ORDER BY … LIMIT`.

```sql
-- Q08 top-3 routes at the 10 busiest origins
WITH routes AS (
    SELECT origin_iata, dest_iata,
           count(*)           AS flights,
           avg(dep_delay_min) AS avg_dep_delay
    FROM flights_weather
    WHERE NOT cancelled
    GROUP BY origin_iata, dest_iata
),
ranked AS (
    SELECT origin_iata, dest_iata, flights, avg_dep_delay,
           dense_rank() OVER (PARTITION BY origin_iata ORDER BY flights DESC)        AS route_rank,
           sum(flights) OVER (PARTITION BY origin_iata)                             AS origin_flights,
           flights::double precision / sum(flights) OVER (PARTITION BY origin_iata) AS route_share
    FROM routes
)
SELECT origin_iata, dest_iata, flights, origin_flights, route_rank, route_share, avg_dep_delay
FROM ranked
WHERE route_rank <= 3
  AND origin_iata IN (SELECT origin_iata FROM routes
                      GROUP BY origin_iata
                      ORDER BY sum(flights) DESC, origin_iata
                      LIMIT 10)
ORDER BY origin_flights DESC, origin_iata, route_rank, dest_iata;
```

#### Q09 — Distance deciles

`ntile(10)` over ~980k rows in **one** unpartitioned window — the whole input through a single
sort — then aggregated per decile.

```sql
-- Q09 distance deciles with ntile over the whole table
WITH f AS (
    SELECT distance_miles, air_time_min, arr_delay_min,
           ntile(10) OVER (ORDER BY distance_miles, sched_dep_utc, operating_carrier,
                                    flight_number, origin_iata, dest_iata) AS decile
    FROM flights_weather
    WHERE NOT cancelled AND NOT diverted
)
SELECT decile,
       count(*)                                                    AS flights,
       min(distance_miles)                                         AS min_miles,
       max(distance_miles)                                         AS max_miles,
       avg(arr_delay_min)                                          AS avg_arr_delay,
       avg(distance_miles / nullif(air_time_min, 0) * 60)          AS avg_speed_mph,
       percentile_disc(0.5) WITHIN GROUP (ORDER BY air_time_min)   AS median_air_time
FROM f
GROUP BY decile
ORDER BY decile;
```

#### Q10 — Airports by taxi-out time

The four distribution functions on one named window, over a `HAVING`-filtered aggregate.

```sql
-- Q10 airports by taxi-out
WITH a AS (
    SELECT origin_iata,
           count(*)                                                  AS departures,
           avg(taxi_out_min)                                         AS avg_taxi_out,
           percentile_cont(0.9) WITHIN GROUP (ORDER BY taxi_out_min) AS p90_taxi_out
    FROM flights_weather
    WHERE taxi_out_min IS NOT NULL
    GROUP BY origin_iata
    HAVING count(*) >= 2000
)
SELECT origin_iata, departures, avg_taxi_out, p90_taxi_out,
       rank()         OVER w AS taxi_rank,
       percent_rank() OVER w AS taxi_percent_rank,
       cume_dist()    OVER w AS taxi_cume_dist,
       ntile(4)       OVER w AS taxi_quartile
FROM a
WINDOW w AS (ORDER BY avg_taxi_out DESC, origin_iata)
ORDER BY taxi_rank;
```

#### Q11 — Routes worse than their carrier

A join of two aggregates at different grains, a z-score, and a rank ordered by an expression
over both sides of the join.

```sql
-- Q11 routes worse than their carrier
WITH route AS (
    SELECT operating_carrier, origin_iata, dest_iata,
           count(*) AS flights, avg(arr_delay_min) AS avg_arr_delay
    FROM flights_weather
    GROUP BY operating_carrier, origin_iata, dest_iata
    HAVING count(*) >= 200
),
carrier AS (
    SELECT operating_carrier, avg(arr_delay_min) AS avg_arr_delay, stddev_samp(arr_delay_min) AS sd_arr_delay
    FROM flights_weather
    GROUP BY operating_carrier
),
scored AS (
    SELECT r.operating_carrier, r.origin_iata, r.dest_iata, r.flights, r.avg_arr_delay,
           r.avg_arr_delay - c.avg_arr_delay                     AS excess_delay,
           (r.avg_arr_delay - c.avg_arr_delay) / c.sd_arr_delay  AS z,
           rank() OVER (PARTITION BY r.operating_carrier
                        ORDER BY r.avg_arr_delay - c.avg_arr_delay DESC) AS worst_rank
    FROM route r
    JOIN carrier c ON c.operating_carrier = r.operating_carrier
    WHERE r.avg_arr_delay > c.avg_arr_delay
)
SELECT operating_carrier, origin_iata, dest_iata, flights, avg_arr_delay, excess_delay, z, worst_rank
FROM scored
WHERE worst_rank <= 2
ORDER BY operating_carrier, worst_rank, origin_iata, dest_iata;
```

#### Q12 — Shares within carrier and within airport

Aggregates **nested inside** windows — `sum(count(*)) OVER`, `avg(avg(…)) OVER` — over two
different partitionings of the same grouped rows, after a `HAVING`.

```sql
-- Q12 aggregates nested in windows
SELECT operating_carrier, origin_iata,
       count(*)                                                                         AS flights,
       count(*)::double precision / sum(count(*)) OVER (PARTITION BY operating_carrier) AS share_of_carrier,
       count(*)::double precision / sum(count(*)) OVER (PARTITION BY origin_iata)       AS share_of_airport,
       rank() OVER (PARTITION BY origin_iata ORDER BY count(*) DESC)                    AS carrier_rank_at_airport,
       avg(avg(dep_delay_min)) OVER (PARTITION BY operating_carrier)                    AS carrier_mean_of_airport_means
FROM flights_weather
GROUP BY operating_carrier, origin_iata
HAVING count(*) >= 1000
ORDER BY operating_carrier, origin_iata;
```

#### Q13 — Route podium per carrier

`first_value`, `nth_value` and `last_value` over an explicit full frame (the default frame would
make `last_value` the current row), collapsed by `DISTINCT`.

```sql
-- Q13 route podium
WITH r AS (
    SELECT operating_carrier, origin_iata || '-' || dest_iata AS route, count(*) AS flights
    FROM flights_weather
    GROUP BY operating_carrier, origin_iata, dest_iata
)
SELECT DISTINCT operating_carrier,
       first_value(route)  OVER w AS route_1,
       nth_value(route, 2) OVER w AS route_2,
       nth_value(route, 3) OVER w AS route_3,
       last_value(route)   OVER w AS least_flown_route,
       count(*)            OVER w AS routes
FROM r
WINDOW w AS (PARTITION BY operating_carrier ORDER BY flights DESC, route
             ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
ORDER BY operating_carrier;
```

### D. Frames

#### Q14 — Daily network volume: `ROWS` vs `RANGE`

A 7-row moving average next to a 7-**day** `RANGE` sum (they differ wherever a date is missing),
a running total, and a fixed-offset `lag`.

```sql
-- Q14 daily network volume, ROWS vs RANGE frames
WITH daily AS (
    SELECT flight_date_local_origin AS d,
           count(*)                          AS flights,
           count(*) FILTER (WHERE cancelled) AS cancelled
    FROM flights_weather
    GROUP BY flight_date_local_origin
)
SELECT d,
       flights,
       avg(flights) OVER (ORDER BY d ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)                  AS flights_ma7_rows,
       sum(flights) OVER (ORDER BY d RANGE BETWEEN INTERVAL '6 days' PRECEDING AND CURRENT ROW) AS flights_7d_range,
       sum(cancelled) OVER (ORDER BY d ROWS UNBOUNDED PRECEDING)                                AS cancelled_ytd,
       flights - lag(flights, 7) OVER (ORDER BY d)                                              AS delta_vs_week_before
FROM daily
ORDER BY d;
```

#### Q15 — `GROUPS` frame: peers are whole days

Carrier-day rows ordered by day only, so every day is a peer group of ~17 rows. `GROUPS`
counts days, `ROWS` would count carrier-rows; a `RANGE` interval frame and a
`1 FOLLOWING AND 1 FOLLOWING` frame sit beside it.

```sql
-- Q15 GROUPS frame over peer groups
WITH cd AS (
    SELECT flight_date_local_origin AS d, operating_carrier, count(*) AS flights
    FROM flights_weather
    WHERE flight_date_local_origin BETWEEN DATE '2019-07-01' AND DATE '2019-07-10'
    GROUP BY flight_date_local_origin, operating_carrier
)
SELECT d, operating_carrier, flights,
       sum(flights) OVER (ORDER BY d GROUPS BETWEEN 2 PRECEDING AND CURRENT ROW)       AS network_3d,
       sum(flights) OVER (PARTITION BY operating_carrier ORDER BY d
                          RANGE BETWEEN INTERVAL '2 days' PRECEDING AND CURRENT ROW)  AS carrier_3d,
       count(*)     OVER (ORDER BY d GROUPS CURRENT ROW)                              AS carriers_that_day,
       sum(flights) OVER (ORDER BY d GROUPS BETWEEN 1 FOLLOWING AND 1 FOLLOWING)      AS network_next_day
FROM cd
ORDER BY d, operating_carrier;
```

#### Q16 — Rolling 60-minute departure bank

A `RANGE` frame with an `INTERVAL` offset over a `timestamptz`, partitioned per airport, on
~440k raw rows — then aggregated, including a percentile of the window's output.

```sql
-- Q16 rolling 60-minute departure bank
WITH dep AS (
    SELECT origin_iata,
           count(*) OVER (PARTITION BY origin_iata ORDER BY sched_dep_utc
                          RANGE BETWEEN INTERVAL '59 minutes' PRECEDING AND CURRENT ROW) AS deps_last_hour
    FROM flights_weather
    WHERE origin_iata IN ('ATL', 'IAH', 'SFO', 'DFW', 'DCA')
)
SELECT origin_iata,
       max(deps_last_hour)                                          AS peak_rolling_hour,
       avg(deps_last_hour)                                          AS avg_rolling_hour,
       percentile_cont(0.99) WITHIN GROUP (ORDER BY deps_last_hour) AS p99_rolling_hour
FROM dep
GROUP BY origin_iata
ORDER BY origin_iata;
```

### E. Offsets and sequences

#### Q17 — Month over month per carrier

`lag`/`lead` and `first_value` on one named window, beside a rank on a different partitioning.

```sql
-- Q17 month over month per carrier, named window
WITH monthly AS (
    SELECT operating_carrier, month,
           count(*)           AS flights,
           avg(arr_delay_min) AS avg_arr_delay
    FROM flights_weather
    GROUP BY operating_carrier, month
)
SELECT operating_carrier,
       month,
       flights,
       avg_arr_delay,
       lag(flights)  OVER w                                                    AS prev_flights,
       lead(flights) OVER w                                                    AS next_flights,
       (flights - lag(flights) OVER w)::double precision / lag(flights) OVER w AS mom_growth,
       avg_arr_delay - first_value(avg_arr_delay) OVER w                       AS delay_vs_first_month,
       rank() OVER (PARTITION BY month ORDER BY avg_arr_delay)                 AS delay_rank_in_month
FROM monthly
WINDOW w AS (PARTITION BY operating_carrier ORDER BY month)
ORDER BY operating_carrier, month;
```

#### Q18 — Tail rotation: delay propagation

`lag` per aircraft over ~975k raw rows (5,849 partitions), checked against the rotation's
continuity, then fed to `corr` and `regr_slope`.

```sql
-- Q18 tail rotation: delay propagation through lag()
WITH legs AS (
    SELECT operating_carrier, origin_iata, dep_delay_min,
           lag(arr_delay_min) OVER rot AS inbound_arr_delay,
           lag(dest_iata)     OVER rot AS inbound_dest
    FROM flights_weather
    WHERE tail_number IS NOT NULL AND NOT cancelled AND NOT diverted
    WINDOW rot AS (PARTITION BY tail_number
                   ORDER BY sched_dep_utc, operating_carrier, flight_number, origin_iata, dest_iata)
)
SELECT operating_carrier,
       count(*)                                                  AS legs,
       count(*) FILTER (WHERE inbound_dest = origin_iata)        AS continuous_legs,
       count(*) FILTER (WHERE inbound_dest <> origin_iata)       AS broken_legs,
       corr(dep_delay_min, inbound_arr_delay)                    AS corr_inbound_vs_dep,
       regr_slope(dep_delay_min, inbound_arr_delay)              AS propagation_slope,
       avg(dep_delay_min) FILTER (WHERE inbound_arr_delay >= 60) AS dep_delay_after_late_inbound
FROM legs
GROUP BY operating_carrier
ORDER BY operating_carrier;
```

#### Q19 — Holiday proximity

`lag` over an aggregate (`lag(avg(…)) OVER`) and a share of the total, over `CASE` buckets.

```sql
-- Q19 holiday proximity buckets
WITH b AS (
    SELECT CASE WHEN days_to_nearest_holiday = 0  THEN 0
                WHEN days_to_nearest_holiday <= 2  THEN 1
                WHEN days_to_nearest_holiday <= 5  THEN 2
                WHEN days_to_nearest_holiday <= 14 THEN 3
                ELSE 4 END AS bucket,
           is_holiday_travel_window, arr_del15, arr_delay_min, origin_sched_departures_this_day
    FROM flights_weather
)
SELECT bucket,
       count(*)                                                               AS flights,
       count(*) FILTER (WHERE is_holiday_travel_window)                       AS in_travel_window,
       bool_or(is_holiday_travel_window)                                      AS any_travel_window,
       count(*) FILTER (WHERE arr_del15)::double precision / count(arr_del15) AS delay_rate,
       avg(arr_delay_min)                                                     AS avg_arr_delay,
       avg(origin_sched_departures_this_day)                                  AS avg_daily_departures,
       count(*)::double precision / sum(count(*)) OVER ()                     AS share_of_flights,
       avg(arr_delay_min) - lag(avg(arr_delay_min)) OVER (ORDER BY bucket)    AS delay_step
FROM b
GROUP BY bucket
ORDER BY bucket;
```

#### Q20 — Congestion vs taxi-out

A bucket computed without integer division (`/ 10.0`, never `/ 10` — § 2.1), a marginal
`lag` and a cumulative share built from two windows over `count(*)`.

```sql
-- Q20 congestion buckets vs taxi-out
WITH b AS (
    SELECT floor(origin_sched_departures_this_hour / 10.0)::int * 10 AS bucket_lo,
           taxi_out_min
    FROM flights_weather
    WHERE taxi_out_min IS NOT NULL
)
SELECT bucket_lo,
       count(*)                                                             AS flights,
       avg(taxi_out_min)                                                    AS avg_taxi_out,
       percentile_cont(0.75) WITHIN GROUP (ORDER BY taxi_out_min)           AS p75_taxi_out,
       avg(taxi_out_min) - lag(avg(taxi_out_min)) OVER (ORDER BY bucket_lo) AS marginal_taxi_out,
       sum(count(*)) OVER (ORDER BY bucket_lo)::double precision
           / sum(count(*)) OVER ()                                          AS cumulative_share
FROM b
GROUP BY bucket_lo
ORDER BY bucket_lo;
```

#### Q21 — Weather trend vs three hours earlier

A windowed `FILTER` on null-skipping aggregates (`max(…) FILTER (WHERE …) OVER ()`) — the form
VaireDB answers by folding the predicate into the argument (gap-analysis § 2.4).

```sql
-- Q21 weather trend, windowed FILTER
WITH t AS (
    SELECT CASE WHEN wx_origin_flight_category_ordinal > wx_origin_m3_flight_category_ordinal THEN 'worsened'
                WHEN wx_origin_flight_category_ordinal < wx_origin_m3_flight_category_ordinal THEN 'improved'
                ELSE 'stable' END AS trend,
           dep_delay_min, cancelled
    FROM flights_weather
    WHERE wx_origin_flight_category_ordinal IS NOT NULL
      AND wx_origin_m3_flight_category_ordinal IS NOT NULL
),
g AS (
    SELECT trend,
           count(*)                                                        AS flights,
           avg(dep_delay_min)                                              AS avg_dep_delay,
           count(*) FILTER (WHERE cancelled)::double precision / count(*)  AS cancel_rate
    FROM t
    GROUP BY trend
)
SELECT trend, flights, avg_dep_delay, cancel_rate,
       avg_dep_delay - max(avg_dep_delay) FILTER (WHERE trend = 'stable') OVER () AS excess_dep_delay,
       cancel_rate / max(cancel_rate) FILTER (WHERE trend = 'stable') OVER ()     AS cancel_rate_ratio
FROM g
ORDER BY trend;
```

#### Q22 — Gaps and islands: longest delayed streak per tail

The difference of two `row_number`s as an island id. On VaireDB today **the operand order is
load-bearing**: the overall row number minus the per-state one is never negative, the reverse
would be, and a negative ranking difference wraps (finding F1).

```sql
-- Q22 gaps and islands
WITH seq AS (
    SELECT tail_number, operating_carrier, flight_date_local_origin, arr_del15,
           row_number() OVER (PARTITION BY tail_number
                              ORDER BY sched_dep_utc, operating_carrier, flight_number, origin_iata, dest_iata)
         - row_number() OVER (PARTITION BY tail_number, arr_del15
                              ORDER BY sched_dep_utc, operating_carrier, flight_number, origin_iata, dest_iata)
             AS island
    FROM flights_weather
    WHERE tail_number IS NOT NULL AND arr_del15 IS NOT NULL
),
streaks AS (
    SELECT tail_number, island,
           min(operating_carrier)        AS carrier,
           count(*)                      AS streak,
           min(flight_date_local_origin) AS first_day,
           max(flight_date_local_origin) AS last_day
    FROM seq
    WHERE arr_del15
    GROUP BY tail_number, island
)
SELECT tail_number, carrier, streak, first_day, last_day,
       rank() OVER (ORDER BY streak DESC) AS streak_rank
FROM streaks
ORDER BY streak DESC, tail_number, first_day
LIMIT 20;
```

#### Q23 — Last leg of the year for the 15 busiest tails

Top-1 per partition by `row_number`, beside a partition count. This is the spelling that answers
on VaireDB; the `DISTINCT ON` spelling of the same question is refused (P05).

```sql
-- Q23 last leg per busiest tail
WITH busiest AS (
    SELECT tail_number
    FROM flights_weather
    WHERE tail_number IS NOT NULL
    GROUP BY tail_number
    ORDER BY count(*) DESC, tail_number
    LIMIT 15
),
legs AS (
    SELECT f.tail_number, f.operating_carrier, f.flight_number, f.origin_iata, f.dest_iata,
           f.flight_date_local_origin,
           count(*) OVER (PARTITION BY f.tail_number) AS legs,
           row_number() OVER (PARTITION BY f.tail_number
                              ORDER BY f.sched_dep_utc DESC, f.operating_carrier, f.flight_number,
                                       f.origin_iata, f.dest_iata) AS rn
    FROM flights_weather f
    JOIN busiest b ON b.tail_number = f.tail_number
)
SELECT tail_number, operating_carrier, flight_number, origin_iata, dest_iata, flight_date_local_origin, legs
FROM legs
WHERE rn = 1
ORDER BY tail_number;
```

#### Q24 — Rank stability: how far each carrier moves month to month

A window over a window's output: `lag` of a monthly `rank`, then aggregated. **Fails on VaireDB**
(finding F1, loud form).

```sql
-- Q24 rank stability
WITH m AS (
    SELECT month, operating_carrier,
           count(*) FILTER (WHERE NOT arr_del15)::double precision / count(arr_del15) AS on_time_rate
    FROM flights_weather
    GROUP BY month, operating_carrier
),
r AS (
    SELECT month, operating_carrier, on_time_rate,
           rank() OVER (PARTITION BY month ORDER BY on_time_rate DESC) AS rnk
    FROM m
),
moves AS (
    SELECT operating_carrier, month, rnk,
           rnk - lag(rnk) OVER (PARTITION BY operating_carrier ORDER BY month) AS rank_change
    FROM r
)
SELECT operating_carrier,
       count(*)                        AS months,
       min(rnk)                        AS best_rank,
       max(rnk)                        AS worst_rank,
       avg(rnk::double precision)      AS avg_rank,
       sum(abs(rank_change))           AS total_rank_movement,
       count(*) FILTER (WHERE rnk = 1) AS months_at_top
FROM moves
GROUP BY operating_carrier
ORDER BY operating_carrier;
```

#### Q25 — Rank movement direction

The same rank difference, but only **compared**, never projected. **Silently wrong on VaireDB**
(finding F1, silent form): every move up is counted as a move down.

```sql
-- Q25 rank movement direction
WITH m AS (
    SELECT month, operating_carrier, count(*) AS flights
    FROM flights_weather
    GROUP BY month, operating_carrier
),
r AS (
    SELECT month, operating_carrier,
           rank() OVER (PARTITION BY month ORDER BY flights DESC, operating_carrier) AS size_rank
    FROM m
),
c AS (
    SELECT operating_carrier,
           size_rank - lag(size_rank) OVER (PARTITION BY operating_carrier ORDER BY month) AS rank_change
    FROM r
)
SELECT operating_carrier,
       count(*) FILTER (WHERE rank_change < 0) AS months_moved_up,
       count(*) FILTER (WHERE rank_change > 0) AS months_moved_down,
       count(*) FILTER (WHERE rank_change = 0) AS months_unchanged
FROM c
GROUP BY operating_carrier
ORDER BY operating_carrier;
```

#### Q26 — Delay change over each tail's five most recent legs

Number the legs from the most recent (`row_number … DESC`), then take the change against the
previous leg (`lag … ASC`) — the outer window sorts by the exact reverse of the inner one.
**Silently wrong on VaireDB** (finding F2): the `lag` is evaluated in the inner window's order,
so it reads the *next* leg.

```sql
-- Q26 delay change over the five most recent legs
WITH r AS (
    SELECT tail_number, sched_dep_utc, operating_carrier, flight_number, origin_iata, dest_iata, arr_delay_min,
           row_number() OVER (PARTITION BY tail_number
                              ORDER BY sched_dep_utc DESC, operating_carrier DESC, flight_number DESC,
                                       origin_iata DESC, dest_iata DESC) AS legs_from_last
    FROM flights_weather
    WHERE tail_number IS NOT NULL AND arr_delay_min IS NOT NULL
),
d AS (
    SELECT legs_from_last,
           arr_delay_min - lag(arr_delay_min) OVER (PARTITION BY tail_number
                                                    ORDER BY sched_dep_utc, operating_carrier, flight_number,
                                                             origin_iata, dest_iata) AS delay_change
    FROM r
)
SELECT legs_from_last,
       count(delay_change)    AS legs,
       avg(delay_change)      AS avg_delay_change,
       avg(abs(delay_change)) AS avg_abs_delay_change
FROM d
WHERE legs_from_last <= 5
GROUP BY legs_from_last
ORDER BY legs_from_last;
```

### F. Joins

#### Q27 — Round-trip asymmetry

A route aggregate joined to its own reverse: a two-column equi-join on non-shard keys, both
sides aggregates. `NULLS LAST` keeps the routes whose return flights carry no arrival delay out
of the top 15 (§ 2.1). From e2e: `test_join_predicates_and_shapes`.

```sql
-- Q27 round-trip asymmetry: a route aggregate joined to its own reverse
WITH r AS (
    SELECT origin_iata, dest_iata, count(*) AS flights, avg(arr_delay_min) AS avg_arr_delay
    FROM flights_weather
    GROUP BY origin_iata, dest_iata
)
SELECT a.origin_iata, a.dest_iata,
       a.flights                          AS outbound_flights,
       b.flights                          AS return_flights,
       a.avg_arr_delay - b.avg_arr_delay  AS delay_asymmetry
FROM r a
JOIN r b ON b.origin_iata = a.dest_iata AND b.dest_iata = a.origin_iata
WHERE a.origin_iata < a.dest_iata AND a.flights >= 500
ORDER BY abs(a.avg_arr_delay - b.avg_arr_delay) DESC NULLS LAST, a.origin_iata, a.dest_iata
LIMIT 15;
```

#### Q28 — Every flight against its airport-day

~980k raw rows joined to a ~50k-row aggregate on `(origin, date)` — the shuffle join of
`test_join_over_a_non_shard_key_shuffles` at dataset scale, feeding an aggregate
(`test_join_feeding_aggregation_and_ordering`).

```sql
-- Q28 every flight against its airport-day: raw rows joined to an aggregate on a non-shard key
WITH airport_day AS (
    SELECT origin_iata, flight_date_local_origin AS d,
           avg(dep_delay_min) AS avg_dep_delay, count(*) AS departures
    FROM flights_weather
    GROUP BY origin_iata, flight_date_local_origin
)
SELECT f.operating_carrier,
       count(*)                                                    AS flights,
       avg(f.dep_delay_min - ad.avg_dep_delay)                     AS dep_delay_vs_airport_day,
       count(*) FILTER (WHERE f.dep_delay_min > ad.avg_dep_delay)  AS worse_than_airport_day,
       avg(ad.departures)                                          AS avg_airport_day_departures
FROM flights_weather f
JOIN airport_day ad ON ad.origin_iata = f.origin_iata AND ad.d = f.flight_date_local_origin
WHERE f.dep_delay_min IS NOT NULL
GROUP BY f.operating_carrier
ORDER BY f.operating_carrier;
```

#### Q29 — Delay bands

A range join — two inequalities and no equality — from a five-row `VALUES` dimension, `LEFT`
so that an empty band still appears. With no equi-key it is a nested-loop join, the operator
the scheduler has to coalesce to distribute (gap-analysis § 1.3). From e2e:
`test_join_predicates_and_shapes`, `test_outer_join_without_an_equijoin_key_keeps_its_unmatched_rows`.

```sql
-- Q29 delay bands: a range (non-equi) LEFT JOIN from a VALUES dimension
SELECT b.band,
       count(f.dep_delay_min)  AS flights,
       avg(f.arr_delay_min)    AS avg_arr_delay,
       avg(f.taxi_out_min)     AS avg_taxi_out
FROM (VALUES ('1 early',     -10000.0,     0.0),
             ('2 on time',        0.0,    15.0),
             ('3 late',          15.0,    60.0),
             ('4 very late',     60.0,   180.0),
             ('5 extreme',      180.0, 10000.0)) AS b(band, lo, hi)
LEFT JOIN flights_weather f ON f.dep_delay_min >= b.lo AND f.dep_delay_min < b.hi
GROUP BY b.band
ORDER BY b.band;
```

#### Q30 — A densified carrier × month grid

`CROSS JOIN` of two `DISTINCT` dimensions, `LEFT JOIN` of sparse facts with `coalesce`, then a
running window over the dense grid — the standard way to report a zero. From e2e:
`test_cross_join_under_an_aggregate`, `test_join_types_over_the_shard_key`.

```sql
-- Q30 densified carrier x month grid: CROSS JOIN of two dimensions, LEFT JOIN of sparse facts
WITH carriers AS (SELECT DISTINCT operating_carrier FROM flights_weather),
     months   AS (SELECT DISTINCT month FROM flights_weather),
     facts    AS (
         SELECT operating_carrier, month, count(*) AS weather_cancellations
         FROM flights_weather
         WHERE cancellation_reason = 'Weather'
         GROUP BY operating_carrier, month
     )
SELECT c.operating_carrier, m.month,
       coalesce(f.weather_cancellations, 0) AS weather_cancellations,
       sum(coalesce(f.weather_cancellations, 0))
           OVER (PARTITION BY c.operating_carrier ORDER BY m.month) AS running_weather_cancellations
FROM carriers c
CROSS JOIN months m
LEFT JOIN facts f ON f.operating_carrier = c.operating_carrier AND f.month = m.month
ORDER BY c.operating_carrier, m.month;
```

#### Q31 — Route churn, January vs July

`FULL JOIN` on a two-column key, classified by which side is missing. Written with `ON`: a
qualified `USING` key under `FULL JOIN` is gap-analysis § 2.7 row 8. From e2e:
`test_join_types_over_the_shard_key`, `test_full_join_using_merges_the_key_column`.

```sql
-- Q31 route churn, January vs July: FULL OUTER JOIN on a two-column key
WITH jan AS (
    SELECT origin_iata, dest_iata, count(*) AS flights
    FROM flights_weather WHERE month = 1 GROUP BY origin_iata, dest_iata
),
jul AS (
    SELECT origin_iata, dest_iata, count(*) AS flights
    FROM flights_weather WHERE month = 7 GROUP BY origin_iata, dest_iata
)
SELECT CASE WHEN jan.flights IS NULL THEN '2 July only'
            WHEN jul.flights IS NULL THEN '3 January only'
            ELSE '1 both' END             AS presence,
       count(*)                           AS routes,
       sum(coalesce(jan.flights, 0))      AS january_flights,
       sum(coalesce(jul.flights, 0))      AS july_flights
FROM jan
FULL JOIN jul ON jul.origin_iata = jan.origin_iata AND jul.dest_iata = jan.dest_iata
GROUP BY 1
ORDER BY 1;
```

#### Q32 — `LATERAL` with a correlated aggregate

The `LATERAL` form the planner decorrelates into a join. A `LATERAL` that keeps its outer
reference — a top-N, or a `VALUES` list — fails instead (P14, P15, finding F7). From e2e:
`test_lateral_join`.

```sql
-- Q32 LATERAL: a correlated aggregate per carrier
SELECT c.operating_carrier, s.extreme_arrivals, s.worst_arr_delay
FROM (SELECT DISTINCT operating_carrier FROM flights_weather) c
JOIN LATERAL (
    SELECT count(*) AS extreme_arrivals, max(f.arr_delay_min) AS worst_arr_delay
    FROM flights_weather f
    WHERE f.operating_carrier = c.operating_carrier AND f.arr_delay_min >= 180
) s ON true
ORDER BY c.operating_carrier;
```

### G. Semi-joins, anti-joins and subqueries

#### Q33 — Aircraft semi- and anti-joins

`EXISTS` and `NOT EXISTS` correlated by equality, each grouped and joined back to the
dimension. From e2e: `test_semi_and_anti_join_spellings`.

```sql
-- Q33 aircraft semi- and anti-joins: EXISTS and NOT EXISTS correlated by equality
WITH tails AS (
    SELECT DISTINCT operating_carrier, tail_number
    FROM flights_weather WHERE tail_number IS NOT NULL
),
never_late AS (
    SELECT operating_carrier, count(*) AS n FROM tails t
    WHERE NOT EXISTS (SELECT 1 FROM flights_weather f WHERE f.tail_number = t.tail_number AND f.arr_del15)
    GROUP BY operating_carrier
),
into_thunderstorm AS (
    SELECT operating_carrier, count(*) AS n FROM tails t
    WHERE EXISTS (SELECT 1 FROM flights_weather f
                  WHERE f.tail_number = t.tail_number AND f.wx_origin_thunderstorm)
    GROUP BY operating_carrier
)
SELECT t.operating_carrier,
       count(*)                       AS tails,
       min(coalesce(nl.n, 0))         AS tails_never_late,
       min(coalesce(it.n, 0))         AS tails_into_thunderstorm
FROM tails t
LEFT JOIN never_late nl        ON nl.operating_carrier = t.operating_carrier
LEFT JOIN into_thunderstorm it ON it.operating_carrier = t.operating_carrier
GROUP BY t.operating_carrier
ORDER BY t.operating_carrier;
```

#### Q34 — `IN` and null-aware `NOT IN` over a nullable column

Every NULL `tail_number` belongs to a cancelled flight, so the last column's candidate list
holds a NULL and the answer is **0**: the trap `NOT IN` sets, which VaireDB answers by
respelling the predicate null-aware (`compat_rewrite::rewrite_not_in_subqueries`). The middle
column filters the NULL out and counts normally. From e2e: `test_not_in_a_subquery_is_null_aware`,
`test_uncorrelated_projection_subquery_shapes_are_answered`.

```sql
-- Q34 IN and null-aware NOT IN over a nullable column
SELECT (SELECT count(*) FROM flights_weather
        WHERE tail_number IN (SELECT tail_number FROM flights_weather WHERE diverted))
           AS flights_by_diverted_aircraft,
       (SELECT count(*) FROM flights_weather
        WHERE tail_number NOT IN (SELECT tail_number FROM flights_weather
                                  WHERE diverted AND tail_number IS NOT NULL))
           AS flights_by_other_aircraft,
       (SELECT count(*) FROM flights_weather
        WHERE tail_number NOT IN (SELECT tail_number FROM flights_weather WHERE cancelled))
           AS not_in_beside_a_null_candidate;
```

#### Q35 — Quantified comparisons

`> ALL`, `< ANY` and a scalar subquery in `WHERE`, each against a 12-row aggregate. From e2e:
`test_quantified_subquery_forms_answer`, `test_any_and_all_over_a_subquery_are_membership_tests`.

```sql
-- Q35 quantified comparisons: carriers against every month of the network
WITH m AS (
    SELECT operating_carrier, month,
           count(*) FILTER (WHERE NOT arr_del15)::double precision / count(arr_del15) AS on_time
    FROM flights_weather GROUP BY operating_carrier, month
),
net AS (
    SELECT month, count(*) FILTER (WHERE NOT arr_del15)::double precision / count(arr_del15) AS on_time
    FROM flights_weather GROUP BY month
),
span AS (
    SELECT operating_carrier, min(on_time) AS worst_month, max(on_time) AS best_month
    FROM m GROUP BY operating_carrier
)
SELECT '1 worst month beats every network month' AS class, operating_carrier
FROM span WHERE worst_month > ALL (SELECT on_time FROM net)
UNION ALL
SELECT '2 best month trails some network month', operating_carrier
FROM span WHERE best_month < ANY (SELECT on_time FROM net)
UNION ALL
SELECT '3 above the mean network month', operating_carrier
FROM span WHERE (worst_month + best_month) / 2 > (SELECT avg(on_time) FROM net)
ORDER BY 1, 2;
```

#### Q36 — Subqueries in the select list

A correlated `count(*)` and an `EXISTS` membership test in the projection, both correlated by
equality — the correlation `ScalarSubqueryToJoin` decorrelates (gap-analysis § 2.6). From e2e:
`test_correlated_projection_subquery_is_answered`, `test_a_membership_test_in_the_select_list_is_answered`.

```sql
-- Q36 subqueries in the select list: a correlated count and an EXISTS membership test
WITH a AS (
    SELECT origin_iata, count(*) AS departures
    FROM flights_weather
    GROUP BY origin_iata
    ORDER BY count(*) DESC, origin_iata
    LIMIT 10
)
SELECT a.origin_iata,
       a.departures,
       (SELECT count(*) FROM flights_weather f WHERE f.dest_iata = a.origin_iata)      AS arrivals,
       EXISTS (SELECT 1 FROM flights_weather f
               WHERE f.origin_iata = a.origin_iata AND f.wx_origin_snow)              AS saw_snow
FROM a
ORDER BY a.departures DESC, a.origin_iata;
```

### H. Set operations

#### Q37 — Route overlap between two carriers

`INTERSECT`, `EXCEPT` and `UNION` — the distinct forms — counted and stacked with
`UNION ALL`. From e2e: `test_set_operation_basics`, `test_set_operations_nested_in_a_query`.

```sql
-- Q37 route overlap between two carriers: INTERSECT, EXCEPT and UNION
WITH dl AS (SELECT origin_iata, dest_iata FROM flights_weather WHERE operating_carrier = 'DL'),
     aa AS (SELECT origin_iata, dest_iata FROM flights_weather WHERE operating_carrier = 'AA')
SELECT '1 both' AS routes, count(*) AS n FROM (SELECT * FROM dl INTERSECT SELECT * FROM aa) s
UNION ALL
SELECT '2 DL only', count(*) FROM (SELECT * FROM dl EXCEPT SELECT * FROM aa) s
UNION ALL
SELECT '3 AA only', count(*) FROM (SELECT * FROM aa EXCEPT SELECT * FROM dl) s
UNION ALL
SELECT '4 either', count(*) FROM (SELECT * FROM dl UNION SELECT * FROM aa) s
ORDER BY 1;
```

#### Q38 — Monday vs Sunday departures as multisets

`INTERSECT ALL` and `EXCEPT ALL` keep duplicates: each Monday departure is matched against at
most one Sunday departure of the same route. From e2e:
`test_multiplicity_set_operations_count_duplicates`.

```sql
-- Q38 Monday vs Sunday departures as multisets: INTERSECT ALL and EXCEPT ALL
WITH mon AS (SELECT origin_iata, dest_iata FROM flights_weather WHERE day_of_week = 1),
     sun AS (SELECT origin_iata, dest_iata FROM flights_weather WHERE day_of_week = 7)
SELECT (SELECT count(*) FROM (SELECT * FROM mon INTERSECT ALL SELECT * FROM sun) s) AS matched_departures,
       (SELECT count(*) FROM (SELECT * FROM mon EXCEPT ALL SELECT * FROM sun) s)    AS monday_surplus,
       (SELECT count(*) FROM (SELECT * FROM sun EXCEPT ALL SELECT * FROM mon) s)    AS sunday_surplus;
```

### I. Selective reads: top-N, paging, pushdown and the shard key

#### Q39 — The 20 worst arrivals

A global `ORDER BY … LIMIT` over raw rows: each shard can only bound its own candidates, and
the merge decides. From e2e: `test_pushed_limit_returns_the_requested_count`.

```sql
-- Q39 top-20 worst arrivals: a global sort and LIMIT over raw rows
SELECT operating_carrier, flight_number, origin_iata, dest_iata, flight_date_local_origin,
       arr_delay_min, dep_delay_min
FROM flights_weather
WHERE arr_delay_min IS NOT NULL
ORDER BY arr_delay_min DESC, sched_dep_utc, operating_carrier, flight_number, origin_iata, dest_iata
LIMIT 20;
```

#### Q40 — A deep page

`LIMIT 10 OFFSET 250000` behind a total order. The limit cannot be pushed as a bare 10, so
every shard produces its first 250,010 rows. From e2e: `test_limit_offset`.

```sql
-- Q40 a deep page: ORDER BY a total key, OFFSET 250000
SELECT operating_carrier, flight_number, origin_iata, dest_iata, sched_dep_local, dep_delay_min
FROM flights_weather
ORDER BY sched_dep_utc, operating_carrier, flight_number, origin_iata, dest_iata
LIMIT 10 OFFSET 250000;
```

#### Q41 — A compound pushed predicate

`OR` groups, `IN` lists, `BETWEEN` on a date, a prefix `LIKE` and `IS NULL`, all pushable
into the shard scans. From e2e: `test_pushed_predicate_keeps_or_grouping`,
`test_like_with_the_default_escape_answers_as_postgres_does`, `test_where_range_scan`.

```sql
-- Q41 a compound pushed predicate: OR groups, IN lists, BETWEEN, LIKE and IS NULL
SELECT operating_carrier,
       count(*)                                    AS flights,
       count(*) FILTER (WHERE tail_number IS NULL) AS without_tail,
       avg(taxi_out_min)                           AS avg_taxi_out
FROM flights_weather
WHERE (origin_iata IN ('ATL', 'DFW', 'IAH') OR dest_iata IN ('SFO', 'SEA'))
  AND flight_date_local_origin BETWEEN DATE '2019-06-01' AND DATE '2019-08-31'
  AND (tail_number LIKE 'N9%' OR tail_number IS NULL)
  AND NOT diverted
GROUP BY operating_carrier
ORDER BY operating_carrier;
```

#### Q42 — A shard-key range

One hour of `sched_dep_utc`, the shard key. Hash sharding gives a range no locality, so every
shard scans: the broadcast baseline for Q43. From e2e: `shard_routing.rs`
(`BETWEEN` on the key broadcasts).

```sql
-- Q42 shard-key range: one hour of scheduled departures
SELECT count(*)                    AS flights,
       count(DISTINCT origin_iata) AS origins,
       avg(dep_delay_min)          AS avg_dep_delay
FROM flights_weather
WHERE sched_dep_utc >= TIMESTAMPTZ '2019-07-04 14:00:00+00'
  AND sched_dep_utc <  TIMESTAMPTZ '2019-07-04 15:00:00+00';
```

#### Q43 — A shard-key point read

An equality on the shard key — the one predicate the router turns into a single shard. From
e2e: `shard_routing.rs` (`test_value_form_hashing_is_string_based`).

```sql
-- Q43 shard-key point read: every departure scheduled at one instant
SELECT operating_carrier, flight_number, origin_iata, dest_iata, dep_delay_min
FROM flights_weather
WHERE sched_dep_utc = TIMESTAMPTZ '2019-07-04 14:00:00+00'
ORDER BY operating_carrier, flight_number, origin_iata, dest_iata;
```

### J. More aggregation, time buckets, text, and two window regressions

#### Q44 — `DISTINCT` aggregates and exact numeric over integer columns

`count(DISTINCT)`, `avg(DISTINCT)`, and `sum`/`avg`/`var_samp`/`stddev_pop` over `bigint`
columns, which PostgreSQL answers in exact `numeric` and DuckDB in `double` (the runner compares
the numbers). From e2e: `test_sum_of_bigint_does_not_wrap`, `test_avg_of_an_integer_is_exact_numeric`,
`test_the_statistics_family_is_exact_numeric`.

```sql
-- Q44 DISTINCT aggregates and the exact-numeric statistics over integer columns
SELECT operating_carrier,
       count(DISTINCT tail_number)                       AS tails,
       count(DISTINCT dest_iata)                         AS destinations,
       avg(DISTINCT distance_miles)                      AS avg_distinct_distance,
       sum(origin_sched_departures_this_hour)            AS sum_hourly_bank,
       avg(days_to_nearest_holiday)                      AS avg_days_to_holiday,
       var_samp(days_to_nearest_holiday)                 AS var_days_to_holiday,
       stddev_pop(origin_sched_departures_this_day)      AS sd_daily_bank
FROM flights_weather
GROUP BY operating_carrier
ORDER BY operating_carrier;
```

#### Q45 — Explicit `GROUPING SETS` with the empty set mixed in

The form the coordinator rewrites so that the grand total survives distribution. From e2e:
`test_an_empty_grouping_set_is_the_grand_total`.

```sql
-- Q45 explicit GROUPING SETS with the empty set mixed in
SELECT operating_carrier, month,
       grouping(operating_carrier, month)  AS grp,
       count(*)                            AS flights,
       avg(arr_delay_min)                  AS avg_arr_delay
FROM flights_weather
WHERE operating_carrier IN ('DL', 'WN', 'HA')
GROUP BY GROUPING SETS ((operating_carrier, month), (month), ())
ORDER BY grp, operating_carrier NULLS LAST, month NULLS LAST;
```

#### Q46 — Weekly trend

`date_trunc('week', …)` buckets on a local timestamp, cast back to `date`, then a `ROWS`
frame and a `lag` over the weeks. No e2e test calls `date_trunc`; this is new coverage.

```sql
-- Q46 weekly trend: date_trunc buckets and a 4-week moving average
WITH w AS (
    SELECT date_trunc('week', sched_dep_local) AS week,
           count(*)                           AS flights,
           avg(arr_delay_min)                 AS avg_arr_delay
    FROM flights_weather
    GROUP BY date_trunc('week', sched_dep_local)
)
SELECT CAST(week AS date)                                          AS week_start,
       flights,
       avg_arr_delay,
       avg(avg_arr_delay) OVER (ORDER BY week ROWS 3 PRECEDING)    AS avg_arr_delay_4w,
       flights - lag(flights) OVER (ORDER BY week)                 AS flights_vs_last_week
FROM w
ORDER BY week;
```

#### Q47 — A window over a filtered partition key, ordered by a window it does not project

Two shapes the e2e suite holds as regressions: a window partitioned by a column the `WHERE`
fixes to one value (the repaired stage cut of its section 12), and a window function in the
outer `ORDER BY` (section 9). From e2e: `test_a_window_partitioned_by_a_filtered_column_answers`,
`test_a_window_function_in_the_outer_order_by_is_answered`.

```sql
-- Q47 a window over a filtered partition key, ordered by a window it does not project
SELECT flight_number, origin_iata, dest_iata, flight_date_local_origin, dep_delay_min,
       row_number() OVER (PARTITION BY operating_carrier
                          ORDER BY dep_delay_min DESC NULLS LAST, sched_dep_utc,
                                   flight_number, origin_iata, dest_iata) AS delay_rank
FROM flights_weather
WHERE operating_carrier = 'HA'
ORDER BY rank() OVER (PARTITION BY operating_carrier
                      ORDER BY dep_delay_min DESC NULLS LAST, sched_dep_utc,
                               flight_number, origin_iata, dest_iata)
LIMIT 10;
```

#### Q48 — Registration shapes

`LIKE`, `SIMILAR TO` and an *anchored* `~` inside a `CASE`, grouped — anchored because
DuckDB's `~` is a full match (§ 2.2). From e2e: `test_the_regex_operators_match_partially_on_both_paths`,
`test_similar_to_is_the_same_language_on_both_paths`.

```sql
-- Q48 registration shapes: LIKE, SIMILAR TO and an anchored regular expression
SELECT CASE WHEN tail_number IS NULL                         THEN '0 missing'
            WHEN tail_number SIMILAR TO 'N[0-9]{1,5}'        THEN '1 N + digits'
            WHEN tail_number ~ '^N[0-9]+[A-Z]{1,2}$'         THEN '2 N + digits + letters'
            WHEN tail_number LIKE 'N%'                       THEN '3 other N-number'
            ELSE '4 not an N-number' END                     AS registration_shape,
       count(*)                                              AS flights,
       count(DISTINCT tail_number)                           AS tails
FROM flights_weather
GROUP BY 1
ORDER BY 1;
```

### K. Calibration

Not analytical: these three blocks exist so that § 6 can separate VaireDB's fixed cost per
statement from the cost of the work. They are compared like every other block.

#### B01 — Round trip, no table

```sql
-- B01 round trip, no table
SELECT 1 AS one;
```

#### B02 — The smallest distributed job

One scan per shard and a count: the floor under every query that reads `flights_weather`.

```sql
-- B02 smallest distributed job: a count over every shard
SELECT count(*) AS flights FROM flights_weather;
```

#### B03 — One full column, aggregated

Every shard reads one column of every row; the aggregate is merged across the shuffle.

```sql
-- B03 one full column read on every shard and aggregated
SELECT count(arr_delay_min) AS flights_with_arr_delay, sum(arr_delay_min) AS total_arr_delay
FROM flights_weather;
```

---

## 4 — Probes: refusals, their neighbours, and the findings' reproductions

Following the e2e convention (`AGENTS.md`): every refusal is paired with the neighbouring form
that loses no clause, which must still answer. That pairing is what shows a refusal is scoped
rather than a blanket rejection. Probes go through the same four steps as the suite, but are
not timed, and a refusal here is usually the expected verdict rather than a failure.

#### P01 — `EXCLUDE CURRENT ROW` (gap-analysis § 2.4, ❌ `42601`)

```sql
-- P01 EXCLUDE CURRENT ROW: refused at parse
WITH daily AS (
    SELECT flight_date_local_origin AS d, count(*) AS flights
    FROM flights_weather
    GROUP BY flight_date_local_origin
)
SELECT d, flights,
       avg(flights) OVER (ORDER BY d ROWS BETWEEN 3 PRECEDING AND 3 FOLLOWING EXCLUDE CURRENT ROW) AS neighbour_avg
FROM daily
ORDER BY d;
```

```sql
-- P01b the same answer without EXCLUDE: answers
WITH daily AS (
    SELECT flight_date_local_origin AS d, count(*) AS flights
    FROM flights_weather
    GROUP BY flight_date_local_origin
)
SELECT d, flights,
       (sum(flights) OVER w - flights)::double precision / (count(*) OVER w - 1) AS neighbour_avg
FROM daily
WINDOW w AS (ORDER BY d ROWS BETWEEN 3 PRECEDING AND 3 FOLLOWING)
ORDER BY d;
```

#### P02 — Windowed `FILTER` on a collecting aggregate (gap-analysis § 2.4, ❌ `0A000`)

```sql
-- P02 string_agg FILTER OVER: refused
WITH r AS (
    SELECT operating_carrier, origin_iata || '-' || dest_iata AS route, count(*) AS flights
    FROM flights_weather
    WHERE operating_carrier = 'HA'
    GROUP BY operating_carrier, origin_iata, dest_iata
)
SELECT route, flights,
       string_agg(route, ',') FILTER (WHERE flights >= 300)
           OVER (ORDER BY flights DESC, route ROWS UNBOUNDED PRECEDING) AS busy_routes_so_far
FROM r
ORDER BY flights DESC, route;
```

```sql
-- P02b the same window with a null-skipping FILTER: answers
WITH r AS (
    SELECT operating_carrier, origin_iata || '-' || dest_iata AS route, count(*) AS flights
    FROM flights_weather
    WHERE operating_carrier = 'HA'
    GROUP BY operating_carrier, origin_iata, dest_iata
)
SELECT route, flights,
       count(*) FILTER (WHERE flights >= 300)
           OVER (ORDER BY flights DESC, route ROWS UNBOUNDED PRECEDING) AS busy_routes_so_far
FROM r
ORDER BY flights DESC, route;
```

#### P03 — Multi-column hypothetical-set aggregate (gap-analysis § 2.3, ❌ `0A000`)

Passes the PostgreSQL syntax check; DuckDB cannot parse it either. The one-column neighbour is
P03b.

```sql
-- P03 rank(a, b) WITHIN GROUP (ORDER BY x, y): refused
SELECT operating_carrier,
       rank(30, 10) WITHIN GROUP (ORDER BY arr_delay_min, dep_delay_min) AS rank_of_30_10
FROM flights_weather
GROUP BY operating_carrier
ORDER BY operating_carrier;
```

#### P03b — Hypothetical-set aggregates, one column (the neighbour of P03)

Where a 30-minute arrival delay would rank inside each carrier's distribution. It passes the
syntax check and VaireDB answers it, but **DuckDB has no hypothetical-set aggregates**, so
there is no result to compare against: this block shows that P03's refusal is scoped to the
multi-column form, and nothing more.

```sql
-- P03b hypothetical-set aggregates, one column: answers
SELECT operating_carrier,
       count(arr_delay_min)                                    AS flights,
       rank(30)         WITHIN GROUP (ORDER BY arr_delay_min)  AS rank_of_30,
       dense_rank(30)   WITHIN GROUP (ORDER BY arr_delay_min)  AS dense_rank_of_30,
       percent_rank(30) WITHIN GROUP (ORDER BY arr_delay_min)  AS percent_rank_of_30,
       cume_dist(30)    WITHIN GROUP (ORDER BY arr_delay_min)  AS cume_dist_of_30
FROM flights_weather
GROUP BY operating_carrier
ORDER BY operating_carrier;
```

#### P04 — Repeated empty grouping set (gap-analysis § 2.3, ❌ `0A000`)

Valid PostgreSQL syntax, and VaireDB refuses it. **DuckDB answers one row**, where PostgreSQL
defines one grand-total row per empty set — two here — so DuckDB is not a valid oracle for this
form, and that is why it is a probe rather than a suite query (§ 2.2).

```sql
-- P04 GROUPING SETS ((), ()): refused
SELECT count(*) AS flights, avg(arr_delay_min) AS avg_arr_delay
FROM flights_weather
GROUP BY GROUPING SETS ((), ());
```

```sql
-- P04b the repeated empty set beside a non-empty one: answers
SELECT operating_carrier, count(*) AS flights
FROM flights_weather
GROUP BY GROUPING SETS ((operating_carrier), (), ())
ORDER BY operating_carrier NULLS LAST, flights;
```

#### P05 — `DISTINCT ON` beside a window function (finding F4, ❌ `0A000`)

```sql
-- P05 DISTINCT ON with a window function: refused
WITH busiest AS (
    SELECT tail_number
    FROM flights_weather
    WHERE tail_number IS NOT NULL
    GROUP BY tail_number
    ORDER BY count(*) DESC, tail_number
    LIMIT 15
)
SELECT DISTINCT ON (f.tail_number)
       f.tail_number, f.operating_carrier, f.flight_number, f.origin_iata, f.dest_iata,
       f.flight_date_local_origin,
       count(*) OVER (PARTITION BY f.tail_number) AS legs
FROM flights_weather f
JOIN busiest b ON b.tail_number = f.tail_number
ORDER BY f.tail_number, f.sched_dep_utc DESC, f.operating_carrier, f.flight_number, f.origin_iata, f.dest_iata;
```

```sql
-- P05b DISTINCT ON without the window: answers (Q23 is the row_number spelling of P05)
WITH busiest AS (
    SELECT tail_number
    FROM flights_weather
    WHERE tail_number IS NOT NULL
    GROUP BY tail_number
    ORDER BY count(*) DESC, tail_number
    LIMIT 15
)
SELECT DISTINCT ON (f.tail_number)
       f.tail_number, f.operating_carrier, f.flight_number, f.origin_iata, f.dest_iata,
       f.flight_date_local_origin
FROM flights_weather f
JOIN busiest b ON b.tail_number = f.tail_number
ORDER BY f.tail_number, f.sched_dep_utc DESC, f.operating_carrier, f.flight_number, f.origin_iata, f.dest_iata;
```

#### P06 — `IGNORE NULLS` (gap-analysis § 3.2, 🚫 `0A000`)

The probe the syntax gate exists for: **the PostgreSQL syntax check rejects it** (`42601`), so it
is not a PostgreSQL statement, and VaireDB's refusal by name is the correct verdict. DuckDB
rejects this standard placement too and answers only its own in-parenthesis spelling,
`last_value(x IGNORE NULLS) OVER (…)`, which VaireDB refuses at parse (`42601`).

```sql
-- P06 last_value IGNORE NULLS: refused, correctly
SELECT flight_number, origin_iata, dest_iata, wx_origin_wind_gust_ms AS gust,
       last_value(wx_origin_wind_gust_ms) IGNORE NULLS
           OVER (ORDER BY sched_dep_utc, operating_carrier, flight_number, origin_iata, dest_iata) AS last_gust
FROM flights_weather
WHERE tail_number = '274NV'
ORDER BY sched_dep_utc, operating_carrier, flight_number, origin_iata, dest_iata;
```

```sql
-- P06b the PostgreSQL spelling of carry-forward: should answer, fails on VaireDB (F6)
WITH s AS (
    SELECT sched_dep_utc, operating_carrier, flight_number, origin_iata, dest_iata,
           wx_origin_wind_gust_ms AS gust,
           count(wx_origin_wind_gust_ms)
               OVER (ORDER BY sched_dep_utc, operating_carrier, flight_number, origin_iata, dest_iata) AS grp
    FROM flights_weather
    WHERE tail_number = '274NV'
)
SELECT flight_number, origin_iata, dest_iata, gust,
       max(gust) OVER (PARTITION BY grp) AS last_gust
FROM s
ORDER BY sched_dep_utc, operating_carrier, flight_number, origin_iata, dest_iata;
```

#### P07–P12 — Reproductions of the findings in § 7

```sql
-- P07 F1 repro: a negative ranking difference, compared (DuckDB: 1)
WITH r AS (
    SELECT x,
           row_number() OVER (ORDER BY x)      AS up,
           row_number() OVER (ORDER BY x DESC) AS down
    FROM (VALUES (1), (2), (3)) AS t(x)
)
SELECT count(*) FILTER (WHERE up - down < 0) AS negatives
FROM r;
```

```sql
-- P08 F1 workaround: Q25 with the rank cast to bigint before it is subtracted
WITH m AS (
    SELECT month, operating_carrier, count(*) AS flights
    FROM flights_weather
    GROUP BY month, operating_carrier
),
r AS (
    SELECT month, operating_carrier,
           CAST(rank() OVER (PARTITION BY month ORDER BY flights DESC, operating_carrier) AS bigint) AS size_rank
    FROM m
),
c AS (
    SELECT operating_carrier,
           size_rank - lag(size_rank) OVER (PARTITION BY operating_carrier ORDER BY month) AS rank_change
    FROM r
)
SELECT operating_carrier,
       count(*) FILTER (WHERE rank_change < 0) AS months_moved_up,
       count(*) FILTER (WHERE rank_change > 0) AS months_moved_down,
       count(*) FILTER (WHERE rank_change = 0) AS months_unchanged
FROM c
GROUP BY operating_carrier
ORDER BY operating_carrier;
```

```sql
-- P09 F2 repro: lag over an input a window already sorted the other way (DuckDB: NULL, 86, 73, …)
WITH m AS (
    SELECT month, count(*) AS flights
    FROM flights_weather
    WHERE operating_carrier = 'HA'
    GROUP BY month
),
r AS (
    SELECT month, flights, row_number() OVER (ORDER BY month DESC) AS months_from_end
    FROM m
)
SELECT month, months_from_end, lag(flights) OVER (ORDER BY month) AS prev_month_flights
FROM r
ORDER BY month;
```

```sql
-- P10 F3 repro: the advertised type of a ranking function (DuckDB: bigint)
SELECT pg_typeof(rank() OVER (ORDER BY x))::text       AS rank_type,
       pg_typeof(row_number() OVER (ORDER BY x))::text AS row_number_type,
       pg_typeof(ntile(2) OVER (ORDER BY x))::text     AS ntile_type
FROM (VALUES (1)) AS t(x);
```

```sql
-- P11 F5 repro: round(numeric, n) keeps the argument's scale on the wire (DuckDB: 1.23)
SELECT round(1.23456::numeric(18,6), 2) AS rounded;
```

The runner reports P11 as a MATCH **by design** — the values are equal, only the text differs
— so read it with `psql` to see `1.230000`. `round(…)::text` is already `1.23` on VaireDB.

```sql
-- P12 F6 scope: the P06b carry-forward under an aggregate, no final ORDER BY: answers
WITH s AS (
    SELECT wx_origin_wind_gust_ms AS gust,
           count(wx_origin_wind_gust_ms)
               OVER (ORDER BY sched_dep_utc, operating_carrier, flight_number, origin_iata, dest_iata) AS grp
    FROM flights_weather
    WHERE tail_number = '274NV'
)
SELECT count(*) AS flights, count(last_gust) AS flights_with_gust, sum(last_gust) AS gust_sum
FROM (SELECT max(gust) OVER (PARTITION BY grp) AS last_gust FROM s) AS z;
```

#### P13 — `NOT IN` over a correlated subquery (gap-analysis § 2.6, ❌ `0A000`)

Refused because either side may be NULL — `origin_iata` and `dest_iata` are declared
nullable, though no row is — and the refusal names the `NOT EXISTS` spelling, which is exact
here. From e2e: `test_not_in_over_a_correlated_subquery_is_refused`, and the ignored
`test_not_in_over_a_correlated_subquery`.

```sql
-- P13 NOT IN over a correlated subquery: refused
SELECT f.operating_carrier, count(*) AS flights_to_non_departure_airports
FROM flights_weather f
WHERE f.dest_iata NOT IN (SELECT g.origin_iata FROM flights_weather g
                          WHERE g.operating_carrier = f.operating_carrier)
GROUP BY f.operating_carrier
ORDER BY f.operating_carrier;
```

```sql
-- P13b the NOT EXISTS spelling, exact here because origin_iata holds no NULL: answers
SELECT f.operating_carrier, count(*) AS flights_to_non_departure_airports
FROM flights_weather f
WHERE NOT EXISTS (SELECT 1 FROM flights_weather g
                  WHERE g.operating_carrier = f.operating_carrier AND g.origin_iata = f.dest_iata)
GROUP BY f.operating_carrier
ORDER BY f.operating_carrier;
```

#### P14–P15 — `LATERAL` that keeps its outer reference (finding F7)

A per-carrier top-N, and the unpivot through `LATERAL (VALUES …)` that gap-analysis § 3.2
recommends in place of `UNPIVOT`. Both fail with `42601`; the `LATERAL` that only filters
(P14b) and the one that aggregates (Q32) answer, and the `UNION ALL` unpivot is Q04.

```sql
-- P14 LATERAL top-N per carrier: fails to serialize (F7)
SELECT c.operating_carrier, t.origin_iata, t.dest_iata, t.flights
FROM (SELECT DISTINCT operating_carrier FROM flights_weather) c
CROSS JOIN LATERAL (
    SELECT f.origin_iata, f.dest_iata, count(*) AS flights
    FROM flights_weather f
    WHERE f.operating_carrier = c.operating_carrier
    GROUP BY f.origin_iata, f.dest_iata
    ORDER BY count(*) DESC, f.origin_iata, f.dest_iata
    LIMIT 3
) t
ORDER BY c.operating_carrier, t.flights DESC, t.origin_iata, t.dest_iata;
```

```sql
-- P14b LATERAL with a correlated filter and no aggregate: answers
SELECT c.operating_carrier, count(*) AS rows_joined
FROM (SELECT DISTINCT operating_carrier FROM flights_weather) c
CROSS JOIN LATERAL (SELECT f.flight_number FROM flights_weather f
                    WHERE f.operating_carrier = c.operating_carrier AND f.arr_delay_min >= 300) t
GROUP BY c.operating_carrier
ORDER BY c.operating_carrier;
```

```sql
-- P15 unpivot through LATERAL over VALUES, as gap-analysis § 3.2 advises: fails to serialize (F7)
SELECT v.cause, count(v.minutes) AS flights_with_cause, sum(v.minutes) AS minutes
FROM flights_weather f
CROSS JOIN LATERAL (VALUES ('carrier', f.reported_carrier_delay_min),
                           ('weather', f.reported_weather_delay_min),
                           ('nas',     f.reported_nas_delay_min)) AS v(cause, minutes)
GROUP BY v.cause
ORDER BY v.cause;
```

---

## 5 — Measured results

Suite `1.1.0`, run of 2026-10-27T09:48:51Z — the same run as the § 6 report. **All 48 queries
pass the PostgreSQL syntax check; 45 return DuckDB's answer on VaireDB.** One fails loudly and
two return a plausible wrong answer, the same three as in suite `1.0.0`: none of the 22 queries
drawn from the e2e suite (Q27–Q48) disagrees.

| Block | Rows | PostgreSQL syntax | VaireDB vs DuckDB | |
|---|---:|:-:|:-:|---|
| Q01–Q23 | 17 · 17 · 19 · 17 · 4 · 21 · 72 · 30 · 10 · 39 · 30 · 170 · 17 · 365 · 170 · 5 · 204 · 17 · 5 · 11 · 3 · 20 · 15 | valid | ✅ | |
| Q24 | — | valid | ❌ | `22003` `[VDB-1019]` — F1, loud |
| Q25 | 17 | valid | ⛔ | every carrier: `months_moved_up = 0` — F1, silent |
| Q26 | 5 | valid | ⛔ | every bucket shifted by one, sign flipped — F2 |
| Q27–Q32, joins | 15 · 17 · 5 · 204 · 3 · 17 | valid | ✅ | |
| Q33–Q36, subqueries | 17 · 1 · 10 · 10 | valid | ✅ | Q34's null-aware `NOT IN` answers 0, as PostgreSQL does |
| Q37–Q38, set operations | 4 · 1 | valid | ✅ | |
| Q39–Q43, selective reads | 20 · 10 · 13 · 1 · 12 | valid | ✅ | |
| Q44–Q48, more aggregation | 17 · 49 · 53 · 10 · 4 | valid | ✅ | |
| B01–B03 | 1 · 1 · 1 | valid | ✅ | calibration |

| Probe | PostgreSQL syntax | VaireDB | DuckDB | Verdict |
|---|:-:|---|---|---|
| P01 `EXCLUDE` | valid | `42601` | answers | expected (gap-analysis § 2.4) · neighbour P01b ✅ |
| P02 `string_agg … FILTER … OVER` | valid | `0A000` | answers | expected (§ 2.4) · neighbour P02b ✅ |
| P03 `rank(a, b) WITHIN GROUP` | valid | `0A000` | parse error | expected (§ 2.3) · neighbour P03b answers on VaireDB; DuckDB cannot run it |
| P04 `GROUPING SETS ((), ())` | valid | `0A000` | 1 row, not PostgreSQL's 2 | expected (§ 2.3) · neighbour P04b ✅ |
| P05 `DISTINCT ON` + window | valid | `0A000` | answers | **not recorded** — F4 · neighbours P05b ✅, Q23 ✅ |
| P06 `IGNORE NULLS` | **`42601`** | `0A000` | parse error | correct: not a PostgreSQL statement (🚫 § 3.2) |
| P06b carry-forward | valid | `XX000` | answers | F6 · neighbour P12 ✅ |
| P07 | valid | `0` | `1` | ⛔ F1 |
| P08 | valid | ✅ | ✅ | F1's workaround holds |
| P09 | valid | `lead`'s values | `lag`'s | ⛔ F2 |
| P10 | valid | `numeric` | `bigint` | F3 |
| P11 | valid | `1.230000` | `1.23` | F5 — equal values, so the runner reports MATCH by design |
| P13 correlated `NOT IN` | valid | `0A000` | answers | expected (gap-analysis § 2.6) · neighbour P13b ✅ |
| P14 `LATERAL` top-N | valid | `42601` | answers | **not recorded** — F7 · neighbours P14b ✅, Q32 ✅ |
| P15 `LATERAL (VALUES …)` unpivot | valid | `42601` | answers | **not recorded** — F7 · neighbour Q04's `UNION ALL` unpivot ✅ |

---

## 6 — Performance: one DuckDB instance vs the distributed cluster

Every run writes its own report to [`results/`](results/), stamped with the UTC time, the suite
version, a hash of the SQL blocks, and the version of every engine and of the host. The figures
below are from
[`bts-flights-weather-perf-20261027T094851Z.md`](results/bts-flights-weather-perf-20261027T094851Z.md)
(suite `1.1.0`). The `1.0.0` run,
[`bts-flights-weather-perf-20261027T084512Z.md`](results/bts-flights-weather-perf-20261027T084512Z.md),
is kept for comparison; reports are comparable only within one suite version.

**Method.** One host, one slice, one CPU budget. The cluster runs in a Docker VM with 6 CPUs, shared
by the coordinator and the 5 cores, and DuckDB is pinned to 6 threads to match. Each block gets
one warm-up and then 5 timed runs per engine in a single session, and the median is reported.
VaireDB is timed by psql's `\timing` (statement sent → last row received over pgwire), DuckDB by
its CLI `.timer`; neither includes process start or connecting. *Exchanges* is the count of
exchange operators in VaireDB's `EXPLAIN`, the points where Ballista cuts a plan into stages —
unknown (`—`) for Q36, whose select-list `EXISTS` is respelled on the execution path and does not
explain.

| | DuckDB, 1 instance | VaireDB, 1 coordinator + 5 cores |
|---|---:|---:|
| Sum of medians, the 45 matching queries | 1.1 s | 22.2 s |
| Fastest · slowest matching query | 2 ms (Q42) · 149 ms (Q22) | 208 ms (Q39) · 832 ms (Q33) |
| `SELECT 1` (B01) | < 1 ms | 109 ms |
| Smallest distributed job, a `count(*)` (B02) | < 1 ms | 271 ms |

VaireDB ÷ DuckDB over the 45 matching queries: **geometric mean 33×**, from 4.3× (Q22) to
146× (Q15). By category:

| Queries | Matching | DuckDB, sum | VaireDB, sum | VaireDB median | Geometric mean ratio |
|---|---:|---:|---:|---:|---:|
| A–E — aggregation, grouping sets, windows (Q01–Q26) | 23 | 728 ms | 11.2 s | 478 ms | 27× |
| F — joins (Q27–Q32) | 6 | 93 ms | 3.2 s | 502 ms | 45× |
| G — semi/anti-joins and subqueries (Q33–Q36) | 4 | 57 ms | 3.0 s | 750 ms | 55× |
| H — set operations (Q37–Q38) | 2 | 58 ms | 1.5 s | 736 ms | 30× |
| I — top-N, paging, pushdown, shard key (Q39–Q43) | 5 | 78 ms | 1.6 s | 271 ms | 42× |
| J — more aggregation, time buckets, text (Q44–Q48) | 5 | 79 ms | 1.8 s | 385 ms | 28× |

Timings move between runs: the same suite measured minutes earlier gave a 36× geometric mean and a
222 ms B02 floor, so treat a difference under ~20 % between two reports as noise.

**At 1M rows, VaireDB's time is mostly fixed cost.** A statement that touches no table already
costs 109 ms, and the smallest distributed job costs 271 ms; the median query spends about 60 %
of its VaireDB time at that floor. DuckDB answers 36 of the 45 queries in under 30 ms; the fastest
VaireDB query takes 208 ms.

**The cost follows the plan's exchanges, not the rows.** One exchange takes a median 262 ms,
two 370 ms, three to four 512–578 ms, five or more 777 ms. The subquery and set-operation
categories are the most expensive per query for exactly that reason: Q35's three quantified
comparisons plan to 28 exchanges and Q37's four set operations to 12, over inputs DuckDB answers
in 12–14 ms. Q15 is still the clearest single case — a `GROUPS` frame over 170 grouped rows,
4 ms on DuckDB and 583 ms on VaireDB — with Q31's `FULL JOIN` of two small aggregates (143×)
beside it. The count is a proxy, not a cost model: Q04 plans 6 exchanges and takes 406 ms.

**The ratio is smallest where DuckDB has real work to do.** The window queries over raw rows —
Q22 gaps and islands (4.3×), Q18 tail rotation (6.0×), Q09 `ntile` over ~980k rows (6.2×), Q16
rolling `RANGE` (6.4×) — and the deep page Q40 (8.2×) take DuckDB 53–149 ms, and VaireDB adds
380–620 ms on top. This is where the fixed overhead begins to amortize.

**A point read on the shard key is not routed.** Q43, an equality on `sched_dep_utc`, takes
254 ms — the same as Q42's one-hour range (271 ms) and the B02 floor — because its plan still
scans all five shards. The router that sends `key = literal` to a single shard is the write
router (`shard_routing.rs`); the read path plans every shard. Not a correctness gap, but the
number a benchmark should watch: Q43 is where shard pruning on reads would show.

**What this run does not show.** 1M rows (~150 MB of Parquet) fit comfortably in one process.
That is DuckDB's best case and the cluster's worst, and the 5 cores here share the very CPUs the
single DuckDB uses, so distribution has no extra hardware to win with. Whether distribution pays
needs the full 42.8M-row dataset, cores on separate hosts, or both. Two caveats on this run
specifically: the cluster images were built before HEAD (`8784734`) was committed, which the
report flags — rebuild them (`make docker`) and reload the table to measure HEAD. And the ⛔ rows
are timed but left out of every ratio, because a fast wrong answer is not a result.

---

## 7 — Findings

None of these is in `docs/research/gap-analysis.md` yet, and that document claims agreement on
everything it omits — see the closing note. In each one, *DuckDB* is the comparison the suite
makes. Where a block is called valid PostgreSQL, that is the syntax check's verdict.

### F1 ⛔ Arithmetic between two ranking values is unsigned — Q24, Q25, P07, P08

`row_number` and `rank` (and, by its advertised type, `dense_rank`) are evaluated as unsigned
64-bit integers, and the difference of two of them is computed in that type, so `-1` wraps to
`18446744073709551615`. A ranking value against a literal is unaffected — the literal makes the
arithmetic signed, and `row_number() - 2` answers `-1` — and so is a difference that is never
negative, which is why Q22's islands are right.

- **Projected**, the wrapped value cannot be sent, and the query fails `22003`
  `[VDB-1019] … Can't cast value 18446744073709551615 to type Int64` (Q24).
- **Compared or aggregated** before it reaches the wire, nothing fails. `up - down < 0` is never
  true (P07: `0`, DuckDB `1`), and Q25 counts every move up a ranking as a move down — HA answers
  0 up / 2 down, where DuckDB answers 2 up / 0 down.
- Workaround: `CAST(rank() OVER (…) AS bigint)` before subtracting (P08 matches).

Same root as F3.

### F2 ⛔ An outer window can run without the sort it needs — Q26, P09

`lag(x) OVER (ORDER BY k)` over a derived table that a window sorted by `k DESC` is evaluated
over the rows in the inner window's order, without re-sorting. P09's `lag` reads the *following*
month — January gets February's `73` and December gets NULL, where DuckDB answers NULL, `86`, `73`,
… — and Q26 shifts every bucket by one and flips the sign of the change, which still reads as a
plausible table. It reproduces on a `VALUES` list as well as on the sharded table; an outer key
unrelated to the inner one (inner `ORDER BY flights DESC`, outer `ORDER BY month`) answers
correctly.

`lead` is affected the same way: over the reversed input it returns `lag`'s values. The defect
appears only while the outer query still *reads* the inner window's column. Left unread, the
planner prunes the inner window, and the outer one answers correctly. That is why wrapping P09
in an aggregate seemed to fix it — the aggregate never read `months_from_end` — while adding that
column as a second outer key (`ORDER BY month, months_from_end DESC`) answered differently wrong.
So there is no workaround a client can rely on. It reads as the gap-analysis § 1.3 class — an
ordering decision that does not survive the plan being cut into stages — but the cause is not
confirmed. It reproduces deterministically on six rows (§ 8).

### F3 🟡 `pg_typeof` names the ranking functions `numeric` — P10

`pg_typeof(rank() OVER …)` is `numeric` for `rank`, `row_number` and `dense_rank`; DuckDB says
`bigint`, which is also PostgreSQL's documented result type. `ntile` is `integer` on both. The
*wire* type is already right: Describe and Execute agree on `bigint`, and the e2e suite pins
that. `pg_typeof` instead answers from the type the window is evaluated in — F1's unsigned
integer — which has no PostgreSQL name narrower than `numeric`. So the same column is `bigint`
to a driver and `numeric` to the SQL that asks. The values print identically.

### F4 ❌ `DISTINCT ON` beside a window function or an aggregate is refused — P05

`0A000`, raised by datafusion-sql's planner (`DISTINCT ON expressions with GROUP BY, aggregation
or window functions are not supported`) before VaireDB sees a plan. The statement is valid
PostgreSQL and DuckDB answers it. Both neighbours answer: `DISTINCT ON` alone (P05b), and the
`row_number` spelling of the same question (Q23). Gap-analysis § 4 currently says `DISTINCT ON`
works, without this restriction.

### F5 🟡 `round(numeric, n)` keeps the argument's scale — P11

`round(1.23456::numeric(18,6), 2)` prints `1.230000`; DuckDB prints `1.23`. The value is right
and `::text` already answers `1.23` — the result column keeps scale 6 where it should have scale
`n`. `round(x)` with no place count already answers scale 0. This is the scalar-function axis,
which gap-analysis has never measured.

### F6 ❌ Partitioning by a running window's output fails `XX000` — P06b, P12

The PostgreSQL carry-forward idiom — a running `count(x)` as a group id, then
`max(x) OVER (PARTITION BY grp)` — fails on an executor:
`[VDB-2002] Job … failed … Internal("Assertion failed: window_expr[0].partition_by().len() ==
ordered_partition_by_indices.len() (left: 1, right: 0): All partition by columns should have an
ordering")`. DuckDB answers it. The same windows under an aggregate with no final `ORDER BY`
answer (P12). A valid query refused with an internal SQLSTATE, and it is the spelling PostgreSQL
users reach for in place of P06's `IGNORE NULLS`.

### F7 ❌ A `LATERAL` that keeps its outer reference fails `42601` — P14, P15

A `LATERAL` subquery that only filters or aggregates by the left row is decorrelated into a join
and answers (Q32, P14b). One the planner cannot rewrite keeps the reference to the left row and
fails in Ballista's plan serializer:
`[VDB-1003] Proto serialization error: outer_ref(c.operating_carrier) is not yet supported`.
Two shapes reach it: a per-group top-N, with `ORDER BY … LIMIT` inside the `LATERAL` (P14), and a
`VALUES` list built from the left row's columns (P15). DuckDB answers both. Two things make it
worse than a plain refusal. The class is `42601`, a *syntax error*, for a statement PostgreSQL
parses and answers. And P15 is exactly the spelling gap-analysis § 3.2 recommends in place of
`UNPIVOT` — the advice points at a form that fails. The `UNION ALL` unpivot (Q04) answers.

### Recording them

By `AGENTS.md`'s doctrine each finding needs two records. The **e2e pair** is in place (§ 8): a
passing test pinning today's behaviour and an `#[ignore = "gap: …"]` test asserting
PostgreSQL's answer. The **gap-analysis row** is still to be written in
`docs/research/gap-analysis.md`: F1 and F2 in § 2.7, whose ⛔ table is currently empty; F3, F4
and F6 on § 2.4's axis, with § 4's `DISTINCT ON` bullet corrected; F5 on the scalar-function
axis; F7 on § 2.6's joins axis, with § 3.2's `UNPIVOT` advice changed to the `UNION ALL`
spelling.

---

## 8 — End-to-end coverage

### 8.1 Every failing block is pinned in the e2e suite

Every block above that errors or answers wrongly is also pinned in VaireDB's own e2e suite,
in section 21 of `tests/e2e/tests/sql_expression_gaps.rs` unless a row names another file. The
blocks stay here unchanged — they are the dataset-scale check — and the e2e tests carry the same
shapes on six rows (3 shards, RF 3), so they need neither the dataset nor DuckDB. Each gap
target asserts PostgreSQL's answer, measured on PostgreSQL 16.15 over those same six rows.

| Blocks | Finding | Passing test — today's behaviour | `#[ignore]` test — PostgreSQL's answer |
|---|---|---|---|
| Q24, Q25, P07, P08 | F1 ⛔ | `test_a_negative_ranking_difference_wraps_instead_of_going_negative` | `test_a_difference_of_two_ranking_values_is_signed` |
| Q26, P09 | F2 ⛔ | `test_a_window_over_the_reverse_of_an_inner_windows_order_skips_its_sort` | `test_a_window_over_the_reverse_of_an_inner_windows_order_is_sorted` |
| P10 | F3 🟡 | `test_pg_typeof_reports_a_ranking_function_as_numeric`; the wire type is `test_ranking_functions_are_bigint` (section 6) | `test_pg_typeof_reports_a_ranking_function_as_bigint` |
| P05, P05b, Q23 | F4 ❌ | `test_distinct_on_beside_a_window_or_an_aggregate_is_refused` | `test_distinct_on_beside_a_window_or_an_aggregate_answers` |
| P11 | F5 🟡 | `test_round_to_n_places_keeps_the_arguments_scale` | `test_round_to_n_places_has_scale_n` |
| P06b, P12 | F6 ❌ | `test_partitioning_by_a_running_windows_output_fails_internally` | `test_partitioning_by_a_running_windows_output_answers` |
| P01, P01b | gap-analysis § 2.4 ❌ `EXCLUDE` | `test_a_frame_exclusion_is_refused_at_parse` | `test_a_frame_exclusion_answers` |
| P02, P02b | § 2.4 ❌ windowed `FILTER`, collecting aggregate | `test_a_windowed_filter_on_string_agg_is_refused`; `array_agg` in `test_the_window_clause_surface_answers_what_postgresql_answers` (section 9) | `test_a_windowed_filter_on_a_collecting_aggregate_answers` |
| P03, P03b | § 2.3 ❌ multi-column hypothetical set | `test_a_multi_column_hypothetical_set_aggregate_is_refused`; one column in `test_the_within_group_family_answers` (section 14) | `test_a_multi_column_hypothetical_set_aggregate_answers` |
| P04, P04b | § 2.3 ❌ `GROUPING SETS ((), ())` | `test_an_empty_grouping_set_is_the_grand_total` (section 19) | `test_a_repeated_empty_grouping_set_answers_once_per_set` |
| P06 | § 3.2 🚫 `IGNORE NULLS` | `test_the_window_clause_surface_answers_what_postgresql_answers` (section 9) | — a decision, so no target |
| P14, P14b, P15, Q32 | F7 ❌ | `test_a_lateral_subquery_that_keeps_its_outer_reference_fails_to_serialize` in `sql_join_gaps.rs` (section 4); the forms that answer in `test_lateral_join` | `test_a_lateral_subquery_that_keeps_its_outer_reference_answers` |
| P13, P13b | § 2.6 ❌ correlated `NOT IN` | `test_not_in_over_a_correlated_subquery_is_refused` in `sql_join_gaps.rs` | `test_not_in_over_a_correlated_subquery` |

Before 2026-10-27, F1–F6, `EXCLUDE` and the multi-column hypothetical set had no e2e test at
all, and no e2e test called `lag`, `lead`, `string_agg`, `round` or `DISTINCT ON`. The
passing half of each ⛔ row pins the *wrong* value on purpose: when a fix lands, that pin fails,
and its `#[ignore]` partner is the test to un-ignore.

```bash
cd tests/e2e
cargo test --test sql_expression_gaps --test sql_join_gaps -- --test-threads=1            # today's behaviour, all pass
cargo test --test sql_expression_gaps --test sql_join_gaps -- --ignored --test-threads=1  # the gap map, all fail
```

Measured on 2026-10-27 on the cluster of § 6: the 10 new passing tests pass, the 11 new
`#[ignore]` tests each fail on their first PostgreSQL assertion, and the two suites stand at
87 passed / 11 ignored (`sql_expression_gaps`) and 37 passed / 3 ignored (`sql_join_gaps`).

### 8.2 The other direction: suite queries drawn from the e2e suite

Suite `1.1.0` adds Q27–Q48, one block per analytical shape that the e2e suite proves supported
and that suite `1.0.0` never ran: outer, cross, range and self joins, `LATERAL`, semi- and
anti-joins including the null-aware `NOT IN`, quantified and select-list subqueries, the six set
operations, top-N, deep paging, compound pushed predicates, shard-key reads, `DISTINCT` and
exact-numeric aggregates, mixed grouping sets, text predicates, and the two window regressions
of sections 9 and 12. Each block's description names the e2e test it comes from; Q46
(`date_trunc`) is the one with no e2e counterpart. All 22 answer, and all 22 agree with DuckDB.
Bringing them over found F7, P13's refusal, and the unrouted point read of § 6.

Left in the e2e suite on purpose, because a cell-by-cell comparison against DuckDB cannot carry
them:

- array and JSON results — `percentile_cont(ARRAY[…])`, `array_agg`, `json_agg` — whose text
  forms differ between the engines (`{1,2}` against `[1, 2]`);
- the write path (section 13 of `sql_expression_gaps.rs`) and the anonymized-column joins
  (`anonymization.rs`), which are not reads of this table;
- the type, literal and label tests (`data_types_*.rs`, sections 7, 10 and 15), which pin a
  wire type or a spelling rather than an analytical answer;
- `USING` and `NATURAL`, which are spellings of Q31's `ON` join rather than new shapes.
