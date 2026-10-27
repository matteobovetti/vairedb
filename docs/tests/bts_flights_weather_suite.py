#!/usr/bin/env python3
"""Runner for bts-flights-weather-analytical-queries.md (the suite document beside this file).

For every `-- Qnn`, `-- Bnn` and `-- Pnn` SQL block of the suite document:

  1. run it on VaireDB;
  2. check it is valid PostgreSQL — `EXPLAIN` on a PostgreSQL 16 server that holds the schema
     only, so the statement is parsed, analyzed and planned but never executed;
  3. run the same text on DuckDB;
  4. compare VaireDB's answer with DuckDB's, cell by cell.

Then every Q and B block that both engines answered is timed — one warm-up and N timed runs
per engine, inside one session — its exchange operators are counted from VaireDB's `EXPLAIN`,
and the comparison is written to a timestamped report.

usage: python3 -I docs/tests/bts_flights_weather_suite.py --duckdb PATH
           [--vdb-port 5432] [--pg-port 5433] [--runs 5] [--duckdb-threads N|docker|default]
           [--only Q01,B02] [--no-perf] [--report-dir docs/tests/results] [--doc PATH]
"""
import argparse, csv, datetime, hashlib, io, math, os, platform, re, statistics, subprocess, sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
DOC = HERE / "bts-flights-weather-analytical-queries.md"
REPO = HERE.parent.parent
BLOCK = re.compile(r"```sql\n(-- ([QBP]\d+[a-z]?)\b *([^\n]*)\n.*?)```", re.S)
SUITE_VERSION = re.compile(r"^\| Suite version \| `([^`]+)`", re.M)
REL, ABS = 1e-9, 1e-9
BOOL = {"t": "true", "f": "false", "true": "true", "false": "false"}
TIMEOUT = 900


# --- engines -----------------------------------------------------------------------------

def psql(port, *extra):
    return ["psql", "-X", "-q", "-v", "ON_ERROR_STOP=1", "-v", "VERBOSITY=verbose",
            "-h", "127.0.0.1", "-p", str(port), "-U", "postgres", *extra, "-f", "-"]


def duckdb(path, *extra):
    return ["duckdb", "-bail", *extra, path]


def sh(cmd, stdin=""):
    p = subprocess.run(cmd, input=stdin, capture_output=True, text=True, timeout=TIMEOUT)
    return p.returncode, p.stdout, p.stderr


def first_error(stderr, code):
    line = next((l for l in stderr.splitlines() if "ERROR" in l or "Error" in l),
                stderr.strip().splitlines()[0] if stderr.strip() else f"exit {code}")
    return re.sub(r"^psql:<stdin>:\d+: ", "", line).strip()[:160]


def query(cmd, sql):
    """Rows (header first) or an error line."""
    code, out, err = sh(cmd, sql)
    if code != 0 or "ERROR" in err or "Error" in err:
        return None, first_error(err, code)
    return list(csv.reader(io.StringIO(out))), None


def pg_syntax(port, sql):
    """None when PostgreSQL accepts the statement, else its SQLSTATE and message."""
    code, _, err = sh(psql(port, "-o", os.devnull), "EXPLAIN " + sql)
    return first_error(err, code) if code != 0 or "ERROR" in err else None


def statement(sql):
    return sql.strip().rstrip(";") + ";\n"


EXCHANGE = re.compile(r"RepartitionExec|CoalescePartitionsExec|SortPreservingMergeExec")


def exchanges(port, sql):
    """Exchange operators in VaireDB's physical plan — where Ballista cuts the plan into stages.

    None when `EXPLAIN` cannot build a physical plan: a statement the coordinator respells on
    the execution path (a select-list `EXISTS`, for one) can answer and still not explain.
    """
    _, out, _ = sh(psql(port, "-At"), "EXPLAIN " + sql)
    if "physical_plan_error" in out or "physical_plan" not in out:
        return None
    return len(EXCHANGE.findall(out))


def time_vairedb(port, sql, runs):
    script = "\\timing on\n\\o " + os.devnull + "\n" + statement(sql) * (runs + 1)
    _, out, _ = sh(psql(port), script)
    return [float(t) for t in re.findall(r"^Time: ([\d.]+) ms", out, re.M)][1:]


def time_duckdb(path, prefix, sql, runs):
    script = prefix + ".timer on\n.output " + os.devnull + "\n" + statement(sql) * (runs + 1)
    _, out, err = sh(duckdb(path), script)
    return [float(t) * 1000 for t in re.findall(r"Run Time \(s\): real ([\d.]+)", out + err)][1:]


# --- comparison --------------------------------------------------------------------------

def cell_eq(a, b):
    if a == b or BOOL.get(a, a) == BOOL.get(b, b):
        return True
    try:
        return math.isclose(float(a), float(b), rel_tol=REL, abs_tol=ABS)
    except ValueError:
        return False


def compare(got, want):
    if got[0] != want[0]:
        return f"labels {got[0]} vs {want[0]}"
    if len(got) != len(want):
        return f"{len(got) - 1} rows vs {len(want) - 1}"
    for i, (g, w) in enumerate(zip(got[1:], want[1:]), 1):
        for j, (a, b) in enumerate(zip(g, w)):
            if not cell_eq(a, b):
                return f"row {i} {got[0][j]}: {a!r} vs {b!r}"
    return None


# --- environment -------------------------------------------------------------------------

def out_of(cmd, stdin=""):
    try:
        code, out, _ = sh(cmd, stdin)
        return out.strip() if code == 0 else ""
    except (OSError, subprocess.TimeoutExpired):
        return ""


def scalar(cmd, sql):
    return out_of([*cmd[:-2], "-At", "-c", sql]) if cmd[0] == "psql" else out_of(cmd, sql)


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()[:12]


def queries_sha():
    """The executable part of the suite: its SQL blocks, so that editing prose changes nothing."""
    blocks = "\n".join(m.group(1) for m in BLOCK.finditer(DOC.read_text()))
    return hashlib.sha256(blocks.encode()).hexdigest()[:12]


def image(container):
    img = out_of(["docker", "inspect", container, "--format", "{{.Image}}"])
    created = out_of(["docker", "image", "inspect", img, "--format", "{{.Created}}"]) if img else ""
    return img.removeprefix("sha256:")[:12], created[:19] + "Z" if created else ""


def environment(a, threads):
    vdb, pg = psql(a.vdb_port), psql(a.pg_port)
    head = out_of(["git", "-C", str(REPO), "rev-parse", "--short", "HEAD"])
    dirty = bool(out_of(["git", "-C", str(REPO), "status", "--porcelain"]))
    head_time = out_of(["git", "-C", str(REPO), "log", "-1", "--format=%cI"])
    cargo = (REPO / "Cargo.toml").read_text()
    ws = re.search(r"\[workspace\.package\][^\[]*?^version\s*=\s*\"([^\"]+)\"", cargo, re.S | re.M)
    coord, core = image("vairedb-e2e-coordinator"), image("vairedb-e2e-core-1")
    stale = ""
    if coord[1] and head_time:
        built = datetime.datetime.fromisoformat(coord[1].replace("Z", "+00:00"))
        if built < datetime.datetime.fromisoformat(head_time):
            stale = f"the coordinator image was built before HEAD was committed ({head_time})"
    cpu = out_of(["sysctl", "-n", "machdep.cpu.brand_string"]) or platform.processor()
    mem = out_of(["sysctl", "-n", "hw.memsize"])
    ram = f"{int(mem) / 2**30:.0f} GiB · " if mem.isdigit() else ""
    set_threads = f"SET threads = {threads};\n" if threads else ""
    duck_settings = out_of(duckdb(a.duckdb, "-list", "-noheader"), set_threads +
                           "SELECT current_setting('threads')::VARCHAR || ' threads · memory_limit ' "
                           "|| current_setting('memory_limit');")
    nodes = scalar(vdb, "SELECT count(*) FROM vairedb_catalog.nodes WHERE state = 'ALIVE'")
    shards = scalar(vdb, "SELECT count(*) FROM vairedb_catalog.shards WHERE table_name = 'flights_weather'")
    docker_info = out_of(["docker", "info", "--format", "{{.OperatingSystem}}|{{.NCPU}}|{{.MemTotal}}"]).split("|")
    return {
        "vairedb": f"workspace `{ws.group(1) if ws else '?'}` · git `{head}`{' (dirty)' if dirty else ''}"
                   f" · coordinator image `{coord[0]}` built {coord[1]} · core image `{core[0]}` built {core[1]}",
        "engine": scalar(vdb, "SELECT version()"),
        "cluster": f"{nodes} cores alive · flights_weather on {shards} shards",
        "duckdb": f"{out_of(['duckdb', '--version'])} · {duck_settings}",
        "postgres": scalar(pg, "SELECT version()").split(" on ")[0],
        "host": f"{cpu} · {os.cpu_count()} logical cores · {ram}{platform.system()} {platform.release()}",
        "docker": (f"{docker_info[0]} VM · {docker_info[1]} CPUs · {int(docker_info[2]) / 2**30:.1f} GiB"
                   if len(docker_info) == 3 and docker_info[2].isdigit() else "n/a"),
        "stale": stale,
    }


def docker_cpus():
    n = out_of(["docker", "info", "--format", "{{.NCPU}}"])
    return int(n) if n.isdigit() else None


# --- report ------------------------------------------------------------------------------

def sqlstate(error):
    m = re.search(r"ERROR:\s+([0-9A-Z]{5}):", error)
    return m.group(1) if m else "error"


TIMER_RESOLUTION_MS = 1.0  # DuckDB's `.timer` prints seconds with three decimals


def ms(x):
    return f"{x:,.1f}" if x is not None else "—"


def duck_ms(x):
    return "< 1" if x is not None and x < TIMER_RESOLUTION_MS else ms(x)


def ratio(v, d):
    """VaireDB ÷ DuckDB; a lower bound when DuckDB ran under the timer's resolution."""
    if not (v and d is not None):
        return "—"
    if d < TIMER_RESOLUTION_MS:
        return f"> {v / TIMER_RESOLUTION_MS:,.0f}×"
    return f"{v / d:,.1f}×"


def write_report(a, env, results, threads):
    now = datetime.datetime.now(datetime.timezone.utc)
    path = Path(a.report_dir) / f"bts-flights-weather-perf-{now:%Y%m%dT%H%M%SZ}.md"
    path.parent.mkdir(parents=True, exist_ok=True)
    version = SUITE_VERSION.search(DOC.read_text())
    timed = [r for r in results if r["duck"] and r["vdb"]]
    good = [r for r in timed if r["verdict"] == "MATCH" and r["id"].startswith("Q")]
    ratios = [statistics.median(r["vdb"]) / max(statistics.median(r["duck"]), TIMER_RESOLUTION_MS)
              for r in good]
    floor = next((statistics.median(r["vdb"]) for r in timed if r["id"] == "B02"), None)
    lines = [
        "# Performance run — BTS flights + weather analytical suite", "",
        "One DuckDB instance against the VaireDB cluster, on the same host, on the same 1,000,000-row",
        "slice. Generated by `docs/tests/bts_flights_weather_suite.py`; do not edit by hand.", "",
        "| | |", "|---|---|",
        f"| Run at (UTC) | {now:%Y-%m-%dT%H:%M:%SZ} |",
        f"| Suite | `docs/tests/{DOC.name}` · version `{version.group(1) if version else '?'}` · "
        f"SQL blocks sha256 `{queries_sha()}` |",
        f"| Runner | `docs/tests/{Path(__file__).name}` · sha256 `{sha(__file__)}` |",
        f"| Method | 1 warm-up + {a.runs} timed runs per engine and query, in one session; median reported |",
        f"| VaireDB | {env['vairedb']} |",
        f"| VaireDB engine | {env['engine']} · {env['cluster']} |",
        f"| DuckDB | {env['duckdb']}{' (pinned to the Docker CPU count)' if a.duckdb_threads == 'docker' and threads else ''} |",
        f"| PostgreSQL (syntax check only) | {env['postgres']} |",
        f"| Host | {env['host']} |",
        f"| Docker | {env['docker']} |", "",
    ]
    if env["stale"]:
        lines += [f"> **Warning:** {env['stale']}, so these timings measure that image, not HEAD.", ""]
    lines += [
        "**What is timed.** VaireDB: psql's `\\timing`, from sending the statement to receiving the",
        "last row over pgwire, with the rows discarded. DuckDB: the CLI's `.timer`, the statement's",
        "wall time with output sent to `/dev/null`. Neither includes process start or connection set-up.",
        "DuckDB's timer resolves 1 ms: a median under it shows as `< 1`, its ratio is a lower bound, and",
        "the geometric mean counts it as 1 ms, which understates the ratio rather than inflating it.",
        "*Exchanges* counts the `RepartitionExec`, `CoalescePartitionsExec` and `SortPreservingMergeExec`",
        "operators in VaireDB's `EXPLAIN` — the points where Ballista cuts a plan into stages.",
        "",
        "## Results", "",
        "| Block | Rows | VaireDB vs DuckDB | PostgreSQL syntax | DuckDB ms | VaireDB ms | VaireDB ÷ DuckDB | VaireDB min–max ms | Exchanges |",
        "|---|---:|---|---|---:|---:|---:|---:|---:|",
    ]
    for r in results:
        if not r["id"].startswith(("Q", "B")):
            continue
        d = statistics.median(r["duck"]) if r["duck"] else None
        v = statistics.median(r["vdb"]) if r["vdb"] else None
        if r["verdict"] == "MATCH":
            mark = "✅ match"
        elif r["vdb_err"]:
            mark = f"❌ VaireDB `{sqlstate(r['vdb_err'])}`"
        elif r["duck_err"]:
            mark = "— DuckDB cannot run it"
        else:
            mark = "⛔ " + r["verdict"]
        syntax = "valid" if r["pg"] is None else r["pg"]
        spread = f"{min(r['vdb']):,.1f}–{max(r['vdb']):,.1f}" if r["vdb"] else "—"
        cells = [f"{r['id']} {r['title']}", str(r["rows"]), mark, syntax, duck_ms(d), ms(v), ratio(v, d), spread,
                 str(r["exch"]) if r["exch"] is not None else "—"]
        lines.append("| " + " | ".join(c.replace("|", "\\|") for c in cells) + " |")
    lines += ["", "## Summary", ""]
    if ratios:
        geo = math.exp(sum(map(math.log, ratios)) / len(ratios))
        lines += [
            f"- Timed: {len(timed)} blocks; {len(good)} Q queries return the same answer on both engines "
            f"and are the basis of every ratio below. A ⛔ row is timed but excluded: a fast wrong answer is not a result.",
            f"- Sum of medians over those {len(good)} queries: DuckDB "
            f"{ms(sum(statistics.median(r['duck']) for r in good))} ms, VaireDB "
            f"{ms(sum(statistics.median(r['vdb']) for r in good))} ms.",
            f"- VaireDB ÷ DuckDB: geometric mean **{geo:,.1f}×**, from {min(ratios):,.1f}× to {max(ratios):,.1f}×.",
        ]
        if floor:
            share = statistics.median(floor / statistics.median(r["vdb"]) for r in good)
            lines.append(
                f"- VaireDB's floor for a distributed job (B02, one scan per shard and a count) is "
                f"{ms(floor)} ms — the median query spends {share:.0%} of its VaireDB time at that floor.")
        by_exch = {}
        for r in good:
            if r["exch"] is not None:
                by_exch.setdefault(min(r["exch"], 5), []).append(statistics.median(r["vdb"]))
        unexplained = sum(r["exch"] is None for r in good)
        lines += ["", "VaireDB time by exchange count, over the same queries"
                  + (f" ({unexplained} without a physical plan to count left out):" if unexplained else ":"),
                  "",
                  "| Exchanges | Queries | VaireDB median ms | range ms |", "|---:|---:|---:|---:|"]
        for n in sorted(by_exch):
            t = by_exch[n]
            lines.append(f"| {n if n < 5 else '≥ 5'} | {len(t)} | {ms(statistics.median(t))} | "
                         f"{ms(min(t))}–{ms(max(t))} |")
    path.write_text("\n".join(lines) + "\n")
    return path


# --- main --------------------------------------------------------------------------------

def main():
    global DOC
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--duckdb", required=True, help="DuckDB database holding flights_weather")
    ap.add_argument("--vdb-port", type=int, default=5432)
    ap.add_argument("--pg-port", type=int, default=5433)
    ap.add_argument("--runs", type=int, default=5)
    ap.add_argument("--duckdb-threads", default="docker",
                    help="N, 'docker' (match the Docker VM's CPU count) or 'default' (DuckDB's own)")
    ap.add_argument("--only", help="comma-separated block ids")
    ap.add_argument("--no-perf", action="store_true")
    ap.add_argument("--report-dir", default=str(HERE / "results"))
    ap.add_argument("--doc", default=str(DOC), help="suite document; the one beside this runner by default")
    a = ap.parse_args()
    DOC = Path(a.doc).resolve()
    only = set(a.only.split(",")) if a.only else None
    threads = (docker_cpus() if a.duckdb_threads == "docker"
               else None if a.duckdb_threads == "default" else int(a.duckdb_threads))
    # PostgreSQL's conventions where DuckDB's defaults differ: the session zone, and NULLs sorting
    # first under DESC (DuckDB sorts them last in both directions).
    prefix = ("SET TimeZone = 'UTC';\nSET default_null_order = 'nulls_last_on_asc_first_on_desc';\n"
              + (f"SET threads = {threads};\n" if threads else ""))
    blocks = [m.groups() for m in BLOCK.finditer(DOC.read_text())]
    results = []
    for sql, bid, title in blocks:
        if only and bid not in only:
            continue
        vdb_rows, vdb_err = query([*psql(a.vdb_port)[:-2], "--csv", "-f", "-"], sql)  # 1. VaireDB
        pg = pg_syntax(a.pg_port, sql)                                                # 2. PostgreSQL
        duck_rows, duck_err = query(duckdb(a.duckdb, "-csv", "-nullvalue", ""), prefix + sql)  # 3. DuckDB
        if vdb_err or duck_err:                                                       # 4. compare
            verdict = f"ERR vairedb: {vdb_err or 'ok'} · duckdb: {duck_err or 'ok'}"
        else:
            diff = compare(vdb_rows, duck_rows)
            verdict = "MATCH" if diff is None else "DIFF " + diff
        rows = len(vdb_rows) - 1 if vdb_rows else "—"
        print(f"{bid:5} rows={rows!s:>4}  pg-syntax={'valid' if pg is None else pg}\n      {verdict}", flush=True)
        results.append({"id": bid, "title": title.strip(), "rows": rows, "pg": pg, "verdict": verdict,
                        "vdb_err": vdb_err, "duck_err": duck_err,
                        "ok": not vdb_err and not duck_err, "vdb": [], "duck": [], "exch": None})
    if not a.no_perf:
        print(f"\ntiming {a.runs} runs + 1 warm-up per engine, DuckDB threads={threads or 'default'}", flush=True)
        for r, (sql, bid, _) in zip(results, [b for b in blocks if not only or b[1] in only]):
            if r["ok"] and bid.startswith(("Q", "B")):
                r["exch"] = exchanges(a.vdb_port, sql)
                r["vdb"] = time_vairedb(a.vdb_port, sql, a.runs)
                r["duck"] = time_duckdb(a.duckdb, prefix, sql, a.runs)
                if not (r["vdb"] and r["duck"]):  # a run failed after the results pass answered
                    r["vdb"], r["duck"] = [], []
                    print(f"{bid:5} timing failed", flush=True)
                    continue
                print(f"{bid:5} duckdb={duck_ms(statistics.median(r['duck']))} ms  "
                      f"vairedb={ms(statistics.median(r['vdb']))} ms", flush=True)
        report = write_report(a, environment(a, threads), results, threads)
        print(f"\nreport: {report.relative_to(REPO) if report.is_relative_to(REPO) else report}")
    bad = [r["id"] for r in results if r["id"].startswith(("Q", "B")) and (r["verdict"] != "MATCH" or r["pg"])]
    print(f"\n{len(results)} blocks · Q/B not matching or not valid PostgreSQL: {', '.join(bad) or 'none'}")
    sys.exit(1 if bad else 0)


if __name__ == "__main__":
    main()
