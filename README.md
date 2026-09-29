# npflive

Live parser for [NPF](https://github.com/tbarbette/npf) outputs. Pipe an NPF run into
`npflive.py`: NPF's output goes through unchanged, while `RESULT` lines are parsed as they
arrive and time series can be plotted live in Grafana (through InfluxDB), one dashboard per run.

```
npf ... ──► npflive.py ──► stdout (NPF output, byte-for-byte)
                │
                ├──► live parsed values (-l / --focus)
                ├──► JSON lines log (--log)
                └──► InfluxDB ──► Grafana dashboard per run (--grafana-url)
```

## Requirements

- Python 3.8+ and `requests` (only needed for the Grafana export).
- For the Grafana export: InfluxDB 2.x and Grafana (tested with Grafana 13.2 and InfluxDB 2.7),
  see [docker-compose.yml](docker-compose.yml).

## Usage

```bash
PYTHONUNBUFFERED=1 npf local --test run.npf | ./npflive.py -l -f EARL:SAMPLED_LATENCY0
```

NPF must be run so that its output reaches the pipe live:

- `PYTHONUNBUFFERED=1` (or `python3 -u npf.py ...`): otherwise Python buffers NPF's output
  when it is piped, and it arrives in large blocks.
- Do not use NPF's `--quiet` (recent versions) and use `--show-full` (older versions): NPF
  must print its scripts' output, which contains the `RESULT` lines.
- If your scripts print results on stderr, add `2>&1` before the pipe.

A saved output can be replayed with `cat output.dnc | ./npflive.py ...`.

### Options

| Option | Description |
|---|---|
| `-q`, `--quiet` | Do not reprint NPF's output. |
| `-l`, `--live-parsing` | Print run headers and parsed results as they arrive: on stderr (stdout stays identical to NPF's output), or on stdout with `-q`. Also enables npflive's messages (dashboard URLs, warnings). Without it, npflive prints nothing of its own. |
| `-f`, `--focus KIND[:NAME,...]` | Only keep these time series, for `-l` and for the Grafana export. Repeatable. |
| `--log FILE` | Append every parsed result as a JSON line (not filtered by `--focus`). |
| `--regex REGEX` | Result regex, replaces NPF's default `result_regex` (repeatable). |
| `--accept-zero TYPE...` | Result types for which a value of 0 is kept (NPF's `accept_zero`). |
| `--grafana-url URL` | Enables the Grafana export, e.g. `http://admin:admin@localhost:3000`. |
| `--archive` | At startup, empty the `npflive` folder: move all its dashboards into the `archive` folder, so `npflive` only contains the upcoming runs. |
| `--influx-url URL` | InfluxDB URL used by npflive (default `http://localhost:8086`). |
| `--influx-grafana-url URL` | InfluxDB URL as seen from Grafana, for its datasource (default `http://influxdb:8086`). |
| `--influx-org`, `--influx-bucket` | InfluxDB organization and bucket (default `npflive`). |
| `--influx-token TOKEN` | InfluxDB API token (default `npflive-example-token`, the docker-compose example). |

Grafana authentication: `user:password` in `--grafana-url`, or a service account token in the
`GRAFANA_TOKEN` environment variable.

### Focus

NPF time series are printed as `KIND-TIME-RESULT-NAME VALUE`, e.g.
`EARL-128-RESULT-SAMPLED_LATENCY0 9` (kind `EARL`, time index `128`, name `SAMPLED_LATENCY0`).

- `-f EARL`: every time series of kind `EARL`.
- `-f EARL:SAMPLED_LATENCY0,FILLING_RATIO0`: only these names within `EARL`.
- `-f EARL:WATT0 -f NRJ`: several specs, merged per kind.

Matching is exact and case-sensitive; one-off results (`RESULT-X 5`) are never focused.
With `-l`, specs that never matched anything are reported on stderr at the end, to catch typos.

## Examples

```bash
# Transparent: same output as npf alone, parsed values on stderr
PYTHONUNBUFFERED=1 npf local --test run.npf | ./npflive.py -l

# Only the parsed values of two EARL series
PYTHONUNBUFFERED=1 npf local --test run.npf | ./npflive.py -q -l -f EARL:SAMPLED_LATENCY0,SAMPLED_LATENCY1

# Live Grafana dashboards
PYTHONUNBUFFERED=1 npf local --test run.npf \
  | ./npflive.py --grafana-url http://admin:admin@localhost:3000 -f EARL:SAMPLED_LATENCY0,FILLING_RATIO0

# Replay a saved output slowly, as if it were live
python3 -c "import sys,time;[(sys.stdout.write(l),sys.stdout.flush(),time.sleep(0.002)) for l in open('output.dnc')]" \
  | ./npflive.py -q --grafana-url http://admin:admin@localhost:3000 -f EARL:SAMPLED_LATENCY0
```

## Grafana setup

```bash
docker compose up -d
```

starts Grafana on port 3000 and InfluxDB on port 8086. The credentials in
[docker-compose.yml](docker-compose.yml) are example values (InfluxDB: user `admin`, password
`changeme123`, org and bucket `npflive`, token `npflive-example-token`); change them for a real
setup. They only apply on InfluxDB's first start, when its volume is empty.

Everything else is done by npflive through the Grafana API at startup: it creates the
`npflive InfluxDB` datasource (uid `npflive-influx`) and the `npflive` folder if they are
missing. Grafana must reach InfluxDB at `--influx-grafana-url`: `http://influxdb:8086` works when
both run from this compose file.

### Dashboards

- One dashboard per NPF run (`[run i/n for test j/k]`), created on the first focused sample of
  the run: runs without time series get no dashboard. With `-l`, its URL is printed on stderr.
- A text panel lists the run's variables, then one XY chart per time series, added as new
  series appear: x is NPF's time index, y the value. Dashboards refresh every 5 s.
- `--archive` keeps the `npflive` folder readable: before the new runs start, all its
  dashboards are moved to the top-level `archive` folder (they keep their URL and data).
- Comparison series (`npf ... repoA repoB`) are not plotted against each other: NPF runs each
  one entirely before the next, so each run just gets its own dashboard.

### InfluxDB data

Each sample is written with its arrival time:

```
EARL,name=SAMPLED_LATENCY0,repo=EARL\ w/\ Heuristic,run_id=...,test=run.npf value=9,t=128
```

measurement = kind, tags `name`, `run_id` (one per dashboard), `repo`, `test`; fields `value`
and `t` (NPF's time index).

## Files

| File | Content |
|---|---|
| `npflive.py` | Command line, stdin → stdout copy, focus filter, live printing, JSON log. |
| `parser.py` | Parsing of NPF output, vendored from NPF: `RESULT` regex and unit conversion (`Test.parse_results`), run and serie headers. |
| `Grafana.py` | InfluxDB writer, Grafana API client, and the exporter running in a background thread so the pipe is never slowed down. |
| `docker-compose.yml` | Example Grafana + InfluxDB setup. |

## Notes

- Parsing follows NPF: units (`ms`, `us`, `ns` → seconds; `K`, `M`, `G` → ×1024ⁿ), and a value
  of 0 is dropped unless its type is in `--accept-zero` or it belongs to a time series.
  Unlike NPF, repeated values are not aggregated (`result_add`/`result_append`): each sample
  is emitted as it arrives.
- Grafana and InfluxDB errors during the run never interrupt the pipe; they are printed on
  stderr with `-l`. Only a failed setup at startup (InfluxDB or Grafana unreachable) is always
  reported, as `npflive.py: error: ...`, and stops npflive before it reads its input.
