#!/usr/bin/env python3
"""Transparent filter for NPF output: echoes stdin to stdout unchanged while
parsing RESULT lines live.

Usage:
    PYTHONUNBUFFERED=1 npf local --test FILE.npf | ./npflive.py --live-parsing

NPF must print its scripts' output (no --quiet in recent versions, --show-full
in older ones), and must be unbuffered, otherwise its output reaches the pipe
in large blocks.

With --quiet, NPF's output is not reprinted; combine with --live-parsing to get
only the parsed results on stdout.

With --grafana-url, focused time series (all of them without --focus) are
written to InfluxDB and plotted live in one Grafana dashboard per run:
    # Grafana auth: user:password in the URL, or the GRAFANA_TOKEN environment variable
    ... | ./npflive.py --grafana-url http://admin:admin@localhost:3000 --focus EARL:SAMPLED_LATENCY0
"""
import argparse
import dataclasses
import json
import os
import re
import sys
import time
from typing import Callable, Dict, List, Optional, Tuple

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from parser import (DEFAULT_ACCEPT_ZERO, DEFAULT_RESULT_REGEX, RunInfo, SerieInfo,
                    compile_regexes, parse_line, parse_run_header, parse_serie_header)


# NPF kinds are [A-Z0-9_]+ and result names [A-Z0-9_:~.@()-]+ (see DEFAULT_RESULT_REGEX)
FOCUS_RE = re.compile(r"(?P<kind>[A-Za-z0-9_]+)(?::(?P<names>[A-Za-z0-9_:~.@()-]+(?:,[A-Za-z0-9_:~.@()-]+)*))?")


def focus_spec(value: str):
    """argparse type for --focus: KIND or KIND:NAME[,NAME...]"""
    m = FOCUS_RE.fullmatch(value)
    if not m:
        raise argparse.ArgumentTypeError(
            f"invalid focus '{value}', expected KIND or KIND:NAME[,NAME...] (e.g. EARL:SAMPLED_LATENCY0,FILLING_RATIO0)")
    names = m.group("names")
    # Keep the given order (it is the panel order), without duplicates
    return m.group("kind"), list(dict.fromkeys(names.split(","))) if names else None


def merge_focus(specs) -> Optional[Dict[str, Optional[List[str]]]]:
    """Merge --focus specs into {kind: names}, names None meaning all results of that kind."""
    if not specs:
        return None
    focus = {}
    for kind, names in specs:
        if kind in focus and (focus[kind] is None or names is None):
            focus[kind] = None
        else:
            focus[kind] = list(dict.fromkeys(focus.get(kind, []) + names)) if names is not None else None
    return focus


def focus_rank(specs) -> Optional[Callable[[Tuple[str, str]], int]]:
    """Sort key giving the position of a (kind, name) series in the --focus arguments.

    -f EARL:A -f NRJ -f EARL:B orders A, then all NRJ series, then B. The sort is stable,
    so series with the same rank (e.g. several NRJ ones) keep their order of appearance.
    """
    if not specs:
        return None
    positions: Dict[Tuple[str, Optional[str]], int] = {}
    for kind, names in specs:
        for name in (names or [None]):
            positions.setdefault((kind, name), len(positions))

    def rank(series: Tuple[str, str]) -> int:
        kind, name = series
        candidates = [positions[k] for k in ((kind, name), (kind, None)) if k in positions]
        return min(candidates) if candidates else len(positions)
    return rank


class Sink:
    """Receives parsed results. Live output goes to stderr unless NPF's output is not reprinted."""

    def __init__(self, log_path=None, live=False, live_out=sys.stderr, focus=None, exporter=None):
        self.log = open(log_path, "a", buffering=1) if log_path else None
        self.exporter = exporter
        self.live = live
        self.live_out = live_out
        self.focus = focus
        self.focus_seen = set()
        self._ctx_key = None
        self._ctx = None

    def _focused(self, result):
        # Only time series are focused: exact match on the kind, then on the result name
        if self.focus is None:
            return True
        if result.time is None or result.kind not in self.focus:
            return False
        names = self.focus[result.kind]
        if names is not None and result.type not in names:
            return False
        self.focus_seen.add((result.kind, result.type if names is not None else None))
        return True

    def unmatched_focus(self):
        """Focus entries that never matched any result, to report typos."""
        missing = []
        for kind, names in (self.focus or {}).items():
            if names is None:
                if (kind, None) not in self.focus_seen:
                    missing.append(kind)
            else:
                missing += [f"{kind}:{n}" for n in names if (kind, n) not in self.focus_seen]
        return missing

    @staticmethod
    def _serie_str(serie: SerieInfo):
        version = f"@{serie.version}" if serie.version != "local" else ""
        return f"[{serie.repo}{version} {serie.test}] " if serie.repo else ""

    def on_serie(self, serie: SerieInfo):
        if self.live:
            print(f"[npflive] serie {self._serie_str(serie).strip()}",
                  file=self.live_out, flush=True)

    def on_run(self, serie: SerieInfo, run: RunInfo):
        if self.exporter:
            self.exporter.on_run(serie, run)
        if self.live:
            s = f" serie {run.serie}/{run.n_series}" if run.n_series > 1 else ""
            print(f"[npflive] {self._serie_str(serie)}run {run.run}/{run.n_runs} test {run.test}/{run.n_tests}{s} {run.variables}",
                  file=self.live_out, flush=True)

    def on_result(self, serie: SerieInfo, run: RunInfo, result):
        if self.log:
            # Serialize the serie/run context once, not for every result
            if self._ctx_key != (id(serie), id(run)):
                self._ctx_key = (id(serie), id(run))
                self._ctx = {"serie": dataclasses.asdict(serie), "run": dataclasses.asdict(run)}
            event = {"timestamp": time.time(), **self._ctx, "result": vars(result)}
            self.log.write(json.dumps(event) + "\n")
        focused = self._focused(result)
        if self.exporter and focused and result.time is not None:
            self.exporter.on_result(serie, run, result)
        if self.live and focused:
            t = f" @{result.kind}={result.time}" if result.time is not None else ""
            print(f"[npflive] {self._serie_str(serie)}{result.type or '<default>'} = {result.value}{t}",
                  file=self.live_out, flush=True)

    def close(self):
        if self.log:
            self.log.close()
        if self.exporter:
            self.exporter.close()


def setup_grafana(ap, args):
    """Check InfluxDB and Grafana before reading stdin, so errors show up before anything is consumed."""
    import Grafana as grafana_module
    from Grafana import Grafana, GrafanaExporter, InfluxWriter
    grafana_module.VERBOSE = args.live_parsing
    try:
        influx = InfluxWriter(args.influx_url, args.influx_org, args.influx_bucket, args.influx_token)
        influx.check()
        grafana = Grafana(args.grafana_url, os.environ.get("GRAFANA_TOKEN"))
        grafana.ensure_datasource(args.influx_grafana_url, args.influx_org, args.influx_bucket, args.influx_token)
        grafana.ensure_folder()
        grafana.check_refresh()
        if args.archive:
            n = grafana.archive_dashboards()
            if args.live_parsing:
                print(f"[npflive] archived {n} dashboard(s) into the archive folder", file=sys.stderr)
    except Exception as e:
        # Fatal: reported even without --live-parsing, in argparse's error format
        ap.exit(1, f"{ap.prog}: error: Grafana export setup failed: {e}\n")
    # Explicitly named series get their panel upfront, in --focus order
    named = list(dict.fromkeys((kind, n) for kind, names in (args.focus or []) for n in (names or [])))
    return GrafanaExporter(grafana, influx, focus_rank(args.focus), named)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--regex", action="append", default=None,
                    help="Result regex (repeatable), replaces NPF's default result_regex")
    ap.add_argument("--accept-zero", nargs="*", default=DEFAULT_ACCEPT_ZERO,
                    help="Result types for which a 0 value is kept (default: %(default)s)")
    ap.add_argument("--log", help="Append parsed results as JSON lines to this file")
    ap.add_argument("-q", "--quiet", action="store_true", help="Do not reprint NPF's output")
    ap.add_argument("-l", "--live-parsing", action="store_true",
                    help="Print run headers and results as they are parsed "
                         "(on stdout with --quiet, on stderr otherwise to keep stdout transparent), "
                         "and npflive's messages. Without it, npflive prints nothing of its own")
    g = ap.add_argument_group("Grafana export", "Grafana auth: user:password in --grafana-url, "
                              "or the GRAFANA_TOKEN environment variable")
    g.add_argument("--grafana-url", help="Grafana URL, enables the export (e.g. http://admin:admin@localhost:3000)")
    g.add_argument("--influx-url", default="http://localhost:8086", help="InfluxDB URL for npflive (default: %(default)s)")
    g.add_argument("--influx-grafana-url", default="http://influxdb:8086",
                   help="InfluxDB URL as seen from Grafana, for its datasource (default: %(default)s)")
    g.add_argument("--influx-org", default="npflive", help="InfluxDB organization (default: %(default)s)")
    g.add_argument("--influx-bucket", default="npflive", help="InfluxDB bucket (default: %(default)s)")
    g.add_argument("--archive", action="store_true",
                   help="At startup, move all dashboards of the npflive folder into the archive folder, "
                        "so that npflive only contains the upcoming runs")
    g.add_argument("--influx-token", default="npflive-example-token",
                   help="InfluxDB API token (default: %(default)s, the docker-compose.yml example)")
    ap.add_argument("-f", "--focus", action="append", type=focus_spec, metavar="KIND[:NAME,...]",
                    help="Only keep these time series, for --live-parsing and the Grafana export. "
                         "KIND is the time series kind (EARL in EARL-1.5-RESULT-X), NAME the exact result names. "
                         "e.g. --focus EARL:SAMPLED_LATENCY0,FILLING_RATIO0 --focus NRJ. "
                         "Repeatable, case-sensitive. Does not filter --log")
    args = ap.parse_args()
    focus = merge_focus(args.focus)
    if args.archive and not args.grafana_url:
        ap.error("--archive requires --grafana-url")
    exporter = setup_grafana(ap, args) if args.grafana_url else None

    regexes = compile_regexes(args.regex or [DEFAULT_RESULT_REGEX])
    sink = Sink(args.log, args.live_parsing,
                sys.stdout if args.quiet else sys.stderr, focus, exporter)
    serie = SerieInfo()
    run = RunInfo()
    stdin, stdout = sys.stdin.buffer, sys.stdout.buffer

    try:
        while True:
            raw = stdin.readline()
            if not raw:
                break
            if not args.quiet:
                stdout.write(raw)
                stdout.flush()

            line = raw.decode("utf-8", errors="replace")
            new_serie = parse_serie_header(line)
            if new_serie:
                serie = new_serie
                run = RunInfo()
                sink.on_serie(serie)
                continue
            header = parse_run_header(line)
            if header:
                run = header
                sink.on_run(serie, run)
                continue
            for result in parse_line(line, regexes, args.accept_zero):
                sink.on_result(serie, run, result)
    except KeyboardInterrupt:
        pass
    except BrokenPipeError:
        # Downstream closed: silence the error on interpreter exit
        devnull = os.open(os.devnull, os.O_WRONLY)
        os.dup2(devnull, sys.stdout.fileno())
    finally:
        sink.close()
    missing = sink.unmatched_focus()
    if missing and args.live_parsing:
        print(f"[npflive] WARNING: focus never matched: {', '.join(missing)}", file=sys.stderr)


if __name__ == "__main__":
    main()
