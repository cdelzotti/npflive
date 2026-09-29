"""Parsing of NPF output lines, vendored from NPF (~/npf3).

Sources:
- default result regex:     npf/tests/sections/__init__.py (config "result_regex")
- default accept_zero:      npf/tests/sections/__init__.py (config "accept_zero")
- value/unit conversion:    npf/tests/test.py, Test.parse_results
- run header format:        npf/tests/test.py (print_header) and npf/models/dataset.py (Run.format_variables)
- serie header format:      npf/tests/regression.py (Regression.run, "Running test" print)
- executor line prefix:     npf/executor/executor.py (Executor._print)

Unlike NPF, results are not aggregated per run (result_add/append/overwrite):
each matched sample is returned as it arrives.
"""
import re
from dataclasses import dataclass, field
from typing import Dict, List, Optional

DEFAULT_RESULT_REGEX = r"(:?(:?(?P<kind>[A-Z0-9_]+)-)?(?P<time_value>[0-9.]+)-)?RESULT(:?-(?P<type>[A-Z0-9_:~.@()-]+))?[ \t]+(?P<value>[-]?[0-9.]+(e[+-][0-9]+)?)[ ]*(?P<multiplier>[nµugmkKGT]?)(?P<unit>s|sec|b|byte|bits)?"
DEFAULT_ACCEPT_ZERO = ["time", "DROP", "DROPPED", "DROPPEDPC", "LOST"]

ANSI_RE = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")

# e.g. "N = 1, RATE = 10 [retrying 2/3 run 1/3 for test 2/10 of serie 1/2]"
RUN_HEADER_RE = re.compile(
    r"^(?P<vars>.*?)\s*\[(?:retrying (?P<retry>\d+)/(?P<n_retry>\d+) )?"
    r"run (?P<run>\d+)/(?P<n_runs>\d+) for test (?P<test>\d+)/(?P<n_tests>\d+)"
    r"(?: of serie (?P<serie>\d+)/(?P<n_series>\d+))?\]\s*$")

# e.g. "[Local] Running test math.npf..." or "[click] Running test router.npf on version abc123..."
SERIE_HEADER_RE = re.compile(
    r"^\[(?P<repo>[^\]]+)\] Running test (?P<test>.+?)(?: on version (?P<version>\S+))?\.\.\.\s*$")


def strip_ansi(line: str) -> str:
    return ANSI_RE.sub("", line)


@dataclass
class Result:
    type: str
    value: float
    kind: str = "time"
    time: Optional[float] = None
    raw_value: str = ""
    multiplier: str = ""
    unit: str = ""
    line: str = ""


@dataclass
class RunInfo:
    variables: Dict[str, str] = field(default_factory=dict)
    run: int = 0
    n_runs: int = 0
    test: int = 0
    n_tests: int = 0
    retry: int = 0
    serie: int = 1
    n_series: int = 1


@dataclass
class SerieInfo:
    repo: str = ""
    test: str = ""
    version: str = "local"


def compile_regexes(regexes: List[str]) -> List[re.Pattern]:
    return [re.compile(r, re.IGNORECASE) for r in regexes]


DEFAULT_REGEXES = compile_regexes([DEFAULT_RESULT_REGEX])


def _accept_zero(accept_zero: List[str], result_type: str) -> bool:
    # Same semantics as Config.match: re.match of each entry against the type
    for m in accept_zero:
        try:
            if re.match(m, result_type):
                return True
        except re.error:
            pass
    return False


def parse_line(line: str, regexes: List[re.Pattern] = DEFAULT_REGEXES,
               accept_zero: List[str] = DEFAULT_ACCEPT_ZERO) -> List[Result]:
    """Return the results found in one output line, converted as NPF does."""
    line = strip_ansi(line).strip()
    results = []
    for result_regex in regexes:
        for nr in result_regex.finditer(line):
            result_type = nr.group("type")
            if result_type is None:
                result_type = ''
            time_ns = nr.group("kind")
            if time_ns is None:
                time_ns = "time"
            time_value = nr.group("time_value")
            try:
                n = float(nr.group("value"))
            except Exception:
                continue
            mult = nr.group("multiplier")
            raw_mult = mult
            unit = ""
            if nr.group("unit"):
                unit = nr.group("unit")
            if unit.lower() == "sec" or unit.lower() == "s":
                unit = "s"

            if unit == "s":
                if mult == "m":
                    n = n / 1000  # Keep all results in seconds
                elif mult == "u" or mult == "µ":
                    n = n / 1000000
                elif mult == "n":
                    n = n / 1000000000
            else:
                mult = mult.upper()

            if mult == "K":
                n *= 1024
            elif mult == "M":
                n *= 1024 * 1024
            elif mult == "G":
                n *= 1024 * 1024 * 1024

            if n != 0 or _accept_zero(accept_zero, result_type) or time_value is not None:
                t = None
                if time_value:
                    try:
                        t = float(time_value)
                    except ValueError:
                        t = None
                results.append(Result(type=result_type, value=n, kind=time_ns, time=t,
                                      raw_value=nr.group("value"), multiplier=raw_mult,
                                      unit=unit, line=line))
    return results


def parse_run_header(line: str) -> Optional[RunInfo]:
    """Parse the header NPF prints before each run, or return None."""
    m = RUN_HEADER_RE.match(strip_ansi(line).strip())
    if not m:
        return None
    variables = {}
    if m.group("vars"):
        for pair in m.group("vars").split(", "):
            k, sep, v = pair.partition(" = ")
            if sep:
                variables[k.strip()] = v.strip()
    return RunInfo(variables=variables,
                   run=int(m.group("run")), n_runs=int(m.group("n_runs")),
                   test=int(m.group("test")), n_tests=int(m.group("n_tests")),
                   retry=int(m.group("retry")) if m.group("retry") else 0,
                   serie=int(m.group("serie")) if m.group("serie") else 1,
                   n_series=int(m.group("n_series")) if m.group("n_series") else 1)


def parse_serie_header(line: str) -> Optional[SerieInfo]:
    """Parse the line NPF prints when starting a test for a repo/version, or return None."""
    m = SERIE_HEADER_RE.match(strip_ansi(line).strip())
    if not m:
        return None
    return SerieInfo(repo=m.group("repo"), test=m.group("test"),
                     version=m.group("version") or "local")
