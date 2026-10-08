"""Grafana/InfluxDB export for npflive.

Time series samples are written to InfluxDB (line protocol over HTTP), and one
Grafana dashboard per NPF run plots them with auto-refresh. Everything on the
Grafana side (datasource, folder, dashboards) goes through its HTTP API.

Network calls happen in a background thread so that npflive's stdin -> stdout
copy is never slowed down or interrupted by Grafana or InfluxDB.
"""
import hashlib
import json
import queue
import re
import sys
import threading
import time
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlsplit, urlunsplit

import requests

DATASOURCE_UID = "npflive-influx"
FOLDER_TITLE = "npflive"     # default dashboard folder (--folder)
FOLDER_UID = "npflive"
ARCHIVE_TITLE = "archive"
ARCHIVE_UID = "npflive-archive"  # top-level folder, next to the dashboard folder

FLUSH_INTERVAL = 0.5        # seconds between InfluxDB writes
FLUSH_LINES = 5000          # write earlier if that many lines are pending
DASHBOARD_INTERVAL = 2.0    # minimum seconds between two updates of the same dashboard
TIMEOUT = 10
REFRESH = "1s"              # dashboard auto-refresh, raised to Grafana's min_refresh_interval if needed
# Grafana's default refresh picker choices. The dashboard's refresh must be one of the
# timepicker.refresh_intervals, or the frontend silently never starts the auto-refresh timer.
REFRESH_INTERVALS = ["5s", "10s", "30s", "1m", "5m", "15m", "30m", "1h", "2h", "1d"]

# Grafana's classic palette: run k always gets RUN_COLORS[k-1], in every panel (--run-coalescing)
RUN_COLORS = ["#7EB26D", "#EAB839", "#6ED0E0", "#EF843C", "#E24D42",
              "#1F78C1", "#BA43A9", "#705DA0", "#508642", "#CCA300"]

_DURATION_UNITS = {"ms": 0.001, "s": 1, "m": 60, "h": 3600, "d": 86400}


def _seconds(duration: str) -> float:
    """Grafana duration ("500ms", "5s", "1m"...) to seconds."""
    m = re.fullmatch(r"(\d+(?:\.\d+)?)(ms|s|m|h|d)", duration.strip())
    if not m:
        raise ValueError(f"invalid duration '{duration}'")
    return float(m.group(1)) * _DURATION_UNITS[m.group(2)]


VERBOSE = False  # set by npflive: messages are only printed with --live-parsing


def _log(msg):
    if VERBOSE:
        print(f"[npflive] {msg}", file=sys.stderr, flush=True)


# Line protocol escaping, see https://docs.influxdata.com/influxdb/v2/reference/syntax/line-protocol/
def _escape_measurement(s: str) -> str:
    return re.sub(r"([,\s\\])", r"\\\1", s)


def _escape_key(s: str) -> str:
    # Tag keys, tag values and field keys
    return re.sub(r"([,=\s\\])", r"\\\1", s)


def _iso(ms: int) -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(ms / 1000)) + f".{ms % 1000:03d}Z"


def _flux_string(s: str) -> str:
    return json.dumps(s)


def line_protocol(measurement: str, tags: Dict[str, str], fields: Dict[str, float], ts_ns: int) -> str:
    tag_str = "".join(f",{_escape_key(k)}={_escape_key(v)}" for k, v in sorted(tags.items()) if v != "")
    field_str = ",".join(f"{_escape_key(k)}={float(v)!r}" for k, v in fields.items())
    return f"{_escape_measurement(measurement)}{tag_str} {field_str} {ts_ns}"


class InfluxWriter:
    def __init__(self, url: str, org: str, bucket: str, token: str):
        self.url = url.rstrip("/")
        self.org = org
        self.bucket = bucket
        self.session = requests.Session()
        self.session.headers["Authorization"] = f"Token {token}"

    def check(self):
        r = self.session.get(f"{self.url}/api/v2/buckets", params={"org": self.org, "name": self.bucket},
                             timeout=TIMEOUT)
        r.raise_for_status()
        if not r.json().get("buckets"):
            raise RuntimeError(f"InfluxDB bucket '{self.bucket}' not found in org '{self.org}'")

    def write(self, lines: List[str]):
        if not lines:
            return
        r = self.session.post(f"{self.url}/api/v2/write",
                              params={"org": self.org, "bucket": self.bucket, "precision": "ns"},
                              data="\n".join(lines).encode(), timeout=TIMEOUT)
        if r.status_code >= 300:
            raise RuntimeError(f"InfluxDB write failed ({r.status_code}): {r.text[:200]}")


class Dashboard:
    """A Grafana dashboard for one NPF run, with one panel per (kind, name) time series."""

    def __init__(self, uid: str, title: str, run_id: str, info_md: str, start_ms: int, bucket: str):
        self.uid = uid
        self.title = title
        self.run_id = run_id
        self.info_md = info_md
        self.start_ms = start_ms
        self.bucket = bucket
        self.series: List[Tuple[str, str]] = []
        self.tags: Dict[str, str] = {"run_id": run_id}  # plus repo/test, see GrafanaExporter
        self.url: Optional[str] = None
        self.refresh = REFRESH
        self.coalesce = False       # one curve per run of the same test (--run-coalescing)
        self.labels: List[str] = []  # extra Grafana dashboard tags (--tags), next to "npflive"
        self.run_labels: List[str] = []  # "1", "2"... "2 retry 1": one curve each when coalescing
        self.key = None             # identity of the test whose runs this dashboard gathers

    def _panel(self, i: int, kind: str, name: str) -> dict:
        # XY chart: x = NPF time series index (t in KIND-t-RESULT-NAME v), y = value.
        # The range ignores the time picker: the run_id already selects this dashboard's samples.
        def query(run: Optional[str] = None, column: str = "value") -> str:
            run_filter = f" and r.run == {_flux_string(run)}" if run is not None else ""
            q = (f"from(bucket: {_flux_string(self.bucket)})\n"
                 f"  |> range(start: 0)\n"
                 f"  |> filter(fn: (r) => r._measurement == {_flux_string(kind)}"
                 f" and r.name == {_flux_string(name)}"
                 f" and r.run_id == {_flux_string(self.run_id)}{run_filter})\n"
                 f"  |> pivot(rowKey: [\"_time\"], columnKey: [\"_field\"], valueColumn: \"_value\")\n"
                 f"  |> keep(columns: [\"t\", \"value\"])\n")
            if column != "value":
                q += f"  |> rename(columns: {{value: {_flux_string(column)}}})\n"
            return q + "  |> sort(columns: [\"t\"])"

        if self.coalesce:
            # One query, hence one frame, per run: each has its own t, so no run gets null values
            # where another run has samples (the XY chart would draw them at y=0)
            targets = [(f"R{j}", query(label, f"run {label}")) for j, label in enumerate(self.run_labels)]
            mapping = {"mapping": "auto",  # every frame, every number field but x is a curve
                       "series": [{"x": {"matcher": {"id": "byName", "options": "t"}}}],
                       "legend": {"showLegend": True, "displayMode": "list", "placement": "bottom"}}
            colors = [{"matcher": {"id": "byName", "options": f"run {label}"},
                       "properties": [{"id": "color", "value": {
                           "mode": "fixed", "fixedColor": RUN_COLORS[(int(label) - 1) % len(RUN_COLORS)]}}]}
                      for label in self.run_labels if label.isdigit()]
        else:
            targets = [("A", query())]
            mapping = {"mapping": "manual",
                       "series": [{"frame": {"matcher": {"id": "byIndex", "options": 0}},
                                   "x": {"matcher": {"id": "byName", "options": "t"}},
                                   "y": {"matcher": {"id": "byName", "options": "value"}}}],
                       "legend": {"showLegend": False}}
            colors = []
        return {
            "id": i + 2,
            "type": "xychart",
            # Without it, the frontend migrates the options from the pre-11.1 XY chart format
            # and mangles the x/y matchers below, leaving the panel empty
            "pluginVersion": "11.1.0",
            "title": f"{kind} {name}",
            "datasource": {"type": "influxdb", "uid": DATASOURCE_UID},
            "gridPos": {"x": (i % 2) * 12, "y": 4 + (i // 2) * 8, "w": 12, "h": 8},
            "targets": [{"refId": ref, "datasource": {"type": "influxdb", "uid": DATASOURCE_UID},
                         "query": q} for ref, q in targets],
            "fieldConfig": {
                "defaults": {"custom": {"show": "points+lines", "pointSize": {"fixed": 3}, "axisLabel": name}},
                "overrides": [
                    {"matcher": {"id": "byName", "options": "t"},
                     "properties": [{"id": "custom.axisLabel", "value": kind}]},
                ] + colors,
            },
            "options": mapping,
        }

    def to_json(self) -> dict:
        panels = [{
            "id": 1,
            "type": "text",
            "title": "Run",
            "gridPos": {"x": 0, "y": 0, "w": 24, "h": 4},
            "options": {"mode": "markdown", "content": self.info_md},
        }]
        panels += [self._panel(i, kind, name) for i, (kind, name) in enumerate(self.series)]
        return {
            "uid": self.uid,
            "title": self.title,
            "tags": list(dict.fromkeys(["npflive"] + self.labels)),
            "timezone": "browser",
            "refresh": self.refresh,
            "timepicker": {"refresh_intervals": list(dict.fromkeys([self.refresh] + REFRESH_INTERVALS))},
            # Absolute times must be ISO 8601 in the dashboard JSON: the frontend misreads epoch ms
            "time": {"from": _iso(self.start_ms - 10000), "to": "now"},
            "panels": panels,
            "schemaVersion": 39,
        }


class Grafana:
    def __init__(self, url: str, token: Optional[str] = None):
        parts = urlsplit(url)
        self.session = requests.Session()
        if token:
            self.session.headers["Authorization"] = f"Bearer {token}"
        elif parts.username:
            self.session.auth = (parts.username, parts.password or "")
        # Strip credentials from the URL used for requests and printed links
        netloc = (parts.hostname or "") + (f":{parts.port}" if parts.port else "")
        self.url = urlunsplit((parts.scheme, netloc, parts.path.rstrip("/"), "", ""))
        self.refresh = REFRESH
        self.folder_uid = FOLDER_UID  # where dashboards are saved, see use_folder

    def check_refresh(self):
        """Use REFRESH unless Grafana's min_refresh_interval is higher: it rejects faster dashboards."""
        settings = self._check(self._req("GET", "/api/frontend/settings"), "settings read")
        server_min = settings.get("minRefreshInterval") or REFRESH
        try:
            if _seconds(server_min) > _seconds(REFRESH):
                self.refresh = server_min
                _log(f"dashboards refresh every {server_min}: Grafana's min_refresh_interval does not allow "
                     f"{REFRESH} (set GF_DASHBOARDS_MIN_REFRESH_INTERVAL={REFRESH})")
        except ValueError:
            self.refresh = server_min

    def _req(self, method, path, **kw) -> requests.Response:
        return self.session.request(method, f"{self.url}{path}", timeout=TIMEOUT, **kw)

    def _check(self, r: requests.Response, what: str):
        if r.status_code >= 300:
            raise RuntimeError(f"Grafana {what} failed ({r.status_code}): {r.text[:200]}")
        return r.json()

    def ensure_datasource(self, influx_url: str, org: str, bucket: str, token: str):
        r = self._req("GET", f"/api/datasources/uid/{DATASOURCE_UID}")
        if r.status_code == 200:
            return
        if r.status_code != 404:
            self._check(r, "datasource lookup")
        self._check(self._req("POST", "/api/datasources", json={
            "uid": DATASOURCE_UID,
            "name": "npflive InfluxDB",
            "type": "influxdb",
            "access": "proxy",
            "url": influx_url,
            "jsonData": {"version": "Flux", "organization": org, "defaultBucket": bucket},
            "secureJsonData": {"token": token},
        }), "datasource creation")
        _log(f"created Grafana datasource '{DATASOURCE_UID}' -> {influx_url}")

    def ensure_folder(self, uid: str = FOLDER_UID, title: str = FOLDER_TITLE):
        r = self._req("GET", f"/api/folders/{uid}")
        if r.status_code == 200:
            return
        if r.status_code != 404:
            self._check(r, "folder lookup")
        self._check(self._req("POST", "/api/folders", json={"uid": uid, "title": title}), "folder creation")

    def use_folder(self, title: str = FOLDER_TITLE):
        """Save the dashboards into the top-level folder with this title, created if missing.
        An existing folder is matched by title, so folders created by hand in Grafana work too."""
        found = self._check(self._req("GET", "/api/folders", params={"limit": 1000}), "folder list")
        for folder in found:
            if folder.get("title") == title:
                self.folder_uid = folder["uid"]
                return
        uid = FOLDER_UID if title == FOLDER_TITLE else "npflive-" + hashlib.sha1(title.encode()).hexdigest()[:12]
        self.ensure_folder(uid, title)
        self.folder_uid = uid

    def archive_dashboards(self) -> int:
        """Empty the dashboard folder: move all its dashboards into the archive folder."""
        if self.folder_uid == ARCHIVE_UID:
            raise RuntimeError(f"--archive cannot be used when dashboards go to the '{ARCHIVE_TITLE}' folder itself")
        self.ensure_folder(ARCHIVE_UID, ARCHIVE_TITLE)
        uids, page = [], 1
        while True:
            found = self._check(self._req("GET", "/api/search", params={
                "folderUIDs": self.folder_uid, "type": "dash-db", "limit": 500, "page": page}), "dashboard search")
            uids += [d["uid"] for d in found if d.get("folderUid") == self.folder_uid and d["uid"] not in uids]
            if len(found) < 500:
                break
            page += 1
        for uid in uids:
            dashboard = self._check(self._req("GET", f"/api/dashboards/uid/{uid}"), "dashboard read")
            self._check(self._req("POST", "/api/dashboards/db", json={
                "dashboard": dashboard["dashboard"],
                "folderUid": ARCHIVE_UID,
                "overwrite": True,
                "message": "npflive: archived",
            }), "dashboard archive")
        return len(uids)

    def save_dashboard(self, dashboard: Dashboard):
        res = self._check(self._req("POST", "/api/dashboards/db", json={
            "dashboard": dashboard.to_json(),
            "folderUid": self.folder_uid,
            "overwrite": True,
            "message": "npflive",
        }), "dashboard update")
        dashboard.url = self.url + res.get("url", f"/d/{dashboard.uid}")


class GrafanaExporter:
    """What npflive talks to: on_run / on_result / close. Work is done in a background thread."""

    def __init__(self, grafana: Grafana, influx: InfluxWriter, order=None,
                 named: Optional[List[Tuple[str, str]]] = None, coalesce: bool = False,
                 labels: Optional[List[str]] = None):
        """order: optional sort key on (kind, name) giving the panel order; default is order of appearance.
        named: (kind, name) series whose panels are created upfront in every dashboard.
        coalesce: the runs of a same test share one dashboard, one curve per run.
        labels: extra Grafana tags given to every dashboard.

        A first dashboard is created right away, before any run header, and the first run adopts it.
        """
        self.grafana = grafana
        self.influx = influx
        self.order = order
        self.named = sorted(named or [], key=order) if order else list(named or [])
        self.coalesce = coalesce
        self.labels = list(labels or [])
        self._count = 0
        self.queue: "queue.Queue" = queue.Queue()
        self.queue.put(("start", time.time()))
        self.thread = threading.Thread(target=self._worker, name="npflive-grafana", daemon=True)
        self.thread.start()

    # Called from the main thread: only enqueue, never block on the network

    def on_run(self, serie, run):
        self.queue.put(("run", serie, run, time.time()))

    def on_result(self, serie, run, result):
        self.queue.put(("result", result, time.time_ns()))

    def close(self):
        self.queue.put(("close",))
        self.thread.join()

    # Worker thread

    def _worker(self):
        lines: List[str] = []
        dashboard: Optional[Dashboard] = None
        described = False           # whether the current dashboard already belongs to a run
        run_label = ""              # "run" tag of the samples: run index (+ retry)
        dirty = False
        last_flush = last_dash = time.monotonic()
        closing = False

        while not closing:
            try:
                item = self.queue.get(timeout=FLUSH_INTERVAL)
            except queue.Empty:
                item = None

            if item is not None:
                if item[0] == "close":
                    closing = True
                elif item[0] == "start":
                    # Created at startup, before any run header: the first run adopts it
                    dashboard, described, dirty = self._new_dashboard(item[1]), False, True
                elif item[0] == "run":
                    serie, run, start = item[1:]
                    # Close the previous run: its samples and panels must land in its own dashboard
                    lines, dirty = self._flush(lines, dashboard, dirty)
                    run_label = f"{run.run}" + (f" retry {run.retry}" if run.retry else "")
                    key = self._test_key(serie, run)
                    if self.coalesce and described and dashboard is not None and dashboard.key == key:
                        # Another run of the same test: same dashboard, new curve
                        for label in self._run_labels(run):
                            if label not in dashboard.run_labels:
                                dashboard.run_labels.append(label)
                                dirty = True
                    else:
                        if dashboard is None or described:
                            dashboard = self._new_dashboard(start)
                        dashboard.key, dashboard.run_labels = key, self._run_labels(run)
                        self._describe(dashboard, serie, run, start)
                        described = True
                        dirty = not self._save(dashboard)  # right away, not debounced
                        last_dash = time.monotonic()
                elif item[0] == "result":
                    result, ts_ns = item[1:]
                    if dashboard is None:
                        dashboard, dirty = self._new_dashboard(time.time()), True
                    if (result.kind, result.type) not in dashboard.series:
                        dashboard.series.append((result.kind, result.type))
                        if self.order:
                            dashboard.series.sort(key=self.order)  # stable: ties keep appearance order
                        dirty = True
                    lines.append(line_protocol(result.kind, {**dashboard.tags, "name": result.type, "run": run_label},
                                               {"value": result.value, "t": result.time}, ts_ns))

            now = time.monotonic()
            if closing or len(lines) >= FLUSH_LINES or now - last_flush >= FLUSH_INTERVAL:
                lines = self._write(lines)
                last_flush = now
            if dirty and dashboard is not None and (closing or dashboard.url is None
                                                     or now - last_dash >= DASHBOARD_INTERVAL):
                dirty = not self._save(dashboard)
                last_dash = now

    def _new_dashboard(self, start) -> Dashboard:
        """Empty dashboard with the named panels, waiting for its run header."""
        self._count += 1
        start_ms = int(start * 1000)
        stamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(start))
        run_id = f"{start_ms}-{self._count}"
        uid = "npf-" + hashlib.sha1(run_id.encode()).hexdigest()[:12]
        dashboard = Dashboard(uid, f"npflive {stamp} (waiting for a run)", run_id,
                              f"Waiting for a run — started {stamp}", start_ms, self.influx.bucket)
        dashboard.series = list(self.named)
        dashboard.refresh = self.grafana.refresh
        dashboard.coalesce = self.coalesce
        dashboard.labels = self.labels
        return dashboard

    @staticmethod
    def _run_labels(run) -> List[str]:
        """Curves expected for this test: all its runs, plus this retry if it is one."""
        labels = [str(k) for k in range(1, max(run.n_runs, run.run, 1) + 1)]
        if run.retry:
            labels.append(f"{run.run} retry {run.retry}")
        return labels

    @staticmethod
    def _test_key(serie, run):
        """Runs with the same key are repetitions of the same test."""
        return (serie.repo, serie.version, serie.test, run.test, run.n_tests, run.serie,
                tuple(sorted(run.variables.items())))

    def _describe(self, dashboard: Dashboard, serie, run, start):
        """Give the dashboard its run: title, variables panel and repo/test tags."""
        repo = serie.repo if serie and serie.repo else "npf"
        test = serie.test if serie else ""
        if run is not None and run.n_runs and self.coalesce:
            where = f"test {run.test}/{run.n_tests} ({run.n_runs} run{'s' if run.n_runs > 1 else ''})"
            variables = run.variables
        elif run is not None and run.n_runs:
            where = f"test {run.test}/{run.n_tests} run {run.run}/{run.n_runs}"
            if run.retry:
                where += f" retry {run.retry}"
            variables = run.variables
        else:
            where, variables = "", {}
        stamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(start))
        dashboard.title = " ".join(x for x in [repo, test, where, stamp] if x)
        info = f"**{repo}** `{test}` {where} — started {stamp}\n\n"
        if variables:
            info += "| Variable | Value |\n|---|---|\n" + "".join(
                f"| {k} | {str(v).replace('|', '&#124;')} |\n" for k, v in variables.items())
        dashboard.info_md = info
        dashboard.tags.update({"repo": repo, "test": test})

    def _flush(self, lines, dashboard, dirty):
        lines = self._write(lines)
        if dirty and dashboard is not None:
            dirty = not self._save(dashboard)
        return lines, dirty

    def _write(self, lines: List[str]) -> List[str]:
        try:
            self.influx.write(lines)
        except Exception as e:
            _log(f"WARNING: {e} ({len(lines)} samples dropped)")
        return []

    def _save(self, dashboard: Dashboard) -> bool:
        new = dashboard.url is None
        try:
            self.grafana.save_dashboard(dashboard)
        except Exception as e:
            _log(f"WARNING: {e}")
            return False
        if new:
            _log(f"dashboard: {dashboard.url}")
        return True
