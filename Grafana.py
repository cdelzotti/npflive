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
FOLDER_TITLE = "npflive"
FOLDER_UID = "npflive"
ARCHIVE_TITLE = "archive"
ARCHIVE_UID = "npflive-archive"  # top-level folder, next to npflive

FLUSH_INTERVAL = 0.5        # seconds between InfluxDB writes
FLUSH_LINES = 5000          # write earlier if that many lines are pending
DASHBOARD_INTERVAL = 2.0    # minimum seconds between two updates of the same dashboard
TIMEOUT = 10


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

    def _panel(self, i: int, kind: str, name: str) -> dict:
        # XY chart: x = NPF time series index (t in KIND-t-RESULT-NAME v), y = value.
        # The range ignores the time picker: the run_id already selects this run's samples.
        query = (f"from(bucket: {_flux_string(self.bucket)})\n"
                 f"  |> range(start: 0)\n"
                 f"  |> filter(fn: (r) => r._measurement == {_flux_string(kind)}"
                 f" and r.name == {_flux_string(name)}"
                 f" and r.run_id == {_flux_string(self.run_id)})\n"
                 f"  |> pivot(rowKey: [\"_time\"], columnKey: [\"_field\"], valueColumn: \"_value\")\n"
                 f"  |> keep(columns: [\"t\", \"value\"])\n"
                 f"  |> sort(columns: [\"t\"])")
        return {
            "id": i + 2,
            "type": "xychart",
            # Without it, the frontend migrates the options from the pre-11.1 XY chart format
            # and mangles the x/y matchers below, leaving the panel empty
            "pluginVersion": "11.1.0",
            "title": f"{kind} {name}",
            "datasource": {"type": "influxdb", "uid": DATASOURCE_UID},
            "gridPos": {"x": (i % 2) * 12, "y": 4 + (i // 2) * 8, "w": 12, "h": 8},
            "targets": [{"refId": "A", "datasource": {"type": "influxdb", "uid": DATASOURCE_UID},
                         "query": query}],
            "fieldConfig": {
                "defaults": {"custom": {"show": "points+lines", "pointSize": {"fixed": 3}}},
                "overrides": [
                    {"matcher": {"id": "byName", "options": "t"},
                     "properties": [{"id": "custom.axisLabel", "value": kind}]},
                    {"matcher": {"id": "byName", "options": "value"},
                     "properties": [{"id": "custom.axisLabel", "value": name}]},
                ],
            },
            "options": {
                "mapping": "manual",
                "series": [{"frame": {"matcher": {"id": "byIndex", "options": 0}},
                            "x": {"matcher": {"id": "byName", "options": "t"}},
                            "y": {"matcher": {"id": "byName", "options": "value"}}}],
                "legend": {"showLegend": False},
            },
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
            "tags": ["npflive"],
            "timezone": "browser",
            "refresh": "5s",  # Grafana's default min_refresh_interval
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

    def archive_dashboards(self) -> int:
        """Empty the npflive folder: move all its dashboards into the archive folder."""
        self.ensure_folder(ARCHIVE_UID, ARCHIVE_TITLE)
        uids, page = [], 1
        while True:
            found = self._check(self._req("GET", "/api/search", params={
                "folderUIDs": FOLDER_UID, "type": "dash-db", "limit": 500, "page": page}), "dashboard search")
            uids += [d["uid"] for d in found if d.get("folderUid") == FOLDER_UID and d["uid"] not in uids]
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
            "folderUid": FOLDER_UID,
            "overwrite": True,
            "message": "npflive",
        }), "dashboard update")
        dashboard.url = self.url + res.get("url", f"/d/{dashboard.uid}")


class GrafanaExporter:
    """What npflive talks to: on_run / on_result / close. Work is done in a background thread."""

    def __init__(self, grafana: Grafana, influx: InfluxWriter):
        self.grafana = grafana
        self.influx = influx
        self.queue: "queue.Queue" = queue.Queue()
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
        pending_run = None          # (serie, run, start) of a run without dashboard yet
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
                elif item[0] == "run":
                    # Close the previous run: its samples and panels must land in its own dashboard
                    lines, dirty = self._flush(lines, dashboard, dirty)
                    dashboard = None
                    pending_run = item[1:]
                elif item[0] == "result":
                    result, ts_ns = item[1:]
                    if dashboard is None:
                        dashboard = self._new_dashboard(*(pending_run or (None, None, time.time())))
                    if (result.kind, result.type) not in dashboard.series:
                        dashboard.series.append((result.kind, result.type))
                        dirty = True
                    lines.append(line_protocol(result.kind, {**dashboard.tags, "name": result.type},
                                               {"value": result.value, "t": result.time}, ts_ns))

            now = time.monotonic()
            if closing or len(lines) >= FLUSH_LINES or now - last_flush >= FLUSH_INTERVAL:
                lines = self._write(lines)
                last_flush = now
            if dirty and dashboard is not None and (closing or dashboard.url is None
                                                     or now - last_dash >= DASHBOARD_INTERVAL):
                dirty = not self._save(dashboard)
                last_dash = now

    def _new_dashboard(self, serie, run, start) -> Dashboard:
        repo = serie.repo if serie and serie.repo else "npf"
        test = serie.test if serie else ""
        if run is not None and run.n_runs:
            where = f"test {run.test}/{run.n_tests} run {run.run}/{run.n_runs}"
            if run.retry:
                where += f" retry {run.retry}"
            variables = run.variables
        else:
            where, variables = "", {}
        stamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(start))
        run_id = f"{int(start * 1000)}-{repo}-{test}-{where}"
        uid = "npf-" + hashlib.sha1(run_id.encode()).hexdigest()[:12]
        title = " ".join(x for x in [repo, test, where, stamp] if x)
        info = f"**{repo}** `{test}` {where} — started {stamp}\n\n"
        if variables:
            info += "| Variable | Value |\n|---|---|\n" + "".join(
                f"| {k} | {str(v).replace('|', '&#124;')} |\n" for k, v in variables.items())
        dashboard = Dashboard(uid, title, run_id, info, int(start * 1000), self.influx.bucket)
        dashboard.tags.update({"repo": repo, "test": test})
        return dashboard

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
