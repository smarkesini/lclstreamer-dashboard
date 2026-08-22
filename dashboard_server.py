#!/usr/bin/env python3
"""Stdlib-only backend for dashboard.html — no pip install required.

Two modes:
  python dashboard_server.py                 demo mode: fabricates ticks for a handful of synthetic
                                               producer ranks + one consumer, so the page is alive with
                                               no LCLStreamer instance anywhere. Default.
  python dashboard_server.py --live           live mode: the demo generator never starts. Producer ranks
                                               (and optionally a consumer) POST ticks to /ingest, exactly
                                               as producer_hook.py / consumer_hook.py do. If nothing has
                                               POSTed yet the page just shows "waiting for the first tick".

Demo and live share the exact same state-update function (`_update`), so what the page renders in demo
mode is not a separate mock UI -- it is the real rendering path fed synthetic numbers instead of real
ones. That is the whole point of a starter kit: swap the data source, not the dashboard.

Endpoints:
  GET  /            dashboard.html
  GET  /events      text/event-stream, one JSON snapshot every ~0.5s (aggregated + rate-derived)
  POST /ingest      one tick: {"kind":"producer"|"consumer", "rank"|"id", "host", "count", "bytes",
                     "wait"(optional), "ts"(optional, seconds), "run"(optional)}
                     count/bytes are CUMULATIVE (exactly what LCLStreamer's own utils.stream.clock()
                     already tracks per rank) -- this server differences them into ev/s and MB/s so
                     the hook stays a one-line, no-math POST. See metrics_schema.md for the full contract.

  python dashboard_server.py [--host 0.0.0.0] [--port 8899] [--live] [--ranks 8] [--stale-sec 5.0]
"""
import argparse
import json
import os
import random
import socket
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

HERE = os.path.dirname(os.path.abspath(__file__))
DASHBOARD_HTML = os.path.join(HERE, "dashboard.html")

STATE = {"producers": {}, "consumers": {}, "run": None, "mode": "demo"}
LOCK = threading.Lock()
STALE_SEC = 5.0          # a rank/consumer with no tick in this long renders dim/stale, not deleted


def _bump(bucket, key, host, count, bytes_, wait=None, ts=None, run=None):
    """Fold one cumulative tick into a rate. Same function for real POSTs and the demo generator, so
    demo mode exercises the identical code path a live deployment will."""
    ts = float(ts) if ts is not None else time.time()
    count = float(count); bytes_ = float(bytes_)
    with LOCK:
        if run:
            STATE["run"] = run
        d = bucket.get(key)
        if d is None:
            bucket[key] = {"host": host, "count": count, "bytes": bytes_, "wait": wait,
                           "ev_s": 0.0, "mb_s": 0.0, "last_ts": ts, "hist": [0.0],
                           "_pc": count, "_pb": bytes_, "_pt": ts}
            return
        dt = ts - d["_pt"]
        dcount = count - d["_pc"]
        dbytes = bytes_ - d["_pb"]
        if dcount < 0 or dt <= 0:                 # counter reset (rank restarted) or clock hiccup
            dcount, dbytes, dt = 0.0, 0.0, max(dt, 1e-6)
        d["ev_s"] = dcount / dt
        d["mb_s"] = (dbytes / dt) / 1e6
        d["host"], d["count"], d["bytes"], d["wait"], d["last_ts"] = host, count, bytes_, wait, ts
        d["_pc"], d["_pb"], d["_pt"] = count, bytes_, ts
        hist = d.setdefault("hist", [])
        hist.append(d["ev_s"])
        del hist[:-16]


def ingest(payload):
    kind = payload.get("kind", "producer")
    host = payload.get("host") or "?"
    count = payload.get("count", 0)
    bytes_ = payload.get("bytes", 0)
    wait = payload.get("wait")
    ts = payload.get("ts")
    run = payload.get("run")
    if kind == "consumer":
        key = payload.get("id") or host
        _bump(STATE["consumers"], key, host, count, bytes_, wait, ts, run)
    else:
        key = payload.get("rank", 0)
        _bump(STATE["producers"], key, host, count, bytes_, wait, ts, run)


def build_snapshot():
    now = time.time()
    with LOCK:
        producers = []
        total_count = total_bytes = 0.0
        for rank, p in sorted(STATE["producers"].items()):
            stale = (now - p["last_ts"]) > STALE_SEC
            total_count += p["count"]; total_bytes += p["bytes"]
            producers.append({
                "rank": rank, "host": p["host"], "count": int(p["count"]),
                "ev_s": 0.0 if stale else round(p["ev_s"], 2),
                "mb_s": 0.0 if stale else round(p["mb_s"], 3),
                "hist": [round(x, 1) for x in p["hist"]], "stale": stale,
            })
        consumers = []
        for key, c in sorted(STATE["consumers"].items(), key=lambda kv: str(kv[0])):
            stale = (now - c["last_ts"]) > STALE_SEC
            lag_events = max(total_count - c["count"], 0) if total_count else None
            lag_frac = (lag_events / total_count) if (lag_events is not None and total_count) else None
            consumers.append({
                "host": c["host"], "count": int(c["count"]),
                "ev_s": 0.0 if stale else round(c["ev_s"], 2),
                "mb_s": 0.0 if stale else round(c["mb_s"], 3),
                "stale": stale, "lag_events": lag_events, "lag_frac": lag_frac,
            })
        return {"mode": STATE["mode"], "ts": now, "run": STATE["run"],
                "producers": producers, "consumers": consumers}


# --------------------------------------------------------------------------------------- demo mode ----
def demo_loop(n_ranks, hosts=None):
    """Fabricates a plausible run: N producer ranks on a couple of hosts at a steady rate with jitter,
    one rank goes quiet for a stretch (exercises the 'stale' rendering), one consumer trails behind by
    a growing-then-draining backlog (exercises the lag bar). Same _bump() real ticks go through."""
    hosts = hosts or [f"sdfmilan{i:03d}" for i in range(1, 3)]
    rng = random.Random(20260817)
    base_rate = [rng.uniform(650, 950) for _ in range(n_ranks)]
    counts = [0.0] * n_ranks
    cbytes = [0.0] * n_ranks
    con_count = 0.0
    con_bytes = 0.0
    quiet_rank = n_ranks - 1 if n_ranks > 2 else None
    t0 = time.time()
    while True:
        now = time.time()
        elapsed = now - t0
        quiet_now = quiet_rank is not None and (25 < (elapsed % 60) < 40)
        for r in range(n_ranks):
            if r == quiet_rank and quiet_now:
                continue                                       # rank goes silent -> ages into "stale"
            rate = max(0.0, base_rate[r] + rng.uniform(-60, 60))
            counts[r] += rate * 0.4
            cbytes[r] += rate * 0.4 * rng.uniform(45_000, 55_000)     # ~50 KB/event, jittered
            _bump(STATE["producers"], r, hosts[r % len(hosts)], counts[r], cbytes[r],
                  wait=elapsed, run="demo:synthetic-run-0001")
        # consumer drains slightly slower than the producers sum, so a small backlog breathes in and out
        produced_now = sum(counts)
        target = produced_now - abs(400 * (((elapsed / 20) % 2) - 1))    # triangular backlog 0..~400
        con_count = max(con_count, min(target, produced_now))
        con_bytes = con_count * 50_000
        _bump(STATE["consumers"], "gpu-sink", "sdfada001", con_count, con_bytes, run="demo:synthetic-run-0001")
        time.sleep(0.4)


# ------------------------------------------------------------------------------------------- server ----
class Handler(BaseHTTPRequestHandler):
    server_version = "lclsdash/0.1"

    def log_message(self, fmt, *args):
        pass                                                    # silence per-request access noise

    def _send(self, code, body: bytes, ctype: str):
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        try:
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError):
            pass

    def do_GET(self):
        if self.path in ("/", "/index.html", "/dashboard.html"):
            with open(DASHBOARD_HTML, "rb") as f:
                self._send(200, f.read(), "text/html; charset=utf-8")
            return
        if self.path == "/events":
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Cache-Control", "no-cache")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            try:
                while True:
                    snap = build_snapshot()
                    chunk = f"data: {json.dumps(snap)}\n\n".encode("utf-8")
                    self.wfile.write(chunk)
                    self.wfile.flush()
                    time.sleep(0.5)
            except (BrokenPipeError, ConnectionResetError):
                return
        self._send(404, b"not found", "text/plain")

    def do_POST(self):
        if self.path != "/ingest":
            self._send(404, b"not found", "text/plain")
            return
        try:
            n = int(self.headers.get("Content-Length", 0))
            payload = json.loads(self.rfile.read(n) or b"{}")
            ingest(payload)
            self._send(204, b"", "text/plain")
        except Exception as e:
            self._send(400, str(e).encode("utf-8"), "text/plain")


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--host", default="0.0.0.0", help="bind address (default 0.0.0.0 -- reachable from "
                     "other nodes, since real MPI ranks run elsewhere; use 127.0.0.1 to keep it local)")
    ap.add_argument("--port", type=int, default=8899)
    ap.add_argument("--live", action="store_true", help="disable the synthetic generator; wait for real "
                     "POST /ingest ticks from producer_hook.py / consumer_hook.py")
    ap.add_argument("--ranks", type=int, default=8, help="demo mode only: number of synthetic ranks")
    ap.add_argument("--stale-sec", type=float, default=5.0)
    args = ap.parse_args()

    global STALE_SEC
    STALE_SEC = args.stale_sec
    STATE["mode"] = "live" if args.live else "demo"

    httpd = ThreadingHTTPServer((args.host, args.port), Handler)
    httpd.daemon_threads = True

    advertise = socket.gethostname()
    print(f"LCLStreamer dashboard serving on http://{advertise}:{args.port}/  (bind {args.host}:{args.port})")
    if args.live:
        print("  mode: LIVE -- waiting for POST /ingest ticks (see producer_hook.py / consumer_hook.py)")
        print(f"  point the hook at http://{advertise}:{args.port}/ingest")
    else:
        print(f"  mode: DEMO -- fabricating {args.ranks} synthetic ranks + 1 consumer, no LCLStreamer needed")
        threading.Thread(target=demo_loop, args=(args.ranks,), daemon=True).start()

    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
