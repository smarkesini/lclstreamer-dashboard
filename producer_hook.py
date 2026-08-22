"""Drop-in metrics hook for LCLStreamer's producer-side main loop.

Grounded in the real source (github.com/lclstream/lclstreamer, inspected 2026-08-17):
src/lclstreamer/cmd/lclstreamer.py already ends its per-rank pipeline with

    for stat in workflow >> clock():
        log_debug(f"[Rank {mpi_rank}] {stat}]")

`clock()` (src/lclstreamer/utils/stream.py) yields exactly the dict this hook wants on every single
event: {"count": <cumulative events>, "size": <cumulative bytes>, "wait": <cumulative wall time>,
"time": <last event unix ts>}. So the entire integration is ONE line next to the existing log_debug
call -- no new instrumentation, no touching the ZMQ data path, nothing that can add latency or a new
failure mode to the actual DAQ pipeline (the hook fails silently and rate-limits itself; see below):

    from producer_hook import DashboardHook
    dash = DashboardHook("http://<dashboard-host>:8899", run=str(parameters.source_identifier))
    ...
    for stat in workflow >> clock():
        log_debug(f"[Rank {mpi_rank}] {stat}]")
        dash.maybe_emit(mpi_rank, socket.gethostname(), stat)

`mpi_rank` and `socket.gethostname()` are already local variables at that point in `main()` -- nothing
extra needs to be computed. Every rank calls this independently; the dashboard server aggregates ranks.

Safety properties (both deliberate, both required for anything bolted onto a running DAQ pipeline):
  - THROTTLED: clock() fires every event (kHz-ish); this hook only actually POSTs at most once per
    `min_interval` seconds per rank, so instrumentation cost does not scale with event rate.
  - NON-BLOCKING / FAIL-SILENT: the POST has a short timeout and every exception is swallowed. If the
    dashboard is down, unreachable, or just slow, LCLStreamer's own event loop never notices -- it must
    not, since the dashboard is a monitoring sidecar, not part of the science pipeline.
"""
import json
import time
import urllib.request


class DashboardHook:
    def __init__(self, url, run=None, min_interval=0.5, timeout=0.3):
        self.ingest_url = url.rstrip("/") + "/ingest"
        self.run = run
        self.min_interval = float(min_interval)
        self.timeout = float(timeout)
        self._last_sent = 0.0

    def maybe_emit(self, rank, host, stat):
        """`stat` is exactly a clock() dict: {"count","size","wait","time"}. Call this every event --
        the throttle below is what keeps it cheap."""
        now = time.time()
        if now - self._last_sent < self.min_interval:
            return
        self._last_sent = now
        payload = {
            "kind": "producer", "rank": rank, "host": host,
            "count": stat.get("count", 0), "bytes": stat.get("size", 0),
            "wait": stat.get("wait", 0.0), "ts": stat.get("time", now),
        }
        if self.run:
            payload["run"] = self.run
        self._post(payload)

    def _post(self, payload):
        try:
            data = json.dumps(payload).encode("utf-8")
            req = urllib.request.Request(
                self.ingest_url, data=data, method="POST",
                headers={"Content-Type": "application/json"},
            )
            urllib.request.urlopen(req, timeout=self.timeout).close()
        except Exception:
            pass                                              # monitoring must never disturb the run


if __name__ == "__main__":
    # smoke test against a running `python dashboard_server.py --live` on localhost:8899
    hook = DashboardHook("http://127.0.0.1:8899", run="smoke-test")
    count = bytes_ = 0
    for i in range(40):
        count += 137
        bytes_ += 137 * 51_200
        hook.maybe_emit(rank=0, host="localhost", stat={"count": count, "size": bytes_, "wait": i * 0.1,
                                                          "time": time.time()})
        time.sleep(0.1)
    print("sent smoke-test ticks (subject to the 0.5s throttle) to http://127.0.0.1:8899/ingest")
