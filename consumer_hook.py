"""Optional metrics hook for the SINK side -- whatever pulls LCLStreamer's ZMQ PUSH stream.

Grounded in examples/pull_script_inspect_zmq.py from the real repo (github.com/lclstream/lclstreamer):
a plain `while True: msg = pull_socket.recv(); ...` loop, one message per event, already counting
`count` and printing "Received N data packets". Same idea as producer_hook.py -- fold in one call:

    from consumer_hook import DashboardHook
    dash = DashboardHook("http://<dashboard-host>:8899", id="gpu-sink")
    ...
    count = 0
    while True:
        msg = pull_socket.recv()
        count += 1
        dash.maybe_emit(host=socket.gethostname(), count=count, bytes_=len(msg))
        ...  # existing HDF5 parsing / GPU handoff unchanged

Reporting the consumer is optional -- the dashboard works with producers alone -- but without it the
"Sink / consumer" panel and the producer/consumer backlog bar have nothing to show. Only ONE process
should report a given consumer `id` (e.g. rank 0 of the sink job, or the single pull script); if several
GPU workers pull from the same queue, either pick one reporter or use a distinct `id` per worker and let
them show up as separate rows -- the dashboard does not assume a particular sink topology.

Same throttle / fail-silent contract as producer_hook.py: this must never be able to stall the pull loop.
"""
import json
import time
import urllib.request


class DashboardHook:
    def __init__(self, url, id="sink", min_interval=0.5, timeout=0.3):
        self.ingest_url = url.rstrip("/") + "/ingest"
        self.id = id
        self.min_interval = float(min_interval)
        self.timeout = float(timeout)
        self._last_sent = 0.0

    def maybe_emit(self, host, count, bytes_, run=None):
        """count/bytes_ are CUMULATIVE totals pulled so far, same convention as the producer side."""
        now = time.time()
        if now - self._last_sent < self.min_interval:
            return
        self._last_sent = now
        payload = {"kind": "consumer", "id": self.id, "host": host,
                   "count": count, "bytes": bytes_, "ts": now}
        if run:
            payload["run"] = run
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
            pass


if __name__ == "__main__":
    hook = DashboardHook("http://127.0.0.1:8899", id="smoke-sink")
    count = bytes_ = 0
    for i in range(40):
        count += 120
        bytes_ += 120 * 50_000
        hook.maybe_emit(host="localhost", count=count, bytes_=bytes_)
        time.sleep(0.1)
    print("sent smoke-test consumer ticks to http://127.0.0.1:8899/ingest")
