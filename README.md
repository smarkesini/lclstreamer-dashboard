# LCLStreamer live dashboard — starter kit

A live monitoring dashboard for LCLStreamer: a self-contained page showing per-rank producer
throughput, aggregate rates, and consumer backlog while a run is in progress, fed over a small HTTP
side channel rather than the data path itself. It works out of the box with zero LCLStreamer setup
(synthetic demo data), and has a documented, minimal hook for wiring in the real thing whenever you're
ready. Nothing here modifies LCLStreamer itself — it's a standalone add-on, one line to wire in.

## Try it right now (no LCLStreamer needed)

```bash
python3 dashboard_server.py
```

Then open the URL it prints (`http://<hostname>:8899/`). You'll see 8 synthetic producer ranks across
2 hosts, a consumer that trails behind with a breathing backlog, and a rolling throughput chart — all
fake, clearly labeled **DEMO DATA** in the header. Stdlib only, nothing to `pip install`.

## What's in here

| file | purpose |
|---|---|
| `dashboard.html` | the page itself — self-contained, no build step, no external libraries |
| `dashboard_server.py` | stdlib HTTP server: serves the page, streams updates over SSE, accepts real ticks |
| `producer_hook.py` | drop into LCLStreamer's main loop — one line, see below |
| `consumer_hook.py` | optional: drop into whatever pulls the ZMQ stream on the sink side |
| `metrics_schema.md` | the JSON tick contract, if you want to feed it from something other than the hooks |

## How it's built

`dashboard.html` is a single self-contained file: vanilla HTML/CSS/JS, CSS custom properties for
light/dark, no frameworks or build step. Rather than embedding a static trace, it opens an `EventSource`
(Server-Sent Events) connection to `dashboard_server.py`'s `/events` endpoint and re-renders on every
message (about twice a second). The server, in turn, exposes `POST /ingest` for real processes to push
ticks into — that's the whole live path.

Everything is stdlib Python + vanilla JS/CSS on purpose: nothing to install on either the LCLS producer
nodes or in the browser, and no dependency to go stale.

## Wiring in the real thing

I read the actual LCLStreamer source (`github.com/lclstream/lclstreamer`) to find the least invasive
hook point, and there's a good one already sitting in your own main loop. `src/lclstreamer/cmd/lclstreamer.py`
ends every rank's pipeline with:

```python
for stat in workflow >> clock():
    log_debug(f"[Rank {mpi_rank}] {stat}]")
```

`clock()` (`utils/stream.py`) already yields exactly `{"count", "size", "wait", "time"}` — cumulative
events, cumulative bytes, cumulative wall time, last timestamp — per rank, per event. That's the whole
tick. So the integration is one added line next to the existing `log_debug`:

```python
from producer_hook import DashboardHook

dash = DashboardHook("http://<dashboard-host>:8899", run=str(parameters.source_identifier))
...
for stat in workflow >> clock():
    log_debug(f"[Rank {mpi_rank}] {stat}]")
    dash.maybe_emit(mpi_rank, socket.gethostname(), stat)
```

`mpi_rank` and `socket.gethostname()` are already local variables right there — nothing new to compute.
`DashboardHook` throttles itself (posts at most once every 0.5s per rank, configurable) and swallows
every exception, so a slow or dead dashboard can never add latency or a new failure mode to the actual
DAQ pipeline. Every MPI rank calls this independently; the server aggregates ranks into the grid.

If you also want the sink/consumer panel and the producer↔consumer backlog bar, do the same one-line
thing wherever you pull the ZMQ stream (e.g. a `pull_script_inspect_zmq.py`-style script) — see
`consumer_hook.py`, which is modeled directly on the real `examples/pull_script_inspect_zmq.py` loop.
Reporting the consumer is optional; the producer side alone already drives most of the dashboard.

Start the server in live mode (no synthetic data at all — it just shows "waiting for the first tick"
until something POSTs):

```bash
python3 dashboard_server.py --live
```

### Where to run `dashboard_server.py`

Producer ranks may be spread across several nodes (same as LCLStreamer's own `BinaryDataStreamingDataHandler`
sink, whose README already flags "update the sink node URL to match the hostname SLURM allocated").
Same constraint here: pick a node every rank can reach — the sink/consumer node is usually the natural
choice, since ranks already open a connection to it — and point `DashboardHook(...)` at
`http://<that-host>:8899`. `--host 0.0.0.0` (the default) binds on all interfaces so it's reachable from
other nodes; use `--host 127.0.0.1` only if you're testing everything on one machine.

## Verified

Both the demo generator and the live `/ingest` path (via `python3 producer_hook.py` / `consumer_hook.py`
as standalone smoke tests) were run end-to-end against `dashboard_server.py` and checked in-browser in
both light and dark mode: rates, the throughput chart, per-rank sparklines, the stale/dim state after a
rank goes quiet, and the consumer backlog bar all came out correct. `--live` mode starts empty and only
ever shows numbers a real `POST /ingest` produced — it never fabricates data under the "LIVE" label.

## Extending it

The rank grid, the aggregate chart, and the consumer panel are all it ships with — deliberately a
starter kit, not a finished product. Natural next panels once real data is flowing: per-rank host
grouping (color by node instead of a flat grid), a run-boundary marker on the chart, GPU-side metrics
from the actual downstream compute if the sink does more than just pull bytes. All of that is new panels reading
the same `/events` snapshot shape — no server changes needed unless the tick schema itself grows.
