# Tick schema (`POST /ingest`)

One JSON object per HTTP POST. `count` and `bytes` are always **cumulative** totals since the
reporting process started — never a delta — because that is exactly what LCLStreamer's own
`utils.stream.clock()` already tracks per rank; the server differences successive ticks into rates,
so the hook never has to do timing math.

## Producer tick

```json
{
  "kind": "producer",
  "rank": 3,
  "host": "sdfmilan042",
  "count": 18422,
  "bytes": 933482112,
  "wait": 3.91,
  "ts": 1755454321.842,
  "run": "mfx100903824:27"
}
```

| field   | type          | required | meaning |
|---------|---------------|----------|---------|
| `kind`  | `"producer"`  | yes      | selects the producer-rank bucket |
| `rank`  | int           | yes      | MPI rank — the dashboard's per-rank key |
| `host`  | string        | yes      | `socket.gethostname()` of that rank |
| `count` | number        | yes      | cumulative events processed (`clock()["count"]`) |
| `bytes` | number        | yes      | cumulative serialized bytes (`clock()["size"]`) |
| `wait`  | number        | no       | cumulative wall time in the stream (`clock()["wait"]`); shown nowhere yet, carried for future use |
| `ts`    | number (unix) | no       | when the tick was generated; server uses receipt time if omitted |
| `run`   | string        | no       | free-form run/experiment label, shown in the header |

## Consumer tick

```json
{"kind": "consumer", "id": "gpu-sink", "host": "sdfada001", "count": 18000, "bytes": 900000000, "ts": 1755454321.9}
```

| field   | type          | required | meaning |
|---------|---------------|----------|---------|
| `kind`  | `"consumer"`  | yes      | selects the consumer bucket |
| `id`    | string        | no       | dashboard row key; defaults to `host` if omitted |
| `host`  | string        | yes      | where the sink process runs |
| `count` | number        | yes      | cumulative events pulled |
| `bytes` | number        | yes      | cumulative bytes pulled |
| `ts`    | number (unix) | no       | as above |

## Server-side derivation

For each `(kind, key)` bucket the server keeps the previous `(count, bytes, ts)` and on every new tick
computes

```
ev_s = (count - prev_count) / (ts - prev_ts)
mb_s = (bytes - prev_bytes) / (ts - prev_ts) / 1e6
```

A decrease in `count` (the reporting process restarted, so its own counters reset to 0) is treated as a
new baseline for that tick — it yields `ev_s = 0` once, not a negative rate.

A bucket with no tick in `--stale-sec` (default 5.0) renders dim/greyed in the UI but is **not**
deleted, so a rank that comes back reuses its tile instead of re-appearing at the end of the grid.

The consumer panel's backlog bar is `lag_events = max(sum(producer.count) - consumer.count, 0)`, i.e.
simple total-events-produced minus total-events-consumed, shown only once at least one consumer has
reported (there is no lag concept with zero consumers).

## What the dashboard does NOT need from you

No rates, no deltas, no aggregation across ranks — that is all done server-side from the raw cumulative
numbers `clock()` already produces. The hook's only job is throttling how often it POSTs (see
`producer_hook.py` / `consumer_hook.py`) and never blocking the real pipeline on a slow or dead
dashboard.
