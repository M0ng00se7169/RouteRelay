# Runbook — WebSocket fan-out / WSBroadcastFailures

Response procedure for delivery failures during the WebSocket broadcast
(`ConnectionManager.send_all` in `app/infrastructure/websockets/managers.py`).

**Alert:** `WSBroadcastFailures` (warning) —
`rate(ws_broadcast_failures_total[5m]) > 0`
(`docker_compose/prometheus-alerts.yml`).

---

## What this alert means

A per-socket `send_bytes` failed during fan-out to a chat's sockets. This is **not** a message-loss
alert on the server side: the message was fanned out to every socket; the failed recipient just did
not receive that one message. The failures signal **client-side instability**, not a server fault.

### Dead-socket semantics (behavior change, ADR-0006 Chunk 4.1)

Fan-out is best-effort by design:

- One dead socket **no longer aborts** delivery to the remaining sockets (the previous unguarded
  loop let a single `send` exception kill the whole broadcast for that chat).
- Failures are counted per socket in `ws_broadcast_failures_total`; the broadcast itself still
  counts as attempted (`ws_messages_broadcast_total` increments, `ws_broadcast_duration_seconds`
  gets a sample).
- The dead socket is *not* proactively removed here — cleanup happens when the client disconnects
  (`remove_connection`) or the chat is deleted (`disconnect_all`).

So the alert firing means clients are dropping mid-broadcast — typically users closing tabs, flaky
mobile connections, or a frontend bug that leaks sockets that are never properly closed.

---

## Triage

```bash
# 1. Failure rate vs broadcast volume (a couple of failures across thousands of
#    broadcasts is background noise; a rising rate is the signal)
curl -s http://localhost:8000/metrics | grep -E \
  '^ws_broadcast_failures_total|^ws_messages_broadcast_total|^ws_connections_active' \
  | tail

# 2. Is it concentrated on the app or spread over time?
curl -s http://localhost:9090/api/v1/query --data-urlencode \
  'query=sum(rate(ws_broadcast_failures_total[5m]))' | python -m json.tool

# 3. Connection churn — high accepted+removed alongside failures = unstable clients
curl -s http://localhost:8000/metrics | grep -E \
  '^ws_connections_accepted_total|^ws_connections_removed_total'

# 4. Any server-side errors during fan-out
docker logs main-app --since 15m 2>&1 | grep -i -E 'websocket|send_bytes' | tail
```

**Decision:**

| Observation | Meaning | Action |
|---|---|---|
| occasional failures, `ws_connections_active` stable | normal client churn (closed tabs, phones sleeping) | none — this is expected background noise |
| failure rate climbing with high connection churn | clients cannot hold connections | check proxy/idle-timeout/LB settings between clients and `:${API_PORT:-8000}` |
| failures after a frontend deploy | new client leaks or mis-handles sockets | roll back / fix the frontend |
| failures + `ws_connections_active` stuck high while real users are gone | server-side socket leak | investigate `remove_connection` paths |

## What NOT to do

- Do not add retries around `send_bytes` in fan-out — the outbox/Kafka path already provides
  durable delivery for broker events; re-sending over a dead socket just burns the counter.
- Do not restart the app for this alert alone: server state here is per-connection, and a restart
  drops every live socket (one mass failure burst) without fixing the client cause.

---

## Related metrics

| Metric | Meaning |
|---|---|
| `ws_broadcast_failures_total` | per-socket send failures during fan-out (the alert's expr) |
| `ws_messages_broadcast_total` | broadcasts attempted (success/failure mix is fine) |
| `ws_broadcast_duration_seconds` | fan-out latency histogram (sampled even on failure) |
| `ws_connections_accepted_total` / `ws_connections_removed_total` | connection churn |
| `ws_connections_active` | live sockets, recomputed from manager bookkeeping (no label cardinality) |
