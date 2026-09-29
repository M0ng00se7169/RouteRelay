# Runbook: Valkey outage

**Severity of this alert: warning, never a page.** The cache is best-effort by
contract (ADR-0008). A Valkey outage costs *latency and some features*, not
availability. Do not page anyone for `CacheErrorsHigh`, and do not restart the
app to "fix" it.

## What actually breaks while Valkey is down

| Area | Behaviour during the outage | User impact |
|---|---|---|
| `GET /chat/{oid}/` (chat detail) | Served from Mongo on every request | Slower, identical answers |
| `GET /chat/{oid}/messages/` | Served from Mongo on every request | Slower, identical answers |
| `POST` / `DELETE` | Fully working; the invalidation call is dropped | None. Worst case is a stale read lasting `CACHE_TTL_SECONDS` |
| `GET /chat/{oid}/presence/` | Reports `count: 0` (or stops refreshing) | Under-reports online users |
| Outbox relay | With `RELAY_LOCK_ENABLED=true` the relay cannot confirm leadership, so it **skips its ticks** | Events are delayed by up to `RELAY_LOCK_TTL_SECONDS`; the outbox is at-least-once, so nothing is lost |
| Kafka delivery | Unaffected | None |

Nothing is written to Mongo through the cache, and no cache state is
authoritative, so **there is no data to reconcile and no replay to run**.

## Triage

### 1. Confirm it is Valkey, not the app

```bash
docker ps --filter name=chat-valkey          # is the container up?
docker logs --tail 50 chat-valkey
docker exec chat-valkey valkey-cli ping      # PONG = the server is fine
docker exec chat-valkey valkey-cli info memory
```

### 2. Read the two signals together

```
cache_errors_total{operation=~"get|set|delete|increment"}   # real Valkey failures (pre-breaker)
circuit_breaker_state{name="valkey"}                         # 1 = the breaker is open or half-open
circuit_breaker_rejected_total{name="valkey"}                # calls short-circuited by the breaker
```

* `cache_errors_total` rising, `circuit_breaker_state{name="valkey"}` flipping to
  1: the server is rejecting or unreachable, and the app has stopped paying the
  connect timeout per request. This is the app behaving correctly.
* Only `circuit_breaker_rejected_total` rising: the breaker is doing its job;
  the underlying failures have already stopped.

If either is high while the app answers requests normally, leave it alone and
watch `up{job="kafka-chat-api"}` and `outbox_pending` for real trouble.

### 3. Is memory pressure involved?

The container runs with `--maxmemory 128mb --maxmemory-policy noeviction` on
purpose. `noeviction` means writes FAIL under pressure rather than silently
evicting — an LRU policy could evict `lock:outbox-relay` and hand the same
lease to two relays at once. So a full Valkey shows up as write errors:

```bash
docker exec chat-valkey valkey-cli info memory
# used_memory_human, and mem_fragmentation_ratio
```

There is no persistence by design (no RDB/AOF): everything in Valkey is
reconstructable ephemera. Restarting the container loses nothing.

## Recovery

**Self-healing, no action required.** The `'valkey'` breaker probes on its
half-open state after `CIRCUIT_BREAKER_RECOVERY_TIME` (default 30s); a
successful probe closes it and the cache warms up again within one
`CACHE_TTL_SECONDS` window. `CacheErrorsHigh` clears ~15 minutes after the last
failure, because it uses the same `increase(...[15m])` recency pattern as the
Kafka alerts.

If the container itself is unhealthy:

```bash
docker compose -f docker_compose/valkey.yaml up -d
docker exec chat-valkey valkey-cli ping
```

No app restart is needed. The app reconnects on its own (the client builds a
fresh connection per command through its pool).

To fully disable the feature while you investigate, set `CACHE_ENABLED=false`
(plus `PRESENCE_ENABLED=false` / `RELAY_LOCK_ENABLED=false` for the others) and
restart the app — with the flags off the container builds no Valkey client at
all. Note the opposite default: the knobs ship **off** in `Config()` and **on**
in the compose `.env`, so a deployment that never added the valkey service is
already in the safe state.

## Escalation

Escalate only if Valkey is healthy but the app still misbehaves:

* `outbox_pending` climbing while `circuit_breaker_state{name="kafka"}` is 0
  and the relay lease is held — that is a relay problem, see
  [kafka-outage.md](kafka-outage.md).
* `GET /chat/{oid}/presence/` returning 500 — the presence endpoint is the only
  new surface that reads the cache directly; the read endpoints are insulated by
  the best-effort wrapper.
