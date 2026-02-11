# Runbook — Kafka outage / OutboxRelayCircuitOpen

Response procedure for the relay's Kafka path failing. Built from the live fire drill of
2026-09-25 (stopped the `kafka` container in the running stack, tripped the breaker, restarted,
recovered — timeline in the appendix). Everything below was observed, not theorized.

**Alert:** `OutboxRelayCircuitOpen` (warning) —
`increase(circuit_breaker_rejected_total{name='kafka'}[15m]) > 0`
(`docker_compose/prometheus-alerts.yml`).

---

## What this alert means

The `'kafka'` circuit breaker (guards the outbox relay's sends) rejected at least one batch in the
last 15 minutes: Kafka failures accumulated to `CIRCUIT_BREAKER_FAILURE_THRESHOLD` (default 5)
consecutive failed sends, the breaker opened, and the relay is now **skipping outbox batches
instead of attempting doomed sends**. Rows are NOT lost — they stay unsent in the Mongo `outbox`
collection and drain automatically once Kafka is back.

**Companion signals you should expect:**

| Alert | During an outage | Why |
|---|---|---|
| `OutboxRelayFailing` | fires **first**, then goes **quiet once the breaker opens** | failures only happen while sends are attempted; the breaker's whole job is to stop attempting |
| `KafkaConsumerReconnecting` | fires (same root cause) | the inbound consumer loop is also reconnecting to the dead broker (self-heals) |
| `KafkaConsumerDown` | stays inactive | the consumer loop task survives outages (reconnect/backoff); it only dies on real task exit |
| `OutboxBacklogGrowing` | usually does NOT fire | threshold 200 + `for: 5m` — only sustained producer-outpacing-relay backlogs hit it |

> If `OutboxRelayCircuitOpen` is firing but `OutboxRelayFailing` is inactive, that is the breaker
> doing its job, not a monitoring bug. The rejection counter is the only live signal while open.

---

## Detection timeline (what happened, automatically)

Measured in the drill, with defaults (`FAILURE_THRESHOLD=5`, `RECOVERY_TIME=30s`,
`OUTBOX_RELAY_POLL_INTERVAL=1s`, aiokafka `request_timeout_ms=40000`):

| T+ | Event | Evidence |
|---|---|---|
| 0s | Kafka stops | messages still accepted (201) — writes are decoupled from Kafka by design |
| ~40s | first send fails | `outbox_publish_errors_total` +1; relay aborts that batch, rows stay unsent (`outbox_pending` > 0) |
| ~40s → ~3.5 min | one failure per relay tick | errors climb 1 → 5 (`OutboxRelayFailing` firing here) |
| ~3.5 min | **breaker opens** | `circuit_breaker_state{name="kafka"}=1.0`; errors FREEZE at 5 |
| +≤45s | **alert fires** | `circuit_breaker_rejected_total{name="kafka"}` starts accruing (~1/tick), Prometheus evaluates within ~2 cycles |

Detection latency is dominated by aiokafka's `request_timeout_ms` (40s default) × threshold, not by
app config. If earlier trips are wanted, lower `request_timeout_ms` in the producer (future knob),
not the breaker threshold.

---

## Triage

```bash
# 1. Is Kafka actually down?
docker ps --format '{{.Names}}\t{{.Status}}' | grep -E '^kafka\b'
docker inspect -f '{{.State.Health.Status}}' kafka

# 2. What is the app seeing? (breaker state, rejections, backlog)
curl -s http://localhost:8000/metrics | grep -E \
  '^circuit_breaker_state\{name="kafka"\}|^circuit_breaker_rejected_total\{name="kafka"\}|^outbox_pending |^outbox_publish_errors_total '

# 3. Alert states as the UI shows them
curl -s http://localhost:9090/api/v1/rules | python -m json.tool | grep -E '"name"|"state"|"health"'

# 4. Broker-side errors in the app log
docker logs main-app --since 15m 2>&1 | grep -i -E 'kafka|Unable to update metadata' | tail
```

**Decision:** Kafka down → recovery procedure below. Kafka healthy but breaker still open → check
network/DNS from `main-app`, listener config (`KAFKA_ADVERTISED_LISTENERS`), and whether half-open
probes keep failing (app log). If probes keep failing the breaker re-opens immediately — fix the
connectivity problem, do not restart the app.

---

## Recovery procedure

1. **Restore Kafka first** (restart the broker / fix connectivity). Nothing else is required —
   there is no manual breaker reset and none is needed.
2. Watch the self-healing sequence (all automatic, ~30–60s after Kafka serves requests):

   ```bash
   docker start kafka
   # then poll:
   curl -s http://localhost:8000/metrics | grep -E \
     '^circuit_breaker_state\{name="kafka"\}|^outbox_pending |^kafka_messages_sent_total'
   ```

   - breaker runs half-open probes every `CIRCUIT_BREAKER_RECOVERY_TIME` (30s); **a failed probe
     re-opens it immediately** (this is the anti-deadlock design — observed during the drill while
     Kafka was still `starting`)
   - first successful probe closes it (`circuit_breaker_state{name="kafka"}=0.0`)
   - the relay drains the backlog: `outbox_pending` → 0, `kafka_messages_sent_total` catches up
3. **No app restart.** Restarting also works (the outbox is durable in Mongo) but is unnecessary.
4. The alert **self-resolves ~15 minutes after the last rejection** (recency window) — do not
   expect it to clear the moment Kafka is back.

**Data-safety:** delivery is at-least-once — the same `event_id` can be redelivered after a crash
between send and mark-as-sent; downstream consumers dedupe on it. Requires the 2026-09-25
send-ack fix (`send_and_wait`); on older builds rows were marked sent before the broker ack and
could be silently lost (see PROJECT_MEMORY).

---

## Config knobs

| Knob | Default | Effect |
|---|---|---|
| `CIRCUIT_BREAKER_FAILURE_THRESHOLD` | 5 | consecutive failed sends before the breaker opens |
| `CIRCUIT_BREAKER_RECOVERY_TIME` | 30s | delay before each half-open probe once open |
| `OUTBOX_RELAY_POLL_INTERVAL` | 1s | relay tick rate (also the rejection accrual rate while open) |
| aiokafka `request_timeout_ms` | 40000 | per-send failure latency; dominates time-to-trip (not currently exposed) |

---

## Appendix — raw drill timeline (2026-09-25)

Poll loop: app `/metrics` + Prometheus `ALERTS{alertname="OutboxRelayCircuitOpen"}`, 15s interval.
Values: `pending` = `outbox_pending`, `errors` = `outbox_publish_errors_total`,
`state` = `circuit_breaker_state{name="kafka"}`, `rej` = `circuit_breaker_rejected_total{name="kafka"}`.

```
outage (kafka stopped)                      recovery (docker start kafka)
[ 1] pending 1  errors 0           alert:inactive    [1] kafka:starting state 1 pending 6
[ 4] pending 6  errors 1           alert:inactive    [4] kafka:healthy  state 0 pending 0
[10]             errors 3          alert:inactive        ^ half-open probe succeeded,
[16] state 1    errors 5 rej 13    alert:inactive            breaker closed, backlog drained
[17]            rej 27             alert:inactive        (kafka_messages_sent_total +6)
[18]            rej 29             alert:FIRING
errors frozen at 5 from [16] on — OutboxRelayFailing quiet while open
```
