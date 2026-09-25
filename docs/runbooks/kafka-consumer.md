# Runbook — Kafka consumer loop / KafkaConsumerDown · KafkaConsumerReconnecting

Response procedure for the inbound Kafka consumer task in `app/application/api/lifespan.py`
(`_kafka_consumer_loop`, the O-1 reconnect wrapper). Covers both alerts on the consumer side:

- **`KafkaConsumerDown` (critical)** — `kafka_consumer_up == 0 and up{job='kafka-chat-api'} == 1`
  (docker_compose/prometheus-alerts.yml). Inbound broker messages are not reaching the WebSocket
  fan-out.
- **`KafkaConsumerReconnecting` (warning)** — `increase(kafka_consumer_reconnects_total[15m]) > 0`.
  The stream died (broker restart, network blip) or ended cleanly; the loop is retrying.

---

## Heartbeat semantics (`kafka_consumer_up`)

Read this before triaging — the gauge does not mean "connected":

- Set to 1 when the app starts the consumer task (`start_kafka_consumer`).
- **Stays 1 through reconnect backoff.** The reconnect wrapper keeps the task alive, so a healthy
  task that is merely retrying is still "up". Broker reachability during a reconnect storm is
  visible via `kafka_consumer_reconnects_total`, not this gauge.
- Drops to 0 via a done-callback on **any non-cancelled completion** of the task — an exception
  *or* a clean return. A finished loop consumes nothing, so "up" would be a lie.
- Also set to 0 by the graceful-stop path (`stop_kafka_consumer`), which is why the alert guards on
  `up{job='kafka-chat-api'} == 1` — a whole app going down drops both signals, and paging for the
  consumer on top of the app would be noise.

So: `KafkaConsumerDown` firing while the app target is up means **the task itself is dead** — the
reconnect loop only exits via cancellation in practice, so this is a real defect (or a manual
stop), not an outage symptom.

## Reconnect + backoff

Both stream exits trigger a reconnect (O-1): exceptions *and* clean iterator exit (aiokafka ends
iteration when the broker closes the stream). Only `asyncio.CancelledError` propagates — that is
the graceful stop path.

- Backoff starts at `KAFKA_CONSUMER_BACKOFF_INITIAL` (default 1.0s) and doubles up to
  `KAFKA_CONSUMER_BACKOFF_MAX` (default 30.0s).
- **Backoff resets to the initial delay on every received message** — a single message proves the
  connection is healthy again.
- Each attempt increments `kafka_consumer_reconnects_total{topic}`; the alert's `[15m]` window is
  a recency signal and clears ~15m after the last reconnect.

---

## Triage

```bash
# 1. Guard first: is the app itself even up? (If not, KafkaConsumerDown is expected noise
#    and the expression will not fire — but check why the target is down.)
curl -s http://localhost:9090/api/v1/query --data-urlencode \
  'query=up{job="kafka-chat-api"}' | python -m json.tool

# 2. Heartbeat + reconnect counters from the app
curl -s http://localhost:8000/metrics | grep -E \
  '^kafka_consumer_up|^kafka_consumer_reconnects_total|^kafka_messages_consumed_total'

# 3. Is the broker reachable from the app's perspective?
docker ps --format '{{.Names}}\t{{.Status}}' | grep -E '^kafka\b'

# 4. Which exit path fired? (exception vs clean-exit wording in the log lines)
docker logs main-app --since 15m 2>&1 | grep -i -E 'consumer stream (died|ended)' | tail
```

**Decision:**

| Observation | Meaning | Action |
|---|---|---|
| `kafka_consumer_up 0` + app target up | consumer task is dead | restart the app (below) |
| `kafka_consumer_up 1` + reconnects accruing | stream down, loop retrying | fix Kafka (see the companion kafka-outage runbook); no app action |
| reconnects stopped but no `kafka_messages_consumed_total` growth | connected but idle/poisoned topic | publish a test message; check `kafka_consumer_malformed_total` |

## Restart procedure

1. The consumer task cannot be restarted independently — it is created in the app lifespan, so
   **restart the app**: `docker restart main-app`. The outbox is durable in Mongo and messages
   produced while the consumer was down stay in the topic (Kafka retains them), so nothing is lost;
   delivery resumes from the topic on startup.
2. Confirm after restart:

   ```bash
   curl -s http://localhost:8000/metrics | grep '^kafka_consumer_up'   # expect 1
   ```

3. `KafkaConsumerDown` clears on the next Prometheus evaluation (~15s) once the gauge is back to 1.
   `KafkaConsumerReconnecting` self-resolves ~15m after the last reconnect — do not expect it to
   clear immediately.

---

## Config knobs

| Knob | Default | Effect |
|---|---|---|
| `KAFKA_CONSUMER_BACKOFF_INITIAL` | 1.0s | first reconnect delay; also the reset target when traffic resumes |
| `KAFKA_CONSUMER_BACKOFF_MAX` | 30.0s | backoff cap between reconnect attempts |
| `new_message_received_topic` (Config default `new-messages`) | — | the consumed topic |

## Companion alerts

- `KafkaConsumerDown` **inhibits** `KafkaConsumerReconnecting` (equal `job`) — while the consumer
  is truly down, reconnect noise is muted in Alertmanager (ADR-0007 §3).
- Broker-outage symptoms on the *outbound* path (relay) live in `docs/runbooks/kafka-outage.md`.
