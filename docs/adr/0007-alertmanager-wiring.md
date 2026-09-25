# ADR-0007: Alertmanager Wiring — Routing, Deduplication, Silences

**Status**: Accepted — fully implemented 2026-09-25 (Chunks 1–4) with the webhook-sink transport (see §9); Telegram receiver remains an open option
**Created**: 2026-09-25
**Scope**: `docker_compose/` (new alertmanager service), `prometheus.yml`, `.env*`; no app code
**Related**: ADR-0006 (metrics + alert rules, Chunk 7.2 deferred Alertmanager wiring),
`docker_compose/prometheus-alerts.yml` (the 6 rules this plan routes), `docs/runbooks/kafka-outage.md`

---

## 0. Purpose

Alert rules exist and are validated (promtool, live fire drill), but alerts currently terminate in
Prometheus's own UI: nobody is paged, grouped, or deduplicated, and there is no way to silence
during planned maintenance. This ADR specifies the **minimal Alertmanager wiring** for the
existing 6-rule `kafka-chat` group, split into small independently verifiable chunks in the
ADR-0006 style.

Design principles:

1. **Reuse the stack's conventions** — one service per compose file, `container_name`, backend
   network, env-driven ports, provisioned config mounted read-only.
2. **No new dependencies in app code** — Alertmanager is purely infrastructure; `make` targets and
   `.env` are the only touchpoints outside `docker_compose/`.
3. **Routing mirrors the runbook split** — the two outbox/relay warnings route together because
   they share one root cause and one runbook (`docs/runbooks/kafka-outage.md`); consumer-side
   alerts route separately; critical always wins over warning.
4. **Silences over config changes** — planned maintenance (drills, deploys) uses silences, never
   rule edits, so history and dashboards stay truthful.

---

## 1. Current state (verified 2026-09-25)

- 6 rules in group `kafka-chat` (`docker_compose/prometheus-alerts.yml`), all evaluated every 15s:
  `OutboxBacklogGrowing` (warning), `OutboxRelayFailing` (warning), `OutboxRelayCircuitOpen`
  (warning, has `runbook_url`), `KafkaConsumerDown` (**critical**), `KafkaConsumerReconnecting`
  (warning), `WSBroadcastFailures` (warning).
- Prometheus (`prom/prometheus:latest`, v3) scrapes `main-app:8000` and itself; `rule_files` wires
  the alerts file; **no `alerting:` section exists yet**.
- No Alertmanager container, no `alertmanager.yaml` anywhere, no `ALERTMANAGER_PORT` in `.env`.
- Annotations in use: `summary`, `description`, `runbook_url` (one rule).

Alert inventory with the labels this plan relies on:

| Alert | Severity | Route (§3) | Runbook |
|---|---|---|---|
| `OutboxRelayFailing` | warning | outbox | kafka-outage.md ✅ linked (Chunk 4) |
| `OutboxRelayCircuitOpen` | warning | outbox | kafka-outage.md ✅ linked |
| `OutboxBacklogGrowing` | warning | outbox | kafka-outage.md ✅ linked (Chunk 4) |
| `KafkaConsumerReconnecting` | warning | consumer | kafka-consumer.md ✅ linked (Chunk 4) |
| `WSBroadcastFailures` | warning | realtime | ws-fanout.md ✅ linked (Chunk 4) |
| `KafkaConsumerDown` | **critical** | consumer | kafka-consumer.md ✅ linked (Chunk 4) |

---

## 2. Chunk 1 — Run Alertmanager in the stack ✅ implemented 2026-09-25

**Files (new):** `docker_compose/alertmanager.yaml`, `docker_compose/alertmanager/alertmanager.yml`
**Files (edit):** `prometheus.yml` (alerting section), `Makefile` (targets), `.env.example`,
`docs/architecture.md`, `README.md`

```yaml
# docker_compose/alertmanager.yaml
services:
  alertmanager:
    image: prom/alertmanager:latest
    container_name: alertmanager
    ports:
      - '${ALERTMANAGER_PORT}:9093'
    volumes:
      # Same pattern as prometheus.yaml: ../ because compose resolves
      # relative paths against THIS file's directory.
      - ./alertmanager/alertmanager.yml:/etc/alertmanager/alertmanager.yml:ro
      - alertmanager-data:/alertmanager
    networks:
      - backend

volumes:
  alertmanager-data:

networks:
  backend:
    driver: bridge
```

```yaml
# prometheus.yml — added section (indentation matches existing file)
alerting:
  alertmanagers:
    - static_configs:
        - targets: ['alertmanager:9093']
```

- **Service discovery note:** `alertmanager:9093` resolves on the shared `backend` network only if
  Alertmanager is attached to it — which requires the prometheus + alertmanager compose files to be
  included in the same `up` invocation. Extend the `prometheus` make target (it already merges
  files) rather than inventing a new one:

  ```make
  PROMETHEUS_FILE = docker_compose/prometheus.yaml
  ALERTMANAGER_FILE = docker_compose/alertmanager.yaml   # new

  prometheus:
  	$(DC) -f $(PROMETHEUS_FILE) -f $(ALERTMANAGER_FILE) -f $(APP_FILE) -f $(KAFKA_FILE) $(ENV) up --build -d
  ```

  `all` and `all-down` gain the file too. (Gotcha learned in this session: an `up` that omits a
  service file leaves that service out of the reconcile — and compose warns about orphan
  containers; keep the file lists symmetric between `up` and `down`.)
- **`.env` / `.env.example`:** add `ALERTMANAGER_PORT=9093` next to `PROMETHEUS_PORT`
  (lesson from the Grafana ephemeral-port incident: always set the port explicitly, in BOTH files).
- **Validation:** `amtool check-config /etc/alertmanager/alertmanager.yml` in the container
  (v3 image: use `--entrypoint=amtool`, same override dance as promtool); Prometheus
  `/api/v1/status/config` shows the alerting section; `up{job=...}` for alertmanager appears via
  the optional self-monitoring scrape (Chunk 2).

**Acceptance:** `curl localhost:${ALERTMANAGER_PORT}/-/ready` → `OK`; firing a hand-written
`ALERTS`-style test via `amtool` or a temporary always-firing rule shows up in
`http://localhost:${ALERTMANAGER_PORT}/#/alerts`.

---

## 3. Chunk 2 — Routing tree ✅ implemented 2026-09-25

The whole design in one tree (start in `alertmanager/alertmanager.yml`):

```yaml
route:
  receiver: default-log          # catch-all: must exist, must never page
  group_by: ['alertname', 'job']
  group_wait: 30s
  group_interval: 5m
  repeat_interval: 4h
  routes:
    # critical pages immediately, no batching delay
    - matchers: [ 'severity="critical"' ]
      receiver: oncall-critical
      group_wait: 0s
      repeat_interval: 1h
      continue: false
    # warnings grouped per subsystem
    - matchers: [ 'severity="warning"' ]
      receiver: team-warnings
      group_wait: 5m
      repeat_interval: 12h
      continue: false

receivers:
  - name: default-log
    # dev stack: land somewhere greppable instead of being dropped
    webhook_configs:
      - url: 'http://main-app:8000/__alertsink'   # Chunk 3 alternative: file/none
        send_resolved: false
  - name: oncall-critical
    <integration chosen in Chunk 3>
  - name: team-warnings
    <integration chosen in Chunk 3>

inhibit_rules:
  # an open circuit explains BOTH relay warnings: while OutboxRelayCircuitOpen
  # fires, mute the noisier error-rate alert (the drill showed exactly this
  # overlap — errors freeze once the breaker opens, but both can co-fire
  # during the trip transition)
  - source_matchers: [ 'alertname="OutboxRelayCircuitOpen"' ]
    target_matchers: [ 'alertname="OutboxRelayFailing"' ]
    equal: ['job']
  # a dead consumer makes reconnect noise irrelevant
  - source_matchers: [ 'alertname="KafkaConsumerDown"' ]
    target_matchers: [ 'alertname="KafkaConsumerReconnecting"' ]
    equal: ['job']
```

Rationale:

- **group_by `[alertname, job]`** — the stack is single-instance today; `job` future-proofs grouping
  without introducing a cluster label we don't emit.
- **`group_wait: 0s` for critical, `5m` for warnings** — matches severity semantics: critical
  (`KafkaConsumerDown`) means inbound delivery is stopped *right now*; warnings describe degraded
  but self-healing states.
- **`repeat_interval` asymmetry** — critical re-pages hourly until acked; warnings re-announce
  twice a day at most. The recency-style alerts (`OutboxRelayCircuitOpen`,
  `KafkaConsumerReconnecting`) self-resolve ~15m after the last bad event, so repeats only matter
  for genuinely persistent problems.
- **inhibit_rules encode the runbook's causal story** — "circuit open explains relay failures" and
  "consumer down explains reconnects" stop the phone from telling the same story twice.
- The catch-all `default-log` receiver MUST remain; an unroutable alert in Alertmanager is dropped
  silently, which is how alerts get lost.

**Validation:** `amtool check-config`; unit-style route checks with `amtool config routes test
--config.file=... severity=warning alertname=OutboxRelayFailing` (each of the 6 alerts × severity
must land in the intended receiver); one live end-to-end firing per receiver.

---

## 4. Chunk 3 — Receiver integration ✅ implemented 2026-09-25 (webhook sink; Telegram deferred)

The plan is agnostic about the transport; candidates, pre-compared:

| Option | Fit for this repo | Notes |
|---|---|---|
| **Telegram bot → the chat's own listener mechanism** | thematically perfect (this IS a chat app; a `ChatListener` + Telegram client already exist in `infrastructure/integrations/notifications/`) | zero new vendor; needs a chat id + bot token in `.env`; ADR-0006 Chunk 6 already built the client |
| Webhook → local sink endpoint on main-app | dev-only, good for tests | pairs with the `default-log` receiver |
| Email/Slack/PagerDuty | realistic on-call | new vendor dependency; use gravity-style service discovery when chosen |

**Recommendation:** Telegram for `team-warnings` + `oncall-critical` (severity → chat/dedup
differences), webhook sink for `default-log` during development. **Deferred decision — ask the
user before provisioning any external service.** Env vars to reserve either way:
`ALERTMANAGER_TELEGRAM_BOT_TOKEN`, `ALERTMANAGER_TELEGRAM_CHAT_ID`.

Note: Alertmanager has no built-in Telegram receiver — integration is via `webhook_configs`
pointing at a tiny relay (or an existing telegram-webhook bridge container). Budget a small
sidecar or reuse the app itself as the relay (it already owns a Telegram client).

---

## 5. Chunk 4 — Hygiene: runbook links and silences ✅ implemented 2026-09-25

1. **Complete the `runbook_url` annotations** ✅ (was: only `OutboxRelayCircuitOpen` had one).
   Same file, mechanical:

1. **Complete the `runbook_url` annotations** (only `OutboxRelayCircuitOpen` has one today). Same
   file, mechanical:
   - `OutboxRelayFailing`, `OutboxBacklogGrowing` → `docs/runbooks/kafka-outage.md` ✅
   - `KafkaConsumerDown`, `KafkaConsumerReconnecting` → new runbook `docs/runbooks/kafka-consumer.md`
     (consumer loop: heartbeat semantics, backoff, restart procedure) ✅
   - `WSBroadcastFailures` → new runbook `docs/runbooks/ws-fanout.md` (fan-out, dead-socket
     semantics) ✅
   - Re-run `promtool check config` and hot-reload Prometheus (`kill -HUP`) after each edit —
     both procedures already proven in this session.
2. **Silences are the maintenance path — document, don't script.** Planned drills/deploys:
   `amtool silences add` (or UI) with:
   - matchers `{alertname=~"OutboxRelayCircuitOpen|OutboxRelayFailing|OutboxBacklogGrowing"}`
     for Kafka drills; `--duration=45m` covers a full drill cycle;
   - author + comment are mandatory house style: `--author=<who> --comment="planned kafka drill"`;
   - post-drill: `amtool silences expire <id>`; `amtool silences query` is the pre-drill "am I
     silencing the right things" check.
   Add a "Silencing" section to the runbook rather than a separate doc.
3. **Alertmanager self-monitoring** ✅ (done early in Chunk 2 because it was free): a
   `scrape_configs` job `alertmanager:9093` so `up{job="alertmanager"}` exists; one more
   `alertmanager` metric
   (`alertmanager_alerts`) may join the Grafana overview later — do NOT add panels before the
   receiver decision lands.

**Acceptance:** every firing alert's UI page links a runbook that actually matches its failure
mode; a dry-run drill can be silenced with one command and leaves no config diffs.

---

## 6. Out of scope / future

- Alertmanager clustering (HA) — single-instance dev stack; add `--cluster.peer` only when a second
  instance exists.
- Alert rules for Alertmanager itself (e.g. `AlertmanagerDown`, `AlertmanagerFailedToSendAlerts`)
  — natural follow-up once self-monitoring lands.
- On-call schedules/escalation — requires the vendor decision from Chunk 3.
- Notification templates (custom Telegram formatting) — after the transport exists.

---

## 9. Implementation status (2026-09-25)

Chunks 1–3 landed the same day as the plan, using the **webhook-sink transport** (no external
credentials — the Telegram option stays available for later and only requires changing receiver
URLs):

- **Sink endpoint:** `POST /ops/alerts` on the app (`application/api/ops/handlers.py`) logs each
  alert into the structured JSON stream — firing maps severity→level (critical→CRITICAL,
  warning→WARNING, unknown→WARNING fallback), resolved logs INFO, runbook_url is appended.
  Deliberately unauthenticated (backend-network only, payload non-sensitive). 6 tests in
  `test/application/api/test_alert_sink.py`; suite 211 → 217.
- **Wiring:** `docker_compose/alertmanager.yaml` (with `alertmanager-data` volume) +
  `docker_compose/alertmanager/alertmanager.yml` exactly as designed in §3; `alerting:` section
  and an `alertmanager` self-monitoring scrape job added to `prometheus.yml`; `ALERTMANAGER_PORT=9093`
  in BOTH `.env` and `.env.example`; `prometheus`/`prometheus-down`/`all`/`all-down` make targets
  extended symmetrically.
- **Validation:** `amtool check-config` SUCCESS (route, 2 inhibit rules, 3 receivers);
  `amtool config routes test` → critical→`oncall-critical`, warning→`team-warnings`,
  unknown→`default-log`; promtool check config still SUCCESS.
- **Live E2E acceptance (§7):** a temporary `vector(1)` critical rule fired in Prometheus →
  Alertmanager `active/critical` → the sink logged `ALERT TestAlertWiring firing: …` at CRITICAL
  (visible in `docker logs main-app` / Loki). Temp rule removed; AM restart cleared the retained
  entry and confirmed the peer re-attaches. Prometheus `/api/v1/alertmanagers` shows the peer;
  `up{job="alertmanager"}` target is up.
- **Deviation from the ADR sketch:** all three receivers use the webhook sink (the sketch's
  `default-log` alternative became the whole transport for now); self-monitoring (planned as
  optional in Chunk 4) was pulled into Chunk 2 because it was free.
- **Chunk 4 (2026-09-25):** all 6 rules in `docker_compose/prometheus-alerts.yml` now carry
  `runbook_url` (the 5 missing ones added): `OutboxRelayFailing`/`OutboxBacklogGrowing` →
  `docs/runbooks/kafka-outage.md`; `KafkaConsumerDown`/`KafkaConsumerReconnecting` → new
  `docs/runbooks/kafka-consumer.md` (heartbeat semantics, backoff, restart procedure);
  `WSBroadcastFailures` → new `docs/runbooks/ws-fanout.md` (best-effort fan-out, dead-socket
  semantics). Validated with promtool (6 rules) and hot-reloaded via `kill -HUP` — every rule
  `health: ok` with its annotation via `/api/v1/rules`. The webhook sink appends `runbook_url`
  to the logged alert message, so the links surface in the JSON logs automatically. The silencing
  section of this chunk had already landed in `docs/runbooks/kafka-outage.md` with Chunks 1–3.

---

## 7. Test strategy

| Layer | Check | Tool |
|---|---|---|
| Config syntax | `amtool check-config` (v3: `--entrypoint=amtool`) | docker run, same mount pattern as promtool |
| Routing correctness | `amtool config routes test` for all 6 alerts × severities | amtool CLI |
| Wiring | Prometheus `/status/config` shows `alerting:`; `/api/v1/alertmanagers` shows the peer | curl |
| End-to-end | temporary always-firing rule → receiver gets it; silence it → notifications stop | live stack |
| Inhibition | fire CircuitOpen + Failing together → only CircuitOpen notifies | live stack / `amtool` |

## 8. Risks

- **Silent drop** on receiver misconfiguration — mitigated by the never-empty catch-all receiver
  and the end-to-end firing test in Chunk 1 acceptance.
- **Compose file-list drift** (up/down/all include different files) — the session's orphan-container
  warning shows this is real; keep the target definitions symmetric, one PR per chunk.
- **Notification storms** during a full outage (6 alerts × repeats) — mitigated by grouping,
  inhibition rules, and the severity-asymmetric repeat intervals.
