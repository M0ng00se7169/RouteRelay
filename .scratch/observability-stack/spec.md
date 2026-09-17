# Spec: Grafana + Loki Observability Stack & Locust Load Test

Status: ready-for-agent
Triage: ready-for-agent

## Problem Statement

The chat backend already emits Prometheus metrics on `main-app:8000/metrics` — HTTP request
counts/latency via `prometheus-fastapi-instrumentator`, plus four custom outbox/Kafka counters
(`outbox_published_total`, `outbox_pending`, `outbox_publish_errors_total`, `kafka_messages_sent_total`)
— and Prometheus is already scraping the app. But there is no way to *see* any of it: there is no
Grafana, no dashboards, no log aggregation. Application logs (uvicorn stdout/stderr) are unstructured
and never collected. A future visitor to the GitHub repo cannot stand up a visual observability story,
and there is no easy way to generate realistic load to validate the system end-to-end.

## Solution

Add an observability stack alongside the existing Docker Compose services:

- **Loki + Promtail** to collect and aggregate the app's text logs. Promtail tails the app container's
  JSON-formatted logs and ships them to Loki with a `container=main-app` label.
- **Grafana**, provisioned automatically with Prometheus and Loki datasources and a single
  "kafka-chat-overview" dashboard that visualizes HTTP throughput/latency, the outbox→Kafka relay
  metrics, and a live Loki logs panel.
- A small **JSON logging** change in the app so Loki receives structured, queryable log lines
  (`level`, `logger`, `message`).
- A **Locust** load test that generates fake chats/messages through the existing public REST API to
  populate the dashboards and exercise the full write path (Mongo + outbox + Kafka relay).

The whole stack is brought up with one Makefile target and documented in the README so a future
visitor can reproduce it.

## User Stories

1. As a developer, I want a Grafana instance running locally, so that I can visualize the Prometheus
   metrics the app already exposes.
2. As a developer, I want a pre-provisioned "kafka-chat-overview" dashboard, so that I don't have to
   build panels by hand the first time I open Grafana.
3. As a developer, I want the dashboard to show HTTP request rate and latency by handler/status, so
   that I can see API performance at a glance.
4. As a developer, I want the dashboard to show the outbox→Kafka relay health (`outbox_pending`,
   `outbox_published_total`, `outbox_publish_errors_total`, `kafka_messages_sent_total`), so that I
   can watch event delivery under load.
5. As a developer, I want the dashboards provisioned automatically (no manual datasource/dashboard
   setup), so that `make` brings up a working Grafana immediately.
6. As a developer, I want Prometheus and Loki both wired as Grafana datasources, so that I can use
   Explore for both metrics and logs from one place.
7. As a developer, I want app logs aggregated in Loki, so that I can search and correlate text logs
   with metric spikes.
8. As a developer, I want the app to emit JSON-structured logs, so that Loki gets clean `level` and
   `logger` labels for querying.
9. As a developer, I want Promtail to ship the `main-app` logs automatically, so that no code change
   is needed each time I restart the stack.
10. As a developer, I want a single Makefile target (e.g. `make observability`) that stands up Loki,
    Promtail, and Grafana on the existing `backend` network, so that it composes with `make all`.
11. As a developer, I want the observability services folded into `make all` / `make all-down`, so
    that a full bring-up includes visualization and log aggregation.
12. As a developer, I want Prometheus to also self-scrape, so that the dashboard can show whether
    Prometheus itself is up.
13. As a developer running load, I want a Locust script that creates chats and posts messages through
    the real API, so that the dashboards and Loki receive realistic data.
14. As a developer, I want the Locust script to reuse the existing request/response schema shapes, so
    that the fake data matches what the API actually accepts.
15. As a developer, I want the load test to drive the write hot path (chat + message + outbox + Kafka
    relay), so that `outbox_pending` and `kafka_messages_sent_total` move during the run.
16. As a developer, I want a short README for the load test explaining how to run it, so that I (or a
    future visitor) can reproduce the load.
17. As a future GitHub visitor, I want the main README to document the new observability stack, the
    new `.env` variables, and the Makefile targets, so that I can stand it up without reading code.
18. As a future GitHub visitor, I want a "Load testing with Locust" section in the README, so that I
    know how to generate the data that fills the dashboards.
19. As a developer, I want the new app-code change (JSON logging) to be additive and not affect
    existing tests, so that `pytest` stays green.
20. As a developer, I want the observability data to persist across container restarts (volumes for
    Prometheus TSDB, Loki chunks, Grafana state), so that dashboards and history survive a `down`/`up`
    cycle.

## Implementation Decisions

- **New compose file `docker_compose/observability.yaml`** adds three services — `loki`,
  `promtail`, `grafana` — all on the existing named `backend` network (compose merges by network
  name), mirroring the style of the existing `prometheus.yaml`. Each uses a named volume for
  persistence (`loki-data`, `grafana-data`).
- **Loki** runs `grafana/loki:latest` with a minimal single-binary config (`auth_enabled: false`,
  filesystem store). Exposed on a `LOKI_PORT` env var.
- **Promtail** runs `grafana/promtail:latest`, reads the app container's Docker JSON log stream
  (relies on the default `json-file` logging driver), and pushes to `loki:3100`. Pipeline stage sets
  `container=main-app` and parses the inner JSON log line so Loki sees `level`/`logger`/`message`.
- **Grafana** runs `grafana/grafana:latest`, exposed on `GRAFANA_PORT`, with admin user/password from
  env vars. It is provisioned via mounted files: a datasources provisioning file (Prometheus →
  `prometheus:9090`, Loki → `loki:3100`) and a dashboards provisioning file pointing at a baked
  dashboard JSON.
- **Dashboard** is a single hand-authored Grafana dashboard JSON (`kafka-chat-overview`) with panels
  for: HTTP request rate (`rate(http_requests_total[1m])` by handler/status), request latency
  (instrumentator histogram p95), in-progress requests, the four outbox/Kafka counters, and a Loki
  logs panel (`{container="main-app"}`). Targets must align with the actual metric names exposed by
  the app and the instrumentator defaults.
- **JSON logging** is an additive module imported and called once inside `create_app()` (before the
  Prometheus instrumentator). It configures the root logger with a stdlib `JSONFormatter` (no
  infrastructure imports). It is gated so it does not interfere with the test suite. This is the only
  application-code change.
- **Makefile** gains an `OBSERVABILITY_FILE` variable and an `observability` target; `all` and
  `all-down` are extended to include it. New convenience down/log targets may be added.
- **`prometheus.yml`** gains a Prometheus self-scrape job (`localhost:9090`) alongside the existing
  `kafka-chat-api` job. `docker_compose/prometheus.yaml` may add a TSDB data volume for persistence.
- **Locust** lives in a top-level `loadtest/` directory (outside `app/` to avoid polluting
  first-party imports and the test path). It contains a `locustfile.py` with `FastHttpUser` tasks for
  `POST /chat/`, `POST /chat/{oid}/messages`, and `GET /chat/{oid}/messages/`, pre-creating a chat in
  `on_start` and reusing its oid; a `requirements.txt` (`locust`, `faker`); and a short `README.md`.
  It drives the existing public API and reuses the documented request schema shapes
  (`{"title": ...}`, `{"text": ...}`).
- **README** extends the existing Observability subsection and Makefile Commands section to cover the
  new stack, new `.env` variables, new Makefile targets, and a Locust subsection.

## Testing Decisions

- **No unit tests for the observability stack itself** — it is infrastructure (compose/config), not
  app logic. Verification is integration: bring the stack up and confirm the services connect.
- **Smoke verification (manual, documented in README):** after `make all`, confirm Grafana datasources
  report "Connected", the `kafka-chat-overview` dashboard loads, and `{container="main-app"}` returns
  recent logs in Loki Explore. This proves the Promtail→Loki→Grafana path works.
- **Load verification:** run Locust headless against `http://localhost:${API_PORT}` and watch Grafana
  panels fill (HTTP rate/latency, `outbox_pending` spike then drain, `kafka_messages_sent_total`
  climb) and Loki stream logs. This is the primary success signal for the feature.
- **App-code regression guard:** the JSON logging change is additive and gated; `cd app && poetry run
  pytest` must remain green. Prior art for app tests is `app/test/` (in-memory container, no Mongo/
  Kafka). The logging change must not require those test doubles.
- **Good-test principle applied:** tests (where they exist) check external behavior (dashboards load,
  metrics move, logs appear, pytest green), not implementation details of the compose files.

## Out of Scope

- Adding MongoDB or Kafka exporters / broker consumer-lag metrics (Prometheus continues to scrape only
  the app).
- Alerting rules / Alertmanager.
- WebSocket broadcast load — the broker→WebSocket consumer loop is a known un-wired path in the app, so
  Locust targets REST + outbox/Kafka only (which is what the dashboards visualize).
- A formal benchmark report with published performance numbers — the user's primary goal is load to
  populate dashboards, not a quantitative SLA document (README benchmark numbers are informal/optional).
- Long-term metric retention tuning, SSO, or multi-tenant Grafana.

## Further Notes

- The app already exposes everything the dashboard needs; this spec adds *visualization and log
  aggregation*, not new metrics.
- The Promtail Docker-log bind-mount path (`/var/lib/docker/containers`) is host-OS-dependent and may
  need a tweak on non-Linux hosts — document this caveat in the README/loadtest README.
- New `.env` variables (`LOKI_PORT`, `GRAFANA_PORT`, `GF_SECURITY_ADMIN_USER`,
  `GF_SECURITY_ADMIN_PASSWORD`) are local-only (`.env` is gitignored); the README documents them.
- The single agreed seam is the **Docker/observability layer** (new compose + Grafana provisioning) plus
  the one additive JSON-logging touch in `create_app`. No new API endpoints, handlers, or DI wiring are
  introduced; Locust reuses the existing public REST API.
