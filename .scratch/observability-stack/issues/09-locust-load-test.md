# 09 — Locust load test

**What to build:** A Locust harness outside the `app/` package that generates fake chats and messages
through the existing public REST API, driving the write hot path (Mongo + outbox + Kafka relay) so the
Grafana dashboards and Loki fill with realistic data. From the user's perspective, running one command
produces load that makes the observability stack meaningful.

**Blocked by:** 01 — Grafana service; 03 — Loki service; 04 — Promtail pipeline; 05 — JSON app
logging; 07 — Makefile observability target (needs the running full stack).

**Status:** ready-for-agent

- [ ] A `loadtest/` directory outside `app/` with a Locustfile using `FastHttpUser`.
- [ ] Tasks hit `POST /chat/`, `POST /chat/{oid}/messages`, and `GET /chat/{oid}/messages/`.
- [ ] A chat is pre-created in `on_start` and its oid reused across tasks.
- [ ] Fake payloads match the existing request schema shapes (`{"title": ...}`, `{"text": ...}`).
- [ ] A `requirements.txt` lists `locust` and `faker`.
- [ ] A short `loadtest/README.md` explains how to run it against the running stack.
- [ ] A headless run populates the dashboard panels (HTTP rate/latency, `outbox_pending` spike then
      drain, `kafka_messages_sent_total` climb) and streams logs to Loki.
