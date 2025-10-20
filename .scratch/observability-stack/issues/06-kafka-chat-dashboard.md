# 06 — kafka-chat-overview Grafana dashboard

**What to build:** A single provisioned Grafana dashboard ("kafka-chat-overview") that visualizes the
system: HTTP request rate and latency by handler/status, the outbox→Kafka relay counters
(`outbox_pending`, `outbox_published_total`, `outbox_publish_errors_total`, `kafka_messages_sent_total`),
and a live Loki logs panel for `container=main-app`. From the user's perspective, opening the
dashboard shows all panels populated once load runs.

**Blocked by:** 02 — Grafana Prometheus + Loki datasources (needs both datasources); 03 — Loki
service (Loki logs panel).

**Status:** ready-for-agent

- [ ] A dashboards provisioning file is added pointing at a baked dashboard JSON.
- [ ] Dashboard JSON includes HTTP rate/latency panels (instrumentator metrics) by handler/status.
- [ ] Dashboard JSON includes panels for the four outbox/Kafka counters.
- [ ] Dashboard JSON includes a Loki logs panel scoped to `container=main-app`.
- [ ] Panel targets match the actual metric names exposed by the app and instrumentator defaults.
- [ ] On Grafana start, the dashboard loads with all panels present (empty until load runs).
