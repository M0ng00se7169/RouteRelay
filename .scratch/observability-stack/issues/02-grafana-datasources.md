# 02 — Grafana Prometheus + Loki datasources

**What to build:** Grafana auto-provisions two datasources — Prometheus (the existing
`prometheus:9090` service) and Loki (added by a later ticket) — so both appear as Connected in the
Grafana UI with no manual setup. From the user's perspective, opening Grafana's datasource list shows
both ready to query.

**Blocked by:** 01 — Grafana service + provisioning directory.

**Status:** ready-for-agent

- [ ] A datasources provisioning file is added under the mounted provisioning directory.
- [ ] Prometheus datasource points at the Prometheus service on the `backend` network.
- [ ] Loki datasource points at the Loki service (resolves once Loki exists).
- [ ] On Grafana start, both datasources report Connected in the UI.
- [ ] No manual datasource configuration is required after bring-up.
