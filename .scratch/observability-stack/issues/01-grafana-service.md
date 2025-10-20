# 01 — Grafana service + provisioning directory

**What to build:** A Grafana container that comes up on the existing `backend` network with a
persistent data volume and a mounted provisioning directory, ready to receive datasource and
dashboard provisioning files. From the user's perspective, `make` (or the new target) launches a
Grafana instance reachable on a configured port, even before any dashboards exist.

**Blocked by:** None — can start immediately.

**Status:** ready-for-agent

- [ ] Grafana service defined on the existing `backend` network alongside the other compose files.
- [ ] Grafana exposed on a configurable port via an env var.
- [ ] Grafana admin user/password sourced from env vars.
- [ ] A persistent Grafana data volume is mounted.
- [ ] A provisioning directory is mounted into the container so subsequent tickets can drop
      datasource/dashboard files.
- [ ] Service starts cleanly as part of the compose stack (no errors on bring-up).
