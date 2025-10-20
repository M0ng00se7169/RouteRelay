# 07 — Makefile observability target

**What to build:** A new Makefile variable and `observability` / `observability-down` targets that
bring up and tear down the Loki + Promtail + Grafana services, folded into the existing `all` /
`all-down` targets so a full bring-up includes visualization and log aggregation. From the user's
perspective, `make all` now stands up the complete observability stack.

**Blocked by:** None — can start immediately.

**Status:** ready-for-agent

- [ ] A new compose-file variable references the observability compose file.
- [ ] An `observability` target brings up the new services on the `backend` network.
- [ ] `all` and `all-down` include the observability file.
- [ ] An `observability-down` target tears the new services down.
- [ ] `make all` brings up the full stack (storages + app + kafka + prometheus + observability) without error.
