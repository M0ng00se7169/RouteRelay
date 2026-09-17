# 04 — Promtail service + log pipeline

**What to build:** A Promtail container that tails the `main-app` container's Docker JSON logs and
ships them to Loki with a `container=main-app` label. From the user's perspective, app logs start
appearing in Loki without any code change on restart.

**Blocked by:** None — can start immediately.

**Status:** ready-for-agent

- [ ] Promtail service defined on the existing `backend` network.
- [ ] Promtail config points its client at the Loki service's push endpoint.
- [ ] Promtail scrapes the `main-app` container log stream (Docker json-file driver).
- [ ] Pipeline stage tags logs with `container=main-app`.
- [ ] On bring-up, recent `main-app` logs become queryable in Loki Explore.
