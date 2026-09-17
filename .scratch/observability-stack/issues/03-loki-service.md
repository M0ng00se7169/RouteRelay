# 03 — Loki service + configuration

**What to build:** A Loki container that runs on the `backend` network with a minimal single-binary
config and a persistent chunk/rule store, ready to receive log pushes from Promtail. From the user's
perspective, Loki is up and accepting logs on its push endpoint as part of the stack.

**Blocked by:** None — can start immediately.

**Status:** ready-for-agent

- [ ] Loki service defined on the existing `backend` network.
- [ ] Loki exposed on a configurable port via an env var.
- [ ] A minimal Loki config file is provided (auth disabled, filesystem store).
- [ ] A persistent Loki data volume is mounted.
- [ ] Loki starts cleanly and accepts pushes on its ingest endpoint.
