# 08 — Prometheus self-scrape + TSDB persistence

**What to build:** Prometheus also scrapes itself, and its time-series database persists across
restarts via a volume, so the dashboard can show Prometheus health and history survives a
down/up cycle. From the user's perspective, the "up" panel reflects Prometheus, and metrics aren't
lost on restart.

**Blocked by:** None — can start immediately.

**Status:** ready-for-agent

- [ ] The Prometheus scrape config gains a self-scrape job (`localhost:9090`) alongside the existing app job.
- [ ] The Prometheus compose service mounts a persistent TSDB volume (optional but recommended).
- [ ] Prometheus starts and reports its own target as up.
- [ ] Metrics survive a `down`/`up` cycle.
