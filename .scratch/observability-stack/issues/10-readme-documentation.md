# 10 — README documentation

**What to build:** The main README documents the new observability stack, the new local `.env`
variables, the Makefile targets, and a Locust load-testing section, so a future GitHub visitor can
stand up and exercise the system without reading code. From the user's perspective, the README is a
complete guide to the visualized, load-tested system.

**Blocked by:** 01, 02, 03, 04, 05, 06, 07, 08, 09 — all stack pieces must be shaped before docs are final.

**Status:** ready-for-agent

- [ ] The Observability subsection documents Grafana, Loki, and Promtail and what the dashboard shows.
- [ ] The Makefile Commands section documents the new targets and that `make all` includes observability.
- [ ] New local `.env` variables are documented (ports, Grafana admin creds).
- [ ] A "Load testing with Locust" subsection points at `loadtest/` and the run command.
- [ ] The host-dependent Promtail Docker-log bind-mount caveat is noted.
