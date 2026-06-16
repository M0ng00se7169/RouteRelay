# My Portfolio Project — Run & Demo Guide

**Simple Kafka Chat** — a multi-user chat backend built with FastAPI, Kafka, and MongoDB,
demonstrating **DDD, CQRS, and event-driven architecture** — plus a full observability
stack and a load-testing harness.

This guide covers everything: run it, explore it, test it, break it, and demo it.

> 💡 **For a visitor:** the quickest wow-effect is the *5-Minute Tour* in Part 1 —
> live dashboards filling with traffic in under 5 minutes of setup.

---

## Part 1 — Quick Start (5-Minute Tour)

### Step 0 — Prerequisites

- **Docker Desktop** — running (whale icon visible). [Get it here](https://www.docker.com/products/docker-desktop/)
- **Python 3.11+** — for running tests and the load generator
- **Make** — optional (Windows users: use the `docker compose` fallbacks below)
- **Git** — to clone the repo

### Step 1 — Clone & configure

```bash
git clone <your-repo-url> kafka-chat
cd kafka-chat
cp .env.example .env   # defaults are fine — no changes needed
```

### Step 2 — Start everything (one command)

```bash
make all
```

<details>
<summary>💻 No make? (Windows users — click here)</summary>

```bash
docker compose --env-file .env \
  -f docker_compose/storages.yaml \
  -f docker_compose/app.yaml \
  -f docker_compose/kafka.yaml \
  -f docker_compose/prometheus.yaml \
  -f docker_compose/observability.yaml \
  up --build -d
```
</details>

**First run takes a few minutes** (Docker pulls images). Then verify:

```bash
docker ps   # ~8 containers, all "Up"
```

### Step 3 — The 60-second health check

| URL | What you should see |
|-----|---------------------|
| http://localhost:8000/api/docs | Swagger UI with the chat API |
| http://localhost:3000 | Grafana login (`admin` / `admin`) |
| http://localhost:9090 | Prometheus UI |
| http://localhost:8090 | Kafka UI (topics & messages) |
| http://localhost:28081 | Mongo Express (raw database view) |

All five open = the whole system is alive. 🎉

### Step 4 — See the magic happen (2 minutes)

Create a chat and send a message:

```bash
# 1. Create a chat
curl -X POST http://localhost:8000/chat/ \
  -H "Content-Type: application/json" \
  -d '{"title": "My First Chat"}'

# Response: {"oid": "<CHAT_ID>", "title": "My First Chat"}
# ⬆ copy the oid value — you need it in the next commands (call it <CHAT_ID>)

# 2. Send a message into it
curl -X POST http://localhost:8000/chat/<CHAT_ID>/messages \
  -H "Content-Type: application/json" \
  -d '{"text": "Hello, world!"}'
```

Now open the dashboard:

1. Go to **http://localhost:3000** → login `admin`/`admin`
2. Left menu → **Dashboards** → click **kafka-chat-overview**
3. Within ~30 seconds you should see request-rate bars and the log panel scrolling.

### Step 5 — Watch the event flow

1. In the curl you just ran, the message was written to MongoDB **and** an outbox row
   (same DB transaction) — the **Transaction Outbox** pattern.
2. A background **relay** polls the outbox and publishes each event to Kafka.
3. A **consumer loop** picks it up and pushes it to every WebSocket client in that chat.
4. Every step of that path draws a line on the Grafana dashboard.

> 💡 To *see* Kafka receive it: Kafka UI → http://localhost:8090 → cluster `local` →
> **Topics** → `new-messages` → **Messages** tab. Your message JSON is there.

---

## Part 2 — Demo Script for Presentations

A tight 6-minute demo flow for a meeting or screen recording:

1. **(0:00) Show the architecture diagram** — `docs/architecture.md` (or the mermaid
   diagram in README). One sentence: *"HTTP handlers delegate to a mediator that routes
   commands, queries, and domain events; Kafka is fed via a Transaction Outbox so a
   Kafka outage delays but never loses events."*
2. **(1:00) `make all`** — narrate what comes up: MongoDB (replica set for transactions),
   Kafka + Zookeeper, the FastAPI app, Prometheus, Loki + Promtail, Grafana.
3. **(2:00) API docs at `/api/docs`** — show the endpoints. Point out this is generated
   from the code (FastAPI auto-docs).
4. **(2:30) Fire one message with curl** (commands from Part 1, Step 4).
5. **(3:00) Grafana dashboard** — point at the panels while explaining the flow:
   request rate → outbox pending (spike & drain) → Kafka sent → consumer → WebSocket
   fan-out.
6. **(4:30) Kafka UI** — show the raw message in the `new-messages` topic.
7. **(5:00) Load test** — run the 5-minute Locust run (Part 3) and let the dashboard
   fill live. End on: *"and it's all wired with DDD layers, CQRS, and a DI container."*

> 💡 **For a recorded demo:** run the Locust load test in the background *before* you
> start recording — the dashboards will already be alive when you open them.

---

## Part 3 — Load / Stress Test (the impressive part)

Full beginner walkthrough: **[docs/stress-test.md](stress-test.md)**. Here's the short version:

```bash
# One-time: install the load generator
pip install -r loadtest/requirements.txt

# Run 50 fake users for 5 minutes
locust -f loadtest/locustfile.py --host http://localhost:8000 \
       --users 50 --spawn-rate 5 --run-time 5m --headless
```

What to watch in Grafana while it runs:

| Panel | Expected behavior |
|-------|-------------------|
| HTTP request rate | Smooth hill: ramp up → plateau |
| HTTP latency p95 | Some rise, stays stable |
| **Outbox pending** | Spikes, then drains back toward 0 (outbox absorbing writes) |
| Kafka messages sent | Climbs steadily |
| Live logs panel | JSON log lines scrolling |

✅ Success looks like: **~0% request failures** in the Locust stats and an outbox that
drains back to ~0 after the run.

---

## Part 4 — Test Suite

The project has **34 test files / 171 tests** (unit + API, all in-memory — no Mongo/Kafka needed).

```bash
cd app
poetry install                    # once
poetry run pytest                 # expect: 171 passed
```

> 💡 The metric assertions use a baseline-delta pattern against a global registry —
> a deliberate house pattern, documented in ADR-0006, to keep tests from flaking.

---

## Part 5 — Explore the Architecture

| What | Where |
|------|-------|
| Architecture deep-dive | `docs/architecture.md` |
| CQRS contract (commands/queries/events) | `docs/cqrs-contract.md` |
| DI container reference | `docs/di-reference.md` |
| Metrics & observability ADR (fully implemented) | `docs/adr/0006-metrics-implementation-plan.md` |
| WebSocket↔Kafka relay ADR | `docs/adr/0005-websocket-kafka-relay-implementation-plan.md` |
| Known issues / gotchas | `docs/known-issues.md` |
| Stress-test walkthrough | `docs/stress-test.md` |

**Code map** (start reading here):

| Concept | File |
|---------|------|
| DI container — single source of truth | `app/logic/init.py` |
| Mediator (CQRS router) | `app/logic/mediator/base.py` |
| Rich domain entities + domain events | `app/domain/entities/messages.py` |
| Command/query/event handlers | `app/logic/commands/messages.py`, `app/logic/queries/messages.py`, `app/logic/events/messages.py` |
| Transaction Outbox relay | `app/infrastructure/outbox/relay.py` |
| WebSocket connection manager | `app/infrastructure/websockets/managers.py` |
| Kafka consumer loop (broker → WS) | `app/application/api/lifespan.py` |
| Metric registry (25 metrics) | `app/infrastructure/metrics.py` |

---

## Part 6 — Advanced Playgrounds

### 6.1 Watch Kafka end-to-end with the smoke-test scripts

Two scripts ship with the app for poking Kafka directly:

```bash
# Terminal 1 — start a consumer listening on 'test-topic'
docker exec -it main-app python kafka_test_consumer.py

# Terminal 2 — send a message
docker exec -it main-app python kafka_test_producer.py
```

Terminal 1 will print:

```
consumed:  test-topic 0 0 None b'Super-pooper message' 1726...
```

(These use the `test-topic`, separate from the app's event topics — good for proving
Kafka itself works, independent of the chat flow.)

### 6.2 WebSocket live updates (the real-time feature)

Connect a browser to a chat room and watch messages arrive **instantly**:

1. Create a chat and grab `<CHAT_ID>` (Part 1, Step 4).
2. Open the browser console (F12) on any page and run:

```js
const ws = new WebSocket(`ws://localhost:8000/chats/<CHAT_ID>/`);
ws.onmessage = e => console.log('GOT:', e.data);
// Should print: GOT: You are now connected!
```

3. In another terminal, post a message to the same chat:

```bash
curl -X POST http://localhost:8000/chat/<CHAT_ID>/messages \
  -H "Content-Type: application/json" \
  -d '{"text": "Hello WebSocket!"}'
```

4. Watch the browser console print the message **within ~1 second** — that's the full
   pipeline: **API → Mongo + outbox (one transaction) → relay → Kafka → consumer loop →
   WebSocket fan-out.**

### 6.3 Prometheus alerts (the safety net)

The project ships 4 alert rules (validated structure; Alertmanager wiring deferred):

```bash
# Check them in the Prometheus UI:
open http://localhost:9090/alerts
```

| Alert | Meaning |
|-------|---------|
| `OutboxBacklogGrowing` | Messages are being written faster than Kafka delivers |
| `OutboxRelayFailing` | The relay can't reach Kafka |
| `KafkaConsumerDown` | The broker→WebSocket consumer loop died |
| `WSBroadcastFailures` | WebSocket sends are failing |

To *see* one fire, stop Kafka while the app is running:

```bash
docker stop kafka    # watch Prometheus alerts page; then:
docker start kafka
```

> ⚠️ **Kafka experiments caution:** after stopping/starting Kafka, restart the app
> (`docker restart main-app`) so the consumer loop and relay reconnect cleanly.

### 6.4 Mongo Express — the raw data

http://localhost:28081 → login `admin`/`admin` → database `chat` → collections
`chat`, `messages`, and `outbox`.

You can literally watch the `outbox` collection drain as the relay delivers events:
rows appear when you post a message, then flip `sent: true` within ~1 second.

### 6.5 Manual testing via Swagger UI

http://localhost:8000/api/docs → "Try it out" on any endpoint. For message creation
the UI generates a proper JSON body automatically — no curl needed.

---

## Part 7 — Tear Down

```bash
make all-down
```

> 💡 Metrics, dashboards, and the database survive a down/up cycle (Docker volumes:
> `prometheus-data`, `loki-data`, `grafana-data`, `dbdata6`) — so your demo setup is
> still there next time. Wipe everything: `docker compose ... down --volumes`.

---

## Appendix A — All the URLs

| Service | URL | Credentials |
|---------|-----|-------------|
| API docs (Swagger) | http://localhost:8000/api/docs | — |
| Prometheus | http://localhost:9090 | — |
| App metrics endpoint | http://localhost:8000/metrics | — |
| Grafana | http://localhost:3000 | `admin` / `admin` |
| Kafka UI | http://localhost:8090 | — |
| Mongo Express | http://localhost:28081 | `admin` / `admin` |
| Locust web UI (when run without `--headless`) | http://localhost:8089 | — |
| Loki | http://localhost:3100 | — |

## Appendix B — All Makefile targets

| Target | Action |
|--------|--------|
| `make all` / `make all-down` | Full stack up/down (includes observability) |
| `make app` / `make app-down` | FastAPI app only |
| `make storages` | MongoDB replica set + Mongo Express |
| `make kafka` | Kafka + Zookeeper + Kafka UI |
| `make prometheus` | Prometheus + app + Kafka |
| `make observability` | Loki + Promtail + Grafana only |
| `make app-logs` | Follow app logs |
| `make app-shell` | Shell into the app container |
| `make prometheus-logs` / `make observability-logs` | Follow infra logs |

## Appendix C — Troubleshooting (quick)

| Problem | Fix |
|---------|-----|
| Port conflict on 8000/3000/9090/8090 | Change the port in `.env` and re-run `make all` |
| Grafana charts empty | Wait ~1 min; ensure you ran `make all` (not `make app`); generate traffic |
| `docker ps` shows a container restarting | `docker logs <container>` — Mongo/Kafka need ~30 s to be ready |
| Tests fail with connection errors | Tests are in-memory; run from `app/` with `poetry run pytest` |
| After Kafka stop/start, WS fan-out dead | `docker restart main-app` to reconnect loops |
| Chat creation returns 400 | Duplicate title — use a fresh title |
