# Stress Testing Guide (No Experience Needed)

This guide walks you through running a **stress test** (also called a *load test*)
on the chat application — even if you have never done anything like this before.

No steps assume prior knowledge. Every command is copy-paste ready, and each one
tells you what result to expect.

> 💡 **Estimated time:** ~20 minutes (5–10 min if Docker is already set up).

---

## What is a stress test, and why do it?

A stress test simulates many fake users clicking around your app at the same time.
Instead of you manually sending one message, a program sends **hundreds** — to answer
questions like:

- Does the app stay fast when 50 people chat at once?
- Do messages still get delivered (via Kafka) when the app is busy?
- Does anything break or pile up (a growing backlog)?

You will launch **fake users** with a tool called **Locust**, then watch what happens
on **dashboards** in a tool called **Grafana**.

### The cast of characters

| Tool        | What it does | Where you'll see it |
|-------------|--------------|---------------------|
| Docker      | Runs the whole app on your machine as "containers" | Terminal |
| Locust      | Generates the fake traffic (the "stress") | Terminal + browser |
| Grafana     | Shows live charts of what's happening | Browser |
| Prometheus  | Collects the numbers Grafana draws | Runs in background |

You don't need to understand any of them deeply. Just follow the steps.

---

## Part 0 — One-time setup (5 minutes)

### What you need installed

1. **Docker Desktop** — [docker.com/products/docker-desktop](https://www.docker.com/products/docker-desktop/)
   - Install it, start it once, and make sure it's running (whale icon in your system tray).
2. **Python 3.11+** — [python.org/downloads](https://www.python.org/downloads/)
   - On Windows, check "Add Python to PATH" during install.
3. **Make** (optional but nice)
   - **Windows:** skip it — the guide gives plain `docker compose` alternatives.
   - **macOS:** comes with developer tools, or `brew install make`.
   - **Linux:** usually preinstalled.

### Check Docker works

Open a terminal and run:

```bash
docker --version
```

✅ **Expect:** something like `Docker version 27.x.x`.
❌ If you see "command not found", Docker isn't installed or not running — start Docker Desktop and try again.

### Get the project and configure it

```bash
# 1. Get the code (or clone your own fork)
git clone <this-repo-url>
cd <project-folder>

# 2. Create the settings file from the template
cp .env.example .env
```

The default settings in `.env` are fine — ports will be 8000 (app), 9090
(Prometheus), 3000 (Grafana). You don't need to change anything.

> 💡 **What is `.env`?** A small text file with settings (ports, passwords) that
> Docker reads. It's already filled with sensible defaults.

---

## Part 1 — Start the whole app (5 minutes)

### The one command

If you have **make**:

```bash
make all
```

If you **don't have make** (typical on Windows), run this instead:

```bash
docker compose --env-file .env \
  -f docker_compose/storages.yaml \
  -f docker_compose/app.yaml \
  -f docker_compose/kafka.yaml \
  -f docker_compose/prometheus.yaml \
  -f docker_compose/observability.yaml \
  up --build -d
```

> 💡 Both commands do exactly the same thing: start MongoDB, Kafka, the chat API,
> Prometheus, and the Grafana/Loki stack in the background.

**First run takes a few minutes** (Docker downloads images). Later runs are fast.

### Verify everything started

```bash
docker ps
```

✅ **Expect:** a list of ~8 containers with status `Up` — names like
`main-app`, `mongodb`, `kafka`, `prometheus`, `grafana`, `loki`, `promtail`.
❌ If a container says `Restarting` or `Exited`, wait 30 seconds and run `docker ps` again — some services wait for others.

### Open the app in your browser

| URL | What you should see |
|-----|---------------------|
| http://localhost:8000/api/docs | The API docs page (a list of endpoints) |
| http://localhost:3000 | Grafana login page |

If both open — 🎉 the app is alive.

**Log in to Grafana** (keep this tab open, you'll need it soon):
- Username: `admin`
- Password: `admin`

(These come from `.env` — `GRAFANA_ADMIN_USER` / `GRAFANA_ADMIN_PASSWORD`.)

---

## Part 2 — Install and run the stress test (5 minutes)

### Install Locust (one time)

In your terminal, from the project root:

```bash
pip install -r loadtest/requirements.txt
```

✅ **Expect:** a wall of "Successfully installed ..." lines ending with `locust` and `faker`.
❌ If `pip` is not found, try `python -m pip install -r loadtest/requirements.txt`.

> 💡 **What did you just install?**
> - **locust** — the load generator itself
> - **faker** — generates realistic fake text for chat messages

### Send the traffic

Copy-paste this (it runs for **5 minutes** — read the "what to expect" while it goes):

```bash
locust -f loadtest/locustfile.py --host http://localhost:8000 \
       --users 50 --spawn-rate 5 --run-time 5m --headless
```

> 💡 **What do these flags mean?**
> - `--users 50` — simulate 50 people using the chat
> - `--spawn-rate 5` — add 5 new users per second (so it ramps up smoothly, not all at once)
> - `--run-time 5m` — stop after 5 minutes
> - `--headless` — don't open the fancy web UI, just print stats in the terminal

### What you'll see

The terminal prints a live stats table that looks like this (numbers will differ):

```
Type     Name                                    # reqs   # fails   Avg   Min   Max
POST     /chat/                                      50      0%     45    30   120
POST     /chat/{id}/messages                       2100      0%     60    25   350
GET      /chat/{id}/messages/                       700      0%     40    20   180
```

✅ **Expect:**
- `# fails` stays at or near **0%** — that means the app handled the load.
- The `messages` rows have far more requests than `/chat/` (each user posts ~3 messages per read).

✅ **At the end** you'll also get a summary with percentiles (Aggregated row: `50%`, `95%`...).
The `95%` column is the most useful single number: it means "95% of requests were faster than this many milliseconds."

While it runs, move on to Part 3 to watch it live in Grafana. 👇

<details>
<summary>🔎 Optional: run with the interactive web UI instead</summary>

```bash
locust -f loadtest/locustfile.py --host http://localhost:8000
```

Then open http://localhost:8089, type "50" users, "5" spawn rate, and press Start.
Same result, prettier charts.
</details>

---

## Part 3 — Watch the dashboards in Grafana (while the test runs)

1. Go to **http://localhost:3000** (login: `admin` / `admin`).
2. In the left menu: **Dashboards** → click **kafka-chat-overview**.

You'll see the pre-built dashboard with ~20 panels. The ones to watch during the test:

| Panel | What it shows | What "healthy" looks like during the test |
|-------|---------------|-------------------------------------------|
| **HTTP request rate** | Requests per second climbing | Rises smoothly as users spawn in |
| **HTTP latency p95** | How slow the slowest 5% of requests are | Stays low (hundreds of ms) — some rise is normal |
| **Outbox pending** | Messages waiting to be delivered to Kafka | Spikes up, then drains back toward 0 |
| **Kafka messages sent** | Messages delivered to Kafka | Climbs steadily during the run |
| **Live logs** (bottom) | Real log lines from the app | JSON lines scrolling as requests arrive |

> 💡 **The outbox story in one sentence:** the app writes messages to a database
> queue ("outbox") first, then a background worker delivers them to Kafka — so a
> temporary spike in "Outbox pending" that *drains back down* is the system working
> as designed, not a problem.

### No data on the charts?

- Charts need ~1 minute to fill; Prometheus collects numbers every 15 seconds.
- Make sure the load test is actually running (its terminal should be printing stats).
- Make sure you started with `make all` (or the long `docker compose` command),
  not just `make app` — the dashboards need Prometheus running.

---

## Part 4 — Read your results (2 minutes)

After Locust finishes, it prints a final summary. Here's how to interpret it like a pro:

| Number | Meaning | Good sign |
|--------|---------|-----------|
| `# fails` % | Share of requests that errored | ~0% |
| Aggregated `95%` | 95% of all requests were faster than this | Stable, not exploding as users ramp |
| `Avg` | Average response time | Well under a second |

In Grafana, zoom the time range to the last 15 minutes and check:

- ✅ **Outbox pending** returned to ~0 after the test → nothing got stuck.
- ✅ **HTTP request rate** formed a smooth hill (ramp up → plateau → stop) → clean run.
- ⚠️ **Latency p95 climbing over time** without recovering → the app struggles at this load (that's a finding worth noting!).

**That's it — you ran a stress test.** 🎉

---

## Troubleshooting

| Problem | Likely cause | Fix |
|---------|--------------|-----|
| `docker: command not found` | Docker Desktop not running | Start Docker Desktop, wait for the whale icon, retry |
| Port already in use (`bind: address already in use`) | Something else uses port 8000/3000/9090 | Close that app, or change the port in `.env` and rerun |
| Grafana charts all empty | Prometheus not started, or test not running | Use `make all`; run the load test; wait ~1 min |
| Locust: `ConnectionRefusedError` | The app isn't up yet | Wait for `docker ps` to show `main-app ... Up`, retry |
| Every request fails 100% | App crashed or still starting | `docker logs main-app` and look at the last lines |
| `4xx` errors on `POST /chat/` in stats | Chat creation failed (e.g. duplicate title) | Restart the test — titles are random, retries are built in |
| Locust is slow to spawn users | Normal | `--spawn-rate 5` means full load after 10 seconds |

Useful log commands:

```bash
make app-logs             # follow the chat app's logs (Ctrl+C to stop)
# or without make:
docker logs main-app -f
```

---

## Cleaning up

Stop everything when you're done:

```bash
make all-down
# or without make:
docker compose --env-file .env \
  -f docker_compose/storages.yaml \
  -f docker_compose/app.yaml \
  -f docker_compose/kafka.yaml \
  -f docker_compose/prometheus.yaml \
  -f docker_compose/observability.yaml \
  down
```

> 💡 Your dashboards and metrics history are saved in Docker volumes — they'll
> still be there next time you run `make all`. To wipe *everything* including
> saved data, add `--volumes` to the `down` command.

---

## Want to go further? (optional experiments)

- **More users:** rerun with `--users 200 --spawn-rate 10` and watch latency.
- **Longer run:** `--run-time 15m` to see if problems appear over time (leaks, growing backlogs).
- **Watch alerts:** open http://localhost:9090 → menu **Alerts** — the project ships
  with 4 alert rules (outbox backlog, relay failures, consumer down, WS broadcast
  failures). During a heavy test you might see one turn from green to red — that's
  the alerting system doing its job.
- **Interactive mode:** run Locust without `--headless` and play with the sliders live.

---

## Quick reference (cheat sheet)

```bash
# Start everything
make all

# Check it's running
docker ps

# Install the load tool (once)
pip install -r loadtest/requirements.txt

# Run the stress test (5 minutes, 50 users)
locust -f loadtest/locustfile.py --host http://localhost:8000 \
       --users 50 --spawn-rate 5 --run-time 5m --headless

# Watch results
# → http://localhost:3000  (Grafana, admin/admin, dashboard "kafka-chat-overview")
# → http://localhost:9090  (Prometheus, Alerts tab)

# App logs
make app-logs

# Stop everything
make all-down
```
