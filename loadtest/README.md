# Locust load testing

A minimal load-test harness for the Kafka-chat API. It drives the Prometheus
metrics behind the `kafka-chat-overview` Grafana dashboard, so run it while the
observability stack is up to watch `http_requests_total` and the outbox counters
move.

## Install

```bash
pip install -r requirements.txt
```

## Run

The API is reachable at `http://localhost:8000` once `make all` (or
`make app`) is running.

Headless (good for a fixed test run):

```bash
locust -f locustfile.py --host http://localhost:8000 \
       --users 50 --spawn-rate 5 --run-time 5m --headless
```

With the web UI:

```bash
locust -f locustfile.py --host http://localhost:8000
# open http://localhost:8089
```

## What it does

`KafkaChatUser`:
- `on_start` — `POST /chat/` to create one chat per user (unique title per
  user; retries up to 3 times and logs a warning if creation fails).
- `post_message` (weight 3) — `POST /chat/{id}/messages` against that chat.
- `list_messages` (weight 1) — `GET /chat/{id}/messages/`.

Payloads match the API schemas exactly: `{'title': ...}` for chat creation,
`{'text': ...}` for messages (faker-generated). Endpoints are tagged with
explicit `name=` so Prometheus labels and the Locust stats stay clean
(`handler` label in the metrics matches `/chat/{id}/messages`), and paths match
the registered routes (including the trailing slash on `GET .../messages/`) so
no request is spent on a 307 redirect.

## Notes

- Requires a reachable Mongo + Kafka backend, just like normal app usage.
- Watch the dashboard's HTTP-rate/latency panels and `outbox_pending` to see the
  Transaction Outbox absorb the write load.
- If you see `4xx` errors for `POST /chat/` in the stats, chat creation is
  failing (e.g. duplicate title); affected users stay idle until the run ends.
