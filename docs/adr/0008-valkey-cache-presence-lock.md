# ADR-0008: Valkey — Cache, Presence, and Outbox Leader Lock

**Status**: Accepted (implemented 2026-09-28 — see §9)
**Created**: 2026-09-28
**Scope**: `docker_compose/` (new valkey service), `app/` (new cache infrastructure + query/command
handler changes), `Makefile`, `.env*`, metrics/alerts; no Kafka or outbox semantics change
**Related**: ADR-0006 (metrics rules D1/D3/D5, `infrastructure/metrics.py`), ADR-0007 (compose +
alert conventions), `docs/di-reference.md`, `docs/architecture.md`, `docs/my_portfolio.md` (Part 8 —
what the project demonstrates)

---

## 0. Purpose and decision

**Decision: adopt Valkey as a self-hosted cache and ephemeral-state store, for three scoped
features — (1) cache-aside for hot reads, (2) chat presence with TTL, (3) a leader lock for the
outbox relay.**

The app itself does not *need* it today: it is stateless by design (outbox), runs one replica,
Mongo persists everything, and Kafka already carries inter-process events. The case for Valkey is
that the three features above are genuinely missing capabilities (each currently impossible or
done poorly), they showcase standard distributed-systems patterns on top of the existing DDD/CQRS
skeleton, and they close a credibility gap for a portfolio project whose story is "event-driven
backend done properly": every serious stack of this shape ships a cache layer.

**What Valkey is explicitly NOT for here** (see §1.3 non-goals): a Kafka replacement, a WS fan-out
transport, or any durable storage. MongoDB and Kafka keep every responsibility they have today.

Valkey over Redis: BSD-licensed Linux Foundation fork of Redis 7.2, RESP2/RESP3 wire-compatible,
backed by the major cloud vendors; the official `valkey-py` client is a drop-in fork of `redis-py`
with the same `asyncio` API. Redis 8's licensing history makes Valkey the safer OSS default for a
self-hosted demo stack, at zero functional cost for our use cases.

### 1.1 Alternatives considered

| Option | Verdict | Why |
|---|---|---|
| Do nothing | Rejected | Ops-simplest, but the three gaps stay closed; wrong call for a portfolio stack |
| Redis (server + redis-py) | Rejected | Functionally equivalent, but Valkey is the unambiguous OSS choice post-license changes |
| Memcached | Rejected | No pub/sub, no lock primitives, no rich types — presence and locks get awkward |
| MongoDB TTL indexes for presence | Rejected | Puts ephemeral, high-churn state on the persistent store — exactly what we keep OFF Mongo |
| Valkey pub/sub for WS fan-out | Deferred | Only matters with >1 replica; Kafka consumer with per-instance `group_id` is the simpler fix then — separate ADR if ever needed |

### 1.2 Non-goals

- No change to outbox semantics (at-least-once stays) or Kafka topics.
- No Valkey persistence (no RDB/AOF): everything in Valkey is reconstructable ephemera.
- No distributed session store, no rate limiting, no Celery-style job queue (future ADRs).
- No multi-replica fan-out rework (§1.1, last row).

### 1.3 Current state (verified 2026-09-28)

- All four query handlers (`logic/queries/messages.py`) hit Mongo on every request; the only
  mitigation is the `'mongo'` circuit breaker (503 fail-fast), not read latency reduction.
- `ConnectionManager` (`infrastructure/websockets/managers.py`) is a plain in-process
  `dict[str, list[WebSocket]]` — no notion of presence, nothing survives or shares across processes.
- `OutboxRelay.run()` polls the outbox collection in **every** process; with N replicas all N poll
  (harmless but wasteful duplicates; the at-least-once contract holds, so this is optimization).
- No valkey/redis anywhere in the repo (compose, deps, config, docs).
- Config knob conventions: `pydantic-settings` `Field(alias='UPPER_SNAKE')` in `settings/config.py`.

---

## 2. Architecture decisions

### 2.1 Client and dependency

- Python package: **`valkey`** (valkey-py, official client) — `valkey.asyncio.Valkey` for the async
  client. Pinned `>=6,<7` at implementation time (check the current stable on PyPI).
- **mypy strict caveat** (learned with aiokafka, PROJECT_MEMORY 2026-09-28): valkey-py's typing is
  incomplete; the `Valkey` client is wrapped once behind our own ABC and typed at that boundary —
  if stubs are missing, add an `ignore_missing_imports` override next to punq/aiokafka in
  `[tool.mypy]`, keep `Any` out of everything downstream of the wrapper.

### 2.2 In-app structure (mirrors the existing patterns exactly)

```
infrastructure/cache/base.py        BaseCacheClient ABC (get/set/delete/exists, TTL args)
infrastructure/cache/valkey.py      ValkeyCacheClient — the ONLY file importing valkey
infrastructure/cache/memory.py      MemoryCacheClient — dict with TTL, for tests + degradation
infrastructure/cache/cached.py      CachedChatsRepository / CachedMessagesRepository proxies
                                    (same decorator-repo pattern as CircuitBreaker*Repository)
infrastructure/presence/…           BasePresenceTracker + ValkeyPresenceTracker (hash + TTL)
infrastructure/locks/…              BaseDistributedLock + ValkeyLeaseLock (SET NX PX + renew)
```

- DI: registered in `logic/init.py` under the ABCs, singletons; overridden with
  `MemoryCacheClient` in `test/fixtures.py` (`init_dummy_container`) — same recipe as the repos.
- **Graceful degradation, D5-style**: every cache call is best-effort. Valkey down ⇒ warning log +
  `cache_errors_total` + pass-through to Mongo; reads and writes never fail because of the cache.
  A third `CircuitBreaker(name='valkey')` instance wraps the client (reuses
  `infrastructure/resilience.py` + the existing `circuit_breaker_state{name}` gauge untouched).
- **Feature flags default OFF in `Config()`**, compose `.env` ships them ON — a bare `Config()`
  (tests, old deploys, prod image without the valkey service) behaves exactly like today.

New knobs (`settings/config.py`, aliases in parens):

| Knob | Default | Meaning |
|---|---|---|
| `valkey_url` (`VALKEY_URL`) | `redis://valkey:6379/0` | valkey-py URL (the `redis://` scheme is protocol-correct) |
| `cache_enabled` (`CACHE_ENABLED`) | `False` | master switch for cache-aside proxies |
| `cache_ttl_seconds` (`CACHE_TTL_SECONDS`) | `60` | read-path TTL, ±10% jitter to de-align expiries |
| `presence_enabled` (`PRESENCE_ENABLED`) | `False` | WS heartbeat → presence tracker |
| `presence_ttl_seconds` (`PRESENCE_TTL_SECONDS`) | `30` | heartbeat refresh 3× per TTL |
| `relay_lock_enabled` (`RELAY_LOCK_ENABLED`) | `False` | outbox relay leader lock |
| `relay_lock_ttl_seconds` (`RELAY_LOCK_TTL_SECONDS`) | `10` | lease renewed every ttl/3 |

### 2.3 Key layout and policies

| Key pattern | Type | TTL | Written by |
|---|---|---|---|
| `chat:{oid}` | string (serialized chat) | cache TTL | cache-aside proxy |
| `chat:ver:{oid}` | counter | — (bumped) | command handlers (invalidation) |
| `messages:{oid}:v{ver}:{offset}:{limit}` | string | cache TTL | cache-aside proxy |
| `presence:{chat_oid}` | hash field-per-socket-id | refreshed per heartbeat | WS manager |
| `lock:outbox-relay` | string (holder id) | lock TTL | relay lease |

- **Versioned invalidation** for the messages list: a new message bumps `chat:ver:{oid}`; page keys
  embed the version, so stale pages die by TTL with zero scan/`KEYS` traffic (no `SCAN` in the hot
  path). `DeleteChatCommandHandler` deletes `chat:{oid}` + the version key outright.
- **Eviction: `noeviction`** with a small `maxmemory` (128 MiB in compose). Writes fail loudly on
  pressure and our D5 wrapper turns that into a log line + pass-through — cache pressure must never
  silently evict the relay lock (allkeys-lru could evict a lock and hand it to two relays; the
  at-least-once outbox makes that non-corrupting but ugly).
- Serialization: `orjson` (already a dep via the broker path — verify at implementation; if it is
  transitive-only, add it explicitly). Domain entities round-trip through their existing
  serialization helpers; a schema-version prefix (`v1:`) inside cached values lets formats evolve
  without stale-cache bugs across deploys.

### 2.4 Metrics (D1/D3/D5 — new section in `infrastructure/metrics.py`)

| Metric | Type | Labels | Emitter |
|---|---|---|---|
| `cache_operations_total` | Counter | `operation` (get/set/delete), `result` (hit/miss/ok/error) | cache proxies + client |
| `cache_errors_total` | Counter | `operation` | ValkeyCacheClient (pre-breaker) |
| `presence_heartbeat_failures_total` | Counter | — | WS manager |
| `outbox_relay_lock_acquired_total` | Counter | — | relay lease loop |
| `outbox_relay_lock_held` | Gauge | — | 0/1 per process — bounded, no instance label needed for a demo |

All bounded labels (D3): no chat oids, no holder ids. Ownership table at the bottom of
`metrics.py` gets the new rows. Alert (Chunk 6): `CacheErrorsHigh` (warning) on
`increase(cache_errors_total{name...})` — same lazy-series caveat as `OutboxRelayCircuitOpen`.

---

## 3. Chunk 1 — Valkey in the stack + cache infrastructure (no business behavior)

**Files (new):** `docker_compose/valkey.yaml`, `app/infrastructure/cache/{base,valkey,memory}.py`,
`app/test/infrastructure/cache/test_valkey_client.py`
**Files (edit):** `Makefile` (valkey/valkey-down targets, added to all/all-down), `.env.example`
(`VALKEY_PORT=6379` + §2.2 knobs), `app/settings/config.py`, `app/logic/init.py`,
`app/test/fixtures.py`, `pyproject.toml` (+ `valkey`)

```yaml
# docker_compose/valkey.yaml — one service per file, storages.yaml conventions
services:
  valkey:
    container_name: chat-valkey
    image: valkey/valkey:8-alpine
    command: ['valkey-server', '--maxmemory', '128mb', '--maxmemory-policy', 'noeviction']
    ports:
      - '${VALKEY_PORT}:6379'
    healthcheck:
      test: ['CMD', 'valkey-cli', 'ping']
      interval: 10s
      timeout: 5s
      retries: 5
    networks:
      - backend
```

Deliverables: `ValkeyCacheClient` (lazy connect, explicit `aclose()` on app shutdown via lifespan —
asyncio clients need explicit disconnect), `MemoryCacheClient` (monotonic-clock TTL expiry) as the
test double AND the fallback client when `CACHE_ENABLED=false` (so handlers never branch on
None), breaker `'valkey'` wired in `init.py`, DI registrations + fixtures overrides.

**Verify:** `docker compose -f docker_compose/valkey.yaml up` + `valkey-cli ping` → PONG; app boots
with flags off with zero behavior change; unit tests exercise both clients against the memory
double + a fakeredis-style in-process server or skipped live test (decide at implementation);
pytest green, ruff clean, `uv run mypy` clean from repo root.

## 4. Chunk 2 — Cache-aside read path

**Files (new):** `app/infrastructure/cache/cached.py`, `app/test/logic/test_cached_repositories.py`
**Files (edit):** `app/logic/init.py` (wrap repos inside the breaker proxies when
`cache_enabled`: order is `CircuitBreaker*(Mongo) → Cached*`), `app/infrastructure/metrics.py`

`CachedChatsRepository.get_chat_by_oid` → check `chat:{oid}`, on miss call inner + set with
jittered TTL. Same for `get_messages` pages. The proxies live **below** the breaker proxies' inner
slot (breaker still guards the Mongo call; a Valkey error is swallowed inside the cached proxy and
never counts as a Mongo failure — important so the `'mongo'` breaker semantics stay pure).

**Verify:** tests — miss→inner-call→set; hit→no inner call (spy double); valkey error →
pass-through + `cache_errors_total`; disabled flag → proxies not registered; `REGISTRY
.get_sample_value` house pattern for all four `cache_operations_total` label combos.

## 5. Chunk 3 — Write-path invalidation

**Files (edit):** `app/logic/commands/messages.py` (`CreateMessageCommandHandler` bumps the version
key; `DeleteChatCommandHandler` deletes detail + version keys), `app/logic/init.py` (handlers get
`BaseCacheClient`), tests in `app/test/logic/test_commands_queries.py` (or a new file)

Commands already own the outbox write; the invalidation is one more best-effort call in the same
handler — cache staleness bounded by TTL even if Valkey hiccups (degradation is correctness-safe:
stale read ≤ TTL, never a write loss).

**Verify:** read-after-write freshness tests: create message → next `GetMessagesQuery` misses the
old page key; delete chat → detail 404s from Mongo, cache empty. All 217 existing tests stay green
(memory double makes invalidation calls no-ops where flags are off).

## 6. Chunk 4 — Presence

**Files (new):** `app/infrastructure/presence/{base,valkey}.py`,
`app/test/infrastructure/presence/test_presence.py`
**Files (edit):** `app/infrastructure/websockets/managers.py` (heartbeat task per accepted socket,
cancel on remove — reuses the O-1 lesson: every background task needs a done-path),
`app/logic/queries/messages.py` (+ `GetChatPresenceQuery` → count of live sockets),
`app/application/api/messages/handlers.py` (+ endpoint), `logic/init.py`, fixtures

Semantics: `accept_connection` → register socket id in the `presence:{chat_oid}` hash and start a
heartbeat loop refreshing every `presence_ttl/3`; abrupt process death leaves a stale field that
the TTL garbage-collects — no cleanup job needed. Presence is API data (a count per chat), NOT a
Prometheus gauge (per-chat labels violate D3).

**Verify:** unit tests with memory tracker + fake clock (heartbeat refresh, TTL expiry, removal);
API test for the new endpoint (flag off → 404/empty, flag on → count); WS manager tests extended.

## 7. Chunk 5 — Outbox relay leader lock

**Files (new):** `app/infrastructure/locks/{base,valkey}.py`,
`app/test/infrastructure/locks/test_lock.py`
**Files (edit):** `app/infrastructure/outbox/relay.py` (optional `lease: BaseDistributedLock | None`
— acquire-or-skip at tick start, renew during long batches, release on cancellation), `logic/init.py`
(`create_outbox_relay` builds the lease when `relay_lock_enabled`), `metrics.py`

Contract: lock lost mid-batch is safe — the outbox is at-least-once, duplicates are already an
expected replay case (relay.py docstring). Flag off (default) ⇒ byte-for-byte today's behavior.

**Verify:** tests — second holder fails `acquire` while first alive (memory lock double), expired
lease acquires, renew extends, tick skipped + `outbox_relay_lock_held 0` when not holder; optional
compose check with `docker compose --scale app=2` documented in the deploy runbook appendix.

## 8. Chunk 6 — Observability + docs sync

**Files (edit):** `docker_compose/prometheus-alerts.yml` (+ `CacheErrorsHigh` warning, runbook_url →
new `docs/runbooks/valkey-outage.md`: triage = cache is a degradation, never an outage; app keeps
serving from Mongo), `docker_compose/grafana/provisioning/dashboards/…` (cache hit-rate row on the
existing `kafka-chat-overview`), `docs/architecture.md` (component + metric + alert tables),
`README.md` (stack + metric list), `docs/di-reference.md` (new registrations),
`docs/stress-test.md` (optional: hit-rate note), `PROJECT_MEMORY.md` §4–5, this ADR §9.

**Verify:** promtool check-config; Grafana panel query returns data through the datasource API;
alert fires during a deliberate `docker stop chat-valkey` drill and clears on start.

---

## 9. Implementation status

All six chunks landed on 2026-09-28. Test count 217 → 333; mypy FULL STRICT clean
(140 files); ruff clean; `uv lock --check` fresh.

| Chunk | Scope | Status | Notes / deviations from the plan |
|---|---|---|---|
| 1 | compose + client infra + DI | Done | `docker_compose/valkey.yaml`, `cache/{base,valkey,memory}.py`, `'valkey'` breaker, DI + fixtures |
| 2 | cache-aside read path | Done | `cache/cached.py`; wired `CircuitBreaker(Cached(Mongo))` exactly as planned |
| 3 | invalidation | Done | **Extra:** `AddTelegramListenerCommandHandler` invalidates too — see below |
| 4 | presence | Done | `presence/{base,valkey,memory}.py`, `GetChatPresenceQuery`, `GET /chat/{oid}/presence/` |
| 5 | relay leader lock | Done | `locks/{base,valkey,memory}.py`, optional `OutboxRelay.lease`, released on shutdown |
| 6 | alerts/dashboard/docs | Done | `CacheErrorsHigh` + `docs/runbooks/valkey-outage.md`; 5 new dashboard panels; README/architecture/di-reference/PROJECT_MEMORY synced |

### Deviations from the plan (and why)

1. **One Valkey import, not three.** The plan listed `infrastructure/presence/valkey.py` and
   `infrastructure/locks/valkey.py` alongside `cache/valkey.py`, which contradicts its own
   "the ONLY file importing `valkey`" rule. The stronger rule won: the three low-level primitives
   the presence tracker and the lease need (`hash_set`/`hash_count`/`hash_delete`, and
   `set_if_absent`/`compare_and_extend`/`compare_and_delete`) live on `BaseCacheClient`, and both
   collaborators are built on that ABC. Consequence: `ValkeyPresenceTracker` and
   `ValkeyLeaseLock` import no driver, so they are testable against `MemoryCacheClient` and the
   driver can be swapped in one file.
2. **`cache/keys.py` added** (not in the plan's file list): the key layout and the `v1:`-prefixed
   orjson codecs. The key layout is a contract between the read proxies and the command handlers,
   so it needed one home rather than being split across both.
3. **`AddTelegramListenerCommandHandler` invalidates the chat detail entry too.** The plan named
   only create-message and delete-chat. Registering a listener mutates the chat document, and the
   cached `Chat` carries its listener set — without this, `GET /chat/{oid}/` would keep serving a
   chat without the new listener for up to `CACHE_TTL_SECONDS`. Correctness beat the plan's
   two-line scope.
4. **`GET /chat/{oid}/presence/` returns `enabled: false, count: 0` when presence is off** rather
   than 404. A chat may legitimately have nobody online, so a bare `0` is ambiguous; the flag
   disambiguates without inventing an error path for a normal condition.
5. **Presence is a whole-key TTL, not per-field.** Hash fields have no TTL in the Valkey 8 baseline
   the compose uses (per-field expiry is a newer feature), so the key TTL is re-armed by every
   heartbeat and a crashed socket's field dies with the key. Documented limitation: if a chat has
   both live and dead sockets, the dead field lingers until the LAST heartbeat stops — an
   over-count, never an under-count, and a no-op for a chat whose sockets are all gone.
6. **The `set_if_absent` fallback is `False` (could not acquire), not `True`.** A cache that cannot
   be read must not be read as "the lock is free". Skipping a relay tick is at-least-once-safe;
   publishing unlocked is not.
7. **`docs/`:** the plan's `docs/stress-test.md` hit-rate note was skipped (nothing in the load
   profile is invalidated by it) and the deploy runbook gained no `--scale app=2` appendix, because
   the relay lock is verified by unit tests plus a live Valkey smoke check rather than a
   multi-replica compose drill.

### Verification performed

- `uv run pytest` — 333 passed (217 pre-existing, all still green).
- `uv run mypy` from the repo root — clean, 140 files, FULL STRICT. The ADR's predicted
  valkey-py friction materialised exactly as written: its commands are typed
  `Union[Awaitable[T], T]`, so the client is held as `Any` at that one boundary (the aiokafka
  pattern) and every method re-asserts its return type in a small closure.
- `uv run ruff check .` — clean.
- **Live Valkey** (`valkey/valkey:8-alpine`): `valkey-cli ping` → PONG, and
  `app/scripts/valkey_smoke.py` exercises the real client, the HSET+EXPIRE pipeline and both Lua
  scripts (15/15 checks). This is what the in-process test double cannot prove.
- **Live degradation drill**: with the container stopped, every operation returned its cold-cache
  value (`get`→`None`, `exists`→`False`, `increment`→`0`, `set_if_absent`→`False`), the `'valkey'`
  breaker opened, and nothing raised — including after the breaker was open.
- `promtool check rules docker_compose/prometheus-alerts.yml` — SUCCESS, 7 rules.
- `docker compose -f docker_compose/valkey.yaml config` — renders.

Known risks, restated: mypy-strict friction on valkey-py (§2.1, handled at the single boundary),
lock-eviction policy choice (`noeviction`, §2.3), presence heartbeat task hygiene (§6), and flag
discipline — every feature defaults off, so the 217-test baseline and single-replica deployments
are untouched until compose explicitly flips a flag.
