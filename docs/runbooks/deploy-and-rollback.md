# Runbook — Deploy from GHCR & roll back to a previous SHA tag

How to run the stack from the image CD publishes (`.github/workflows/cd.yml` →
`ghcr.io/m0ng00se7169/routerelay`) and how to walk a bad release back to any previous commit.
Written for the single-host compose deployment this repo ships with.

> **Not for local development** — that flow stays `make all` (build + repo bind-mount +
> `--reload`). This runbook is for running *pinned, built* images.

---

## Tag scheme (what CD pushes)

| Tag | Meaning |
|---|---|
| `latest` | newest successful build from `main` (moves on every CD run) |
| `<full-commit-sha>` | immutable — exactly the code at that commit; **always exists for every CD release** |

CD only pushes after the CI run on the same commit passes (lint, tests, promtool/amtool configs),
so any tag in the registry is known-good by the pipeline's definition.

**Find the SHA you want to roll back to:** the CD workflow run history
(Actions → CD → each run names its SHA), or `git log --oneline main` locally — every pushed
`main` commit has a matching image tag.

---

## First deploy (or re-deploy of current main)

1. **Authenticate once** (private package; skip if already flipped public or logged in):

   ```bash
   echo <a-PAT-with-read:packages> | docker login ghcr.io -u <username> --password-stdin
   ```

2. **Pin what you're deploying** in `.env` (explicit beats implicit — same lesson as
   `GRAFANA_PORT`):

   ```env
   APP_IMAGE=ghcr.io/m0ng00se7169/routerelay:<commit-sha>   # or :latest
   ```

3. **Bring the stack up from images.** Compose's `-f` override merge cannot *remove* keys, so
   `deploy/compose/docker-compose.deploy.yml` **replaces** `main-app` (no `build:`, no repo
   bind-mount, no `--reload`) rather than patching `app.yaml`:

   ```bash
   docker compose \
     -f deploy/compose/docker-compose.deploy.yml \
     -f docker_compose/storages.yaml \
     -f docker_compose/kafka.yaml \
     --env-file .env up -d
   ```

4. **Verify** (full drill-down below):

   ```bash
   docker inspect main-app --format '{{.Config.Image}}'   # must show the pinned tag
   curl -s http://localhost:8000/metrics | grep '^application_info'
   curl -s http://localhost:9090/-/healthy               # if the obs stack is up
   ```

---

## Rollback to a previous SHA tag

**Scenario:** `main` moved, CD pushed a new image, and the live deployment is misbehaving
(e.g. 5xx climbing, consumer loop dead, bad release). Roll back **without** reverting git.

1. **Stop and remove the current container** (keeps Mongo/Kafka volumes — data is untouched):

   ```bash
   docker rm -f main-app
   ```

2. **Pin the previous good SHA** in `.env`:

   ```env
   APP_IMAGE=ghcr.io/m0ng00se7169/routerelay:<previous-good-sha>
   ```

3. **Re-run the `up` command** from the first-deploy section. Compose creates `main-app` from
   the pinned tag; everything else reconciles in place.

4. **Verify the rollback landed** (all four should agree):

   ```bash
   docker inspect main-app --format '{{.Config.Image}}'   # shows the old SHA tag
   docker inspect main-app --format '{{.Image}}'          # digest differs from the bad one
   curl -s http://localhost:8000/metrics | grep '^application_info'   # app serving metrics
   curl -s http://localhost:9090/api/v1/query --data-urlencode 'query=up{job="kafka-chat-api"}'
   # expect: up == 1; then watch alerts clear
   ```

5. **Post-rollback hygiene:** confirm the alerts that fired clear (recency-style ones take
   ~15m — see `docs/runbooks/kafka-outage.md` for the timing behavior), and open the fix-forward
   PR; the broken commit still has its SHA tag if you need to inspect it later.

**Do NOT** `git revert` as the rollback mechanism — that changes the repo and triggers a fresh
CD build; pinning the old tag restores service in seconds without a build. Revert in git only
as the *fix-forward* follow-up.

---

## What state is where (blast radius)

| State | Lives in | Survives an app rollback? |
|---|---|---|
| Chats, messages, outbox rows | Mongo (`chat-mongodb`, `dbdata6` volume) | ✅ yes — data is app-version-agnostic |
| Topic offsets | Kafka (`kafka` container) | ✅ yes — consumer re-joins its group |
| Alert/silence state | Alertmanager (`alertmanager-data` volume) | ✅ yes |
| App logs | JSON stdout → Promtail → Loki | ✅ yes (centralized, not in the container) |
| In-memory app state | `main-app` container | ❌ no — by design; connections drop, clients reconnect |

The outbox pattern makes the app image stateless by design: swapping `main-app` mid-flight loses
nothing durable; unsent rows drain once the new container is up.

---

## Related

- `.github/workflows/cd.yml` — what pushes the tags (gated on CI passing)
- `deploy/compose/docker-compose.deploy.yml` — the image-based service definition (with the
  override-merge gotcha documented inline)
- `docs/runbooks/kafka-outage.md` — alert behavior during the rollback window
