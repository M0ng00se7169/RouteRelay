# syntax=docker/dockerfile:1

# uv 0.12.19 - the exact version the lockfile was validated against (matches
# CI's astral-sh/setup-uv pin). The builder runs `uv lock --check` so a stale
# uv.lock fails the build early instead of silently resolving fresh deps.
FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim AS builder

WORKDIR /app
COPY pyproject.toml uv.lock ./

RUN uv lock --check

# The venvs are built OUTSIDE /app on purpose: the dev compose file
# (docker_compose/app.yaml) bind-mounts ../app/ over /app, which would hide a
# .venv living inside the workdir. Same path in builder and runtime stages, so
# venv shebangs/symlinks stay valid after the COPY. UV_COMPILE_BYTECODE
# pre-warms __pycache__ for faster container start.
RUN --mount=type=cache,target=/root/.cache/uv \
    UV_COMPILE_BYTECODE=1 UV_PROJECT_ENVIRONMENT=/opt/venv \
    uv sync --frozen --no-dev --no-install-project

RUN --mount=type=cache,target=/root/.cache/uv \
    UV_PROJECT_ENVIRONMENT=/opt/venv-dev \
    uv sync --frozen --all-groups --no-install-project

# --- dev: all dependency groups + root user, for the compose dev workflow ---
# Selected via `target: dev` in docker_compose/app.yaml (compose builds the
# LAST stage by default, and prod must be last so GHCR gets the slim image).
FROM python:3.12-slim-bookworm AS dev

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

COPY --from=builder /opt/venv-dev /opt/venv-dev
ENV PATH="/opt/venv-dev/bin:$PATH"

WORKDIR /app
COPY ./app/ ./

EXPOSE 8000

# --- prod: runtime deps only, non-root, the image CD pushes to GHCR ---------
FROM python:3.12-slim-bookworm AS prod

RUN useradd --create-home appuser

COPY --from=builder --chown=appuser:appuser /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

WORKDIR /app
COPY --chown=appuser:appuser ./app/ ./

USER appuser
EXPOSE 8000

# Both compose files override `command:`; this CMD is for a bare `docker run`
# of the pulled image (no --reload - that is dev-only).
CMD ["uvicorn", "--factory", "application.api.main:create_app", "--timeout-graceful-shutdown", "2", "--host", "0.0.0.0", "--port", "8000", "--ws=websockets"]
