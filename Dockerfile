FROM python:3.12-slim-bookworm AS builder

COPY poetry.lock pyproject.toml ./

RUN python -m pip install poetry==2.3.0 poetry-plugin-export && \
    poetry export -o requirements.prod.txt --without-hashes && \
    poetry export --with=dev -o requirements.dev.txt --without-hashes

FROM python:3.12-slim-bookworm AS dev

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

COPY --from=builder requirements.dev.txt /app

RUN apt-get update && \
    apt-get install -y --no-install-recommends build-essential && \
    rm -rf /var/lib/apt/lists/* && \
    pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.dev.txt

COPY /app/ /app/**

EXPOSE 8000