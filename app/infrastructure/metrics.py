"""Single registry of Prometheus metrics.

Import metrics from here; never define ad-hoc metrics in feature modules.

Each metric below is defined once, at module level (no DI — prometheus_client
registries are global and stateless enough that no lifecycle is needed). The
module is organized into sections matching instrumentation sites; the table at
the bottom maps every metric to its owner/emitter.

Design rules (ADR-0006, Section 2):
- D1: Module-level metrics only — no DI registration.
- D3: Labels are bounded, closed sets (topic names, message class names).
      Never label by chat_oid / user oid — unbounded labels blow up memory.
- D5: Metrics are non-fatal — a telemetry failure must never break the
      business flow: all metric updates in business code go through the
      ``safe_*`` helpers below.

See ``docs/adr/0006-metrics-implementation-plan.md`` (Section 3) for the full
inventory and rationale.
"""

import logging
from collections.abc import Callable
from typing import Any

from prometheus_client import (
    Counter,
    Gauge,
    Histogram,
)


logger = logging.getLogger(__name__)


# --- Safe-observe helpers (ADR-0006, Chunk 1.2) -----------------------------
# Metrics are non-fatal (D5): a telemetry failure must never break the business
# flow. All metric updates in business code go through one of these helpers,
# which swallow — and log at WARNING — any exception the update raises.


def _metric_name(metric: Any) -> str:
	"""Best-effort display name for log messages — never raises."""
	try:
		return str(metric._name)
	except Exception:
		return repr(metric)


def _safe_observe(description: str, update: Callable[[], Any]) -> None:
	"""Run one metric update, swallowing any exception it raises.

	Only an in-process metric bookkeeping bug (wrong label names, duplicate
	label sets, invalid amounts, ...) can get us here — the process is
	unaffected either way, so log at WARNING and move on.
	"""
	try:
		update()
	except Exception:
		logger.warning('Metric update failed for %s', description, exc_info=True)


def safe_observe(fn: Callable[..., Any], *args: Any, **kwargs: Any) -> None:
	"""Call ``fn(*args, **kwargs)`` — any metric mutator — without letting an
	exception propagate to the caller. Use this for mutators without a
	dedicated wrapper, e.g.::

	    safe_observe(outbox_publish_duration_seconds.observe, elapsed)
	"""
	_safe_observe(repr(fn), lambda: fn(*args, **kwargs))


def safe_inc(metric: Counter, /, amount: float = 1.0, **labels: str) -> None:
	"""Safely increment a counter, e.g.::

	    safe_inc(outbox_published_total)              # +1
	    safe_inc(kafka_consumer_errors_total, topic='chat-events')
	"""
	name = _metric_name(metric)
	if labels:
		_safe_observe(
			f'{name} {labels}',
			lambda: metric.labels(**labels).inc(amount),
		)
	else:
		_safe_observe(name, lambda: metric.inc(amount))


def safe_set(metric: Gauge, value: float) -> None:
	"""Safely set a gauge, e.g. ``safe_set(outbox_pending, 42)``."""
	_safe_observe(_metric_name(metric), lambda: metric.set(value))


# --- Outbox relay -----------------------------------------------------------
# Emitted by app/infrastructure/outbox/relay.py. The outbox_* metrics stay
# label-free: per-row error attribution is not actionable (ADR-0006, Chunk 2.2
# note). kafka_messages_sent_total is topic-labelled (G9) — the label set is
# the closed list of configured topic names (D3).

outbox_published_total = Counter(
	'outbox_published_total',
	'Number of outbox rows successfully published to Kafka by the relay.',
)

outbox_publish_errors_total = Counter(
	'outbox_publish_errors_total',
	'Number of outbox rows that failed to publish to Kafka (will be retried).',
)

outbox_pending = Gauge(
	'outbox_pending',
	'Number of unsent outbox rows currently awaiting delivery.',
)

kafka_messages_sent_total = Counter(
	'kafka_messages_sent_total',
	'Number of messages handed to the Kafka producer by the relay, per topic.',
	['topic'],
)

outbox_publish_duration_seconds = Histogram(
	'outbox_publish_duration_seconds',
	'Time the relay spends sending one outbox row to Kafka, per topic. '
	'Includes failed attempts (e.g. timeout latency).',
	['topic'],
)


# --- Kafka consumer (inbound) -----------------------------------------------
# Emitted by app/application/api/lifespan.py (_kafka_consumer_loop). Topic
# labels use the configured topic names (D3); malformed payloads have no
# reliable identity, so their counter stays label-free.

kafka_messages_consumed_total = Counter(
	'kafka_messages_consumed_total',
	'Number of messages received from Kafka by the consumer loop, per topic '
	'(counted before parsing).',
	['topic'],
)

kafka_consumer_events_published_total = Counter(
	'kafka_consumer_events_published_total',
	'Number of events successfully published to the mediator by the consumer '
	'loop, per topic.',
	['topic'],
)

kafka_consumer_errors_total = Counter(
	'kafka_consumer_errors_total',
	'Number of unexpected exceptions raised while processing consumed '
	'messages, per topic.',
	['topic'],
)

kafka_consumer_malformed_total = Counter(
	'kafka_consumer_malformed_total',
	'Number of consumed messages that failed validation (missing '
	'chat_oid/message) — counted instead of published.',
)

kafka_consumer_up = Gauge(
	'kafka_consumer_up',
	'1 while the Kafka consumer loop task is running, 0 after graceful stop '
	'or unexpected task death.',
)


# --- WebSocket manager -------------------------------------------------------
# Emitted by app/infrastructure/websockets/managers.py (ConnectionManager). The
# active-connections gauge is recomputed from connections_map on every accept/
# remove so it can never drift from the manager's own bookkeeping. Labels stay
# out entirely: keys are chat oids — unbounded (D3).

ws_connections_active = Gauge(
	'ws_connections_active',
	'Current number of accepted WebSocket connections across all chats, as '
	'tracked by the connection manager.',
)

ws_connections_accepted_total = Counter(
	'ws_connections_accepted_total',
	'Number of WebSocket connections successfully accepted by the manager.',
)

ws_connections_removed_total = Counter(
	'ws_connections_removed_total',
	'Number of WebSocket connections removed from the manager (only counted '
	'when a socket was actually registered).',
)

ws_messages_broadcast_total = Counter(
	'ws_messages_broadcast_total',
	'Number of successful fan-out broadcast calls (one increment per '
	'send_all call that completed without a fatal error), per broadcast key.',
)

ws_broadcast_failures_total = Counter(
	'ws_broadcast_failures_total',
	'Number of per-socket send failures during fan-out — one dead socket no '
	'longer aborts delivery to the remaining sockets.',
)

ws_broadcast_duration_seconds = Histogram(
	'ws_broadcast_duration_seconds',
	'Time a send_all fan-out takes, including failed per-socket attempts.',
)


# --- Mediator (CQRS flow volume) ---------------------------------------------
# Emitted by app/logic/mediator/base.py. Labels use the mediator message class
# name — the set is closed (the events/commands/queries defined in logic/) and
# small, so cardinality is bounded (D3). Counted after handler resolution, so
# unregistered messages raise instead of being counted as handled.

mediator_events_published_total = Counter(
	'mediator_events_published_total',
	'Number of events handed to their handlers by Mediator.publish, per '
	'event class.',
	['event'],
)

mediator_commands_handled_total = Counter(
	'mediator_commands_handled_total',
	'Number of commands dispatched to registered handlers by '
	'Mediator.handle_command, per command class (unregistered commands are '
	'not counted).',
	['command'],
)

mediator_queries_handled_total = Counter(
	'mediator_queries_handled_total',
	'Number of queries dispatched to their registered handler by '
	'Mediator.handle_query, per query class.',
	['query'],
)


# --- DB operations (persistence failures) ------------------------------------
# Emitted by the command/query handlers (app/logic/commands|queries/messages.py)
# around their repository calls. Counts exceptions raised by persistence calls
# only; domain errors (ChatNotFoundException, duplicate title, ...) are not DB
# failures and stay uncounted. Labels are a closed vocabulary of operation and
# collection names (D3); the exception class name is folded into `operation`
# (e.g. ``insert.timeout``) instead of adding a fourth unbounded label.

db_operation_errors_total = Counter(
	'db_operation_errors_total',
	'Number of exceptions raised by repository (persistence) calls in the '
	'command/query handlers, per operation, collection and exception class.',
	['operation', 'collection', 'exception'],
)


# --- Telegram notifications (G3) ---------------------------------------------
# Emitted by ListenerAddedEventHandler (app/logic/events/messages.py). Both
# counters are label-free and measure delivery attempts only: when the client
# is None (Telegram unconfigured) nothing is counted — silence must be
# distinguishable from failure.

telegram_notifications_sent_total = Counter(
	'telegram_notifications_sent_total',
	'Number of Telegram notifications successfully handed to the '
	'notification client.',
)

telegram_notifications_failed_total = Counter(
	'telegram_notifications_failed_total',
	'Number of Telegram notification attempts that raised — logged and '
	'swallowed so the event pipeline keeps running.',
)


# --- Application info (G5) ----------------------------------------------------
# Emitted by create_app() (app/application/api/main.py). A constant 1 with the
# version as a label — standard Prometheus build-info pattern for deployment
# tracking in dashboards (`application_info{version="..."} 1`).

application_info = Gauge(
	'application_info',
	'Build info: always 1, the deployed version is the ``version`` label.',
	['version'],
)


# --- Metric ownership table (ADR-0006, Section 3) ---------------------------
#
# | Metric                          | Type    | Labels | Emitted by            |
# |---------------------------------|---------|--------|-----------------------|
# | http_requests_total             | Counter | handler, method, status | instrumentator (default) |
# | http_request_duration_seconds   | Histogram | handler, method, status | instrumentator (default) |
# | outbox_published_total          | Counter | —      | relay                 |
# | outbox_publish_errors_total     | Counter | —      | relay                 |
# | outbox_pending                  | Gauge   | —      | relay                 |
# | kafka_messages_sent_total       | Counter | topic  | relay                 |
# | outbox_publish_duration_seconds | Histogram | topic  | relay                 |
# | kafka_messages_consumed_total   | Counter | topic  | consumer loop         |
# | kafka_consumer_events_published_total | Counter | topic | consumer loop  |
# | kafka_consumer_errors_total     | Counter | topic  | consumer loop         |
# | kafka_consumer_malformed_total  | Counter | —      | consumer loop         |
# | kafka_consumer_up               | Gauge   | —      | consumer lifecycle    |
# | ws_connections_active           | Gauge   | —      | WS manager            |
# | ws_connections_accepted_total   | Counter | —      | WS manager            |
# | ws_connections_removed_total    | Counter | —      | WS manager            |
# | ws_messages_broadcast_total     | Counter | —      | WS manager            |
# | ws_broadcast_failures_total     | Counter | —      | WS manager            |
# | ws_broadcast_duration_seconds   | Histogram | —    | WS manager            |
# | mediator_events_published_total | Counter | event  | mediator              |
# | mediator_commands_handled_total | Counter | command | mediator             |
# | mediator_queries_handled_total  | Counter | query  | mediator              |
# | db_operation_errors_total       | Counter | operation, collection, exception | command/query handlers |
# | telegram_notifications_sent_total   | Counter | — | Telegram handler (Chunk 6.1) |
# | telegram_notifications_failed_total | Counter | — | Telegram handler (Chunk 6.1) |
# | application_info                | Gauge   | version | create_app() (Chunk 6.2) |
#
# Every metric above is now owned in the table; the historical per-chunk
# trailer was removed once all planned metrics landed in it.
