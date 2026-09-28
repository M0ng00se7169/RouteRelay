"""Minimal async circuit breaker for the Mongo persistence path.

Why: motor waits ``serverSelectionTimeoutMS`` (3s) before failing when Mongo is
down, so every request pays the timeout while the outage lasts. A circuit
breaker fails fast instead: after N consecutive failures the breaker opens and
subsequent calls are rejected immediately with ``CircuitOpenError`` (mapped to
HTTP 503 in application/api/main.py), letting Mongo recover without the request
path hammering it. State is exposed as Prometheus metrics (see metrics.py).

State machine: closed -> open (N consecutive failures) -> half-open (after
``recovery_time``) -> closed (first success) / open again (any failure).

One shared breaker instance guards the whole Mongo path — chats and messages
collections hit the same server, so per-collection breakers would just delay
tripping. The repository proxies below are explicit decorators rather than a
generic ``__getattr__`` proxy so every guarded call site stays greppable.
"""

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from time import monotonic
from typing import Any

from infrastructure.metrics import (
    circuit_breaker_rejected_total,
    circuit_breaker_state,
    safe_inc,
    safe_set,
)
from infrastructure.repositories.messages.base import (
    BaseChatsRepository,
    BaseMessagesRepository,
)


@dataclass(eq=False)
class CircuitOpenError(Exception):
    """Raised when a guarded call is rejected because the breaker is open."""

    name: str

    @property
    def message(self) -> str:
        return f'circuit breaker {self.name!r} is open, dependency unavailable'


class CircuitBreaker:
    """Async circuit breaker over ``call()``-style awaitables.

    Concurrency note: detection is based on *consecutive* failures, and
    half-open allows exactly one probe call — under concurrent calls the
    breaker may trip or allow one probe later than a strictly serialized
    execution would. Acceptable here: the breaker guards availability, not
    correctness, and the race window is one request wide.
    """

    def __init__(
        self,
        name: str,
        failure_threshold: int = 5,
        recovery_time: float = 30.0,
    ) -> None:
        self.name = name
        self.failure_threshold = failure_threshold
        self.recovery_time = recovery_time

        self._failures = 0
        self._opened_at: float | None = None
        self._probe_in_flight = False

    # --- state ------------------------------------------------------------

    @property
    def state(self) -> str:
        """'closed' | 'open' | 'half_open'."""
        if self._opened_at is None:
            return 'closed'
        if self._probe_in_flight or monotonic() - self._opened_at >= self.recovery_time:
            return 'half_open'
        return 'open'

    def _open(self) -> None:
        self._opened_at = monotonic()
        self._probe_in_flight = False
        self._failures = 0
        safe_set(circuit_breaker_state.labels(name=self.name), 1.0)

    def _reset(self) -> None:
        self._opened_at = None
        self._probe_in_flight = False
        self._failures = 0
        safe_set(circuit_breaker_state.labels(name=self.name), 0.0)

    # --- guarding ----------------------------------------------------------

    async def call(self, operation: Callable[[], Awaitable[Any]]) -> Any:
        """Run ``operation()`` under the breaker.

        Raises ``CircuitOpenError`` immediately (fail fast) when open; retries
        are left to the caller — the outbox relay naturally retries every
        poll interval, and API requests surface 503 to the client.
        """
        is_probe = False
        if self._opened_at is not None:
            if self._probe_in_flight:
                safe_inc(circuit_breaker_rejected_total, name=self.name)
                raise CircuitOpenError(name=self.name)
            if monotonic() - self._opened_at >= self.recovery_time:
                # Half-open: allow exactly one probe. Concurrent callers keep
                # failing fast while the probe is in flight.
                is_probe = True
                self._probe_in_flight = True
            else:
                safe_inc(circuit_breaker_rejected_total, name=self.name)
                raise CircuitOpenError(name=self.name)

        try:
            result = await operation()
        except CircuitOpenError:
            raise
        except Exception:
            if is_probe:
                # The probe failed -> the dependency is still down. Reopen
                # immediately with a fresh timer; going back through the
                # consecutive-failure count would leave the breaker stuck in
                # half-open (probe flag never cleared, threshold never met).
                self._open()
            else:
                self._failures += 1
                if self._failures >= self.failure_threshold:
                    self._open()
            raise
        else:
            # Any success (including the half-open probe) closes the breaker.
            self._reset()
            return result


@dataclass
class CircuitBreakerChatsRepository(BaseChatsRepository):
    """Chats repository proxy failing fast when the shared breaker is open."""

    inner: Any
    breaker: CircuitBreaker

    async def check_chat_exists_by_title(self, title: str) -> bool:
        return await self.breaker.call(
            lambda: self.inner.check_chat_exists_by_title(title=title),
        )

    async def get_chat_by_oid(self, oid: str) -> Any:
        return await self.breaker.call(
            lambda: self.inner.get_chat_by_oid(oid=oid),
        )

    async def add_chat(self, chat: Any, session=None) -> None:
        await self.breaker.call(
            lambda: self.inner.add_chat(chat, session=session),
        )

    async def get_all_chats(self, filters: Any) -> Any:
        return await self.breaker.call(
            lambda: self.inner.get_all_chats(filters=filters),
        )

    async def delete_chat_by_oid(self, chat_oid: str, session=None) -> None:
        await self.breaker.call(
            lambda: self.inner.delete_chat_by_oid(chat_oid, session=session),
        )

    async def add_telegram_listener(self, chat_oid: str, telegram_chat_id: str, session=None) -> None:
        await self.breaker.call(
            lambda: self.inner.add_telegram_listener(
                chat_oid=chat_oid,
                telegram_chat_id=telegram_chat_id,
                session=session,
            ),
        )

    async def get_all_chat_listeners(self, chat_oid: str) -> Any:
        return await self.breaker.call(
            lambda: self.inner.get_all_chat_listeners(chat_oid=chat_oid),
        )


@dataclass
class CircuitBreakerMessagesRepository(BaseMessagesRepository):
    """Messages repository proxy failing fast when the shared breaker is open."""

    inner: Any
    breaker: CircuitBreaker

    async def add_message(self, message: Any, session=None) -> None:
        await self.breaker.call(
            lambda: self.inner.add_message(message, session=session),
        )

    async def get_messages(self, chat_oid: str, filters: Any) -> Any:
        return await self.breaker.call(
            lambda: self.inner.get_messages(chat_oid=chat_oid, filters=filters),
        )
