import asyncio
import logging
from abc import (
    ABC,
    abstractmethod,
)
from collections import defaultdict
from dataclasses import (
    dataclass,
    field,
)
from time import perf_counter
from uuid import uuid4

from fastapi import WebSocket

from infrastructure.metrics import (
    presence_heartbeat_failures_total,
    safe_inc,
    safe_observe,
    safe_set,
    ws_broadcast_duration_seconds,
    ws_broadcast_failures_total,
    ws_connections_accepted_total,
    ws_connections_active,
    ws_connections_removed_total,
    ws_messages_broadcast_total,
)
from infrastructure.presence.base import BasePresenceTracker

logger = logging.getLogger(__name__)


@dataclass
class BaseConnectionManager(ABC):
    connections_map: dict[str, list[WebSocket]] = field(
        default_factory=lambda: defaultdict(list),
        kw_only=True,
    )

    @abstractmethod
    async def accept_connection(self, websocket: WebSocket, key: str) -> None:
        ...

    @abstractmethod
    async def remove_connection(self, websocket: WebSocket, key: str) -> None:
        ...

    @abstractmethod
    async def send_all(self, key: str, bytes_: bytes) -> None:
        ...

    @abstractmethod
    async def disconnect_all(self, key: str) -> None:
        ...


@dataclass
class ConnectionManager(BaseConnectionManager):
    lock_map: dict[str, asyncio.Lock] = field(default_factory=dict)
    # ADR-0008 Chunk 4: presence tracking. None (the default, and every test
    # that does not ask for it) disables presence entirely — no heartbeat task
    # is started, so the pre-ADR behaviour is unchanged.
    presence_tracker: BasePresenceTracker | None = None
    # Per-socket heartbeat tasks, keyed by the socket itself, so remove_connection
    # can cancel the one belonging to the socket that actually disconnected.
    # Leaked tasks would keep beating for a socket nobody can reach.
    _heartbeat_tasks: dict[WebSocket, asyncio.Task[None]] = field(
        default_factory=dict, repr=False,
    )
    # Presence id per socket. A WebSocket has no stable id, and the presence
    # hash needs one; a uuid keeps the mapping explicit and removable.
    _presence_ids: dict[WebSocket, str] = field(default_factory=dict, repr=False)

    def _recompute_active_gauge(self) -> None:
        # Recomputed from the manager's own bookkeeping (not incremented/decremented)
        # so the gauge can never drift from connections_map. No label: keys are chat
        # oids — unbounded, and per-key cardinality is forbidden (ADR-0006, D3).
        safe_set(ws_connections_active, sum(len(v) for v in self.connections_map.values()))

    async def accept_connection(self, websocket: WebSocket, key: str) -> None:
        await websocket.accept()

        if key not in self.lock_map:
            self.lock_map[key] = asyncio.Lock()

        async with self.lock_map[key]:
            self.connections_map[key].append(websocket)
            safe_inc(ws_connections_accepted_total)
            self._recompute_active_gauge()

        self._start_presence(websocket, key)

    async def remove_connection(self, websocket: WebSocket, key: str) -> None:
        # Presence is torn down BEFORE the early return below: a socket that
        # never made it into connections_map must still not leave a heartbeat
        # task running, and its presence entry must be released.
        await self._stop_presence(websocket, key)

        if key not in self.lock_map or key not in self.connections_map:
            return
        async with self.lock_map[key]:
            if websocket in self.connections_map[key]:
                self.connections_map[key].remove(websocket)
                # Only counted when a socket was actually registered — removing an
                # unknown socket is a no-op, not a disconnect event.
                safe_inc(ws_connections_removed_total)
                self._recompute_active_gauge()

    # --- presence (ADR-0008) ------------------------------------------------

    def _start_presence(self, websocket: WebSocket, key: str) -> None:
        if self.presence_tracker is None:
            return
        socket_id = str(uuid4())
        self._presence_ids[websocket] = socket_id
        task = asyncio.create_task(
            self._presence_heartbeat(websocket, key, socket_id),
            name=f'presence-heartbeat-{socket_id[:8]}',
        )
        # O-1 lesson: a background task that dies silently is worse than one that
        # never started. If the heartbeat dies, the entry stops being refreshed
        # and the chat silently reports fewer users — count it loudly.
        task.add_done_callback(self._log_heartbeat_exit)
        self._heartbeat_tasks[websocket] = task

    async def _presence_heartbeat(self, websocket: WebSocket, key: str, socket_id: str) -> None:
        tracker = self.presence_tracker
        if tracker is None:
            return
        interval = getattr(tracker, 'heartbeat_interval', 10.0)
        try:
            await tracker.register(key, socket_id)
            while True:
                await asyncio.sleep(interval)
                await tracker.refresh(key, socket_id)
        except asyncio.CancelledError:
            # Normal path: remove_connection (or app shutdown) cancels us.
            raise
        except Exception:
            # Presence is best-effort, but its failure must be visible: a dead
            # heartbeat means the chat under-reports its users.
            safe_inc(presence_heartbeat_failures_total)
            logger.exception('Presence heartbeat failed for chat %s', key)

    def _log_heartbeat_exit(self, task: asyncio.Task[None]) -> None:
        # Log only, never count: the heartbeat counts its OWN failure before it
        # returns, and counting here too would double-report every incident.
        # This callback exists purely so a death nobody handled (a clean return,
        # an error raised outside the heartbeat's try) is still visible instead
        # of leaving the chat quietly under-reporting its users.
        if task.cancelled():
            return
        logger.error('Presence heartbeat task ended unexpectedly: %s', task.exception())

    async def _stop_presence(self, websocket: WebSocket, key: str) -> None:
        task = self._heartbeat_tasks.pop(websocket, None)
        if task is not None:
            task.cancel()
        socket_id = self._presence_ids.pop(websocket, None)
        tracker = self.presence_tracker
        if socket_id is None or tracker is None:
            return
        try:
            await tracker.remove(key, socket_id)
        except Exception:
            # Best-effort: the entry also expires on its own TTL, so a failure
            # here only delays the "gone" signal by one TTL.
            safe_inc(presence_heartbeat_failures_total)
            logger.warning('Failed to remove presence entry for %s', socket_id, exc_info=True)

    async def send_all(self, key: str, bytes_: bytes) -> None:
        # Fan-out is best-effort: one dead socket must not abort delivery to the
        # remaining sockets (behavior change vs. the previous unguarded loop —
        # flagged as a bug fix in ADR-0006, Chunk 4.1). Failures are counted per
        # socket; the broadcast itself still counts as attempted.
        failures = 0
        started_at = perf_counter()
        try:
            for websocket in self.connections_map.get(key, []):
                try:
                    await websocket.send_bytes(bytes_)
                except Exception:  # noqa: BLE001 — best-effort fan-out, one dead socket must not abort
                    failures += 1
                    safe_inc(ws_broadcast_failures_total)
            safe_inc(ws_messages_broadcast_total)
        finally:
            safe_observe(
                ws_broadcast_duration_seconds.observe,
                perf_counter() - started_at,
            )

    async def disconnect_all(self, key: str) -> None:
        if key not in self.lock_map or key not in self.connections_map:
            return
        async with self.lock_map[key]:
            for websocket in self.connections_map[key]:
                # Chat is gone — release presence and stop the heartbeat before
                # closing, so the chat does not keep reporting live sockets.
                await self._stop_presence(websocket, key)
                await websocket.send_json({
                    'message': 'Chat has been deleted',
                })
                await websocket.close()
