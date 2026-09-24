import asyncio
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

from fastapi import WebSocket

from infrastructure.metrics import (
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


@dataclass
class BaseConnectionManager(ABC):
    connections_map: dict[str, list[WebSocket]] = field(
        default_factory=lambda: defaultdict(list),
        kw_only=True,
    )

    @abstractmethod
    async def accept_connection(self, websocket: WebSocket, key: str):
        ...

    @abstractmethod
    async def remove_connection(self, websocket: WebSocket, key: str):
        ...

    @abstractmethod
    async def send_all(self, key: str, bytes_: bytes):
        ...

    @abstractmethod
    async def disconnect_all(self, key: str):
        ...


@dataclass
class ConnectionManager(BaseConnectionManager):
    lock_map: dict[str, asyncio.Lock] = field(default_factory=dict)

    def _recompute_active_gauge(self) -> None:
        # Recomputed from the manager's own bookkeeping (not incremented/decremented)
        # so the gauge can never drift from connections_map. No label: keys are chat
        # oids — unbounded, and per-key cardinality is forbidden (ADR-0006, D3).
        safe_set(ws_connections_active, sum(len(v) for v in self.connections_map.values()))

    async def accept_connection(self, websocket: WebSocket, key: str):
        await websocket.accept()

        if key not in self.lock_map:
            self.lock_map[key] = asyncio.Lock()

        async with self.lock_map[key]:
            self.connections_map[key].append(websocket)
            safe_inc(ws_connections_accepted_total)
            self._recompute_active_gauge()

    async def remove_connection(self, websocket: WebSocket, key: str):
        if key not in self.lock_map or key not in self.connections_map:
            return
        async with self.lock_map[key]:
            if websocket in self.connections_map[key]:
                self.connections_map[key].remove(websocket)
                # Only counted when a socket was actually registered — removing an
                # unknown socket is a no-op, not a disconnect event.
                safe_inc(ws_connections_removed_total)
                self._recompute_active_gauge()

    async def send_all(self, key: str, bytes_: bytes):
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
                except Exception:
                    failures += 1
                    safe_inc(ws_broadcast_failures_total)
            safe_inc(ws_messages_broadcast_total)
        finally:
            safe_observe(
                ws_broadcast_duration_seconds.observe,
                perf_counter() - started_at,
            )

    async def disconnect_all(self, key: str):
        if key not in self.lock_map or key not in self.connections_map:
            return
        async with self.lock_map[key]:
            for websocket in self.connections_map[key]:
                await websocket.send_json({
                    'message': 'Chat has been deleted',
                })
                await websocket.close()
