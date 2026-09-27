from unittest.mock import AsyncMock

import pytest
from prometheus_client import REGISTRY

from infrastructure.websockets.managers import (
    BaseConnectionManager,
    ConnectionManager,
)


class FakeWebSocket:
    def __init__(self):
        self.accept = AsyncMock()
        self.send_bytes = AsyncMock()
        self.send_json = AsyncMock()
        self.send_text = AsyncMock()
        self.close = AsyncMock()
        self.received = []


class DyingWebSocket(FakeWebSocket):
    def __init__(self):
        super().__init__()
        self.send_bytes = AsyncMock(side_effect=RuntimeError('socket gone'))


WS_METRIC_NAMES = (
    'ws_connections_active',
    'ws_connections_accepted_total',
    'ws_connections_removed_total',
    'ws_messages_broadcast_total',
    'ws_broadcast_failures_total',
)

WS_HISTOGRAM = 'ws_broadcast_duration_seconds'


def _value(name: str) -> float:
    value = REGISTRY.get_sample_value(name)
    return value if value is not None else 0.0


def _hist_count() -> float:
    value = REGISTRY.get_sample_value(WS_HISTOGRAM + '_count')
    return value if value is not None else 0.0


@pytest.fixture
def manager():
    return ConnectionManager()


@pytest.fixture
def metric_baselines():
    return {name: _value(name) for name in WS_METRIC_NAMES} | {'ws_broadcast_duration_seconds_count': _hist_count()}


@pytest.mark.asyncio
async def test_accept_connection_registers_socket(manager):
    ws = FakeWebSocket()
    await manager.accept_connection(ws, key='c1')

    assert ws in manager.connections_map['c1']
    ws.accept.assert_awaited_once()


@pytest.mark.asyncio
async def test_send_all_broadcasts_to_all_sockets(manager):
    ws1, ws2 = FakeWebSocket(), FakeWebSocket()
    await manager.accept_connection(ws1, key='c1')
    await manager.accept_connection(ws2, key='c1')

    await manager.send_all('c1', b'payload')

    ws1.send_bytes.assert_awaited_once_with(b'payload')
    ws2.send_bytes.assert_awaited_once_with(b'payload')


@pytest.mark.asyncio
async def test_remove_connection_deregisters_socket(manager):
    ws = FakeWebSocket()
    await manager.accept_connection(ws, key='c1')

    await manager.remove_connection(ws, key='c1')

    assert ws not in manager.connections_map['c1']


@pytest.mark.asyncio
async def test_disconnect_all_closes_and_notifies(manager):
    ws = FakeWebSocket()
    await manager.accept_connection(ws, key='c1')

    await manager.disconnect_all('c1')

    ws.send_json.assert_awaited_once_with({'message': 'Chat has been deleted'})
    ws.close.assert_awaited_once()


def test_base_manager_is_abstract():
    with pytest.raises(TypeError):
        BaseConnectionManager()


# --- WS metrics (ADR-0006, Chunk 4.1) ---------------------------------------
# The metric registry is global across tests, so assertions are made on deltas
# against baselines captured before each test acts (house pattern, test_relay.py).


@pytest.mark.asyncio
async def test_accept_and_remove_move_counters_and_active_gauge(manager, metric_baselines):
    ws1, ws2 = FakeWebSocket(), FakeWebSocket()

    await manager.accept_connection(ws1, key='c1')
    await manager.accept_connection(ws2, key='c1')

    assert _value('ws_connections_accepted_total') - metric_baselines['ws_connections_accepted_total'] == 2
    assert _value('ws_connections_active') == 2

    await manager.remove_connection(ws1, key='c1')
    await manager.remove_connection(ws2, key='c1')

    assert _value('ws_connections_removed_total') - metric_baselines['ws_connections_removed_total'] == 2
    assert _value('ws_connections_active') == 0


@pytest.mark.asyncio
async def test_active_gauge_is_global_across_keys(manager, metric_baselines):
    # The gauge is label-free (chat oids are unbounded — D3): it must sum every
    # key's connections, not just one chat's.
    ws1 = FakeWebSocket()
    ws2 = FakeWebSocket()

    await manager.accept_connection(ws1, key='c1')
    await manager.accept_connection(ws2, key='c2')

    assert _value('ws_connections_active') == 2


@pytest.mark.asyncio
async def test_remove_unknown_socket_counts_nothing(manager, metric_baselines):
    await manager.remove_connection(FakeWebSocket(), key='never-seen')

    assert _value('ws_connections_removed_total') - metric_baselines['ws_connections_removed_total'] == 0


@pytest.mark.asyncio
async def test_broadcast_success_counts_and_times_fan_out(manager, metric_baselines):
    await manager.accept_connection(FakeWebSocket(), key='c1')
    await manager.accept_connection(FakeWebSocket(), key='c1')

    await manager.send_all('c1', b'payload')

    assert _value('ws_messages_broadcast_total') - metric_baselines['ws_messages_broadcast_total'] == 1
    assert _value('ws_broadcast_failures_total') - metric_baselines['ws_broadcast_failures_total'] == 0
    assert (
        _hist_count() - metric_baselines['ws_broadcast_duration_seconds_count'] == 1
    )


@pytest.mark.asyncio
async def test_dead_socket_counts_failure_without_breaking_fan_out(manager, metric_baselines):
    # Chunk 4.1 behavior fix: one dead socket must not abort delivery to the
    # remaining sockets; the failure is counted, the broadcast still succeeds.
    healthy = FakeWebSocket()
    await manager.accept_connection(DyingWebSocket(), key='c1')
    await manager.accept_connection(healthy, key='c1')

    await manager.send_all('c1', b'payload')

    healthy.send_bytes.assert_awaited_once_with(b'payload')
    assert _value('ws_broadcast_failures_total') - metric_baselines['ws_broadcast_failures_total'] == 1
    assert _value('ws_messages_broadcast_total') - metric_baselines['ws_messages_broadcast_total'] == 1
    assert (
        _hist_count() - metric_baselines['ws_broadcast_duration_seconds_count'] == 1
    )


@pytest.mark.asyncio
async def test_broadcast_to_unknown_key_still_observes_duration(manager, metric_baselines):
    # No sockets on the key: the fan-out is trivially successful (zero sends).
    await manager.send_all('no-such-chat', b'payload')

    assert _value('ws_messages_broadcast_total') - metric_baselines['ws_messages_broadcast_total'] == 1
    assert (
        _hist_count() - metric_baselines['ws_broadcast_duration_seconds_count'] == 1
    )
