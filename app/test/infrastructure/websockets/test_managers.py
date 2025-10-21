from unittest.mock import AsyncMock

import pytest
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


@pytest.fixture
def manager():
    return ConnectionManager()


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
