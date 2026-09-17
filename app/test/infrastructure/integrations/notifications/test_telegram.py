from unittest.mock import AsyncMock

import pytest
from httpx import (
    AsyncClient,
    Response,
)
from infrastructure.integrations.notifications.clients.base import BaseNotificationClient
from infrastructure.integrations.notifications.clients.telegram import TelegramNotificationClient
from infrastructure.integrations.notifications.dtos import Notification


@pytest.mark.asyncio
async def test_format_notification_includes_title_and_text():
    client = TelegramNotificationClient(
        bot_token='TOKEN',
        chat_id='123',
        http_client=AsyncClient(),
        send_url='https://api.telegram.org',
    )

    formatted = await client._format_notification(Notification(title='T', text='B'))

    assert formatted == 'T\nB\n'


@pytest.mark.asyncio
async def test_send_calls_telegram_api():
    get_mock = AsyncMock(return_value=Response(200))
    http_client = AsyncMock(spec=AsyncClient)
    http_client.get = get_mock

    client = TelegramNotificationClient(
        bot_token='TOKEN',
        chat_id='123',
        http_client=http_client,
        send_url='https://api.telegram.org',
    )

    await client.send(Notification(title='T', text='B'))

    get_mock.assert_awaited_once()
    called_url = get_mock.call_args.kwargs['url']
    called_params = get_mock.call_args.kwargs['params']
    assert called_url == 'https://api.telegram.org/botTOKEN/sendMessage'
    assert called_params['chat_id'] == '123'
    assert called_params['text'] == 'T\nB\n'


@pytest.mark.asyncio
async def test_base_client_is_abstract():
    with pytest.raises(TypeError):
        BaseNotificationClient()
