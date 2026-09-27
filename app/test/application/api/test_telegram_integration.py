import unittest.mock

import pytest
from prometheus_client import REGISTRY
from punq import Scope

from domain.events.messages import ListenerAddedEvent
from infrastructure.integrations.notifications.clients.base import BaseNotificationClient
from infrastructure.integrations.notifications.dtos import Notification
from infrastructure.message_brokers.base import BaseMessageBroker
from infrastructure.websockets.managers import BaseConnectionManager
from logic.commands.messages import (
    AddTelegramListenerCommand,
    CreateChatCommand,
)
from logic.events.messages import ListenerAddedEventHandler
from logic.exceptions.messages import ChatNotFoundException
from logic.mediator.base import Mediator
from test.fixtures import init_dummy_container


def _counter(name: str) -> float:
    value = REGISTRY.get_sample_value(name)
    return value if value is not None else 0.0


class _SpyNotificationClient(BaseNotificationClient):
    """Notification client double that records calls instead of hitting Telegram."""

    def __init__(self) -> None:
        self.sent: list[Notification] = []
        self.fail = False

    async def _format_notification(self, notification: Notification) -> str:
        return f'{notification.title}\n{notification.text}\n'

    async def send(self, notification: Notification) -> None:
        if self.fail:
            raise RuntimeError('API Error')
        self.sent.append(notification)


@pytest.fixture
def telegram_container():
    # Dummy container: in-memory repos/broker so no Mongo/Kafka is touched.
    container = init_dummy_container()
    spy = _SpyNotificationClient()
    container.register(BaseNotificationClient, instance=spy, scope=Scope.singleton)

    # init_container() built the mediator before the spy was registered (and
    # Telegram is unconfigured in tests, so the handler got client=None).
    # Rewire just the ListenerAddedEvent handler to the spy so the full
    # command -> event -> notification flow can be observed.
    mediator: Mediator = container.resolve(Mediator)
    mediator.events_map[ListenerAddedEvent] = [
        ListenerAddedEventHandler(
            message_broker=container.resolve(BaseMessageBroker),
            connection_manager=container.resolve(BaseConnectionManager),
            notification_client=spy,
        ),
    ]

    return container, spy


@pytest.mark.asyncio
async def test_listener_added_event_sends_telegram_notification(telegram_container):
    _, spy = telegram_container
    handler = ListenerAddedEventHandler(
        message_broker=unittest.mock.MagicMock(),
        connection_manager=unittest.mock.MagicMock(),
        notification_client=spy,
    )

    sent_before = _counter('telegram_notifications_sent_total')
    await handler.handle(ListenerAddedEvent(chat_oid='chat_123', listener_oid='listener_456'))

    assert len(spy.sent) == 1
    assert spy.sent[0].title == 'New Listener Added'
    assert 'listener_456' in spy.sent[0].text
    assert _counter('telegram_notifications_sent_total') - sent_before == 1


@pytest.mark.asyncio
async def test_telegram_notification_failure_does_not_break_pipeline(telegram_container):
    _, spy = telegram_container
    spy.fail = True
    handler = ListenerAddedEventHandler(
        message_broker=unittest.mock.MagicMock(),
        connection_manager=unittest.mock.MagicMock(),
        notification_client=spy,
    )

    failed_before = _counter('telegram_notifications_failed_total')
    sent_before = _counter('telegram_notifications_sent_total')

    # Must not raise despite the client failing.
    await handler.handle(ListenerAddedEvent(chat_oid='chat_123', listener_oid='listener_456'))

    assert spy.sent == []
    assert _counter('telegram_notifications_failed_total') - failed_before == 1
    assert _counter('telegram_notifications_sent_total') - sent_before == 0


@pytest.mark.asyncio
async def test_listener_added_event_without_client_is_noop():
    handler = ListenerAddedEventHandler(
        message_broker=unittest.mock.MagicMock(),
        connection_manager=unittest.mock.MagicMock(),
        notification_client=None,
    )

    # Must not raise when Telegram is not configured.
    sent_before = _counter('telegram_notifications_sent_total')
    failed_before = _counter('telegram_notifications_failed_total')
    await handler.handle(ListenerAddedEvent(chat_oid='chat_123', listener_oid='listener_456'))

    # Unconfigured Telegram is not a delivery attempt: nothing is counted.
    assert _counter('telegram_notifications_sent_total') - sent_before == 0
    assert _counter('telegram_notifications_failed_total') - failed_before == 0


@pytest.mark.asyncio
async def test_add_telegram_listener_command_publishes_event_and_notifies(telegram_container):
    container, spy = telegram_container
    mediator: Mediator = container.resolve(Mediator)

    chat, *_ = await mediator.handle_command(CreateChatCommand(title='telegram-test-chat'))
    spy.sent.clear()  # discard any notifications from chat creation

    sent_before = _counter('telegram_notifications_sent_total')
    listener, *_ = await mediator.handle_command(
        AddTelegramListenerCommand(chat_oid=chat.oid, telegram_chat_id='12345'),
    )

    assert listener.oid == '12345'
    assert len(spy.sent) == 1
    assert spy.sent[0].title == 'New Listener Added'
    assert _counter('telegram_notifications_sent_total') - sent_before == 1


@pytest.mark.asyncio
async def test_add_telegram_listener_command_unknown_chat_raises(telegram_container):
    container, _ = telegram_container
    mediator: Mediator = container.resolve(Mediator)

    with pytest.raises(ChatNotFoundException):
        await mediator.handle_command(
            AddTelegramListenerCommand(chat_oid='missing-chat', telegram_chat_id='12345'),
        )
