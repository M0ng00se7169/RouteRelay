from dataclasses import dataclass
import logging

from domain.events.messages import (
    ChatDeletedEvent,
    ListenerAddedEvent,
    NewChatCreatedEvent,
    NewMessageReceivedEvent,
    NewMessageReceivedFromBrokerEvent,
)
from infrastructure.integrations.notifications.clients.base import BaseNotificationClient
from infrastructure.integrations.notifications.dtos import Notification
from infrastructure.metrics import safe_inc
from infrastructure.metrics import (
    telegram_notifications_failed_total,
    telegram_notifications_sent_total,
)
from logic.events.base import EventHandler


# NOTE: Kafka delivery is no longer performed here. The Transaction Outbox relay
# (infrastructure.outbox.relay.OutboxRelay) is the sole writer to Kafka, reading
# unsent rows written atomically with the business data. These handlers keep only
# in-process side effects (e.g. WebSocket disconnect on chat deletion). The
# inherited message_broker/connection_manager/broker_topic fields remain part of
# the EventHandler contract but are no longer used for sending.

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class NewChatCreatedEventHandler(EventHandler):
    async def handle(self, event: NewChatCreatedEvent) -> None:
        # Outbox relay delivers the event to Kafka; no in-process side effect here.
        ...


@dataclass(frozen=True)
class ListenerAddedEventHandler(EventHandler):
    notification_client: BaseNotificationClient | None = None

    async def handle(self, event: ListenerAddedEvent) -> None:
        # Outbox relay delivers the event to Kafka; no in-process side effect here.
        # If Telegram client is configured, send notification
        if self.notification_client:
            try:
                notification = Notification(
                    title="New Listener Added",
                    text=f"Chat: {event.chat_oid}\nListener ID: {event.listener_oid}"
                )
                await self.notification_client.send(notification)
                # Delivery attempt metrics (ADR-0006, Chunk 6.1): when the
                # client is None (unconfigured) nothing is counted.
                safe_inc(telegram_notifications_sent_total)
            except Exception as e:
                # Log failure without breaking the event pipeline
                safe_inc(telegram_notifications_failed_total)
                logger.warning("Failed to send Telegram notification: %s", e)

@dataclass(frozen=True)
class NewMessageReceivedEventHandler(EventHandler):
    async def handle(self, event: NewMessageReceivedEvent) -> None:
        # Outbox relay delivers the event to Kafka; no in-process side effect here.
        ...

@dataclass(frozen=True)
class ChatDeletedEventHandler(EventHandler):
    async def handle(self, event: ChatDeletedEvent) -> None:
        # Outbox relay delivers the event to Kafka; the disconnect is a WS concern only.
        await self.connection_manager.disconnect_all(key=event.chat_oid)

@dataclass(frozen=True)
class NewMessageReceivedFromBrokerEventHandler(EventHandler):
    async def handle(self, event: NewMessageReceivedFromBrokerEvent) -> None:
        await self.connection_manager.send_all(
            key=event.chat_oid,
            bytes_=event.message.encode(),
        )