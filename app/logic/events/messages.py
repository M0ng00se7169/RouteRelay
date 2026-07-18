from dataclasses import dataclass

from domain.events.messages import (
    ChatDeletedEvent,
    ListenerAddedEvent,
    NewChatCreatedEvent,
    NewMessageReceivedEvent,
    NewMessageReceivedFromBrokerEvent,
)
from logic.events.base import EventHandler


# NOTE: Kafka delivery is no longer performed here. The Transaction Outbox relay
# (infrastructure.outbox.relay.OutboxRelay) is the sole writer to Kafka, reading
# unsent rows written atomically with the business data. These handlers keep only
# in-process side effects (e.g. WebSocket disconnect on chat deletion). The
# inherited message_broker/connection_manager/broker_topic fields remain part of
# the EventHandler contract but are no longer used for sending.


@dataclass(frozen=True)
class NewChatCreatedEventHandler(EventHandler):
    async def handle(self, event: NewChatCreatedEvent) -> None:
        # Outbox relay delivers the event to Kafka; no in-process side effect here.
        ...


@dataclass(frozen=True)
class ListenerAddedEventHandler(EventHandler):
    async def handle(self, event: ListenerAddedEvent) -> None:
        # Outbox relay delivers the event to Kafka; no in-process side effect here.
        ...


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
