from domain.events.base import BaseEvent
from domain.events.messages import (
    ChatDeletedEvent,
    ListenerAddedEvent,
    NewChatCreatedEvent,
    NewMessageReceivedEvent,
)
from settings.config import Config


def resolve_topic(event: BaseEvent) -> str:
	# Normalize the event_title vs title inconsistency across domain events by dispatching on type.
	match event:
		case NewChatCreatedEvent():
			return Config().new_chats_event_topic
		case NewMessageReceivedEvent():
			return Config().new_message_received_topic
		case ChatDeletedEvent():
			return Config().chat_deleted_topic
		case ListenerAddedEvent():
			return Config().new_listener_added_topic
		case _:
			raise ValueError(f'No topic configured for event type {type(event).__name__}')
