from dataclasses import dataclass, field
from datetime import datetime

from domain.entities.base import BaseEntity
from domain.events.messages import NewMessageReceivedEvent, NewChatCreatedEvent
from domain.values.messages import Title, Text


@dataclass(eq=False)
class Message(BaseEntity):
    text: Text
    created_at: datetime = field(
        default_factory=datetime.now,
        kw_only=True,
    )


@dataclass(eq=False)
class Chat(BaseEntity):
    title: Title
    created_at: datetime = field(
        default_factory=datetime.now,
        kw_only=True,
    )
    messages: set[Message] = field(
        default_factory=set,
        kw_only=True
    )

    @classmethod
    def create_chat(cls, title: Title) -> 'Chat':
        new_chat = cls(title=title)
        new_chat.register_event(NewChatCreatedEvent(
            chat_oid=new_chat.oid,
            chat_title=new_chat.title.as_generic_type()
        ))

        return new_chat

    def add_message(self, message: Message):
        self.messages.add(message)
        self.register_event(NewMessageReceivedEvent(
            message_text=message.text.as_generic_type(),
            message_oid=message.oid,
            chat_oid=self.oid
        ))
