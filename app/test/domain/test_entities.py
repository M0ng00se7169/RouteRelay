import pytest

from domain.entities.messages import (
    Chat,
    ChatListener,
    Message,
)
from domain.events.messages import (
    ChatDeletedEvent,
    ListenerAddedEvent,
    NewChatCreatedEvent,
    NewMessageReceivedEvent,
)
from domain.exceptions.base import ApplicationException
from domain.exceptions.chats import ListenerAlreadyExistsException
from domain.exceptions.messages import (
    EmptyTextException,
    TitleTooLongException,
)
from domain.values.messages import (
    Text,
    Title,
)


def test_title_valid():
    assert Title('room').as_generic_type() == 'room'


def test_title_too_long_raises():
    with pytest.raises(TitleTooLongException):
        Title('x' * 300)


def test_text_valid():
    assert Text('hi').as_generic_type() == 'hi'


def test_text_empty_raises():
    with pytest.raises(EmptyTextException):
        Text('')


def test_chat_create_registers_event():
    chat = Chat.create_chat(title=Title('room'))

    events = chat.pull_events()
    assert any(isinstance(e, NewChatCreatedEvent) for e in events)


def test_chat_delete_registers_event():
    chat = Chat.create_chat(title=Title('room'))

    chat.delete()
    events = chat.pull_events()

    assert any(isinstance(e, ChatDeletedEvent) for e in events)


def test_chat_add_message_registers_event():
    chat = Chat.create_chat(title=Title('room'))
    chat.pull_events()

    chat.add_message(Message(chat_oid=chat.oid, text=Text('hello')))
    events = chat.pull_events()

    assert any(isinstance(e, NewMessageReceivedEvent) for e in events)


def test_chat_add_listener_registers_event():
    chat = Chat.create_chat(title=Title('room'))
    chat.pull_events()

    chat.add_listener(ChatListener(oid='tg-1'))
    events = chat.pull_events()

    listener_event = next(
        e for e in events if isinstance(e, ListenerAddedEvent)
    )
    assert listener_event.listener_oid == 'tg-1'


def test_chat_add_duplicate_listener_raises():
    chat = Chat.create_chat(title=Title('room'))
    chat.add_listener(ChatListener(oid='tg-1'))

    with pytest.raises(ListenerAlreadyExistsException):
        chat.add_listener(ChatListener(oid='tg-1'))


def test_application_exception_has_message():
    exc = EmptyTextException()
    assert isinstance(exc, ApplicationException)
    assert exc.message
