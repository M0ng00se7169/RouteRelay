from datetime import datetime

from domain.entities.messages import (
    Chat,
    Message,
)
from domain.values.messages import (
    Text,
    Title,
)
from infrastructure.repositories.messages.converters import (
    convert_chat_document_to_entity,
    convert_chat_entity_to_document,
    convert_chat_listener_document_to_entity,
    convert_message_document_to_entity,
    convert_message_entity_to_document,
)


def test_message_entity_to_document():
    message = Message(oid='m1', chat_oid='c1', text=Text('hi'), created_at='t')
    doc = convert_message_entity_to_document(message)

    assert doc == {'oid': 'm1', 'text': 'hi', 'created_at': 't', 'chat_oid': 'c1'}


def test_message_document_to_entity():
    entity = convert_message_document_to_entity({'oid': 'm1', 'text': 'hi', 'created_at': 't', 'chat_oid': 'c1'})

    assert entity.oid == 'm1'
    assert entity.text.as_generic_type() == 'hi'
    assert entity.chat_oid == 'c1'


def test_chat_entity_to_document():
    chat = Chat.create_chat(title=Title('room'))
    doc = convert_chat_entity_to_document(chat)

    assert doc['oid'] == chat.oid
    assert doc['title'] == 'room'
    assert 'created_at' in doc


def test_chat_document_to_entity_with_listeners():
    doc = {
        'oid': 'c1',
        'title': 'room',
        'created_at': datetime(2024, 1, 1),
        'listeners': ['tg-1', 'tg-2'],
    }
    chat = convert_chat_document_to_entity(doc)

    assert chat.oid == 'c1'
    assert chat.title.as_generic_type() == 'room'
    assert {listener.oid for listener in chat.listeners} == {'tg-1', 'tg-2'}


def test_chat_document_to_entity_no_listeners_key():
    doc = {'oid': 'c1', 'title': 'room', 'created_at': datetime(2024, 1, 1)}
    chat = convert_chat_document_to_entity(doc)

    assert chat.listeners == set()


def test_chat_listener_document_to_entity():
    listener = convert_chat_listener_document_to_entity('tg-7')

    assert listener.oid == 'tg-7'
