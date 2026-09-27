import orjson

from domain.events.messages import (
	ChatDeletedEvent,
	ListenerAddedEvent,
	NewChatCreatedEvent,
	NewMessageReceivedEvent,
)
from infrastructure.message_brokers.converters import convert_event_to_broker_message


def test_converter_roundtrip_new_chat_created():
	event = NewChatCreatedEvent(chat_oid='chat-1', chat_title='General')

	payload = convert_event_to_broker_message(event)

	assert isinstance(payload, bytes)
	assert payload != b'{}'
	decoded = orjson.loads(payload)
	assert decoded['chat_oid'] == 'chat-1'
	assert decoded['chat_title'] == 'General'
	assert decoded['event_id'] == str(event.event_id)


def test_converter_roundtrip_new_message_received():
	event = NewMessageReceivedEvent(
		message_text='hi',
		message_oid='msg-1',
		chat_oid='chat-1',
	)

	payload = convert_event_to_broker_message(event)

	assert isinstance(payload, bytes)
	assert payload != b'{}'
	decoded = orjson.loads(payload)
	assert decoded['message_text'] == 'hi'
	assert decoded['chat_oid'] == 'chat-1'


def test_converter_roundtrip_chat_deleted():
	event = ChatDeletedEvent(chat_oid='chat-1')

	payload = convert_event_to_broker_message(event)

	assert isinstance(payload, bytes)
	assert payload != b'{}'
	decoded = orjson.loads(payload)
	assert decoded['chat_oid'] == 'chat-1'


def test_converter_roundtrip_listener_added():
	event = ListenerAddedEvent(chat_oid='chat-1', listener_oid='listener-1')

	payload = convert_event_to_broker_message(event)

	assert isinstance(payload, bytes)
	assert payload != b'{}'
	decoded = orjson.loads(payload)
	assert decoded['chat_oid'] == 'chat-1'
	assert decoded['listener_oid'] == 'listener-1'
