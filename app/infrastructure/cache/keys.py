"""Cache key layout and payload serialization (ADR-0008 §2.3).

Key layout:

| Key                               | Type    | Written by                     |
|-----------------------------------|---------|--------------------------------|
| ``chat:{oid}``                    | string  | chat cache proxy (read)        |
| ``chat:ver:{oid}``                | counter | command handlers (invalidate)  |
| ``messages:{oid}:v{ver}:{o}:{l}`` | string  | messages cache proxy (read)    |
| ``presence:{chat_oid}``           | hash    | WS manager heartbeat           |
| ``lock:outbox-relay``             | string  | relay lease                    |

**Versioned invalidation.** The messages list is paginated and mutated on every
post, so deleting "the cached page" would mean either a SCAN (banned in the hot
path) or tracking every key ever written. Instead each write bumps
``chat:ver:{oid}`` and the page key embeds the version: after a bump every
previously cached page becomes unreachable and dies on its own TTL. One INCR,
no listing, no wildcard delete.

**Payload format.** Values are ``v1:``-prefixed orjson. The prefix is the schema
version: a deploy that changes a cached shape bumps it, and old entries become
unreadable rather than being deserialized into a wrong-shaped entity (an
unknown prefix is treated as a miss, so the worst case is one extra Mongo read).
"""

from datetime import datetime
from typing import Any

import orjson

from domain.entities.messages import (
	Chat,
	ChatListener,
	Message,
)
from domain.values.messages import (
	Text,
	Title,
)

# Bump when the shape of any cached payload changes.
CACHE_SCHEMA_VERSION = 'v1:'
_PREFIX_BYTES = CACHE_SCHEMA_VERSION.encode()


def chat_cache_key(chat_oid: str) -> str:
	"""Key of the serialized chat detail entry."""
	return f'chat:{chat_oid}'


def chat_version_key(chat_oid: str) -> str:
	"""Key of the counter bumped on every write to the chat."""
	return f'chat:ver:{chat_oid}'


def messages_cache_key(chat_oid: str, version: int, offset: int, limit: int) -> str:
	"""Key of one page of the chat's messages, bound to a version generation."""
	return f'messages:{chat_oid}:v{version}:{offset}:{limit}'


def presence_cache_key(chat_oid: str) -> str:
	"""Key of the presence hash (one field per live socket)."""
	return f'presence:{chat_oid}'


def relay_lock_key() -> str:
	"""Key of the outbox relay leader lock."""
	return 'lock:outbox-relay'


# --- chat --------------------------------------------------------------------


def serialize_chat(chat: Chat) -> bytes:
	"""Serialize a chat for ``chat:{oid}``.

	Only the fields a cached read actually needs are stored. The entity's
	``messages`` set, ``is_deleted`` flag and pending ``_events`` are not part of
	what the read path serves, so keeping them would cost on every read — and
	would make cached entries wrong the moment the entity was mutated.
	"""
	return _PREFIX_BYTES + orjson.dumps({
		'oid': chat.oid,
		'title': chat.title.as_generic_type(),
		'created_at': chat.created_at.isoformat(),
		'listeners': [listener.oid for listener in chat.listeners],
	})


def deserialize_chat(payload: bytes) -> Chat | None:
	"""Rebuild a chat from ``chat:{oid}``; None when the payload is unusable."""
	document = _loads(payload)
	if document is None:
		return None
	return Chat(
		oid=str(document['oid']),
		title=Title(value=str(document['title'])),
		created_at=_parse_datetime(document['created_at']),
		listeners={
			ChatListener(oid=str(listener_oid))
			for listener_oid in document.get('listeners', [])
		},
	)


# --- messages pages ----------------------------------------------------------


def serialize_messages(messages: list[Message], count: int) -> bytes:
	"""Serialize one page of messages together with the chat's total count.

	The count is part of the payload because the endpoint returns it next to the
	items; caching the page without it would still force a COUNT query on every
	read.
	"""
	return _PREFIX_BYTES + orjson.dumps({
		'count': count,
		'items': [
			{
				'oid': message.oid,
				'chat_oid': message.chat_oid,
				'text': message.text.as_generic_type(),
				'created_at': message.created_at.isoformat(),
			}
			for message in messages
		],
	})


def deserialize_messages(payload: bytes) -> tuple[list[Message], int] | None:
	"""Rebuild a cached page; None when the payload is unusable."""
	document = _loads(payload)
	if document is None:
		return None
	messages = [
		Message(
			oid=str(item['oid']),
			chat_oid=str(item['chat_oid']),
			text=Text(value=str(item['text'])),
			created_at=_parse_datetime(item['created_at']),
		)
		for item in document['items']
	]
	return messages, int(document['count'])


# --- internals ---------------------------------------------------------------


def _loads(payload: bytes) -> dict[str, Any] | None:
	"""Decode a cached payload, or None if it is not a usable current-version one.

	Never raises: a corrupt, truncated or older-schema entry is a cache MISS, not
	a request failure. This is the D5 rule applied to deserialization — the safest
	reading of "I cannot understand this" is "go to Mongo".
	"""
	if not payload.startswith(_PREFIX_BYTES):
		return None
	try:
		document = orjson.loads(payload[len(_PREFIX_BYTES):])
	except orjson.JSONDecodeError:
		return None
	if not isinstance(document, dict):
		return None
	return document


def _parse_datetime(value: Any) -> datetime:
	"""Rebuild a cached timestamp.

	Entities are created with ``datetime.now()`` (naive local time, see
	domain/entities/base.py) and the serializer writes ``isoformat()``, so the
	round trip is exact. Attaching a timezone here would make a cached entity
	differ from a freshly read one, which is a far worse outcome than a naive
	timestamp. A malformed value falls back to "now" rather than failing the read.
	"""
	if isinstance(value, str):
		try:
			return datetime.fromisoformat(value)
		except ValueError:
			pass
	return datetime.now()  # noqa: DTZ005 — deliberate: mirrors the entity's naive local timestamps
