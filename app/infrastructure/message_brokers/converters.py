import orjson

from domain.events.base import BaseEvent


def convert_event_to_broker_message(event: BaseEvent) -> bytes:
	# Domain events are @dataclass instances with kw_only UUID/datetime fields on BaseEvent,
	# so plain orjson.dumps cannot serialize them without OPT_SERIALIZE_DATACLASS.
	return orjson.dumps(event, option=orjson.OPT_SERIALIZE_DATACLASS)
