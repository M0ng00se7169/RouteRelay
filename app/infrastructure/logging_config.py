import json
import logging
import sys
from datetime import (
    datetime,
    timezone,
)


class JSONFormatter(logging.Formatter):
	"""Stdlib logging formatter that emits one JSON object per line.

	Loki/Promtail parse these structured lines into `level`/`logger`/`message`
	labels. No infrastructure imports — this keeps the domain/logic layers free
	of any third-party logging dependencies.
	"""

	def format(self, record: logging.LogRecord) -> str:
		payload = {
			'timestamp': datetime.fromtimestamp(
				record.created, tz=timezone.utc,
			).isoformat(),
			'level': record.levelname,
			'logger': record.name,
			'message': record.getMessage(),
		}
		if record.exc_info:
			payload['exception'] = self.formatException(record.exc_info)
		return json.dumps(payload, default=str)


def configure_json_logging() -> None:
	"""Attach the JSON formatter to the root logger's existing handlers.

	Idempotent: only configures handlers that lack a JSON formatter, so repeated
	calls (or test re-imports) don't stack formatters. Intended to be invoked
	once inside `create_app` before the Prometheus instrumentator.
	"""
	root = logging.getLogger()
	if any(isinstance(h.formatter, JSONFormatter) for h in root.handlers):
		return
	formatter = JSONFormatter()
	for handler in root.handlers:
		handler.setFormatter(formatter)
	# Ensure at least one stream handler emits JSON even if the root logger has
	# no handlers configured yet (e.g. minimal test runners).
	if not root.handlers:
		stream = logging.StreamHandler(sys.stdout)
		stream.setFormatter(formatter)
		root.addHandler(stream)
