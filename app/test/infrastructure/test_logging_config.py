import logging
import pytest

from infrastructure.logging_config import (
    JSONFormatter,
    configure_json_logging,
)


def test_json_formatter_emits_single_line():
	record = logging.LogRecord(
		name='test.logger',
		level=logging.INFO,
		pathname=__file__,
		lineno=1,
		msg='hello %s',
		args=('world',),
		exc_info=None,
	)
	formatted = JSONFormatter().format(record)
	assert formatted.startswith('{')
	assert '"message": "hello world"' in formatted


def test_configure_idempotent_and_no_handlers_fallback():
	root = logging.getLogger()
	saved = root.handlers[:]
	try:
		root.handlers.clear()
		# No handlers -> configure should attach a JSON stream handler.
		configure_json_logging()
		assert any(isinstance(h.formatter, JSONFormatter) for h in root.handlers)
		# Second call is idempotent (does not stack formatters).
		before = len(root.handlers)
		configure_json_logging()
		assert len(root.handlers) == before
	finally:
		root.handlers[:] = saved
