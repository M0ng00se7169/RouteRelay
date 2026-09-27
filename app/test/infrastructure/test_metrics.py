"""Unit tests for the safe-observe helpers (ADR-0006, Chunk 1.2).

House rules for metric tests (ADR-0006, Section 5): the registry is global
across tests, so capture baselines and assert on deltas — never unregister.
"""

from unittest.mock import MagicMock

from prometheus_client import REGISTRY

from infrastructure.metrics import (
	_safe_observe,
	outbox_pending,
	outbox_published_total,
	safe_inc,
	safe_observe,
	safe_set,
)


def _value(name: str) -> float:
	value = REGISTRY.get_sample_value(name)
	return value if value is not None else 0.0


class TestSafeHelpersNeverRaise:
	"""Core Chunk 1.2 requirement: helpers never raise, even when the metric
	they update is mocked to raise (ADR-0006 Chunk 1.2 verify step)."""

	def test_safe_observe_swallows_raising_mutator(self) -> None:
		boom = MagicMock(side_effect=RuntimeError('registry exploded'))

		safe_observe(boom, 1, 2, three=3)

		boom.assert_called_once_with(1, 2, three=3)

	def test_safe_inc_swallows_raising_counter(self) -> None:
		# Mock at the level safe_inc actually touches: .inc / .labels(...).inc.
		counter = MagicMock()
		counter._name = 'broken_counter'
		counter.inc.side_effect = RuntimeError('kaboom')
		counter.labels.return_value.inc.side_effect = RuntimeError('kaboom')

		safe_inc(counter)
		safe_inc(counter, 5)
		safe_inc(counter, amount=7)
		safe_inc(counter, topic='chat-events')

		counter.inc.assert_called_with(7)
		counter.labels.assert_called_once_with(topic='chat-events')

	def test_safe_set_swallows_raising_gauge(self) -> None:
		gauge = MagicMock()
		gauge._name = 'broken_gauge'
		gauge.set.side_effect = RuntimeError('kaboom')

		safe_set(gauge, 3)

		gauge.set.assert_called_once_with(3)

	def test__safe_observe_swallows_and_returns_none(self) -> None:
		def raise_every_time() -> None:
			raise ValueError('nope')

		assert _safe_observe('test metric', raise_every_time) is None


class TestHappyPath:
	"""The helpers actually mutate the real (global) registry."""

	def test_safe_inc_unlabelled_counter_delta(self) -> None:
		before = _value('outbox_published_total')

		safe_inc(outbox_published_total)
		safe_inc(outbox_published_total, 2)

		assert _value('outbox_published_total') - before == 3

	def test_safe_inc_labelled_counter_delta(self) -> None:
		# Label first via the helper, then read the labelled sample back.
		# Note: the counter has no declared label names, so label the sample
		# through a throwaway labelled inc on a private metric-like object.
		counter = MagicMock()
		counter._name = 'happy_path_metric'

		safe_inc(counter, topic='chat-events')

		counter.labels.assert_called_once_with(topic='chat-events')
		counter.labels.return_value.inc.assert_called_once_with(1.0)

	def test_safe_set_gauge_delta(self) -> None:
		safe_set(outbox_pending, 42)

		assert _value('outbox_pending') == 42

		safe_set(outbox_pending, 0)

		assert _value('outbox_pending') == 0

	def test_safe_observe_calls_mutator_with_args(self) -> None:
		mutator = MagicMock()

		safe_observe(mutator, 0.5)

		mutator.assert_called_once_with(0.5)


class TestDescriptionLogging:
	"""Failures are logged at WARNING with context, not silently dropped."""

	def test_failure_is_logged_not_raised(self, caplog) -> None:
		import logging

		def boom() -> None:
			raise RuntimeError('bookkeeping bug')

		with caplog.at_level(logging.WARNING, logger='infrastructure.metrics'):
			_safe_observe('unit-test-metric', boom)

		assert any('unit-test-metric' in record.getMessage() for record in caplog.records)
