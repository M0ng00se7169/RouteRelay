"""End-to-end: open breaker -> API answers 503 (fail fast) instead of hanging.

The 'mongo' breaker is a singleton of the (lru-cached) production container and
is shared by every dummy container built on top of it, so these tests MUST
reset it (finally block) — otherwise later tests would keep failing fast even
though their in-memory repos are perfectly healthy.
"""

from fastapi import FastAPI
from fastapi.testclient import TestClient

from application.api.main import create_app
from infrastructure.resilience import CircuitBreaker
from logic.init import init_container
from test.fixtures import init_dummy_container


def _wrapped_client() -> tuple[TestClient, CircuitBreaker]:
	container = init_dummy_container(wrap_repos_with_breaker=True)
	app: FastAPI = create_app()
	app.dependency_overrides[init_container] = lambda: container
	return TestClient(app=app), container.resolve(CircuitBreaker)


def test_open_breaker_returns_503_with_retry_after() -> None:
	client, breaker = _wrapped_client()

	try:
		# Force the breaker open directly (the trip path itself is covered by
		# the unit tests in test_resilience.py); what matters here is the
		# HTTP contract: the endpoint's broad ``except ApplicationException``
		# must NOT swallow CircuitOpenError into a 400.
		breaker._open()

		resp = client.get('/chat/')
		assert resp.status_code == 503, resp.text
		assert 'open' in resp.json()['detail']['error'].lower()
		assert 'retry-after' in {key.lower() for key in resp.headers}
	finally:
		breaker._reset()

	# Reset -> the same endpoint is served normally through the proxy again.
	resp = client.get('/chat/')
	assert resp.status_code == 200


def test_closed_breaker_serves_requests_through_proxy() -> None:
	client, breaker = _wrapped_client()
	assert breaker.state == 'closed'

	resp = client.get('/chat/')
	assert resp.status_code == 200
	assert resp.json()['items'] == []
