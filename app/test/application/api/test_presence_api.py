"""Chat presence endpoint (ADR-0008, Chunk 4).

Presence is API data, not a Prometheus gauge (a per-chat label would be
unbounded, ADR-0006 D3), so it needs an endpoint. The tests pin both states:
feature off (the default everywhere except compose) and feature on with live
sockets.
"""

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from application.api.main import create_app
from logic.init import init_container
from test.fixtures import init_dummy_container


def _client(presence_enabled: bool) -> TestClient:
	app: FastAPI = create_app()
	# One container per app so every request in a test shares the same in-memory
	# state (init_dummy_container is not cached).
	container = init_dummy_container(presence_enabled=presence_enabled)
	app.dependency_overrides[init_container] = lambda: container
	return TestClient(app=app)


@pytest.fixture
def client() -> TestClient:
	return _client(presence_enabled=False)


@pytest.fixture
def presence_client() -> TestClient:
	return _client(presence_enabled=True)


@pytest.fixture(scope='module')
def auth_headers() -> dict[str, str]:
	app: FastAPI = create_app()
	with TestClient(app=app) as c:
		resp = c.post('/auth/token', data={'username': 'admin', 'password': 'admin'})
	assert resp.status_code == 200, resp.text
	return {'Authorization': f"Bearer {resp.json()['access_token']}"}


def _create_chat(client: TestClient, headers: dict[str, str]) -> str:
	resp = client.post('/chat/', json={'title': 'General'}, headers=headers)
	assert resp.status_code == 201, resp.text
	oid: str = resp.json()['oid']
	return oid


def test_presence_reports_disabled_when_the_flag_is_off(client: TestClient) -> None:
	# A 0 with no qualifier would look like a real measurement; the flag says
	# "we are not counting".
	resp = client.get('/chat/any-chat/presence/')

	assert resp.status_code == 200
	body = resp.json()
	assert body['enabled'] is False
	assert body['count'] == 0


def test_presence_is_zero_without_sockets(presence_client: TestClient) -> None:
	resp = presence_client.get('/chat/any-chat/presence/')

	assert resp.status_code == 200
	body = resp.json()
	assert body['enabled'] is True
	assert body['count'] == 0


def test_presence_counts_a_connected_socket(
	presence_client: TestClient,
	auth_headers: dict[str, str],
) -> None:
	oid = _create_chat(presence_client, auth_headers)

	with presence_client.websocket_connect(f'/chats/{oid}/') as ws:
		ws.receive_text()
		body = presence_client.get(f'/chat/{oid}/presence/').json()
		assert body['count'] == 1

	# The socket is gone once the context exits: the manager removed its presence
	# entry on disconnect, so the count drops back immediately.
	after = presence_client.get(f'/chat/{oid}/presence/').json()
	assert after['count'] == 0


def test_presence_counts_every_socket(
	presence_client: TestClient,
	auth_headers: dict[str, str],
) -> None:
	oid = _create_chat(presence_client, auth_headers)

	with (
		presence_client.websocket_connect(f'/chats/{oid}/') as first,
		presence_client.websocket_connect(f'/chats/{oid}/') as second,
	):
		first.receive_text()
		second.receive_text()
		body = presence_client.get(f'/chat/{oid}/presence/').json()

	assert body['count'] == 2


def test_presence_is_scoped_per_chat(
	presence_client: TestClient,
	auth_headers: dict[str, str],
) -> None:
	first_chat = _create_chat(presence_client, auth_headers)
	resp = presence_client.post('/chat/', json={'title': 'Other'}, headers=auth_headers)
	second_chat: str = resp.json()['oid']

	with presence_client.websocket_connect(f'/chats/{first_chat}/') as ws:
		ws.receive_text()
		assert presence_client.get(f'/chat/{first_chat}/presence/').json()['count'] == 1
		assert presence_client.get(f'/chat/{second_chat}/presence/').json()['count'] == 0
