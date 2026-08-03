from test.fixtures import init_dummy_container

from fastapi import FastAPI
from fastapi.testclient import TestClient

import pytest

from application.api.main import create_app
from logic.init import init_container


@pytest.fixture
def client() -> TestClient:
	app: FastAPI = create_app()
	# Build ONE dummy container so every request in a test shares the same
	# in-memory repositories (init_dummy_container is not cached, so binding
	# the function directly would give each request a fresh empty store).
	container = init_dummy_container()
	app.dependency_overrides[init_container] = lambda: container
	return TestClient(app=app)


@pytest.fixture(scope='module')
def auth_headers() -> dict[str, str]:
	"""Bearer token for write endpoints (POST/DELETE enforce auth)."""
	app: FastAPI = create_app()
	with TestClient(app=app) as c:
		resp = c.post('/auth/token', data={'username': 'admin', 'password': 'admin'})
	assert resp.status_code == 200, resp.text
	return {'Authorization': f"Bearer {resp.json()['access_token']}"}


def _create_chat(client: TestClient, title: str = 'General', headers: dict[str, str] | None = None) -> str:
	resp = client.post('/chat/', json={'title': title}, headers=headers)
	assert resp.status_code == 201, resp.text
	return resp.json()['oid']


def test_create_chat_success(client: TestClient, auth_headers: dict[str, str]):
	resp = client.post('/chat/', json={'title': 'Room A'}, headers=auth_headers)
	assert resp.status_code == 201
	body = resp.json()
	assert body['title'] == 'Room A'
	assert 'oid' in body


def test_create_chat_without_token_returns_401(client: TestClient):
	resp = client.post('/chat/', json={'title': 'Room A'})
	assert resp.status_code == 401
	assert resp.headers['www-authenticate'] == 'Bearer'


def test_create_chat_with_garbage_token_returns_401(client: TestClient):
	resp = client.post('/chat/', json={'title': 'Room A'}, headers={'Authorization': 'Bearer not.a.token'})
	assert resp.status_code == 401


def test_create_chat_duplicate_returns_400(client: TestClient, auth_headers: dict[str, str]):
	client.post('/chat/', json={'title': 'Dup'}, headers=auth_headers)
	resp = client.post('/chat/', json={'title': 'Dup'}, headers=auth_headers)
	assert resp.status_code == 400
	assert 'already exists' in resp.json()['detail']['error'].lower()


def test_create_chat_invalid_payload_returns_422(client: TestClient, auth_headers: dict[str, str]):
	resp = client.post('/chat/', json={}, headers=auth_headers)
	assert resp.status_code == 422


def test_get_chat_detail_success(client: TestClient, auth_headers: dict[str, str]):
	oid = _create_chat(client, headers=auth_headers)
	resp = client.get(f'/chat/{oid}/')
	assert resp.status_code == 200
	assert resp.json()['oid'] == oid


def test_get_chat_detail_not_found_returns_400(client: TestClient):
	resp = client.get('/chat/does-not-exist/')
	assert resp.status_code == 400
	assert 'not found' in resp.json()['detail']['error'].lower()


def test_get_all_chats_listeners_success(client: TestClient, auth_headers: dict[str, str]):
	oid = _create_chat(client, headers=auth_headers)
	resp = client.get(f'/chat/{oid}/listeners/')
	assert resp.status_code == 200
	assert resp.json() == []


def test_add_telegram_listener_success(client: TestClient, auth_headers: dict[str, str]):
	oid = _create_chat(client, headers=auth_headers)
	resp = client.post(f'/chat/{oid}/listeners/', json={'telegram_chat_id': '123'}, headers=auth_headers)
	assert resp.status_code == 201
	assert resp.json()['listener_id'] == '123'


def test_add_telegram_listener_chat_not_found(client: TestClient, auth_headers: dict[str, str]):
	resp = client.post('/chat/missing/listeners/', json={'telegram_chat_id': '123'}, headers=auth_headers)
	assert resp.status_code == 400
	assert 'not found' in resp.json()['detail']['error'].lower()


def test_get_all_chats_listeners_missing_chat_returns_400(client: TestClient):
	resp = client.get('/chat/missing/listeners/')
	assert resp.status_code == 400
	assert 'not found' in resp.json()['detail']['error'].lower()


def test_delete_chat_success(client: TestClient, auth_headers: dict[str, str]):
	oid = _create_chat(client, headers=auth_headers)
	resp = client.delete(f'/chat/{oid}/', headers=auth_headers)
	assert resp.status_code == 204

	detail = client.get(f'/chat/{oid}/')
	assert detail.status_code == 400


def test_delete_chat_without_token_returns_401(client: TestClient, auth_headers: dict[str, str]):
	oid = _create_chat(client, headers=auth_headers)
	resp = client.delete(f'/chat/{oid}/')
	assert resp.status_code == 401


def test_delete_chat_not_found(client: TestClient, auth_headers: dict[str, str]):
	resp = client.delete('/chat/missing/', headers=auth_headers)
	assert resp.status_code == 400
	assert 'not found' in resp.json()['detail']['error'].lower()


def test_create_message_success(client: TestClient, auth_headers: dict[str, str]):
	oid = _create_chat(client, headers=auth_headers)
	resp = client.post(f'/chat/{oid}/messages', json={'text': 'hello'}, headers=auth_headers)
	assert resp.status_code == 201
	body = resp.json()
	assert 'oid' in body
	assert body['text'] == 'hello'


def test_create_message_without_token_returns_401(client: TestClient, auth_headers: dict[str, str]):
	oid = _create_chat(client, headers=auth_headers)
	resp = client.post(f'/chat/{oid}/messages', json={'text': 'hello'})
	assert resp.status_code == 401


def test_create_message_chat_not_found(client: TestClient, auth_headers: dict[str, str]):
	resp = client.post('/chat/missing/messages', json={'text': 'hi'}, headers=auth_headers)
	assert resp.status_code == 400
	assert 'not found' in resp.json()['detail']['error'].lower()


def test_create_message_invalid_payload_returns_422(client: TestClient, auth_headers: dict[str, str]):
	oid = _create_chat(client, headers=auth_headers)
	resp = client.post(f'/chat/{oid}/messages', json={}, headers=auth_headers)
	assert resp.status_code == 422


def test_get_chat_messages_success(client: TestClient, auth_headers: dict[str, str]):
	oid = _create_chat(client, headers=auth_headers)
	client.post(f'/chat/{oid}/messages', json={'text': 'first'}, headers=auth_headers)
	client.post(f'/chat/{oid}/messages', json={'text': 'second'}, headers=auth_headers)

	resp = client.get(f'/chat/{oid}/messages/')
	assert resp.status_code == 200
	messages = resp.json()['items']
	assert {m['text'] for m in messages} == {'first', 'second'}


def test_get_chat_messages_empty(client: TestClient, auth_headers: dict[str, str]):
	oid = _create_chat(client, headers=auth_headers)
	resp = client.get(f'/chat/{oid}/messages/')
	assert resp.status_code == 200
	assert resp.json()['items'] == []


def test_get_chat_messages_chat_not_found(client: TestClient):
	# Unknown chat -> get_messages returns an empty page (200), not an error.
	resp = client.get('/chat/missing/messages/')
	assert resp.status_code == 200
	assert resp.json()['items'] == []


def test_get_all_chats_list_success(client: TestClient, auth_headers: dict[str, str]):
	_create_chat(client, title='One', headers=auth_headers)
	_create_chat(client, title='Two', headers=auth_headers)
	resp = client.get('/chat/')
	assert resp.status_code == 200
	titles = {c['title'] for c in resp.json()['items']}
	assert titles == {'One', 'Two'}


def test_websocket_connect_to_existing_chat(client: TestClient, auth_headers: dict[str, str]):
	oid = _create_chat(client, headers=auth_headers)
	with client.websocket_connect(f'/chats/{oid}/') as ws:
		data = ws.receive_text()
		assert data == 'You are now connected!'


def test_websocket_connect_to_missing_chat_closes(client: TestClient):
	# chat_oid is a valid UUID but no such chat exists -> handler accepts,
	# sends an error payload, then closes the socket.
	with client.websocket_connect('/chats/00000000-0000-0000-0000-000000000000/') as ws:
		payload = ws.receive_json()
		assert 'error' in payload
		assert 'not found' in payload['error'].lower()


def test_metrics_endpoint_exposes_application_info(client: TestClient):
	# Chunk 6.2 (ADR-0006): the build-info gauge must be present on /metrics
	# with the configured version as a label and value 1.
	from prometheus_client import REGISTRY

	from settings.config import Config

	version = Config().app_version

	resp = client.get('/metrics')
	assert resp.status_code == 200

	sample = REGISTRY.get_sample_value('application_info', {'version': version})
	assert sample == 1.0
	assert f'application_info{{version="{version}"}} 1.0' in resp.text
