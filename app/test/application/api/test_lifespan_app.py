from test.fixtures import init_dummy_container

from fastapi import FastAPI
from fastapi.testclient import TestClient

from application.api.main import create_app
from logic.init import init_container


def test_app_lifespan_starts_and_stops_broker_and_relay():
	# One shared dummy container so lifespan + requests see the same repos.
	container = init_dummy_container()
	app: FastAPI = create_app()
	app.dependency_overrides[init_container] = lambda: container

	# Entering the TestClient context runs the lifespan (init broker + relay),
	# exiting stops the relay and closes the broker, all without real Kafka.
	with TestClient(app) as client:
		# A simple request proves the app is live after lifespan startup.
		resp = client.post('/chat/', json={'title': 'Lifespan Chat'})
		assert resp.status_code == 201


def test_app_lifespan_used_by_client_fixture():
	# The shared dummy container is exercised by normal requests, which run
	# the lifespan (broker start/close + relay start/stop) under the hood.
	container = init_dummy_container()
	app: FastAPI = create_app()
	app.dependency_overrides[init_container] = lambda: container

	with TestClient(app) as client:
		resp = client.get('/chat/')
		assert resp.status_code == 200
		assert 'items' in resp.json()
