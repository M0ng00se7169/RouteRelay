import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from application.api.main import create_app
from logic.init import init_container
from test.fixtures import init_dummy_container


@pytest.fixture
def app() -> FastAPI:
    app = create_app()
    app.dependency_overrides[init_container] = init_dummy_container

    return app


@pytest.fixture
def client(app: FastAPI) -> TestClient:
    return TestClient(app=app)


@pytest.fixture
def auth_headers() -> dict[str, str]:
    """Bearer token for write endpoints (POST/DELETE enforce auth)."""
    token_app = create_app()
    with TestClient(token_app) as c:
        resp = c.post('/auth/token', data={'username': 'admin', 'password': 'admin'})
    assert resp.status_code == 200, resp.text
    return {'Authorization': f"Bearer {resp.json()['access_token']}"}
