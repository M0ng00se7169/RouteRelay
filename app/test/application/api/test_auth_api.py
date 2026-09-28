import time

import pytest
from fastapi import (
    Depends,
    FastAPI,
)
from fastapi.testclient import TestClient

from application.api.auth.handlers import router as auth_router
from application.api.main import create_app
from settings.security import get_current_user


@pytest.fixture
def client() -> TestClient:
    app: FastAPI = create_app()
    return TestClient(app=app, raise_server_exceptions=False)


@pytest.fixture
def token(client: TestClient) -> str:
    resp = client.post('/auth/token', data={'username': 'admin', 'password': 'admin'})
    assert resp.status_code == 200, resp.text
    access_token: str = resp.json()['access_token']
    return access_token


class TestTokenEndpoint:
    def test_issue_token_for_valid_credentials(self, client: TestClient) -> None:
        resp = client.post('/auth/token', data={'username': 'admin', 'password': 'admin'})
        assert resp.status_code == 200
        body = resp.json()
        assert body['token_type'] == 'bearer'
        # JWT format: three dot-separated base64url segments
        assert body['access_token'].count('.') == 2

    def test_rejects_wrong_password(self, client: TestClient) -> None:
        resp = client.post('/auth/token', data={'username': 'admin', 'password': 'wrong'})
        assert resp.status_code == 422 or resp.status_code == 401

    def test_rejects_unknown_user(self, client: TestClient) -> None:
        resp = client.post('/auth/token', data={'username': 'nobody', 'password': 'admin'})
        assert resp.status_code == 401

    def test_missing_form_returns_422(self, client: TestClient) -> None:
        resp = client.post('/auth/token', data={})
        assert resp.status_code == 422


class TestGetCurrentUserDependency:
    def test_valid_token_authenticates(self, token: str) -> None:
        app = FastAPI()
        app.include_router(auth_router)

        @app.get('/whoami')
        def whoami(user: str = Depends(get_current_user)) -> dict[str, str]:
            return {'user': user}

        client = TestClient(app)
        resp = client.get('/whoami', headers={'Authorization': f'Bearer {token}'})
        assert resp.status_code == 200
        assert resp.json() == {'user': 'admin'}

    def test_missing_token_returns_401(self) -> None:
        app = FastAPI()
        app.include_router(auth_router)

        @app.get('/whoami')
        def whoami(user: str = Depends(get_current_user)) -> dict[str, str]:
            return {'user': user}

        client = TestClient(app)
        resp = client.get('/whoami')
        assert resp.status_code == 401
        assert resp.headers['www-authenticate'] == 'Bearer'

    def test_expired_token_returns_401(self) -> None:
        from infrastructure.serializers.jwt import encode_token

        app = FastAPI()
        app.include_router(auth_router)

        @app.get('/whoami')
        def whoami(user: str = Depends(get_current_user)) -> dict[str, str]:
            return {'user': user}

        client = TestClient(app)
        expired = encode_token({
            'sub': 'admin',
            'token_type': 'access',
            'exp': int(time.time()) - 10,
        })
        resp = client.get('/whoami', headers={'Authorization': f'Bearer {expired}'})
        assert resp.status_code == 401

    def test_garbage_token_returns_401(self) -> None:
        app = FastAPI()
        app.include_router(auth_router)

        @app.get('/whoami')
        def whoami(user: str = Depends(get_current_user)) -> dict[str, str]:
            return {'user': user}

        client = TestClient(app)
        resp = client.get('/whoami', headers={'Authorization': 'Bearer not.a.token'})
        assert resp.status_code == 401
