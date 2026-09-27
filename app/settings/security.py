"""OAuth2 password-flow auth backed by the pure-stdlib JWT serializer.

Replaces the previous in-memory token store: tokens are now stateless,
signed HS256 JWTs (see infrastructure/serializers/jwt.py), so no
server-side storage is needed and any app instance can verify a token.
"""

import hmac
import time

from fastapi import (
    Depends,
    HTTPException,
    status,
)
from fastapi.security import OAuth2PasswordBearer

from infrastructure.serializers.jwt import (
    create_token,
    verify_token,
)

from settings.config import Config


# Where clients obtain tokens (the POST /auth/token route).
oauth2_scheme = OAuth2PasswordBearer(tokenUrl='/auth/token')

# Lifetime of issued access tokens, in seconds.
TOKEN_TTL_SECONDS = 3600


def authenticate_user(username: str, password: str) -> bool:
    """Check credentials against the configured single demo user."""
    config = Config()
    return hmac.compare_digest(
        username.encode(), config.auth_username.encode(),
    ) and hmac.compare_digest(password.encode(), config.auth_password.encode())


def issue_token(username: str) -> str:
    """Create a signed JWT access token for the given user."""
    return create_token(
        username,
        claims={'token_type': 'access', 'exp': int(time.time()) + TOKEN_TTL_SECONDS},
    )


def get_current_user(token: str = Depends(oauth2_scheme)) -> str:
    """FastAPI dependency: validate a Bearer JWT and return its subject (user id).

    Raises:
        HTTPException: 401 for missing, malformed, forged or expired tokens,
            with a WWW-Authenticate header per the OAuth2 spec.
    """
    try:
        return verify_token(token)
    except ValueError as error:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={'error': str(error)},
            headers={'WWW-Authenticate': 'Bearer'},
        )
