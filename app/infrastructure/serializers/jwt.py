"""Pure-stdlib HS256 JWT serializer (no dependencies)."""

import base64
import hashlib
import hmac
import json
import time
from typing import (
    Any,
)

from settings.config import Config


# HS256 spec: base64url encode without padding
def _b64url_encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode().rstrip("=")


def _b64url_decode(data: str) -> bytes:
    # Add padding only when needed
    data += "=" * (-len(data) % 4)
    return base64.urlsafe_b64decode(data)


def _base64url_encode(value: Any) -> str:
    if isinstance(value, str):
        return _b64url_encode(value.encode())
    # Encode the JSON as ASCII bytes, then base64url-encode those bytes
    return _b64url_encode(json.dumps(value).encode("ascii"))


def _base64url_decode(value: str) -> Any:
    # Add padding only when needed
    value += "=" * (-len(value) % 4)
    return json.loads(_b64url_decode(value).decode("utf-8"))


def _sign(signing_input: str) -> bytes:
    """HS256 signature: HMAC-SHA256 over the signing input, keyed by the secret."""
    secret = Config().jwt_secret.encode()
    return hmac.new(secret, signing_input.encode(), hashlib.sha256).digest()


def encode_token(payload: dict[str, Any]) -> str:
    """Sign an arbitrary claims payload and return the encoded JWT string."""
    header = {"alg": "HS256", "typ": "JWT"}
    header_str = _base64url_encode(header)
    payload_str = _base64url_encode(payload)
    signing_input = f"{header_str}.{payload_str}"
    signature_str = _b64url_encode(_sign(signing_input))
    return f"{signing_input}.{signature_str}"


def create_token(subject: str, claims: dict[str, Any] | None = None) -> str:
    """Create a signed JWT token.

    Args:
        subject: The user's identifier. Always included as the 'sub' claim,
            overriding any 'sub' present in claims.
        claims: Optional additional claims to merge with the payload.

    Returns:
        A signed JWT token string.
    """
    payload = dict(claims) if claims else {}
    payload["sub"] = subject
    return encode_token(payload)


def verify_token(token: str) -> str:
    """Verify a signed JWT token and return the subject.

    Raises:
        ValueError: If the token is malformed, has an invalid signature, or is expired.
    """
    try:
        header_str, payload_str, signature_str = token.split(".")
    except ValueError:
        raise ValueError("Invalid token format: expected 3 dot-separated parts")

    if not header_str or not payload_str or not signature_str:
        raise ValueError("Invalid token format: empty header, payload, or signature")

    # Verify signature (constant-time comparison)
    signing_input = f"{header_str}.{payload_str}"
    actual_signature = _b64url_decode(signature_str)
    if not hmac.compare_digest(_sign(signing_input), actual_signature):
        raise ValueError("Invalid token signature")

    # Decode payload
    try:
        payload = json.loads(_b64url_decode(payload_str).decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError):
        raise ValueError("Invalid token payload")

    # Verify required claims
    if "sub" not in payload:
        raise ValueError("Token missing required 'sub' claim")

    # Verify expiration if present
    exp = payload.get("exp")
    if exp is not None and exp < int(time.time()):
        raise ValueError("Token expired")

    subject = payload["sub"]
    # ValueError (not TypeError): the caller-facing contract for a bad token is
    # one exception type, and the app maps ValueError -> HTTP 401.
    if not isinstance(subject, str):
        raise ValueError("Token 'sub' claim is not a string")  # noqa: TRY004
    return subject
