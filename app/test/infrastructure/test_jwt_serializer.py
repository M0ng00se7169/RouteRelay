"""Tests for the JWT serializer utility."""

import time

import pytest
from infrastructure.serializers.jwt import create_token, encode_token, verify_token

pytest.importorskip("infrastructure.serializers.jwt", reason="jwt.py must exist")


class TestCreateToken:
    def test_creates_valid_token(self):
        token = create_token("testuser")
        assert verify_token(token) == "testuser"

    def test_creates_token_with_custom_claims(self):
        claims = {"sub": "456", "role": "admin"}
        token = create_token("otheruser", claims)
        assert verify_token(token) == "otheruser"

    def test_creates_token_with_exp(self):
        claims = {"sub": "789", "exp": int(time.time()) + 3600}
        token = create_token("user789", claims)
        assert verify_token(token) == "user789"

    def test_merges_claims_correctly(self):
        claims = {"sub": "101", "extra": "data"}
        token = create_token("user101", claims)
        assert verify_token(token) == "user101"


class TestVerifyToken:
    def test_raises_on_bad_format(self):
        with pytest.raises(ValueError):
            verify_token("not-a-valid-token")

    def test_raises_on_bad_signature(self):
        token = create_token("user123")
        with pytest.raises(ValueError):
            verify_token(f"{token}.modified")

    def test_raises_on_expired_token(self):
        claims = {"sub": "user456", "exp": 1000000000}
        token = create_token("user456", claims)
        with pytest.raises(ValueError):
            verify_token(token)

    def test_raises_on_missing_sub(self):
        # create_token always injects 'sub', so craft the payload directly
        token = encode_token({"role": "admin"})
        with pytest.raises(ValueError):
            verify_token(token)
