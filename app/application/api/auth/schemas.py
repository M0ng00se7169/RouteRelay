from pydantic import BaseModel


class TokenResponseSchema(BaseModel):
    """OAuth2-compliant token response consumed by Swagger UI's Authorize flow."""

    access_token: str
    token_type: str = 'bearer'
