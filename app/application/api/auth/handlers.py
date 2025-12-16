from fastapi import (
    APIRouter,
    Depends,
    HTTPException,
    status,
)
from fastapi.security import OAuth2PasswordRequestForm

from application.api.auth.schemas import TokenResponseSchema
from settings.security import (
    authenticate_user,
    issue_token,
)

router = APIRouter(tags=['Auth'])


@router.post(
    '/token',
    status_code=status.HTTP_200_OK,
    summary='Exchange credentials for a JWT access token',
    description=(
        'OAuth2 password flow. Accepts username/password form data and returns '
        "an OAuth2-compliant token response, so Swagger UI's Authorize button "
        'works out of the box.'
    ),
    responses={
        status.HTTP_200_OK: {'model': TokenResponseSchema},
        status.HTTP_401_UNAUTHORIZED: {'model': TokenResponseSchema},
    },
)
async def token_handler(form: OAuth2PasswordRequestForm = Depends()) -> TokenResponseSchema:
    if not authenticate_user(form.username, form.password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={'error': 'Incorrect username or password'},
            headers={'WWW-Authenticate': 'Bearer'},
        )
    return TokenResponseSchema(access_token=issue_token(form.username))
