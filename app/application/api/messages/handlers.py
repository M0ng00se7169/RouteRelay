from fastapi import APIRouter, status, Depends, HTTPException
from punq import Container

from application.api.messages.schemas import CreateChatResponseSchema, CreateChatRequestSchema, CreateMessageSchema
from application.api.schemas import ErrorSchema
from domain.exceptions.base import ApplicationException
from logic.commands.messages import CreateChatCommand
from logic.init import init_container
from logic.mediator import Mediator

router = APIRouter(tags=['Chat'])


@router.post(
    '/',
    status_code=status.HTTP_201_CREATED,
    description='Create a new chat. Returns 400 error if chat with the same name already exists',
    responses={
        status.HTTP_201_CREATED: {'model': CreateChatResponseSchema},
        status.HTTP_400_BAD_REQUEST: {'model': ErrorSchema},
    },
)
async def create_chat_handler(
    schema: CreateChatRequestSchema,
    container: Container = Depends(init_container)
):
    """
    Create a new chat based on the provided request schema.
    Handles chat creation using the specified title and returns the response schema.
    Raises an HTTP 400 Bad Request with the exception message in case of an ApplicationException.
    """
    mediator: Mediator = container.resolve(Mediator)

    try:
        chat, *_ = await mediator.handle_command(CreateChatCommand(title=schema.title))
    except ApplicationException as exception:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail={'error': exception.message})

    return CreateChatResponseSchema.from_entity(chat)


@router.post(
    '/{chat_oid}/messages',
    status_code=status.HTTP_201_CREATED,
    description='Create a new message. Returns 400 error if message with the same oid already exists',
    responses={
        status.HTTP_201_CREATED: {'model': CreateMessageSchema},
        status.HTTP_400_BAD_REQUEST: {'model': ErrorSchema}
    }
)
async def create_message_handler(
    chat_oid: str,
    schema: CreateMessageSchema,
    container: Container = Depends(init_container),
) -> CreateChatResponseSchema:
    mediator: Mediator = container.resolve(Mediator)

    try:
        message, *_ = await mediator.handle_command(CreateMessageCommand(text=schema.text, chat_oid=chat_oid))
    except ApplicationException as exception:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail={'error': exception.message})

    return CreateChatResponseSchema.from_entity(message)
