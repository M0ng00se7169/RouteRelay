from fastapi import (
    APIRouter,
    Depends,
    HTTPException,
    status,
)
from punq import Container

from application.api.messages.filters import (
    GetAllChatsFilters,
    GetMessagesFilters,
)
from application.api.messages.schemas import (
    AddTelegramListenerResponseSchema,
    AddTelegramListenerSchema,
    ChatDetailSchema,
    ChatListenerListItemSchema,
    CreateChatRequestSchema,
    CreateChatResponseSchema,
    CreateMessageResponseSchema,
    CreateMessageSchema,
    GetAllChatsQueryResponseSchema,
    GetChatPresenceResponseSchema,
    GetMessagesQueryResponseSchema,
    MessageDetailSchema,
)
from application.api.schemas import ErrorSchema
from domain.exceptions.base import ApplicationException
from logic.commands.messages import (
    AddTelegramListenerCommand,
    CreateChatCommand,
    CreateMessageCommand,
    DeleteChatCommand,
)
from logic.init import init_container
from logic.mediator.base import Mediator
from logic.queries.messages import (
    GetAllChatsListenersQuery,
    GetAllChatsQuery,
    GetChatDetailQuery,
    GetChatPresenceQuery,
    GetMessagesQuery,
)
from settings.config import Config
from settings.security import get_current_user

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
        container: Container = Depends(init_container),
        _user: str = Depends(get_current_user),
) -> CreateChatResponseSchema:
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
        status.HTTP_400_BAD_REQUEST: {'model': ErrorSchema},
    },
)
async def create_message_handler(
        chat_oid: str,
        schema: CreateMessageSchema,
        container: Container = Depends(init_container),
        _user: str = Depends(get_current_user),
) -> CreateMessageResponseSchema:
    mediator: Mediator = container.resolve(Mediator)

    try:
        message, *_ = await mediator.handle_command(CreateMessageCommand(text=schema.text, chat_oid=chat_oid))
    except ApplicationException as exception:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail={'error': exception.message})

    return CreateMessageResponseSchema.from_entity(message)


@router.get(
    '/{chat_oid}/',
    status_code=status.HTTP_200_OK,
    description='Get info about chat and all it\'s messages',
    responses={
        status.HTTP_200_OK: {'model': ChatDetailSchema},
        status.HTTP_400_BAD_REQUEST: {'model': ErrorSchema},
    },
)
async def get_chat_detail_handler(
        chat_oid: str,
        container: Container = Depends(init_container),
) -> ChatDetailSchema:
    mediator: Mediator = container.resolve(Mediator)

    try:
        chat = await mediator.handle_query(GetChatDetailQuery(chat_oid=chat_oid))
    except ApplicationException as exception:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail={'error': exception.message})

    return ChatDetailSchema.from_entity(chat)


@router.get(
    '/{chat_oid}/messages/',
    status_code=status.HTTP_200_OK,
    description='All sent messages in the chat',
    responses={
        status.HTTP_200_OK: {'model': GetMessagesQueryResponseSchema},
        status.HTTP_400_BAD_REQUEST: {'model': ErrorSchema},
    },
)
async def get_chat_messages_handler(
        chat_oid: str,
        filters: GetMessagesFilters = Depends(),
        container: Container = Depends(init_container),
) -> GetMessagesQueryResponseSchema:
    mediator: Mediator = container.resolve(Mediator)

    try:
        messages, count = await mediator.handle_query(
            GetMessagesQuery(chat_oid=chat_oid, filters=filters.to_infrastructure()),
        )
    except ApplicationException as exception:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail={'error': exception.message})

    return GetMessagesQueryResponseSchema(
        count=count,
        limit=filters.limit,
        offset=filters.offset,
        items=[MessageDetailSchema.from_entity(message) for message in messages],
    )


@router.get(
    '/',
    status_code=status.HTTP_200_OK,
    description="Get all open chats at this moment",
    responses={
        status.HTTP_200_OK: {'model': GetAllChatsQueryResponseSchema},
        status.HTTP_400_BAD_REQUEST: {'model': ErrorSchema},
    },
    summary="Retrieve a list of all chats",
)
async def get_all_chats_handler(
        filters: GetAllChatsFilters = Depends(),
        container: Container = Depends(init_container),
) -> GetAllChatsQueryResponseSchema:
    mediator: Mediator = container.resolve(Mediator)
    try:
        chats, count = await mediator.handle_query(
            GetAllChatsQuery(filters=filters.to_infrastructure()),
        )
    except ApplicationException as exception:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail={'error': exception.message})
    return GetAllChatsQueryResponseSchema(
        count=count,
        limit=filters.limit,
        offset=filters.offset,
        items=[ChatDetailSchema.from_entity(chat) for chat in chats],
    )


@router.delete(
    '/{chat_oid}/',
    status_code=status.HTTP_204_NO_CONTENT,
    summary='Delete chat after conversation ends',
    description='Delete chat by provided chat_oid',
)
async def delete_chat_handler(
    chat_oid: str,
    container: Container = Depends(init_container),
    _user: str = Depends(get_current_user),
) -> None:
    mediator: Mediator = container.resolve(Mediator)

    try:
        await mediator.handle_command(DeleteChatCommand(chat_oid=chat_oid))
    except ApplicationException as exception:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail={'error': exception.message})


@router.post(
    '/{chat_oid}/listeners/',
    status_code=status.HTTP_201_CREATED,
    summary='Add telegram tech support listener to chat',
    description='Add telegram tech support listener to chat',
    operation_id='addTelegramListenerToChat',
    response_model=AddTelegramListenerResponseSchema,
)
async def add_chat_listener_handler(
    chat_oid: str,
    schema: AddTelegramListenerSchema,
    container: Container = Depends(init_container),
    _user: str = Depends(get_current_user),
) -> AddTelegramListenerResponseSchema:
    mediator: Mediator = container.resolve(Mediator)

    try:
        listener, *_ = await mediator.handle_command(
            AddTelegramListenerCommand(
                chat_oid=chat_oid,
                telegram_chat_id=schema.telegram_chat_id,
            ),
        )
    except ApplicationException as exception:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={'error': exception.message},
        )

    return AddTelegramListenerResponseSchema.from_entity(listener)


@router.get(
    '/{chat_oid}/presence/',
    status_code=status.HTTP_200_OK,
    description='Number of WebSocket clients currently attached to this chat (ADR-0008). '
                'Returns enabled=false and count=0 when presence is disabled.',
    responses={
        status.HTTP_200_OK: {'model': GetChatPresenceResponseSchema},
    },
    summary='Retrieve the live WebSocket count for this chat',
    operation_id='getChatPresence',
)
async def get_chat_presence_handler(
    chat_oid: str,
    container: Container = Depends(init_container),
) -> GetChatPresenceResponseSchema:
    mediator: Mediator = container.resolve(Mediator)
    config: Config = container.resolve(Config)

    count = await mediator.handle_query(GetChatPresenceQuery(chat_oid=chat_oid))

    return GetChatPresenceResponseSchema(
        chat_oid=chat_oid,
        # With presence off the manager never registers sockets, so the tracker
        # is empty by construction. Say so in the response instead of letting a
        # 0 look like a real measurement.
        count=count if config.presence_enabled else 0,
        enabled=config.presence_enabled,
    )


@router.get(
    '/{chat_oid}/listeners/',
    status_code=status.HTTP_200_OK,
    description='Retrieve all listeners for this chat',
    responses={
        status.HTTP_200_OK: {'model': list[ChatListenerListItemSchema]},
        status.HTTP_400_BAD_REQUEST: {'model': ErrorSchema},
    },
    summary='Retrieve all listeners for this chat',
    operation_id='getAllChatListeners',
)
async def get_all_chat_listeners_handler(
    chat_oid: str,
    container: Container = Depends(init_container),
) -> list[ChatListenerListItemSchema]:
    mediator: Mediator = container.resolve(Mediator)

    try:
        chat_listeners = await mediator.handle_query(GetAllChatsListenersQuery(chat_oid=chat_oid))
    except ApplicationException as exception:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail={'error': exception.message})

    return [ChatListenerListItemSchema.from_entity(chat_listener=chat_listener) for chat_listener in chat_listeners]
