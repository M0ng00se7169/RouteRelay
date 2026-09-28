from uuid import UUID

from fastapi import (
    APIRouter,
    Depends,
)
from fastapi.websockets import WebSocket
from punq import Container
from starlette.websockets import WebSocketDisconnect

from infrastructure.websockets.managers import BaseConnectionManager
from logic.exceptions.messages import ChatNotFoundException
from logic.init import init_container
from logic.mediator.base import Mediator
from logic.queries.messages import GetChatDetailQuery

router = APIRouter(tags=['chats'])


@router.websocket('/{chat_oid}/')
async def messages_handlers(
        chat_oid: UUID,
        websocket: WebSocket,
        container: Container = Depends(init_container),
) -> None:
    connection_manager: BaseConnectionManager = container.resolve(BaseConnectionManager)
    mediator: Mediator = container.resolve(Mediator)
    try:
        await mediator.handle_query(GetChatDetailQuery(chat_oid=str(chat_oid)))
    except ChatNotFoundException as error:
        await websocket.accept()
        await websocket.send_json(data={'error': error.message})
        await websocket.close()
        return

    await connection_manager.accept_connection(websocket=websocket, key=str(chat_oid))

    await websocket.send_text("You are now connected!")

    try:
        while True:
            await websocket.receive_text()

    except WebSocketDisconnect:
        await connection_manager.remove_connection(websocket=websocket, key=str(chat_oid))
