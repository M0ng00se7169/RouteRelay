from infrastructure.repositories.messages.base import BaseChatsRepository
from infrastructure.repositories.messages.memory import MemoryChatRepository
from logic.init import init_container
from punq import (
    Container,
    Scope,
)


def init_dummy_container() -> Container:
    container = init_container()
    container.register(BaseChatsRepository, MemoryChatRepository, scope=Scope.singleton)

    return container
