from punq import Container, Scope

from infrastructure.repositories.messages.base import BaseChatRepository
from infrastructure.repositories.messages.memory import MemoryChatRepository
from logic.init import _init_container


def init_dummy_container() -> Container:
    container = _init_container()
    container.register(BaseChatRepository, MemoryChatRepository, scope=Scope.singleton)

    return container
