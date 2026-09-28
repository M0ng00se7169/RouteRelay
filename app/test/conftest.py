from punq import Container
from pytest import fixture

from infrastructure.repositories.messages.base import BaseChatsRepository
from logic.mediator.base import Mediator
from test.fixtures import init_dummy_container


@fixture(scope='function')
def container() -> Container:
    return init_dummy_container()


@fixture()
def mediator(container: Container) -> Mediator:
    return container.resolve(Mediator)


@fixture()
def chat_repository(container: Container) -> BaseChatsRepository:
    repo: BaseChatsRepository = container.resolve(BaseChatsRepository)
    return repo
