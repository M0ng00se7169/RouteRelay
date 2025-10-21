from dataclasses import dataclass
from test.fixtures import init_dummy_container

import pytest

from logic.exceptions.mediator import (
    CommandHandlersNotRegisteredException,
    EventHandlersNotRegisteredException,
)
from logic.mediator.base import (
    BaseCommand,
    Mediator,
)


@dataclass(frozen=True)
class _UnregisteredCommand(BaseCommand):
    pass


@dataclass
class _UnregisteredEvent:
    event_id: str = 'e'
    occurred_at: object = None


@pytest.mark.asyncio
async def test_handle_command_without_registered_handler_raises():
    mediator: Mediator = init_dummy_container().resolve(Mediator)

    with pytest.raises(CommandHandlersNotRegisteredException):
        await mediator.handle_command(_UnregisteredCommand())


def test_command_exception_message():
    exc = CommandHandlersNotRegisteredException(_UnregisteredCommand)
    assert 'handlers' in exc.message.lower()


def test_event_exception_message():
    exc = EventHandlersNotRegisteredException(_UnregisteredEvent)
    assert 'handlers' in exc.message.lower()
