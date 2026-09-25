"""Mediator flow-volume counters (ADR-0006, Chunk 5.1).

The metric registry is global across tests, so all assertions are made on
deltas against baselines captured before each test acts (house pattern,
test/infrastructure/outbox/test_relay.py). Labelled metrics are asserted via
``get_sample_value(name, {label: value})`` — the label-less lookup returns
None once labels exist.
"""

from dataclasses import dataclass
from test.fixtures import init_dummy_container

import pytest
from prometheus_client import REGISTRY

from domain.events.messages import (
    ChatDeletedEvent,
    NewChatCreatedEvent,
)
from logic.commands.base import BaseCommand
from logic.exceptions.mediator import CommandHandlersNotRegisteredException
from logic.mediator.base import Mediator
from logic.queries.messages import GetAllChatsListenersQuery


def _labeled(name: str, label: str, value: str) -> float:
    sample = REGISTRY.get_sample_value(name, {label: value})
    return sample if sample is not None else 0.0


@pytest.fixture
def baselines():
    return {
        'events': {
            'NewChatCreatedEvent': _labeled('mediator_events_published_total', 'event', 'NewChatCreatedEvent'),
            'ChatDeletedEvent': _labeled('mediator_events_published_total', 'event', 'ChatDeletedEvent'),
        },
        'commands': {
            'CreateChatCommand': _labeled('mediator_commands_handled_total', 'command', 'CreateChatCommand'),
        },
        'queries': {
            'GetAllChatsListenersQuery': _labeled('mediator_queries_handled_total', 'query', 'GetAllChatsListenersQuery'),
        },
    }


@pytest.fixture
def mediator() -> Mediator:
    return init_dummy_container().resolve(Mediator)


@pytest.mark.asyncio
async def test_publish_increments_per_event_class(mediator, baselines):
    await mediator.publish([
        NewChatCreatedEvent(chat_oid='c1', chat_title='room'),
        NewChatCreatedEvent(chat_oid='c2', chat_title='room-2'),
        ChatDeletedEvent(chat_oid='c1'),
    ])

    assert (
        _labeled('mediator_events_published_total', 'event', 'NewChatCreatedEvent')
        - baselines['events']['NewChatCreatedEvent']
    ) == 2
    assert (
        _labeled('mediator_events_published_total', 'event', 'ChatDeletedEvent')
        - baselines['events']['ChatDeletedEvent']
    ) == 1


@pytest.mark.asyncio
async def test_handle_command_counts_after_handler_resolution(mediator, baselines):
    # Registering nothing extra: CreateChatCommand is registered by the dummy
    # container's mediator. A single dispatch must count exactly one handled.
    from logic.commands.messages import CreateChatCommand

    await mediator.handle_command(CreateChatCommand(title='metrics-room'))

    assert (
        _labeled('mediator_commands_handled_total', 'command', 'CreateChatCommand')
        - baselines['commands']['CreateChatCommand']
    ) == 1


@pytest.mark.asyncio
async def test_unregistered_command_raises_and_is_not_counted(mediator):
    @dataclass(frozen=True)
    class _UnregisteredCommand(BaseCommand):
        pass

    before = _labeled('mediator_commands_handled_total', 'command', '_UnregisteredCommand')

    with pytest.raises(CommandHandlersNotRegisteredException):
        await mediator.handle_command(_UnregisteredCommand())

    assert _labeled('mediator_commands_handled_total', 'command', '_UnregisteredCommand') == before


@pytest.mark.asyncio
async def test_handle_query_counts_per_query_class(mediator, baselines):
    # The counter measures flow volume: the query is counted when the mediator
    # dispatches it, even if the handler then raises a domain exception
    # (ChatNotFoundException for an unknown chat) — that exception must still
    # propagate to the caller.
    from logic.exceptions.messages import ChatNotFoundException

    with pytest.raises(ChatNotFoundException):
        await mediator.handle_query(GetAllChatsListenersQuery(chat_oid='no-such-chat-oid'))

    assert (
        _labeled('mediator_queries_handled_total', 'query', 'GetAllChatsListenersQuery')
        - baselines['queries']['GetAllChatsListenersQuery']
    ) == 1
