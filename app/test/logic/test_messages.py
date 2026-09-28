from typing import cast

import pytest
from faker import Faker

from domain.entities.messages import Chat
from domain.values.messages import Title
from infrastructure.repositories.messages.base import BaseChatsRepository
from infrastructure.repositories.messages.memory import MemoryChatRepository
from logic.commands.messages import CreateChatCommand
from logic.exceptions.messages import ChatWithThatTitleAlreadyExistsException
from logic.mediator.base import Mediator


@pytest.mark.asyncio
async def test_create_chat_command_success(
    chat_repository: BaseChatsRepository,
    mediator: Mediator,
    faker: Faker,
) -> None:
    results = await mediator.handle_command(CreateChatCommand(title=faker.text()))
    chat: Chat = results[0]

    assert await chat_repository.check_chat_exists_by_title(title=chat.title.as_generic_type())


@pytest.mark.asyncio
async def test_create_chat_command_title_already_exists(
    chat_repository: BaseChatsRepository,
    mediator: Mediator,
    faker: Faker,
) -> None:
    title_text = faker.text()
    chat = Chat(title=Title(title_text))
    await chat_repository.add_chat(chat)

    # The dummy container wires the in-memory repo; reading its backing list
    # directly is the simplest existence check here.
    memory_repo = cast(MemoryChatRepository, chat_repository)
    assert chat in memory_repo._saved_chats

    with pytest.raises(ChatWithThatTitleAlreadyExistsException):
        await mediator.handle_command(CreateChatCommand(title=title_text))

    assert len(memory_repo._saved_chats) == 1
