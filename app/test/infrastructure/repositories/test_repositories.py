from unittest.mock import (
    AsyncMock,
    MagicMock,
)

import pytest

from domain.entities.messages import (
    Chat,
    ChatListener,
    Message,
)
from domain.values.messages import (
    Text,
    Title,
)
from infrastructure.repositories.filters.messages import (
    GetAllChatsFilters,
    GetMessagesFilters,
)
from infrastructure.repositories.messages.base import (
    BaseChatsRepository,
    BaseMessagesRepository,
)
from infrastructure.repositories.messages.memory import MemoryChatRepository
from infrastructure.repositories.messages.mongo import (
    MongoDBChatsRepository,
    MongoDBMessagesRepository,
)


def _make_chat(title: str = 'room') -> Chat:
    return Chat.create_chat(title=Title(title))


def _make_message(chat_oid: str, text: str = 'hi') -> Message:
    return Message(oid='m1', chat_oid=chat_oid, text=Text(text), created_at='2024-01-01T00:00:00')


# --- MemoryChatRepository -------------------------------------------------


@pytest.mark.asyncio
async def test_memory_add_and_get_chat():
    repo = MemoryChatRepository()
    chat = _make_chat()
    await repo.add_chat(chat)

    fetched = await repo.get_chat_by_oid(chat.oid)

    assert fetched is chat


@pytest.mark.asyncio
async def test_memory_get_missing_chat_returns_none():
    repo = MemoryChatRepository()

    assert await repo.get_chat_by_oid('missing') is None


@pytest.mark.asyncio
async def test_memory_check_exists_by_title():
    repo = MemoryChatRepository()
    await repo.add_chat(_make_chat('alpha'))

    assert await repo.check_chat_exists_by_title('alpha') is True
    assert await repo.check_chat_exists_by_title('beta') is False


@pytest.mark.asyncio
async def test_memory_get_all_chats_paginates():
    repo = MemoryChatRepository()
    for name in ['a', 'b', 'c']:
        await repo.add_chat(_make_chat(name))

    from infrastructure.repositories.filters.messages import GetAllChatsFilters
    page, total = await repo.get_all_chats(filters=GetAllChatsFilters(limit=2, offset=1))

    assert total == 3
    assert [c.title.as_generic_type() for c in page] == ['b', 'c']


@pytest.mark.asyncio
async def test_memory_delete_chat():
    repo = MemoryChatRepository()
    chat = _make_chat()
    await repo.add_chat(chat)

    await repo.delete_chat_by_oid(chat.oid)

    assert await repo.get_chat_by_oid(chat.oid) is None


@pytest.mark.asyncio
async def test_memory_add_telegram_listener_attaches():
    repo = MemoryChatRepository()
    chat = _make_chat()
    await repo.add_chat(chat)

    await repo.add_telegram_listener(chat.oid, 'tg-1')

    listeners = await repo.get_all_chat_listeners(chat.oid)
    assert ChatListener(oid='tg-1') in set(listeners)


@pytest.mark.asyncio
async def test_memory_add_telegram_listener_missing_chat_is_noop():
    repo = MemoryChatRepository()

    await repo.add_telegram_listener('ghost', 'tg-1')

    assert await repo.get_all_chat_listeners('ghost') == []


# --- MongoDBChatsRepository (mocked Motor) --------------------------------


def _mock_mongo_collection(find_one=None, find_cursor=(), count=0):
    collection = MagicMock()

    class _Cursor:
        def __init__(self, docs):
            self._docs = docs

        def skip(self, *a, **k):
            return self

        def limit(self, *a, **k):
            return self

        def __aiter__(self):
            async def _gen():
                for doc in self._docs:
                    yield doc
            return _gen()

    collection.find_one = AsyncMock(return_value=find_one)
    collection.insert_one = AsyncMock()
    collection.delete_one = AsyncMock()
    collection.update_one = AsyncMock()
    collection.count_documents = AsyncMock(return_value=count)
    collection.find = MagicMock(return_value=_Cursor(find_cursor))
    return collection


def _mock_client(collection):
    # client[db_name][coll_name] -> collection
    db = MagicMock()
    db.__getitem__ = MagicMock(return_value=collection)
    client = MagicMock()
    client.__getitem__ = MagicMock(return_value=db)
    return client


@pytest.mark.asyncio
async def test_mongo_get_chat_by_oid_found():
    doc = {'oid': 'c1', 'title': 'hello', 'created_at': 't', 'listeners': ['tg-x']}
    collection = _mock_mongo_collection(find_one=doc)
    repo = MongoDBChatsRepository(
        mongo_db_client=_mock_client(collection),
        mongo_db_db_name='db',
        mongo_db_collection_name='chats',
    )

    chat = await repo.get_chat_by_oid('c1')

    assert chat.oid == 'c1'
    assert chat.title.as_generic_type() == 'hello'
    assert {listener.oid for listener in chat.listeners} == {'tg-x'}


@pytest.mark.asyncio
async def test_mongo_get_chat_by_oid_missing():
    collection = _mock_mongo_collection(find_one=None)
    repo = MongoDBChatsRepository(
        mongo_db_client=_mock_client(collection),
        mongo_db_db_name='db',
        mongo_db_collection_name='chats',
    )

    assert await repo.get_chat_by_oid('nope') is None


@pytest.mark.asyncio
async def test_mongo_check_exists_by_title():
    collection = _mock_mongo_collection(find_one={'title': 'hi'})
    repo = MongoDBChatsRepository(
        mongo_db_client=_mock_client(collection),
        mongo_db_db_name='db',
        mongo_db_collection_name='chats',
    )

    assert await repo.check_chat_exists_by_title('hi') is True


@pytest.mark.asyncio
async def test_mongo_add_chat_inserts_document():
    collection = _mock_mongo_collection()
    repo = MongoDBChatsRepository(
        mongo_db_client=_mock_client(collection),
        mongo_db_db_name='db',
        mongo_db_collection_name='chats',
    )
    session = object()
    chat = _make_chat('new')

    await repo.add_chat(chat, session=session)

    collection.insert_one.assert_awaited_once()
    assert collection.insert_one.call_args.kwargs['session'] is session


@pytest.mark.asyncio
async def test_mongo_get_all_chats():
    doc = {'oid': 'c1', 'title': 'hello', 'created_at': 't', 'listeners': []}
    collection = _mock_mongo_collection(find_cursor=[doc], count=1)
    repo = MongoDBChatsRepository(
        mongo_db_client=_mock_client(collection),
        mongo_db_db_name='db',
        mongo_db_collection_name='chats',
    )

    chats, total = await repo.get_all_chats(GetAllChatsFilters(limit=10, offset=0))

    assert total == 1
    assert chats[0].oid == 'c1'


@pytest.mark.asyncio
async def test_mongo_delete_chat():
    collection = _mock_mongo_collection()
    repo = MongoDBChatsRepository(
        mongo_db_client=_mock_client(collection),
        mongo_db_db_name='db',
        mongo_db_collection_name='chats',
    )

    await repo.delete_chat_by_oid('c1')

    collection.delete_one.assert_awaited_once_with({'oid': 'c1'}, session=None)


@pytest.mark.asyncio
async def test_mongo_add_telegram_listener():
    collection = _mock_mongo_collection()
    repo = MongoDBChatsRepository(
        mongo_db_client=_mock_client(collection),
        mongo_db_db_name='db',
        mongo_db_collection_name='chats',
    )

    await repo.add_telegram_listener('c1', 'tg-9')

    collection.update_one.assert_awaited_once()
    args = collection.update_one.call_args
    assert args.args[0] == {'oid': 'c1'}
    assert args.args[1] == {'$push': {'listeners': 'tg-9'}}


@pytest.mark.asyncio
async def test_mongo_get_all_chat_listeners():
    doc = {'oid': 'c1', 'title': 't', 'created_at': 'x', 'listeners': ['tg-1', 'tg-2']}
    collection = _mock_mongo_collection(find_one=doc)
    repo = MongoDBChatsRepository(
        mongo_db_client=_mock_client(collection),
        mongo_db_db_name='db',
        mongo_db_collection_name='chats',
    )

    listeners = await repo.get_all_chat_listeners('c1')

    assert {listener.oid for listener in listeners} == {'tg-1', 'tg-2'}


# --- MongoDBMessagesRepository (mocked Motor) -----------------------------


@pytest.mark.asyncio
async def test_mongo_add_message():
    collection = _mock_mongo_collection()
    repo = MongoDBMessagesRepository(
        mongo_db_client=_mock_client(collection),
        mongo_db_db_name='db',
        mongo_db_collection_name='messages',
    )
    session = object()
    message = _make_message('c1', 'hello')

    await repo.add_message(message, session=session)

    collection.insert_one.assert_awaited_once()
    assert collection.insert_one.call_args.kwargs['session'] is session


@pytest.mark.asyncio
async def test_mongo_get_messages():
    doc = {'oid': 'm1', 'text': 'hi', 'created_at': 't', 'chat_oid': 'c1'}
    collection = _mock_mongo_collection(find_cursor=[doc], count=1)
    repo = MongoDBMessagesRepository(
        mongo_db_client=_mock_client(collection),
        mongo_db_db_name='db',
        mongo_db_collection_name='messages',
    )

    messages, total = await repo.get_messages('c1', GetMessagesFilters(limit=10, offset=0))

    assert total == 1
    assert messages[0].text.as_generic_type() == 'hi'


# --- Abstract contracts remain abstract ------------------------------------


def test_base_repos_cannot_instantiate():
    with pytest.raises(TypeError):
        BaseChatsRepository()
    with pytest.raises(TypeError):
        BaseMessagesRepository()
