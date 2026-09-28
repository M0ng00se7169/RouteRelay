from dataclasses import dataclass

from motor.core import AgnosticClient, AgnosticCollection

from domain.entities.messages import (
    Chat,
    ChatListener,
    Message,
)
from infrastructure.repositories.filters.messages import (
    GetAllChatsFilters,
    GetMessagesFilters,
)
from infrastructure.repositories.messages.base import (
    BaseChatsRepository,
    BaseMessagesRepository,
)
from infrastructure.repositories.messages.converters import (
    convert_chat_document_to_entity,
    convert_chat_entity_to_document,
    convert_chat_listener_document_to_entity,
    convert_message_document_to_entity,
    convert_message_entity_to_document,
)


@dataclass
class BaseMongoDBRepository:
    mongo_db_client: AgnosticClient
    mongo_db_db_name: str
    mongo_db_collection_name: str

    @property
    def _collection(self) -> AgnosticCollection:
        return self.mongo_db_client[self.mongo_db_db_name][self.mongo_db_collection_name]


@dataclass
class MongoDBChatsRepository(BaseChatsRepository, BaseMongoDBRepository):

    async def get_chat_by_oid(self, oid: str) -> Chat | None:
        chat_document = await self._collection.find_one(filter={'oid': oid})

        if not chat_document:
            return None

        return convert_chat_document_to_entity(chat_document)

    async def check_chat_exists_by_title(self, title: str) -> bool:
        return bool(await self._collection.find_one(filter={'title': title}))

    async def add_chat(self, chat: Chat, session=None) -> None:
        await self._collection.insert_one(convert_chat_entity_to_document(chat), session=session)

    async def get_all_chats(self, filters: GetAllChatsFilters) -> tuple[list[Chat], int]:
        cursor = self._collection.find().skip(filters.offset).limit(filters.limit)

        chats = [
            convert_chat_document_to_entity(chat_document=chat_document)
            async for chat_document in cursor
        ]

        count = await self._collection.count_documents({})

        return chats, count

    async def delete_chat_by_oid(self, chat_oid: str, session=None) -> None:
        await self._collection.delete_one({'oid': chat_oid}, session=session)

    async def add_telegram_listener(self, chat_oid: str, telegram_chat_id: str, session=None) -> None:
        await self._collection.update_one(
            {'oid': chat_oid},
            {'$push': {'listeners': telegram_chat_id}},
            session=session,
        )

    async def get_all_chat_listeners(self, chat_oid: str) -> list[ChatListener]:
        chat = await self.get_chat_by_oid(oid=chat_oid)

        if chat is None:
            return []

        return [convert_chat_listener_document_to_entity(listener_id=listener.oid) for listener in chat.listeners]


@dataclass
class MongoDBMessagesRepository(BaseMessagesRepository, BaseMongoDBRepository):
    async def add_message(self, message: Message, session=None) -> None:
        await self._collection.insert_one(
            document=convert_message_entity_to_document(message),
            session=session,
        )

    async def get_messages(self, chat_oid: str, filters: GetMessagesFilters) -> tuple[list[Message], int]:
        find = {'chat_oid': chat_oid}
        cursor = self._collection.find(find).skip(filters.offset).limit(filters.limit)

        messages = [
            convert_message_document_to_entity(message_document=message_document)
            async for message_document in cursor
        ]
        count = await self._collection.count_documents(filter=find)

        return messages, count
