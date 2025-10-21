from functools import lru_cache

from infrastructure.message_brokers.base import BaseMessageBroker
from infrastructure.message_brokers.kafka import KafkaMessageBroker
from infrastructure.outbox.base import BaseOutboxRepository
from infrastructure.outbox.mapper import resolve_topic
from infrastructure.outbox.mongo import MongoOutboxRepository
from infrastructure.outbox.relay import (
    build_relay,
    OutboxRelay,
)
from infrastructure.outbox.session import (
    MongoSessionProvider,
    SessionProvider,
)
from infrastructure.repositories.messages.base import (
    BaseChatsRepository,
    BaseMessagesRepository,
)
from infrastructure.repositories.messages.mongo import (
    MongoDBChatsRepository,
    MongoDBMessagesRepository,
)
from infrastructure.websockets.managers import (
    BaseConnectionManager,
    ConnectionManager,
)
from motor.motor_asyncio import AsyncIOMotorClient
from punq import (
    Container,
    Scope,
)

from domain.events.messages import (
    ChatDeletedEvent,
    ListenerAddedEvent,
    NewChatCreatedEvent,
    NewMessageReceivedEvent,
)
from logic.commands.messages import (
    AddTelegramListenerCommand,
    AddTelegramListenerCommandHandler,
    CreateChatCommand,
    CreateChatCommandHandler,
    CreateMessageCommand,
    CreateMessageCommandHandler,
    DeleteChatCommand,
    DeleteChatCommandHandler,
)
from logic.events.messages import (
    ChatDeletedEventHandler,
    ListenerAddedEventHandler,
    NewChatCreatedEventHandler,
    NewMessageReceivedEventHandler,
    NewMessageReceivedFromBrokerEvent,
    NewMessageReceivedFromBrokerEventHandler,
)
from logic.mediator.base import Mediator
from logic.mediator.event import EventMediator
from logic.queries.messages import (
    GetAllChatsListenersQuery,
    GetAllChatsListenersQueryHandler,
    GetAllChatsQuery,
    GetAllChatsQueryHandler,
    GetChatDetailQuery,
    GetChatDetailQueryHandler,
    GetMessagesQuery,
    GetMessagesQueryHandler,
)
from settings.config import Config


@lru_cache(1)
def init_container():
    return _init_container()


def _init_container() -> Container:
    container = Container()

    container.register(Config, instance=Config(), scope=Scope.singleton)

    config: Config = container.resolve(Config)

    def create_mongodb_client():
        return AsyncIOMotorClient(config.mongodb_connection_uri, serverSelectionTimeoutMS=3000)

    container.register(AsyncIOMotorClient, factory=create_mongodb_client, scope=Scope.singleton)
    client = container.resolve(AsyncIOMotorClient)

    def init_chats_mongodb_repository() -> BaseChatsRepository:
        return MongoDBChatsRepository(
            mongo_db_client=client,
            mongo_db_db_name=config.mongodb_chat_database,
            mongo_db_collection_name=config.mongodb_chat_collection,
        )

    def init_messages_mongodb_repository() -> BaseMessagesRepository:
        return MongoDBMessagesRepository(
            mongo_db_client=client,
            mongo_db_db_name=config.mongodb_chat_database,
            mongo_db_collection_name=config.mongodb_messages_collection,
        )

    container.register(BaseChatsRepository, factory=init_chats_mongodb_repository, scope=Scope.singleton)
    container.register(BaseMessagesRepository, factory=init_messages_mongodb_repository, scope=Scope.singleton)

    def init_outbox_mongodb_repository() -> BaseOutboxRepository:
        return MongoOutboxRepository(
            collection=client[config.mongodb_chat_database][config.mongodb_outbox_collection],
            _topic_resolver=resolve_topic,
        )

    container.register(BaseOutboxRepository, factory=init_outbox_mongodb_repository, scope=Scope.singleton)
    container.register(SessionProvider, instance=MongoSessionProvider(client=client), scope=Scope.singleton)

    # Query handlers
    container.register(GetChatDetailQueryHandler)
    container.register(GetMessagesQueryHandler)
    container.register(GetAllChatsQueryHandler)
    container.register(GetAllChatsListenersQueryHandler)

    def create_message_broker() -> BaseMessageBroker:
        return KafkaMessageBroker(
            bootstrap_servers=config.kafka_url,
            group_id='chat',
            metadata_max_age_ms=30000,
        )

    container.register(BaseMessageBroker, factory=create_message_broker, scope=Scope.singleton)
    container.register(BaseConnectionManager, instance=ConnectionManager(), scope=Scope.singleton)

    def create_outbox_relay() -> OutboxRelay:
        return build_relay(
            outbox_repository=container.resolve(BaseOutboxRepository),
            message_broker=container.resolve(BaseMessageBroker),
            config=config,
        )

    container.register(OutboxRelay, factory=create_outbox_relay, scope=Scope.singleton)

    # Build + wire the mediator from THIS container. Kept as a reusable function
    # so the test dummy container can rebuild the mediator against its overridden
    # (in-memory) repositories. Command handlers receive this same mediator
    # instance, breaking the Mediator -> handler -> Mediator resolve cycle that
    # punq 0.7.0 cannot detect.
    mediator = build_mediator(container, config)
    container.register(Mediator, instance=mediator, scope=Scope.singleton)
    container.register(EventMediator, instance=mediator, scope=Scope.singleton)

    return container


def build_mediator(container: Container, config: Config) -> Mediator:
    # NOTE: command handlers are registered via factories (not class-based)
    # because punq 0.8.0's getfullargspec introspection recurses on Python 3.13
    # for dataclasses whose fields are annotated with a TypeVar (e.g.
    # _mediator: EventMediator). Each factory resolves its dependencies from the
    # container passed in, so rebuilding build_mediator() against an overridden
    # container yields handlers wired to the overridden repositories.
    mediator = Mediator()

    def init_create_chat_command_handler() -> CreateChatCommandHandler:
        return CreateChatCommandHandler(
            _mediator=mediator,
            chats_repository=container.resolve(BaseChatsRepository),
            outbox_repository=container.resolve(BaseOutboxRepository),
            session_provider=container.resolve(SessionProvider),
        )

    def init_create_message_command_handler() -> CreateMessageCommandHandler:
        return CreateMessageCommandHandler(
            _mediator=mediator,
            messages_repository=container.resolve(BaseMessagesRepository),
            chats_repository=container.resolve(BaseChatsRepository),
            outbox_repository=container.resolve(BaseOutboxRepository),
            session_provider=container.resolve(SessionProvider),
        )

    def init_delete_chat_command_handler() -> DeleteChatCommandHandler:
        return DeleteChatCommandHandler(
            _mediator=mediator,
            chats_repository=container.resolve(BaseChatsRepository),
            outbox_repository=container.resolve(BaseOutboxRepository),
            session_provider=container.resolve(SessionProvider),
        )

    def init_add_telegram_listener_command_handler() -> AddTelegramListenerCommandHandler:
        return AddTelegramListenerCommandHandler(
            _mediator=mediator,
            chats_repository=container.resolve(BaseChatsRepository),
            outbox_repository=container.resolve(BaseOutboxRepository),
            session_provider=container.resolve(SessionProvider),
        )

    container.register(CreateChatCommandHandler, factory=init_create_chat_command_handler)
    container.register(CreateMessageCommandHandler, factory=init_create_message_command_handler)
    container.register(DeleteChatCommandHandler, factory=init_delete_chat_command_handler)
    container.register(
        AddTelegramListenerCommandHandler,
        factory=init_add_telegram_listener_command_handler,
    )

    create_chat_handler = container.resolve(CreateChatCommandHandler)
    create_message_handler = container.resolve(CreateMessageCommandHandler)
    delete_chat_handler = container.resolve(DeleteChatCommandHandler)
    add_telegram_listener_handler = container.resolve(AddTelegramListenerCommandHandler)
    new_chat_created_event_handler = NewChatCreatedEventHandler(
        message_broker=container.resolve(BaseMessageBroker),
        connection_manager=container.resolve(BaseConnectionManager),
        broker_topic=config.new_chats_event_topic,
    )
    new_message_received_handler = NewMessageReceivedEventHandler(
        message_broker=container.resolve(BaseMessageBroker),
        connection_manager=container.resolve(BaseConnectionManager),
        broker_topic=config.new_message_received_topic,
    )
    new_message_received_from_broker_event_handler = NewMessageReceivedFromBrokerEventHandler(
        message_broker=container.resolve(BaseMessageBroker),
        connection_manager=container.resolve(BaseConnectionManager),
        broker_topic=config.new_message_received_topic,
    )
    chat_deleted_event_handler = ChatDeletedEventHandler(
        message_broker=container.resolve(BaseMessageBroker),
        connection_manager=container.resolve(BaseConnectionManager),
        broker_topic=config.chat_deleted_topic,
    )
    new_listener_added_handler = ListenerAddedEventHandler(
        message_broker=container.resolve(BaseMessageBroker),
        connection_manager=container.resolve(BaseConnectionManager),
        broker_topic=config.new_listener_added_topic,
    )

    mediator.register_event(
        NewChatCreatedEvent,
        [new_chat_created_event_handler],
    )
    mediator.register_event(
        NewMessageReceivedEvent,
        [new_message_received_handler],
    )
    mediator.register_event(
        NewMessageReceivedFromBrokerEvent,
        [new_message_received_from_broker_event_handler],
    )
    mediator.register_event(
        ChatDeletedEvent,
        [chat_deleted_event_handler],
    )
    mediator.register_command(
        CreateChatCommand,
        [create_chat_handler],
    )
    mediator.register_command(
        CreateMessageCommand,
        [create_message_handler],
    )
    mediator.register_command(
        DeleteChatCommand,
        [delete_chat_handler],
    )
    mediator.register_query(
        GetChatDetailQuery,
        container.resolve(GetChatDetailQueryHandler),
    )
    mediator.register_query(
        GetAllChatsListenersQuery,
        container.resolve(GetAllChatsListenersQueryHandler),
    )
    mediator.register_query(
        GetMessagesQuery,
        container.resolve(GetMessagesQueryHandler),
    )
    mediator.register_query(
        GetAllChatsQuery,
        container.resolve(GetAllChatsQueryHandler),
    )
    mediator.register_event(
        ListenerAddedEvent,
        [new_listener_added_handler],
    )
    mediator.register_command(
        AddTelegramListenerCommand,
        [add_telegram_listener_handler],
    )

    return mediator
