from functools import lru_cache
from typing import Any

from httpx import AsyncClient
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
    NewMessageReceivedFromBrokerEvent,
)
from infrastructure.cache.base import BaseCacheClient
from infrastructure.cache.cached import (
    CachedChatsRepository,
    CachedMessagesRepository,
)
from infrastructure.cache.keys import relay_lock_key
from infrastructure.cache.memory import MemoryCacheClient
from infrastructure.cache.valkey import ValkeyCacheClient
from infrastructure.integrations.notifications.clients.base import BaseNotificationClient
from infrastructure.integrations.notifications.clients.telegram import TelegramNotificationClient
from infrastructure.locks.base import BaseDistributedLock
from infrastructure.locks.valkey import ValkeyLeaseLock
from infrastructure.message_brokers.base import BaseMessageBroker
from infrastructure.message_brokers.kafka import KafkaMessageBroker
from infrastructure.outbox.base import BaseOutboxRepository
from infrastructure.outbox.mapper import resolve_topic
from infrastructure.outbox.mongo import MongoOutboxRepository
from infrastructure.outbox.relay import (
    OutboxRelay,
    build_relay,
)
from infrastructure.outbox.session import (
    MongoSessionProvider,
    SessionProvider,
)
from infrastructure.presence.base import BasePresenceTracker
from infrastructure.presence.valkey import ValkeyPresenceTracker
from infrastructure.repositories.messages.base import (
    BaseChatsRepository,
    BaseMessagesRepository,
)
from infrastructure.repositories.messages.mongo import (
    MongoDBChatsRepository,
    MongoDBMessagesRepository,
)
from infrastructure.resilience import (
    CircuitBreaker,
    CircuitBreakerChatsRepository,
    CircuitBreakerMessagesRepository,
)
from infrastructure.websockets.managers import (
    BaseConnectionManager,
    ConnectionManager,
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
    GetChatPresenceQuery,
    GetChatPresenceQueryHandler,
    GetMessagesQuery,
    GetMessagesQueryHandler,
)
from settings.config import Config


@lru_cache(1)
def init_container() -> Container:
    return _init_container()


def _valkey_enabled(config: Config) -> bool:
    """Whether any ADR-0008 feature needs a real Valkey connection.

    The client is only built when at least one feature flag is on: a deployment
    with all flags off (the default, and every test) must not even construct a
    Valkey client, so nothing in the process can try to reach a server that
    isn't there.
    """
    return config.cache_enabled or config.presence_enabled or config.relay_lock_enabled


def _init_container() -> Container:
    container = Container()

    container.register(Config, instance=Config(), scope=Scope.singleton)

    config: Config = container.resolve(Config)

    def create_mongodb_client() -> AsyncIOMotorClient[dict[str, Any]]:
        return AsyncIOMotorClient(config.mongodb_connection_uri, serverSelectionTimeoutMS=3000)

    container.register(AsyncIOMotorClient, factory=create_mongodb_client, scope=Scope.singleton)
    client = container.resolve(AsyncIOMotorClient)

    # Mongo calls are guarded by a circuit breaker (infrastructure/resilience.py):
    # while Mongo is down the breaker opens after N consecutive failures and the
    # API fails fast with 503 instead of hanging on serverSelectionTimeoutMS.
    # One shared breaker — both collections hit the same Mongo server. Only the
    # Mongo repos are wrapped; test containers override these registrations with
    # in-memory repos and never see the proxies.
    def create_mongo_circuit_breaker() -> CircuitBreaker:
        return CircuitBreaker(
            name='mongo',
            failure_threshold=config.circuit_breaker_failure_threshold,
            recovery_time=config.circuit_breaker_recovery_time,
        )

    container.register(CircuitBreaker, factory=create_mongo_circuit_breaker, scope=Scope.singleton)

    # --- Valkey (ADR-0008) --------------------------------------------------
    # A third breaker instance, private like the 'kafka' one: it is deliberately
    # NOT registered under the CircuitBreaker type (that key is the mongo one),
    # it just rides along inside the cache client.
    def create_valkey_circuit_breaker() -> CircuitBreaker:
        return CircuitBreaker(
            name='valkey',
            failure_threshold=config.circuit_breaker_failure_threshold,
            recovery_time=config.circuit_breaker_recovery_time,
        )

    def create_cache_client() -> BaseCacheClient:
        # Registered even with the flags off (Chunk 1): the presence tracker,
        # the relay lease and the command handlers all resolve the client, so
        # they never branch on None. With no Valkey configured the in-memory
        # client stands in and the features are simply inert.
        if _valkey_enabled(config):
            return ValkeyCacheClient(
                url=config.valkey_url,
                breaker=create_valkey_circuit_breaker(),
            )
        return MemoryCacheClient()

    container.register(BaseCacheClient, factory=create_cache_client, scope=Scope.singleton)

    def create_presence_tracker() -> BasePresenceTracker:
        return ValkeyPresenceTracker(
            cache=container.resolve(BaseCacheClient),
            ttl_seconds=config.presence_ttl_seconds,
        )

    container.register(BasePresenceTracker, factory=create_presence_tracker, scope=Scope.singleton)

    def create_relay_lease() -> BaseDistributedLock | None:
        # None when the flag is off — build_relay's `lease=None` is exactly the
        # pre-ADR-0008 behaviour, byte for byte.
        if not config.relay_lock_enabled:
            return None
        return ValkeyLeaseLock(
            cache=container.resolve(BaseCacheClient),
            key=relay_lock_key(),
            ttl_seconds=config.relay_lock_ttl_seconds,
        )

    container.register(BaseDistributedLock, factory=create_relay_lease, scope=Scope.singleton)

    def init_chats_mongodb_repository() -> BaseChatsRepository:
        # Cache proxy INSIDE the breaker proxy (ADR-0008 §4): the 'mongo'
        # breaker must keep measuring Mongo only, so Valkey errors are swallowed
        # below the breaker and never counted as Mongo failures.
        repository: BaseChatsRepository = MongoDBChatsRepository(
            mongo_db_client=client,
            mongo_db_db_name=config.mongodb_chat_database,
            mongo_db_collection_name=config.mongodb_chat_collection,
        )
        if config.cache_enabled:
            repository = CachedChatsRepository(
                inner=repository,
                cache=container.resolve(BaseCacheClient),
                ttl_seconds=config.cache_ttl_seconds,
            )
        return CircuitBreakerChatsRepository(
            inner=repository,
            breaker=container.resolve(CircuitBreaker),
        )

    def init_messages_mongodb_repository() -> BaseMessagesRepository:
        repository: BaseMessagesRepository = MongoDBMessagesRepository(
            mongo_db_client=client,
            mongo_db_db_name=config.mongodb_chat_database,
            mongo_db_collection_name=config.mongodb_messages_collection,
        )
        if config.cache_enabled:
            repository = CachedMessagesRepository(
                inner=repository,
                cache=container.resolve(BaseCacheClient),
                ttl_seconds=config.cache_ttl_seconds,
            )
        return CircuitBreakerMessagesRepository(
            inner=repository,
            breaker=container.resolve(CircuitBreaker),
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
    container.register(GetChatPresenceQueryHandler)

    def create_message_broker() -> BaseMessageBroker:
        return KafkaMessageBroker(
            bootstrap_servers=config.kafka_url,
            group_id='chat',
            metadata_max_age_ms=30000,
        )

    container.register(BaseMessageBroker, factory=create_message_broker, scope=Scope.singleton)

    def create_connection_manager() -> BaseConnectionManager:
        # ADR-0008: presence_tracker=None keeps the manager exactly as it was
        # before the ADR — no heartbeat tasks, no presence writes.
        tracker: BasePresenceTracker | None = None
        if config.presence_enabled:
            tracker = container.resolve(BasePresenceTracker)
        return ConnectionManager(presence_tracker=tracker)

    container.register(BaseConnectionManager, factory=create_connection_manager, scope=Scope.singleton)

    def create_outbox_relay() -> OutboxRelay:
        # O-2: the relay gets its own 'kafka' breaker — separate instance from
        # the 'mongo' breaker (deliberately NOT registered under the
        # CircuitBreaker type; that key belongs to the mongo one), same config
        # knobs (threshold / recovery time).
        kafka_breaker = CircuitBreaker(
            name='kafka',
            failure_threshold=config.circuit_breaker_failure_threshold,
            recovery_time=config.circuit_breaker_recovery_time,
        )
        return build_relay(
            outbox_repository=container.resolve(BaseOutboxRepository),
            message_broker=container.resolve(BaseMessageBroker),
            config=config,
            circuit_breaker=kafka_breaker,
            # ADR-0008: None unless RELAY_LOCK_ENABLED, in which case every
            # replica races for the same Valkey lease and only the holder polls.
            lease=container.resolve(BaseDistributedLock),
        )

    container.register(OutboxRelay, factory=create_outbox_relay, scope=Scope.singleton)

    # Telegram notifications (see docs/adr/issue4.md): the client is only wired
    # when a bot token is configured. When it is absent, BaseNotificationClient
    # stays unregistered and ListenerAddedEventHandler skips notifications.
    if config.telegram_bot_token:
        def init_telegram_notification_client() -> TelegramNotificationClient:
            return TelegramNotificationClient(
                bot_token=config.telegram_bot_token,
                chat_id=config.telegram_chat_id,
                http_client=AsyncClient(),
                send_url=config.telegram_api_url,
            )

        container.register(
            BaseNotificationClient,
            factory=init_telegram_notification_client,
            scope=Scope.singleton,
        )

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
            cache=container.resolve(BaseCacheClient),
        )

    def init_delete_chat_command_handler() -> DeleteChatCommandHandler:
        return DeleteChatCommandHandler(
            _mediator=mediator,
            chats_repository=container.resolve(BaseChatsRepository),
            outbox_repository=container.resolve(BaseOutboxRepository),
            session_provider=container.resolve(SessionProvider),
            cache=container.resolve(BaseCacheClient),
        )

    def init_add_telegram_listener_command_handler() -> AddTelegramListenerCommandHandler:
        return AddTelegramListenerCommandHandler(
            _mediator=mediator,
            chats_repository=container.resolve(BaseChatsRepository),
            outbox_repository=container.resolve(BaseOutboxRepository),
            session_provider=container.resolve(SessionProvider),
            # ADR-0008 §9 deviation 3: invalidates the cached chat detail entry
            # (it carries the listener set). Always resolved — with the feature
            # flag off it is the in-memory client, like the other handlers.
            cache=container.resolve(BaseCacheClient),
        )

    # NOTE: handlers are registered via factories (deferred), NOT instances:
    # punq caches instance-registrations in _singletons immediately, which would
    # defeat init_dummy_container()'s rebuild — the test dummy container overrides
    # the repositories and re-runs build_mediator, and each factory must resolve
    # the (overridden) dependencies at that later point in time.
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

    # Kafka delivery happens via the outbox relay, so these handlers only keep
    # in-process side effects; broker_topic is carried for the EventHandler
    # contract (see logic/events/messages.py).
    notification_client: BaseNotificationClient | None = None
    if config.telegram_bot_token:
        notification_client = container.resolve(BaseNotificationClient)

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
        notification_client=notification_client,
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
    mediator.register_query(
        GetChatPresenceQuery,
        container.resolve(GetChatPresenceQueryHandler),
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
