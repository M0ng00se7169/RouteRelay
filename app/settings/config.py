from pydantic import Field
from pydantic_settings import BaseSettings


class Config(BaseSettings):
    mongodb_connection_uri: str = Field(default='mongodb://mongodb:27017', alias='MONGO_DB_CONNECTION_URI')
    mongodb_chat_database: str = Field(default='chat', alias='MONGODB_CHAT_DATABASE')
    mongodb_chat_collection: str = Field(default='chat', alias='MONGODB_CHAT_COLLECTION')
    mongodb_messages_collection: str = Field(default='messages', alias='MONGODB_MESSAGES_COLLECTION')
    mongodb_outbox_collection: str = Field(default='outbox', alias='MONGODB_OUTBOX_COLLECTION')

    outbox_relay_poll_interval: float = Field(default=1.0, alias='OUTBOX_RELAY_POLL_INTERVAL')

    # Circuit breaker guarding the Mongo persistence path
    # (infrastructure/resilience.py): after this many consecutive failures the
    # breaker opens and calls fail fast with 503 until recovery_time elapses.
    circuit_breaker_failure_threshold: int = Field(default=5, alias='CIRCUIT_BREAKER_FAILURE_THRESHOLD')
    circuit_breaker_recovery_time: float = Field(default=30.0, alias='CIRCUIT_BREAKER_RECOVERY_TIME')

    # JWT signing secret for the HS256 serializer (infrastructure/serializers/jwt.py).
    jwt_secret: str = Field(default='supersecret', alias='JWT_SECRET')

    # OAuth2 password flow: single demo user. Credentials are compared in
    # constant time (settings/security.py).
    auth_username: str = Field(default='admin', alias='AUTH_USERNAME')
    auth_password: str = Field(default='admin', alias='AUTH_PASSWORD')

    new_chats_event_topic: str = Field(default='new-chats-topic')
    new_message_received_topic: str = Field(default='new-messages')
    kafka_url: str = Field(default='kafka:29092')

    # Kafka consumer loop reconnect (O-1, application/api/lifespan.py): after the
    # stream dies or exits, the loop retries with exponential backoff capped at
    # the max below. Backoff resets to the initial delay on every received message.
    kafka_consumer_backoff_initial: float = Field(default=1.0, alias='KAFKA_CONSUMER_BACKOFF_INITIAL')
    kafka_consumer_backoff_max: float = Field(default=30.0, alias='KAFKA_CONSUMER_BACKOFF_MAX')
    chat_deleted_topic: str = Field(default='chat-deleted-topic')
    new_listener_added_topic: str = Field(default='listener-added-topic')

    # Telegram notifications (see docs/adr/issue4.md). Notifications are
    # disabled when the bot token is empty.
    telegram_bot_token: str = Field(default='', alias='TELEGRAM_BOT_TOKEN')
    telegram_chat_id: str = Field(default='', alias='TELEGRAM_CHAT_ID')
    telegram_api_url: str = Field(default='https://api.telegram.org', alias='TELEGRAM_API_URL')

    # Prometheus server port (the Prometheus container, NOT the app's /metrics endpoint,
    # which is served on the API port).
    prometheus_port: int = Field(default=9090, alias='PROMETHEUS_PORT')

    # Deployed build version, reported on the application_info metric
    # (ADR-0006, Chunk 6.2). Override via env (e.g. the git tag) per deployment.
    app_version: str = Field(default='0.1.0', alias='APP_VERSION')
