from pydantic import Field
from pydantic_settings import BaseSettings


class Config(BaseSettings):
    mongodb_connection_uri: str = Field(default='mongodb://mongodb:27017', alias='MONGO_DB_CONNECTION_URI')
    mongodb_chat_database: str = Field(default='chat', alias='MONGODB_CHAT_DATABASE')
    mongodb_chat_collection: str = Field(default='chat', alias='MONGODB_CHAT_COLLECTION')
    mongodb_messages_collection: str = Field(default='messages', alias='MONGODB_MESSAGES_COLLECTION')
    mongodb_outbox_collection: str = Field(default='outbox', alias='MONGODB_OUTBOX_COLLECTION')

    outbox_relay_poll_interval: float = Field(default=1.0, alias='OUTBOX_RELAY_POLL_INTERVAL')

    new_chats_event_topic: str = Field(default='new-chats-topic')
    new_message_received_topic: str = Field(default='new-messages')
    kafka_url: str = Field(default='kafka:29092')
    chat_deleted_topic: str = Field(default='chat-deleted-topic')
    new_listener_added_topic: str = Field(default='listener-added-topic')

    # Prometheus server port (the Prometheus container, NOT the app's /metrics endpoint,
    # which is served on the API port).
    prometheus_port: int = Field(default=9090, alias='PROMETHEUS_PORT')
