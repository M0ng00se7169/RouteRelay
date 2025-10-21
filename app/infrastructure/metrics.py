from prometheus_client import (
    Counter,
    Gauge,
)


# Outbox relay metrics.
outbox_published_total = Counter(
	'outbox_published_total',
	'Number of outbox rows successfully published to Kafka by the relay.',
)

outbox_publish_errors_total = Counter(
	'outbox_publish_errors_total',
	'Number of outbox rows that failed to publish to Kafka (will be retried).',
)

outbox_pending = Gauge(
	'outbox_pending',
	'Number of unsent outbox rows currently awaiting delivery.',
)

kafka_messages_sent_total = Counter(
	'kafka_messages_sent_total',
	'Number of messages handed to the Kafka producer by the relay.',
)
