import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import (
	FastAPI,
	Request,
	status,
)
from fastapi.responses import JSONResponse

# Direct import from the defining module: prometheus_fastapi_instrumentator's
# __init__ re-exports it implicitly, which strict mypy (no_implicit_reexport)
# rejects.
from prometheus_fastapi_instrumentator.instrumentation import (
	PrometheusFastApiInstrumentator,
)

from application.api.auth.handlers import router as auth_router
from application.api.lifespan import (
	close_cache_client,
	close_message_broker,
	init_message_broker,
	start_kafka_consumer,
	start_relay,
	stop_kafka_consumer,
	stop_relay,
)
from application.api.messages.handlers import router as message_router
from application.api.messages.websockets.messages import router as message_ws_router
from application.api.ops.handlers import router as ops_router
from infrastructure.logging_config import configure_json_logging
from infrastructure.metrics import (
	application_info,
	safe_set,
)
from infrastructure.resilience import CircuitOpenError
from settings.config import Config


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
	await init_message_broker(app)
	relay_task: asyncio.Task[None] = await start_relay(app)
	kafka_consumer_task: asyncio.Task[None] = await start_kafka_consumer(app)
	yield
	await stop_kafka_consumer(kafka_consumer_task, app)
	# Relay first: it releases the outbox leader lock on shutdown, which needs
	# the cache client still open (ADR-0008).
	await stop_relay(relay_task)
	await close_message_broker(app)
	await close_cache_client(app)


def create_app() -> FastAPI:
	app = FastAPI(
		title='Simple Kafka Chat',
		docs_url='/api/docs',
		description='A simple kafka + ddd example.',
		debug=True,
		lifespan=lifespan,
	)
	app.include_router(auth_router, prefix='/auth')
	app.include_router(message_router, prefix='/chat')
	app.include_router(message_ws_router, prefix='/chats')
	# Alertmanager webhook sink (ADR-0007 Chunk 3): alerts land in the JSON
	# logs (Loki-visible) until a paging transport is chosen.
	app.include_router(ops_router)

	# Build-info gauge (ADR-0006, Chunk 6.2): application_info{version=...} = 1,
	# read from APP_VERSION (defaults to 0.1.0) for deployment tracking.
	config = Config()
	safe_set(application_info.labels(version=config.app_version), 1)

	# Circuit breaker open (infrastructure/resilience.py) -> fail fast with 503
	# and a Retry-After hint instead of the endpoints' generic 400. Registered at
	# app level because CircuitOpenError is not an ApplicationException: the
	# endpoints' broad ``except ApplicationException`` must not swallow it.
	@app.exception_handler(CircuitOpenError)
	async def circuit_open_handler(request: Request, exc: CircuitOpenError) -> JSONResponse:
		return JSONResponse(
			status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
			content={'detail': {'error': exc.message}},
			headers={'Retry-After': str(int(config.circuit_breaker_recovery_time))},
		)

	# Emit JSON-structured log lines (level/logger/message) so Loki receives
	# clean, queryable labels. Must run before the instrumentator so its logs are
	# also captured in the structured format.
	configure_json_logging()

	# Expose Prometheus metrics on the API port (8000). The /metrics endpoint is
	# scraped by the Prometheus server running on its own port (PROMETHEUS_PORT).
	PrometheusFastApiInstrumentator().instrument(app).expose(app, endpoint='/metrics')

	return app
