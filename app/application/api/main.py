import asyncio
from contextlib import asynccontextmanager

from application.api.lifespan import (
	close_message_broker,
	init_message_broker,
	start_relay,
	stop_relay,
)
from application.api.messages.handlers import router as message_router
from application.api.messages.websockets.messages import router as message_ws_router
from fastapi import FastAPI
from infrastructure.logging_config import configure_json_logging
from prometheus_fastapi_instrumentator import PrometheusFastApiInstrumentator


@asynccontextmanager
async def lifespan(app: FastAPI):
	await init_message_broker(app)
	relay_task: asyncio.Task = await start_relay(app)
	yield
	await stop_relay(relay_task)
	await close_message_broker(app)


def create_app() -> FastAPI:
	app = FastAPI(
		title='Simple Kafka Chat',
		docs_url='/api/docs',
		description='A simple kafka + ddd example.',
		debug=True,
		lifespan=lifespan,
	)
	app.include_router(message_router, prefix='/chat')
	app.include_router(message_ws_router, prefix='/chats')

	# Emit JSON-structured log lines (level/logger/message) so Loki receives
	# clean, queryable labels. Must run before the instrumentator so its logs are
	# also captured in the structured format.
	configure_json_logging()

	# Expose Prometheus metrics on the API port (8000). The /metrics endpoint is
	# scraped by the Prometheus server running on its own port (PROMETHEUS_PORT).
	PrometheusFastApiInstrumentator().instrument(app).expose(app, endpoint='/metrics')

	return app
