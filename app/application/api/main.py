from contextlib import asynccontextmanager

from application.api.lifespan import (
    start_kafka,
    stop_kafka,
)
from application.api.messages.handlers import router as message_router
from fastapi import FastAPI


@asynccontextmanager
async def lifespan(app: FastAPI):
    await start_kafka()
    yield
    await stop_kafka()


def create_app() -> FastAPI:
    app = FastAPI(
        title='Simple Kafka Chat',
        docs_url='/api/docs',
        description='A simple kafka + ddd example.',
        debug=True,
        lifespan=lifespan,
    )
    app.include_router(message_router, prefix='/chat')

    return app
