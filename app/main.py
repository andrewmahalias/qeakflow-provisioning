from contextlib import asynccontextmanager

from arq import create_pool
from arq.connections import RedisSettings
from fastapi import FastAPI

from app.config import get_settings
from app.db import create_tables, make_engine, make_session_factory
from app.provisioning.queue import ArqQueue
from app.routers.dev import router as dev_router
from app.routers.webhook import router
from app.schemas import HealthResponse


def create_app(session_factory=None, queue=None, dev_mode: bool | None = None) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        settings = get_settings()
        engine = None
        pool = None

        if session_factory is None:
            engine = make_engine(settings.database_url)
            await create_tables(engine)
            app.state.session_factory = make_session_factory(engine)

        if queue is None:
            pool = await create_pool(
                RedisSettings.from_dsn(settings.redis_url)
            )
            app.state.queue = ArqQueue(pool)

        yield

        if pool:
            await pool.close()
        if engine:
            await engine.dispose()

    app = FastAPI(
        title="Stripe provisioning pipeline",
        lifespan=lifespan,
    )

    settings = get_settings()
    is_dev_mode = settings.dev_mode if dev_mode is None else dev_mode
    app.state.dev_mode = is_dev_mode

    if session_factory is not None:
        app.state.session_factory = session_factory

    if queue is not None:
        app.state.queue = queue

    app.include_router(router)

    if is_dev_mode:
        app.include_router(dev_router, prefix="/dev")

    @app.get("/health", response_model=HealthResponse)
    async def health():
        return {"status": "ok"}

    return app


app = create_app()
