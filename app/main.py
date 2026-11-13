from contextlib import asynccontextmanager

from arq import create_pool
from arq.connections import RedisSettings
from fastapi import FastAPI

from app.config import get_settings
from app.db import create_tables, make_engine, make_session_factory
from app.queue import ArqQueue
from app.dev import router as dev_router
from app.webhook import router


def create_app(session_factory=None, queue=None, dev_mode: bool | None = None) -> FastAPI:
    """Dependencies can be injected (tests); otherwise they are built on startup."""

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
            pool = await create_pool(RedisSettings.from_dsn(settings.redis_url))
            app.state.queue = ArqQueue(pool)
        yield
        if pool:
            await pool.close()
        if engine:
            await engine.dispose()

    app = FastAPI(title="Stripe provisioning pipeline", lifespan=lifespan)
    app.state.dev_mode = get_settings().dev_mode if dev_mode is None else dev_mode
    if session_factory is not None:
        app.state.session_factory = session_factory
    if queue is not None:
        app.state.queue = queue
    app.include_router(router)
    if app.state.dev_mode:
        app.include_router(dev_router)

    @app.get("/health")
    async def health():
        return {"status": "ok"}

    return app


app = create_app()
