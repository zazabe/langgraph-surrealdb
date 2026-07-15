from __future__ import annotations

from collections.abc import AsyncGenerator, Generator
from contextlib import asynccontextmanager, contextmanager

from surrealdb import AsyncSurreal, Surreal
from surrealdb.types import Value

from langgraph_surrealdb.database.interface import (
    SurrealAsyncConnection,
    SurrealConnection,
)
from langgraph_surrealdb.database.settings import SurrealSaverSettings


@contextmanager
def surreal_client(
    settings: SurrealSaverSettings,
) -> Generator[SurrealConnection, None, None]:
    with Surreal(settings.db.url) as db:
        if settings.db.auth.mode == "token":
            db.authenticate(settings.db.auth.token)
        else:
            db.signin(settings.db.auth.payload())
        db.use(settings.db.namespace, settings.db.database)
        yield SurrealConnection(db)


@asynccontextmanager
async def async_surreal_client(
    settings: SurrealSaverSettings,
) -> AsyncGenerator[SurrealAsyncConnection, None]:
    async with AsyncSurreal(settings.db.url) as db:
        if settings.db.auth.mode == "token":
            await db.authenticate(settings.db.auth.token)
        else:
            await db.signin(settings.db.auth.payload())
        await db.use(settings.db.namespace, settings.db.database)
        yield SurrealAsyncConnection(db)


def select_one_result(result: Value) -> dict[str, Value]:
    if isinstance(result, dict):
        return {str(key): value for key, value in result.items()}
    raise ValueError(f"Expected dict, got {type(result)}")


def select_result(result: Value) -> list[dict[str, Value]]:
    if isinstance(result, list):
        return [select_one_result(row) for row in result]
    return []
