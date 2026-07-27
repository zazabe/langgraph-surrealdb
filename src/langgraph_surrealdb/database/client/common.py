from __future__ import annotations

from collections.abc import AsyncGenerator, Generator
from contextlib import asynccontextmanager, contextmanager

from surrealdb import AsyncSurreal, Surreal
from surrealdb.types import Value

from langgraph_surrealdb.database.client.interface import (
    SurrealAsyncConnection,
    SurrealConnection,
)
from langgraph_surrealdb.database.settings import (
    RecordAuth,
    RootAuth,
    SurrealSaverSettings,
    TokenAuth,
)


@contextmanager
def surreal_client(
    settings: SurrealSaverSettings,
) -> Generator[SurrealConnection, None, None]:
    with Surreal(settings.db.url) as db:
        match settings.db.auth:
            case TokenAuth():
                db.authenticate(settings.db.auth.token)
            case RecordAuth():
                db.signin(
                    {
                        "database": settings.db.database,
                        "namespace": settings.db.namespace,
                        "variables": {
                            "username": settings.db.auth.username,
                            "password": settings.db.auth.password,
                        },
                        "access": settings.db.auth.access,
                    }
                )
            case RootAuth():
                db.signin(
                    {
                        "username": settings.db.auth.username,
                        "password": settings.db.auth.password,
                    }
                )
        db.use(settings.db.namespace, settings.db.database)
        yield SurrealConnection(db)


@asynccontextmanager
async def async_surreal_client(
    settings: SurrealSaverSettings,
) -> AsyncGenerator[SurrealAsyncConnection, None]:
    async with AsyncSurreal(settings.db.url) as db:
        match settings.db.auth:
            case TokenAuth():
                await db.authenticate(settings.db.auth.token)
            case RecordAuth():
                await db.signin(
                    {
                        "database": settings.db.database,
                        "namespace": settings.db.namespace,
                        "variables": {
                            "username": settings.db.auth.username,
                            "password": settings.db.auth.password,
                        },
                        "access": settings.db.auth.access,
                    }
                )
            case RootAuth():
                await db.signin(
                    {
                        "username": settings.db.auth.username,
                        "password": settings.db.auth.password,
                    }
                )
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
