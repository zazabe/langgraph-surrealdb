from __future__ import annotations

import re
from collections.abc import AsyncGenerator, Generator
from contextlib import asynccontextmanager, contextmanager

from surrealdb import AsyncSurreal, Surreal
from surrealdb.types import Value

from langgraph_surrealdb.database.client.interface import (
    SurrealAsyncConnection,
    SurrealConnection,
)
from langgraph_surrealdb.settings import (
    RecordAuth,
    RootAuth,
    SurrealDatabaseSettings,
    TokenAuth,
)


@contextmanager
def surreal_client(
    settings: SurrealDatabaseSettings,
) -> Generator[SurrealConnection, None, None]:
    with Surreal(settings.url) as db:
        match settings.auth:
            case TokenAuth():
                db.authenticate(settings.auth.token)
            case RecordAuth():
                db.signin(
                    {
                        "database": settings.database,
                        "namespace": settings.namespace,
                        "variables": {
                            "username": settings.auth.username,
                            "password": settings.auth.password,
                        },
                        "access": settings.auth.access,
                    }
                )
            case RootAuth():
                db.signin(
                    {
                        "username": settings.auth.username,
                        "password": settings.auth.password,
                    }
                )
        db.use(settings.namespace, settings.database)
        yield SurrealConnection(db)


@asynccontextmanager
async def async_surreal_client(
    settings: SurrealDatabaseSettings,
) -> AsyncGenerator[SurrealAsyncConnection, None]:
    async with AsyncSurreal(settings.url) as db:
        match settings.auth:
            case TokenAuth():
                await db.authenticate(settings.auth.token)
            case RecordAuth():
                await db.signin(
                    {
                        "database": settings.database,
                        "namespace": settings.namespace,
                        "variables": {
                            "username": settings.auth.username,
                            "password": settings.auth.password,
                        },
                        "access": settings.auth.access,
                    }
                )
            case RootAuth():
                await db.signin(
                    {
                        "username": settings.auth.username,
                        "password": settings.auth.password,
                    }
                )
        await db.use(settings.namespace, settings.database)
        yield SurrealAsyncConnection(db)


def select_one_result(result: Value) -> dict[str, Value]:
    if isinstance(result, dict):
        return {str(key): value for key, value in result.items()}
    raise ValueError(f"Expected dict, got {type(result)}")


def select_result(result: Value) -> list[dict[str, Value]]:
    if isinstance(result, list):
        return [select_one_result(row) for row in result]
    return []


def validate_table_name(table: str) -> None:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,63}", table):
        raise ValueError(f"Invalid SurrealDB table name: {table!r}")
