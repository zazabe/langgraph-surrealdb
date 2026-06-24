from __future__ import annotations

import os
from collections.abc import AsyncGenerator, Generator
from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass

from surrealdb import AsyncSurreal, Surreal
from surrealdb.types import Value

from langgraph_surrealdb.database.interface import (
    SurrealAsyncConnection,
    SurrealConnection,
)


@dataclass(frozen=True)
class SurrealConnSettings:
    url: str
    namespace: str
    database: str
    username: str | None
    password: str | None
    token: str | None

    @classmethod
    def from_env(cls) -> SurrealConnSettings:
        return cls(
            url=os.getenv("SURREAL_URL") or "",
            namespace=os.getenv("SURREAL_NS") or "",
            database=os.getenv("SURREAL_DB") or "",
            username=os.getenv("SURREAL_USER") or None,
            password=os.getenv("SURREAL_PASS") or None,
            token=os.getenv("SURREAL_TOKEN") or None,
        )


def _auth_payload(settings: SurrealConnSettings) -> dict[str, Value] | None:
    if settings.token:
        return None
    if settings.username and settings.password:
        return {"username": settings.username, "password": settings.password}
    return None


@contextmanager
def surreal_client(
    settings: SurrealConnSettings,
) -> Generator[SurrealConnection, None, None]:
    with Surreal(settings.url) as db:
        auth = _auth_payload(settings)
        if auth:
            db.signin(auth)
        elif settings.token:
            db.authenticate(settings.token)
        db.use(settings.namespace, settings.database)
        yield db


@asynccontextmanager
async def async_surreal_client(
    settings: SurrealConnSettings,
) -> AsyncGenerator[SurrealAsyncConnection, None]:
    async with AsyncSurreal(settings.url) as db:
        auth = _auth_payload(settings)
        if auth:
            await db.signin(auth)
        elif settings.token:
            await db.authenticate(settings.token)
        await db.use(settings.namespace, settings.database)
        yield db


def select_one_result(result: Value) -> dict[str, Value]:
    if isinstance(result, dict):
        return {str(key): value for key, value in result.items()}
    raise ValueError(f"Expected dict, got {type(result)}")


def select_result(result: Value) -> list[dict[str, Value]]:
    if isinstance(result, list):
        return [select_one_result(row) for row in result]
    return []
