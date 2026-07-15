from __future__ import annotations

import os
from abc import ABC, abstractmethod
from collections.abc import AsyncGenerator, Generator
from contextlib import asynccontextmanager, contextmanager
from typing import Annotated, Literal

from pydantic import BaseModel, Field, StringConstraints
from surrealdb import AsyncSurreal, Surreal
from surrealdb.types import Value

from langgraph_surrealdb.database.interface import (
    QueryRawResult,
    SurrealAsyncConnection,
    SurrealConnection,
)

NonEmpty = Annotated[str, StringConstraints(
    strip_whitespace=True, min_length=1)]


class BaseAuth(BaseModel, ABC):
    @abstractmethod
    def payload(self) -> dict[str, Value]: ...


class RootAuth(BaseAuth):
    mode: Literal["root"] = "root"
    username: NonEmpty
    password: NonEmpty

    def payload(self) -> dict[str, Value]:
        return {"username": self.username, "password": self.password}


class RecordAuth(BaseAuth):
    mode: Literal["record"] = "record"
    username: NonEmpty
    password: NonEmpty
    access: NonEmpty

    def payload(self) -> dict[str, Value]:
        return {
            "username": self.username,
            "password": self.password,
            "access": self.access,
        }


class TokenAuth(BaseAuth):
    mode: Literal["token"] = "token"
    token: NonEmpty

    def payload(self) -> dict[str, Value]:
        return {"token": self.token}


DatabaseAuth = Annotated[RootAuth | RecordAuth |
                         TokenAuth, Field(discriminator="mode")]


class SurrealSaverDatabaseSettings(BaseModel):
    url: NonEmpty
    namespace: NonEmpty
    database: NonEmpty
    auth: DatabaseAuth


class SurrealSaverSettings(BaseModel):
    db: SurrealSaverDatabaseSettings

    @classmethod
    def from_env(cls) -> SurrealSaverSettings:
        username = os.getenv("SURREAL_USER") or ""
        password = os.getenv("SURREAL_PASS") or ""
        access = os.getenv("SURREAL_ACCESS") or ""
        token = os.getenv("SURREAL_TOKEN") or ""

        if username and password and access:
            auth = RecordAuth(username=username,
                              password=password, access=access)
        elif username and password:
            auth = RootAuth(username=username, password=password)
        elif token:
            auth = TokenAuth(token=token)
        else:
            raise ValueError("No authentication provided")

        return cls(
            db=SurrealSaverDatabaseSettings(
                url=os.getenv("SURREAL_URL") or "",
                namespace=os.getenv("SURREAL_NS") or "",
                database=os.getenv("SURREAL_DB") or "",
                auth=auth,
            )
        )


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
