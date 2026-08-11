import re
from typing import Annotated, Literal, Self

from pydantic import BaseModel, Field, StringConstraints
from pydantic_settings import BaseSettings, SettingsConfigDict

AUTH_MODES = Literal["root", "record", "token"]
STORE_INDEX_DISTANCE_TYPES = Literal["cosine"]


NonEmpty = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]


def validate_table_name(table_name: str) -> bool:
    return bool(
        re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", table_name) and len(table_name) <= 64
    )


class RootAuth(BaseModel):
    mode: Literal["root"] = "root"
    username: NonEmpty
    password: NonEmpty


class RecordAuth(BaseModel):
    mode: Literal["record"] = "record"
    username: NonEmpty
    password: NonEmpty
    access: NonEmpty


class TokenAuth(BaseModel):
    mode: Literal["token"] = "token"
    token: NonEmpty


DatabaseAuth = Annotated[RootAuth | RecordAuth | TokenAuth, Field(discriminator="mode")]


class SurrealEnvSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="LANGGRAPH_SURREALDB_",
        extra="ignore",
    )

    url: NonEmpty
    ns: NonEmpty
    db: NonEmpty
    table_prefix: str | None = None

    auth_mode: AUTH_MODES
    user: str | None = None
    password: str | None = None
    access: str | None = None
    token: str | None = None


class SurrealDatabaseSettings(BaseModel):
    url: NonEmpty
    namespace: NonEmpty
    database: NonEmpty
    auth: DatabaseAuth
    table_prefix: str | None = None

    @classmethod
    def from_env(cls) -> Self:
        cfg: SurrealEnvSettings = SurrealEnvSettings.model_validate({})
        match cfg.auth_mode:
            case "root":
                if not (cfg.user and cfg.password):
                    raise ValueError("root mode requires user/password")
                auth = RootAuth(username=cfg.user, password=cfg.password)
            case "record":
                if not (cfg.user and cfg.password and cfg.access):
                    raise ValueError("record mode requires user/password/access")
                auth = RecordAuth(
                    username=cfg.user, password=cfg.password, access=cfg.access
                )
            case "token":
                if not cfg.token:
                    raise ValueError("token mode requires token")
                auth = TokenAuth(token=cfg.token)

        return cls(
            url=cfg.url,
            namespace=cfg.ns,
            database=cfg.db,
            auth=auth,
            table_prefix=cfg.table_prefix,
        )
