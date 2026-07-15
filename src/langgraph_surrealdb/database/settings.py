import re
from typing import Annotated, Literal, Self

from pydantic import BaseModel, Field, StringConstraints, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

NonEmpty = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]


def _validate_table_name(table_name: str) -> bool:
    return bool(
        re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", table_name) and len(table_name) <= 64
    )


class SurrealEnvSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="LANGGRAPH_SURREALDB_",
        extra="ignore",
    )

    url: NonEmpty
    ns: NonEmpty
    db: NonEmpty
    table_prefix: str | None = None

    auth_mode: Literal["root", "record", "token"]
    user: str | None = None
    password: str | None = None
    access: str | None = None
    token: str | None = None

    @model_validator(mode="after")
    def validate_auth(self) -> Self:
        if self.auth_mode == "root":
            if not (self.user and self.password):
                raise ValueError("root mode requires SURREAL_USER and SURREAL_PASSWORD")
            if self.token or self.access:
                raise ValueError("root mode forbids SURREAL_TOKEN and SURREAL_ACCESS")
        elif self.auth_mode == "record":
            if not (self.user and self.password and self.access):
                raise ValueError("record mode requires USER/PASSWORD/ACCESS")
            if self.token:
                raise ValueError("record mode forbids SURREAL_TOKEN")
        else:  # token
            if not self.token:
                raise ValueError("token mode requires SURREAL_TOKEN")
            if self.user or self.password or self.access:
                raise ValueError("token mode forbids USER/PASSWORD/ACCESS")
        return self


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


class SurrealSaverDatabaseSettings(BaseModel):
    url: NonEmpty
    namespace: NonEmpty
    database: NonEmpty
    auth: DatabaseAuth


class SurrealSaverSettings(BaseModel):
    checkpoints_table: str = "checkpoints"
    writes_table: str = "writes"
    db: SurrealSaverDatabaseSettings

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

        checkpoints_table = (
            cfg.table_prefix + "_checkpoints" if cfg.table_prefix else "checkpoints"
        )
        writes_table = cfg.table_prefix + "_writes" if cfg.table_prefix else "writes"

        return cls(
            checkpoints_table=checkpoints_table,
            writes_table=writes_table,
            db=SurrealSaverDatabaseSettings(
                url=cfg.url,
                namespace=cfg.ns,
                database=cfg.db,
                auth=auth,
            ),
        )

    @model_validator(mode="after")
    def validate_table_names(self) -> Self:
        if self.checkpoints_table == self.writes_table:
            raise ValueError(
                f"checkpoints_table ({self.checkpoints_table}) and writes_table ({self.writes_table}) cannot be the same"
            )
        if not _validate_table_name(self.checkpoints_table):
            raise ValueError(
                f"checkpoints_table ({self.checkpoints_table}) is not a valid table name"
            )
        if not _validate_table_name(self.writes_table):
            raise ValueError(
                f"writes_table ({self.writes_table}) is not a valid table name"
            )
        return self
