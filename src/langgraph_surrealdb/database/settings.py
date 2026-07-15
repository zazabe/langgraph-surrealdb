from typing import Annotated, Literal, Self

from pydantic import BaseModel, Field, StringConstraints, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

NonEmpty = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]


class SurrealEnvSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="LANGGRAPH_SURREALDB_",
        extra="ignore",
    )

    url: NonEmpty
    ns: NonEmpty
    db: NonEmpty

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

        return cls(
            db=SurrealSaverDatabaseSettings(
                url=cfg.url,
                namespace=cfg.ns,
                database=cfg.db,
                auth=auth,
            )
        )
