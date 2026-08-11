from typing import Literal, Self

from langgraph.store.base import TTLConfig
from pydantic import BaseModel, Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from langgraph_surrealdb.settings import SurrealDatabaseSettings, validate_table_name

STORE_INDEX_DISTANCE_TYPES = Literal["cosine"]


class SurrealStoreEnvSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="LANGGRAPH_SURREALDB_STORE_",
        extra="ignore",
    )

    index_enabled: bool = Field(default=False)
    index_dimensions: int = Field(default=1536)
    index_fields: list[str] = Field(default_factory=lambda: ["$"])
    index_distance_type: STORE_INDEX_DISTANCE_TYPES = Field(default="cosine")

    ttl_enabled: bool = Field(default=False)
    ttl_refresh_on_read: bool = Field(default=True)
    ttl_default_ttl: float | None = None
    ttl_sweep_interval_minutes: int | None = None


class SurrealStoreIndexSettings(BaseModel):
    """Configuration for semantic search in a SurrealDB store."""

    enabled: bool = Field(default=False)
    dimensions: int = Field(default=1536)
    fields: list[str] = Field(default_factory=lambda: ["$"])
    distance_type: STORE_INDEX_DISTANCE_TYPES = Field(default="cosine")

    @model_validator(mode="after")
    def validate_index_config(self) -> Self:
        if self.dimensions <= 0:
            raise ValueError("dimensions must be a positive integer")
        if len(self.fields) == 0:
            raise ValueError("fields must be a non-empty list")
        if not all(isinstance(field, str) for field in self.fields):
            raise ValueError("fields must be a list of strings")
        return self


class SurrealStoreTTLSettings(BaseModel):
    enabled: bool = Field(default=False)
    refresh_on_read: bool = Field(default=True)
    default_ttl: float | None = None
    sweep_interval_minutes: int | None = None

    def into_ttl_config(self) -> TTLConfig:
        return TTLConfig(
            refresh_on_read=self.refresh_on_read,
            default_ttl=self.default_ttl,
            sweep_interval_minutes=self.sweep_interval_minutes,
        )


class SurrealStoreSettings(BaseModel):
    store_table: str = "store"
    index: SurrealStoreIndexSettings = Field(default_factory=SurrealStoreIndexSettings)
    ttl: SurrealStoreTTLSettings = Field(default_factory=SurrealStoreTTLSettings)
    db: SurrealDatabaseSettings

    @property
    def vector_table(self) -> str:
        return f"{self.store_table}_vector"

    @classmethod
    def from_env(cls) -> Self:
        db = SurrealDatabaseSettings.from_env()
        cfg = SurrealStoreEnvSettings.model_validate({})

        index = SurrealStoreIndexSettings(
            enabled=cfg.index_enabled,
            dimensions=cfg.index_dimensions,
            fields=cfg.index_fields,
            distance_type=cfg.index_distance_type,
        )
        ttl = SurrealStoreTTLSettings(
            enabled=cfg.ttl_enabled,
            refresh_on_read=(
                cfg.ttl_refresh_on_read if cfg.ttl_refresh_on_read is not None else True
            ),
            default_ttl=cfg.ttl_default_ttl,
            sweep_interval_minutes=cfg.ttl_sweep_interval_minutes,
        )

        return cls(
            store_table=(db.table_prefix + "_store" if db.table_prefix else "store"),
            index=index,
            ttl=ttl,
            db=db,
        )

    @model_validator(mode="after")
    def validate_table_names(self) -> Self:
        if not validate_table_name(self.store_table):
            raise ValueError(
                f"store_table ({self.store_table}) is not a valid table name"
            )
        return self
