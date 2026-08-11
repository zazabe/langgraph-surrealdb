from typing import Self

from pydantic import BaseModel, model_validator

from langgraph_surrealdb.settings import SurrealDatabaseSettings, validate_table_name


class SurrealCheckpointSettings(BaseModel):
    checkpoints_table: str = "checkpoints"
    writes_table: str = "writes"
    db: SurrealDatabaseSettings

    @classmethod
    def from_env(cls) -> Self:
        db = SurrealDatabaseSettings.from_env()
        return cls(
            checkpoints_table=(
                db.table_prefix + "_checkpoints" if db.table_prefix else "checkpoints"
            ),
            writes_table=(db.table_prefix + "_writes" if db.table_prefix else "writes"),
            db=db,
        )

    @model_validator(mode="after")
    def validate_table_names(self) -> Self:
        if self.checkpoints_table == self.writes_table:
            raise ValueError(
                f"checkpoints_table ({self.checkpoints_table}) and writes_table ({self.writes_table}) cannot be the same"
            )
        if not validate_table_name(self.checkpoints_table):
            raise ValueError(
                f"checkpoints_table ({self.checkpoints_table}) is not a valid table name"
            )
        if not validate_table_name(self.writes_table):
            raise ValueError(
                f"writes_table ({self.writes_table}) is not a valid table name"
            )
        return self
