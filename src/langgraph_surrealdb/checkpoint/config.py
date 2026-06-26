from __future__ import annotations

from collections.abc import Mapping
from typing import Annotated, Any

from langchain_core.runnables import RunnableConfig
from pydantic import BaseModel, ConfigDict, StringConstraints

NonEmptyStr = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]
NamespaceStr = Annotated[str, StringConstraints(strip_whitespace=True)]


class _ConfigurableModel(BaseModel):
    model_config = ConfigDict(extra="allow", arbitrary_types_allowed=True)

    @classmethod
    def _extract_configurable(cls, config: RunnableConfig | None) -> Mapping[str, Any]:
        if config is None:
            return {}

        configurable = config.get("configurable", {})
        if not isinstance(configurable, Mapping):
            raise TypeError("config['configurable'] must be a mapping")
        return configurable


class FullCheckpointConfig(_ConfigurableModel):
    thread_id: NonEmptyStr
    checkpoint_ns: NamespaceStr = ""
    checkpoint_id: NonEmptyStr | None = None

    @classmethod
    def from_config(cls, config: RunnableConfig) -> FullCheckpointConfig:
        return cls.model_validate(cls._extract_configurable(config))

    def require_checkpoint_id(self) -> NonEmptyStr:
        if self.checkpoint_id is None:
            raise ValueError("config['configurable']['checkpoint_id'] is required")
        return self.checkpoint_id


class PartialCheckpointConfig(_ConfigurableModel):
    thread_id: NonEmptyStr | None = None
    checkpoint_ns: NamespaceStr | None = None
    checkpoint_id: NonEmptyStr | None = None

    @classmethod
    def from_config(cls, config: RunnableConfig | None) -> PartialCheckpointConfig:
        return cls.model_validate(cls._extract_configurable(config))
