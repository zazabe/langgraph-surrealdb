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


class CheckpointLookupConfig(_ConfigurableModel):
    thread_id: NonEmptyStr
    checkpoint_ns: NamespaceStr = ""
    checkpoint_id: NonEmptyStr | None = None

    @classmethod
    def from_runnable_config(cls, config: RunnableConfig) -> CheckpointLookupConfig:
        return cls.model_validate(cls._extract_configurable(config))


class CheckpointWriteConfig(_ConfigurableModel):
    thread_id: NonEmptyStr
    checkpoint_ns: NamespaceStr = ""
    checkpoint_id: NonEmptyStr

    @classmethod
    def from_runnable_config(cls, config: RunnableConfig) -> CheckpointWriteConfig:
        return cls.model_validate(cls._extract_configurable(config))


class CheckpointListFilterConfig(_ConfigurableModel):
    thread_id: NonEmptyStr | None = None
    checkpoint_ns: NamespaceStr | None = None
    checkpoint_id: NonEmptyStr | None = None

    @classmethod
    def from_runnable_config(
        cls, config: RunnableConfig | None
    ) -> CheckpointListFilterConfig:
        return cls.model_validate(cls._extract_configurable(config))


class CheckpointBeforeConfig(_ConfigurableModel):
    checkpoint_id: NonEmptyStr | None = None

    @classmethod
    def from_runnable_config(
        cls, config: RunnableConfig | None
    ) -> CheckpointBeforeConfig:
        return cls.model_validate(cls._extract_configurable(config))
