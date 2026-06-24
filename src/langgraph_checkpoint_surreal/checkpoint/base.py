from __future__ import annotations

import random
import threading
from collections.abc import AsyncIterator, Iterator, Mapping, Sequence
from contextlib import contextmanager
from typing import Any

from langchain_core.runnables import RunnableConfig
from langgraph.checkpoint.base import (
    WRITES_IDX_MAP,
    BaseCheckpointSaver,
    ChannelVersions,
    Checkpoint,
    CheckpointMetadata,
    CheckpointTuple,
    DeltaChannelHistory,
)
from langgraph.checkpoint.serde.base import SerializerProtocol

from langgraph_checkpoint_surreal.database.common import (
    SurrealConnSettings,
    surreal_client,
)
from langgraph_checkpoint_surreal.database.interface import SurrealConnection
from langgraph_checkpoint_surreal.database.models.checkpoint import (
    DbCheckpoint,
    DbCheckpointId,
)
from langgraph_checkpoint_surreal.database.models.write import DbWrite
from langgraph_checkpoint_surreal.database.repository.checkpoints import (
    DbCheckpointsRepository,
)
from langgraph_checkpoint_surreal.database.repository.writes import DbWritesRepository

_AIO_ERROR_MSG = (
    "The SurrealSaver does not support async methods. "
    "Consider using AsyncSurrealSaver instead.\n"
    "from langgraph.checkpoint.surreal.aio import AsyncSurrealSaver\n"
)


class SurrealSaver(BaseCheckpointSaver[str]):
    """Checkpoint saver backed by SurrealDB."""

    conn: SurrealConnection
    is_setup: bool

    def __init__(
        self,
        conn: SurrealConnection,
        *,
        serde: SerializerProtocol | None = None,
    ) -> None:
        super().__init__(serde=serde)
        self.repo_checkpoints = DbCheckpointsRepository(conn)
        self.repo_writes = DbWritesRepository(conn)
        self.is_setup = False
        self.lock = threading.Lock()

    @classmethod
    @contextmanager
    def from_env(cls) -> Iterator[SurrealSaver]:
        settings = SurrealConnSettings.from_env()
        with cls.from_settings(settings) as saver:
            yield saver

    @classmethod
    @contextmanager
    def from_settings(cls, settings: SurrealConnSettings) -> Iterator[SurrealSaver]:
        with surreal_client(settings) as conn:
            yield cls(conn)

    def setup(self) -> None:
        if self.is_setup:
            return
        self.repo_checkpoints.setup()
        self.repo_writes.setup()
        self.is_setup = True

    def get_tuple(self, config: RunnableConfig) -> CheckpointTuple | None:
        configurable = config.get("configurable", {})
        checkpoint_id = configurable.get("checkpoint_id")
        checkpoint_ns = configurable.get("checkpoint_ns", "")
        thread_id = configurable.get("thread_id", "")
        with self.lock:
            self.setup()
            if checkpoint_id:
                db_checkpoint_id = DbCheckpointId.from_ids(
                    thread_id, checkpoint_ns, checkpoint_id
                )
                checkpoint = self.repo_checkpoints.get_by_id(db_checkpoint_id)
            else:
                checkpoint = self.repo_checkpoints.get_latest(thread_id, checkpoint_ns)

            if not checkpoint:
                return None
            else:
                return self._fetch_checkpoint_tuple(checkpoint)

    def list(
        self,
        config: RunnableConfig | None,
        *,
        filter: dict[str, Any] | None = None,
        before: RunnableConfig | None = None,
        limit: int | None = None,
    ) -> Iterator[CheckpointTuple]:
        configurable = config.get("configurable", {}) if config else {}
        thread_id = configurable.get("thread_id")
        checkpoint_ns = configurable.get("checkpoint_ns")
        checkpoint_id = configurable.get("checkpoint_id")
        before_checkpoint_id = None
        if before:
            before_configurable = before.get("configurable", {})
            before_checkpoint_id = before_configurable.get("checkpoint_id")

        with self.lock:
            self.setup()
            checkpoints = self.repo_checkpoints.list(
                thread_id,
                checkpoint_ns,
                checkpoint_id,
                filter,
                before_checkpoint_id,
                limit,
            )
            for checkpoint in checkpoints:
                yield self._fetch_checkpoint_tuple(checkpoint)

    def put(
        self,
        config: RunnableConfig,
        checkpoint: Checkpoint,
        metadata: CheckpointMetadata,
        new_versions: ChannelVersions,
    ) -> RunnableConfig:
        db_checkpoint = DbCheckpoint.create(self.serde, config, checkpoint, metadata)
        with self.lock:
            self.setup()
            self.repo_checkpoints.upsert(db_checkpoint)
        return db_checkpoint.to_config()

    def put_writes(
        self,
        config: RunnableConfig,
        writes: Sequence[tuple[str, Any]],
        task_id: str,
        task_path: str = "",
    ) -> None:
        replace = all(w[0] in WRITES_IDX_MAP for w in writes)
        configurable = config.get("configurable", {})
        thread_id = configurable.get("thread_id", "")
        checkpoint_ns = configurable.get("checkpoint_ns", "")
        checkpoint_id = configurable.get("checkpoint_id", "")

        with self.lock:
            self.setup()
            for idx, (channel, value) in enumerate(writes):
                use_idx = WRITES_IDX_MAP.get(channel, idx)
                write = DbWrite.create(
                    self.serde,
                    thread_id,
                    checkpoint_ns,
                    checkpoint_id,
                    task_id,
                    use_idx,
                    channel,
                    value,
                )

                if replace:
                    self.repo_writes.upsert(write)
                else:
                    exists = self.repo_writes.get_by_id(write.id)
                    if not exists:
                        self.repo_writes.create(write)

    def delete_thread(self, thread_id: str) -> None:
        with self.lock:
            self.setup()
            self.repo_checkpoints.delete_thread(thread_id)
            self.repo_writes.delete_thread(thread_id)

    def get_delta_channel_history(
        self, *, config: RunnableConfig, channels: Sequence[str]
    ) -> Mapping[str, DeltaChannelHistory]:
        # Keep exact contract by delegating to base implementation first.
        return super().get_delta_channel_history(config=config, channels=channels)

    async def aget_tuple(self, config: RunnableConfig) -> CheckpointTuple | None:
        raise NotImplementedError(_AIO_ERROR_MSG)

    async def alist(
        self,
        config: RunnableConfig | None,
        *,
        filter: dict[str, Any] | None = None,
        before: RunnableConfig | None = None,
        limit: int | None = None,
    ) -> AsyncIterator[CheckpointTuple]:
        raise NotImplementedError(_AIO_ERROR_MSG)
        yield

    async def aput(
        self,
        config: RunnableConfig,
        checkpoint: Checkpoint,
        metadata: CheckpointMetadata,
        new_versions: ChannelVersions,
    ) -> RunnableConfig:
        raise NotImplementedError(_AIO_ERROR_MSG)

    def get_next_version(self, current: str | None, channel: None) -> str:
        if current is None:
            current_v = 0
        elif isinstance(current, int):
            current_v = current
        else:
            current_v = int(current.split(".")[0])
        next_v = current_v + 1
        next_h = random.random()
        return f"{next_v:032}.{next_h:016}"

    def _fetch_checkpoint_tuple(self, checkpoint: DbCheckpoint) -> CheckpointTuple:
        pending_writes = self.repo_writes.fetch(
            checkpoint.thread_id, checkpoint.checkpoint_ns, checkpoint.checkpoint_id
        )
        pending = [write.to_pending_write(self.serde) for write in pending_writes]

        return CheckpointTuple(
            RunnableConfig(
                configurable={
                    "thread_id": checkpoint.thread_id,
                    "checkpoint_ns": checkpoint.checkpoint_ns,
                    "checkpoint_id": checkpoint.checkpoint_id,
                }
            ),
            checkpoint.to_checkpoint(self.serde),
            checkpoint.metadata,
            checkpoint.to_parent_config(),
            pending,
        )
