from __future__ import annotations

import asyncio
import random
from collections.abc import AsyncIterator, Iterator, Mapping, Sequence
from contextlib import asynccontextmanager
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

from langgraph_surrealdb.checkpoint.config import (
    FullCheckpointConfig,
    PartialCheckpointConfig,
)
from langgraph_surrealdb.database.common import (
    SurrealConnSettings,
    async_surreal_client,
)
from langgraph_surrealdb.database.interface import SurrealAsyncConnection
from langgraph_surrealdb.database.models.checkpoint import (
    DbCheckpoint,
    DbCheckpointId,
)
from langgraph_surrealdb.database.models.write import DbWrite
from langgraph_surrealdb.database.repository.checkpoints import (
    DbAsyncCheckpointsRepository,
)
from langgraph_surrealdb.database.repository.writes import (
    DbAsyncWritesRepository,
)


class AsyncSurrealSaver(BaseCheckpointSaver[str]):
    """Asynchronous SurrealDB checkpoint saver."""

    def __init__(
        self,
        conn: SurrealAsyncConnection,
        *,
        serde: SerializerProtocol | None = None,
    ) -> None:
        super().__init__(serde=serde)
        self.repo_checkpoints = DbAsyncCheckpointsRepository(conn)
        self.repo_writes = DbAsyncWritesRepository(conn)
        self.lock = asyncio.Lock()
        self.loop = asyncio.get_running_loop()
        self.is_setup = False

    @classmethod
    @asynccontextmanager
    async def from_env(cls) -> AsyncIterator[AsyncSurrealSaver]:
        settings = SurrealConnSettings.from_env()
        async with cls.from_settings(settings) as saver:
            yield saver

    @classmethod
    @asynccontextmanager
    async def from_settings(
        cls, settings: SurrealConnSettings
    ) -> AsyncIterator[AsyncSurrealSaver]:
        async with async_surreal_client(settings) as conn:
            yield cls(conn)

    def get_tuple(self, config: RunnableConfig) -> CheckpointTuple | None:
        try:
            if asyncio.get_running_loop() is self.loop:
                raise asyncio.InvalidStateError(
                    "Synchronous calls to AsyncSurrealSaver are only allowed from a different thread."
                )
        except RuntimeError:
            pass
        return asyncio.run_coroutine_threadsafe(
            self.aget_tuple(config), self.loop
        ).result()

    def list(
        self,
        config: RunnableConfig | None,
        *,
        filter: dict[str, Any] | None = None,
        before: RunnableConfig | None = None,
        limit: int | None = None,
    ) -> Iterator[CheckpointTuple]:
        try:
            if asyncio.get_running_loop() is self.loop:
                raise asyncio.InvalidStateError(
                    "Synchronous calls to AsyncSurrealSaver are only allowed from a different thread."
                )
        except RuntimeError:
            pass
        aiter_ = self.alist(config, filter=filter, before=before, limit=limit)
        while True:
            try:
                yield asyncio.run_coroutine_threadsafe(
                    anext(aiter_),  # type: ignore[arg-type]  # noqa: F821
                    self.loop,
                ).result()
            except StopAsyncIteration:
                break

    def put(
        self,
        config: RunnableConfig,
        checkpoint: Checkpoint,
        metadata: CheckpointMetadata,
        new_versions: ChannelVersions,
    ) -> RunnableConfig:
        return asyncio.run_coroutine_threadsafe(
            self.aput(config, checkpoint, metadata, new_versions), self.loop
        ).result()

    def put_writes(
        self,
        config: RunnableConfig,
        writes: Sequence[tuple[str, Any]],
        task_id: str,
        task_path: str = "",
    ) -> None:
        return asyncio.run_coroutine_threadsafe(
            self.aput_writes(config, writes, task_id, task_path), self.loop
        ).result()

    def delete_thread(self, thread_id: str) -> None:
        return asyncio.run_coroutine_threadsafe(
            self.adelete_thread(thread_id), self.loop
        ).result()

    def get_delta_channel_history(
        self, *, config: RunnableConfig, channels: Sequence[str]
    ) -> Mapping[str, DeltaChannelHistory]:
        return asyncio.run_coroutine_threadsafe(
            self.aget_delta_channel_history(config=config, channels=channels), self.loop
        ).result()

    async def setup(self) -> None:
        async with self.lock:
            if self.is_setup:
                return
            await self.repo_checkpoints.setup()
            await self.repo_writes.setup()
            self.is_setup = True

    async def probe(self) -> None:
        async with self.lock:
            if self.is_setup:
                return
            await self.repo_checkpoints.probe()
            await self.repo_writes.probe()
            self.is_setup = True

    async def _ensure_ready(self) -> None:
        if self.is_setup:
            return
        try:
            await self.probe()
        except Exception as e:
            raise RuntimeError(
                "SurrealDB checkpoint schema is not initialized. "
                "Call setup() on this saver instance before use."
            ) from e

    async def aget_tuple(self, config: RunnableConfig) -> CheckpointTuple | None:
        await self._ensure_ready()
        checkpoint_config = FullCheckpointConfig.from_config(config)
        checkpoint_id = checkpoint_config.checkpoint_id
        checkpoint_ns = checkpoint_config.checkpoint_ns
        thread_id = checkpoint_config.thread_id
        async with self.lock:
            if checkpoint_id:
                db_checkpoint_id = DbCheckpointId.from_ids(
                    thread_id, checkpoint_ns, checkpoint_id
                )
                checkpoint = await self.repo_checkpoints.get_by_id(db_checkpoint_id)
            else:
                checkpoint = await self.repo_checkpoints.get_latest(
                    thread_id, checkpoint_ns
                )

            if not checkpoint:
                return None
            else:
                return await self._fetch_checkpoint_tuple(checkpoint)

    async def alist(
        self,
        config: RunnableConfig | None,
        *,
        filter: dict[str, Any] | None = None,
        before: RunnableConfig | None = None,
        limit: int | None = None,
    ) -> AsyncIterator[CheckpointTuple]:
        await self._ensure_ready()

        config_filter = PartialCheckpointConfig.from_config(config)
        thread_id = config_filter.thread_id
        checkpoint_ns = config_filter.checkpoint_ns
        checkpoint_id = config_filter.checkpoint_id
        before_checkpoint_id = PartialCheckpointConfig.from_config(before).checkpoint_id

        async with self.lock:
            checkpoints = await self.repo_checkpoints.list(
                thread_id,
                checkpoint_ns,
                checkpoint_id,
                filter,
                before_checkpoint_id,
                limit,
            )
            for checkpoint in checkpoints:
                yield await self._fetch_checkpoint_tuple(checkpoint)

    async def aput(
        self,
        config: RunnableConfig,
        checkpoint: Checkpoint,
        metadata: CheckpointMetadata,
        new_versions: ChannelVersions,
    ) -> RunnableConfig:
        await self._ensure_ready()

        db_checkpoint = DbCheckpoint.create(self.serde, config, checkpoint, metadata)
        async with self.lock:
            await self.repo_checkpoints.upsert(db_checkpoint)
        return db_checkpoint.to_config()

    async def aput_writes(
        self,
        config: RunnableConfig,
        writes: Sequence[tuple[str, Any]],
        task_id: str,
        task_path: str = "",
    ) -> None:
        await self._ensure_ready()

        replace = all(w[0] in WRITES_IDX_MAP for w in writes)
        checkpoint_config = FullCheckpointConfig.from_config(config)
        thread_id = checkpoint_config.thread_id
        checkpoint_ns = checkpoint_config.checkpoint_ns
        checkpoint_id = checkpoint_config.require_checkpoint_id()

        async with self.lock:
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
                    await self.repo_writes.upsert(write)
                else:
                    exists = await self.repo_writes.get_by_id(write.id)
                    if not exists:
                        await self.repo_writes.create(write)

    async def adelete_thread(self, thread_id: str) -> None:
        await self._ensure_ready()
        async with self.lock:
            await self.repo_checkpoints.delete_thread(thread_id)
            await self.repo_writes.delete_thread(thread_id)

    async def aget_delta_channel_history(
        self, *, config: RunnableConfig, channels: Sequence[str]
    ) -> Mapping[str, DeltaChannelHistory]:
        return await super().aget_delta_channel_history(
            config=config, channels=channels
        )

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

    async def _fetch_checkpoint_tuple(
        self, checkpoint: DbCheckpoint
    ) -> CheckpointTuple:
        pending_writes = await self.repo_writes.fetch(
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
