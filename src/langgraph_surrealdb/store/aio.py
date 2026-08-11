from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator, Iterable, Sequence
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from types import TracebackType
from typing import cast

from langgraph.store.base import (
    Embeddings,
    GetOp,
    ListNamespacesOp,
    Op,
    PutOp,
    Result,
    SearchOp,
    ensure_embeddings,
)
from langgraph.store.base.batch import AsyncBatchedBaseStore
from langgraph.store.base.embed import AEmbeddingsFunc, EmbeddingsFunc

from langgraph_surrealdb.database import SurrealAsyncConnection, async_surreal_client
from langgraph_surrealdb.database.models.store import (
    DbStoreItem,
    DbStoreItemScored,
    DbStoreModelFactory,
)
from langgraph_surrealdb.database.models.store_vector import (
    DbStoreItemWithVectorRequests,
    DbStoreItemWithVectors,
    DbStoreVector,
    DbStoreVectorModelFactory,
)
from langgraph_surrealdb.database.repository.store import (
    DbAsyncStoreRepository,
    SearchQuery,
    SearchQueryIndex,
)
from langgraph_surrealdb.database.repository.store_vector import (
    DbAsyncStoreVectorRepository,
)
from langgraph_surrealdb.store.base import group_ops, matches_namespace
from langgraph_surrealdb.store.settings import SurrealStoreSettings

logger = logging.getLogger(__name__)


class AsyncSurrealStore(AsyncBatchedBaseStore):
    """Asynchronous LangGraph store backed by SurrealDB."""

    supports_ttl = True

    def __init__(
        self,
        conn: SurrealAsyncConnection,
        *,
        settings: SurrealStoreSettings,
        embed: Embeddings | EmbeddingsFunc | AEmbeddingsFunc | str | None,
    ) -> None:
        super().__init__()
        self.store_factory = DbStoreModelFactory(table=settings.store_table)
        self.vector_factory = DbStoreVectorModelFactory(table=settings.vector_table)
        self.store_repo = DbAsyncStoreRepository(conn, self.store_factory, settings.ttl)
        self.vector_repo = DbAsyncStoreVectorRepository(
            conn, self.vector_factory, settings.index
        )

        self.lock = asyncio.Lock()
        self.is_setup = False
        self.settings = settings
        self.embeddings = None
        if embed is not None and settings.index.enabled is True:
            self.embeddings = ensure_embeddings(embed)

        self.ttl_config = None
        if settings.ttl.enabled:
            self.ttl_config = settings.ttl.into_ttl_config()

        self._ttl_stop_event = asyncio.Event()
        self._ttl_sweeper_task: asyncio.Task[None] | None = None

    @classmethod
    @asynccontextmanager
    async def from_env(
        cls,
        *,
        embed: Embeddings | EmbeddingsFunc | AEmbeddingsFunc | str | None = None,
    ) -> AsyncIterator[AsyncSurrealStore]:
        settings = SurrealStoreSettings.from_env()
        async with cls.from_settings(
            settings,
            embed=embed,
        ) as store:
            yield store

    @classmethod
    @asynccontextmanager
    async def from_settings(
        cls,
        settings: SurrealStoreSettings,
        *,
        embed: Embeddings | EmbeddingsFunc | AEmbeddingsFunc | str | None = None,
    ) -> AsyncIterator[AsyncSurrealStore]:
        async with async_surreal_client(settings.db) as conn:
            store = cls(
                conn,
                settings=settings,
                embed=embed,
            )
            yield store

    async def setup(self) -> None:
        async with self.lock:
            if self.is_setup:
                return
            await self.store_repo.setup()
            await self.vector_repo.setup(store_table=self.store_factory.table)
            self.is_setup = True

    async def probe(self) -> None:
        async with self.lock:
            if self.is_setup:
                return
            await self.store_repo.probe()
            await self.vector_repo.probe()
            self.is_setup = True

    async def _ensure_ready(self) -> None:
        if self.is_setup:
            return
        try:
            await self.probe()
        except Exception as exc:
            raise RuntimeError(
                "SurrealDB store schema is not initialized. "
                "Call setup() on this store instance before use."
            ) from exc

    async def abatch(self, ops: Iterable[Op]) -> list[Result]:
        await self._ensure_ready()
        grouped, count = group_ops(ops)
        results: list[Result] = [None] * count
        async with self.lock:
            if GetOp in grouped:
                await self._batch_get(
                    cast(Sequence[tuple[int, GetOp]], grouped[GetOp]), results
                )
            if SearchOp in grouped:
                await self._batch_search(
                    cast(Sequence[tuple[int, SearchOp]], grouped[SearchOp]), results
                )
            if ListNamespacesOp in grouped:
                await self._batch_list_namespaces(
                    cast(
                        Sequence[tuple[int, ListNamespacesOp]],
                        grouped[ListNamespacesOp],
                    ),
                    results,
                )
            if PutOp in grouped:
                await self._batch_put(cast(Sequence[tuple[int, PutOp]], grouped[PutOp]))
        return results

    async def _batch_get(
        self,
        ops: Sequence[tuple[int, GetOp]],
        results: list[Result],
    ) -> None:
        for result_index, op in ops:
            item_id = self.store_factory.create_id(namespace=op.namespace, key=op.key)
            item = await self.store_repo.get_by_id(item_id)
            if item is None:
                continue
            if (
                op.refresh_ttl
                and self.settings.ttl.enabled
                and item.ttl_minutes is not None
            ):
                expires_at = datetime.now(UTC) + timedelta(minutes=item.ttl_minutes)
                await self.store_repo.refresh_expiry(item.id, expires_at)
            results[result_index] = item.to_item()

    async def _batch_put(
        self,
        ops: Sequence[tuple[int, PutOp]],
    ) -> None:
        deduplicated: dict[tuple[tuple[str, ...], str], PutOp] = {}
        for _, op in ops:
            deduplicated[(op.namespace, op.key)] = op

        prepared: list[DbStoreItemWithVectorRequests] = []
        for op in deduplicated.values():
            item_id = self.store_factory.create_id(namespace=op.namespace, key=op.key)
            item = await self.store_repo.get_by_id(item_id)
            if op.value is None:
                if item is not None:
                    if self._is_index_enabled():
                        await self.vector_repo.delete_by_item(item)
                    await self.store_repo.delete(item_id)
                continue

            if item is None:
                item = self.store_factory.create_record(
                    namespace=op.namespace,
                    key=op.key,
                    value=op.value,
                    ttl=op.ttl,
                )
            else:
                item.update(
                    value=op.value,
                    ttl=op.ttl,
                )

            fields = []
            if self._is_index_enabled():
                if op.index is None:
                    fields = self.settings.index.fields
                elif op.index is not False:
                    fields = op.index
            item_with_requests = DbStoreItemWithVectorRequests(item, fields)
            prepared.append(item_with_requests)

        items_with_vectors = await self._generate_vectors(prepared)

        for item_with_vectors in items_with_vectors:
            await self.store_repo.upsert(item_with_vectors.item)
            if self._is_index_enabled():
                await self.vector_repo.delete_by_item(item_with_vectors.item)
                for vector in item_with_vectors.vectors:
                    await self.vector_repo.upsert(vector)

    async def _batch_search(
        self,
        ops: Sequence[tuple[int, SearchOp]],
        results: list[Result],
    ) -> None:
        query_texts = []
        query_vectors = []
        queries: dict[int, SearchQueryIndex] = {}

        query_texts = set(
            op.query for _, op in ops if self._is_index_enabled() and op.query
        )
        if self.embeddings and self._is_index_enabled():
            query_vectors = await self.embeddings.aembed_documents(list(query_texts))

        for query_text, query_vector in zip(query_texts, query_vectors, strict=True):
            queries[hash(query_text)] = SearchQueryIndex(
                text=query_text, embedding=query_vector
            )

        for result_index, op in ops:
            query = queries.get(hash(op.query))
            if op.query and query is None:
                raise ValueError("Query not found in cache: " + op.query)
            result = await self.store_repo.search(
                SearchQuery(
                    namespace=list(op.namespace_prefix),
                    query=query,
                    filters=op.filter,
                    limit=op.limit,
                    offset=op.offset,
                )
            )

            if op.refresh_ttl and self.settings.ttl.enabled:
                await self._refresh_items(result)
            results[result_index] = [item.to_item() for item in result]

    async def _refresh_items(
        self, items: Sequence[DbStoreItem | DbStoreItemScored]
    ) -> None:
        now = datetime.now(UTC)
        for result in items:
            item = result.item if isinstance(result, DbStoreItemScored) else result
            if item.ttl_minutes is not None:
                await self.store_repo.refresh_expiry(
                    item.id, now + timedelta(minutes=item.ttl_minutes)
                )

    async def _batch_list_namespaces(
        self,
        ops: Sequence[tuple[int, ListNamespacesOp]],
        results: list[Result],
    ) -> None:
        raw_namespaces = await self.store_repo.list_namespaces()
        for result_index, op in ops:
            namespaces = {tuple(namespace) for namespace in raw_namespaces}
            if op.match_conditions:
                namespaces = {
                    namespace
                    for namespace in namespaces
                    if all(
                        matches_namespace(
                            namespace, condition.path, condition.match_type
                        )
                        for condition in op.match_conditions
                    )
                }
            if op.max_depth is not None:
                namespaces = {namespace[: op.max_depth] for namespace in namespaces}
            ordered = sorted(namespaces)
            results[result_index] = ordered[op.offset : op.offset + op.limit]

    async def sweep_ttl(self) -> int:
        await self._ensure_ready()
        async with self.lock:
            deleted = await self.store_repo.sweep_ttl()
            return len(deleted)

    async def start_ttl_sweeper(
        self, sweep_interval_minutes: float | None = None
    ) -> asyncio.Task[None]:
        """Periodically delete expired store items based on TTL.

        Returns:
            Task that can be awaited or cancelled.
        """
        if not self.ttl_config:
            return asyncio.create_task(asyncio.sleep(0))

        if self._ttl_sweeper_task is not None and not self._ttl_sweeper_task.done():
            return self._ttl_sweeper_task

        self._ttl_stop_event.clear()

        interval = float(
            sweep_interval_minutes or self.ttl_config.get("sweep_interval_minutes") or 5
        )
        logger.info(f"Starting store TTL sweeper with interval {interval} minutes")

        async def _sweep_loop() -> None:
            while not self._ttl_stop_event.is_set():
                try:
                    try:
                        await asyncio.wait_for(
                            self._ttl_stop_event.wait(),
                            timeout=interval * 60,
                        )
                        break
                    except TimeoutError:
                        pass

                    expired_items = await self.sweep_ttl()
                    if expired_items > 0:
                        logger.info(f"Store swept {expired_items} expired items")
                except asyncio.CancelledError:
                    break
                except Exception as exc:
                    logger.exception("Store TTL sweep iteration failed", exc_info=exc)

        task = asyncio.create_task(_sweep_loop())
        task.set_name("ttl_sweeper")
        self._ttl_sweeper_task = task
        return task

    async def stop_ttl_sweeper(self, timeout: float | None = None) -> bool:
        """Stop the TTL sweeper task if it's running.

        Args:
            timeout: Maximum time to wait for the task to stop, in seconds.
                If `None`, wait indefinitely.

        Returns:
            bool: True if the task was successfully stopped or wasn't running,
                False if the timeout was reached before the task stopped.
        """
        if self._ttl_sweeper_task is None or self._ttl_sweeper_task.done():
            return True

        logger.info("Stopping TTL sweeper task")
        self._ttl_stop_event.set()

        if timeout is not None:
            try:
                await asyncio.wait_for(self._ttl_sweeper_task, timeout=timeout)
                success = True
            except TimeoutError:
                success = False
        else:
            await self._ttl_sweeper_task
            success = True

        if success:
            self._ttl_sweeper_task = None
            logger.info("TTL sweeper task stopped")
        else:
            logger.warning("Timed out waiting for TTL sweeper task to stop")

        return success

    async def __aenter__(self) -> AsyncSurrealStore:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        # Ensure the TTL sweeper task is stopped when exiting the context
        if hasattr(self, "_ttl_sweeper_task") and self._ttl_sweeper_task is not None:
            # Set the event to signal the task to stop
            self._ttl_stop_event.set()
            # We don't wait for the task to complete here to avoid blocking
            # The task will clean up itself gracefully

    async def _generate_vectors(
        self, items: list[DbStoreItemWithVectorRequests]
    ) -> list[DbStoreItemWithVectors]:
        if not items or not self.embeddings:
            return [
                DbStoreItemWithVectors(item=item.item, vectors=[]) for item in items
            ]

        texts = [request.text for item in items for request in item.vector_requests]
        embeddings = await self.embeddings.aembed_documents(texts)
        if len(embeddings) != len(texts):
            raise ValueError(
                f"Embedding count mismatch: expected {len(texts)}, got {len(embeddings)}"
            )

        expected_dims = self.settings.index.dimensions
        embeddings_index = 0
        results: list[DbStoreItemWithVectors] = []
        for item in items:
            vectors: list[DbStoreVector] = []
            for request in item.vector_requests:
                embedding = embeddings[embeddings_index]
                embeddings_index += 1
                if len(embedding) != expected_dims:
                    raise ValueError(
                        "Embedding dimension mismatch: "
                        f"expected {expected_dims}, got {len(embedding)}"
                    )
                vector = self.vector_factory.create_record(
                    item=item.item,
                    field=request.field,
                    indexed_text=request.text,
                    embedding=embedding,
                )
                vectors.append(vector)
            results.append(item.with_vectors(vectors))
        return results

    def _is_index_enabled(self) -> bool:
        return self.settings.index.enabled
