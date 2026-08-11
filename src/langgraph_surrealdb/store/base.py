from __future__ import annotations

import concurrent.futures
import logging
import threading
from collections import defaultdict
from collections.abc import Iterable, Iterator, Sequence
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from typing import cast

from langgraph.store.base import (
    BaseStore,
    Embeddings,
    GetOp,
    ListNamespacesOp,
    Op,
    PutOp,
    Result,
    SearchOp,
    ensure_embeddings,
)
from langgraph.store.base.embed import EmbeddingsFunc

from langgraph_surrealdb.database import SurrealConnection, surreal_client
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
    DbStoreRepository,
    SearchQuery,
    SearchQueryIndex,
)
from langgraph_surrealdb.database.repository.store_vector import (
    DbStoreVectorRepository,
)
from langgraph_surrealdb.store.settings import SurrealStoreSettings

logger = logging.getLogger(__name__)


class SurrealStore(BaseStore):
    """LangGraph store backed by SurrealDB."""

    supports_ttl = True

    def __init__(
        self,
        conn: SurrealConnection,
        *,
        settings: SurrealStoreSettings,
        embed: Embeddings | EmbeddingsFunc | str | None,
    ) -> None:
        super().__init__()
        self.store_factory = DbStoreModelFactory(table=settings.store_table)
        self.vector_factory = DbStoreVectorModelFactory(table=settings.vector_table)
        self.store_repo = DbStoreRepository(conn, self.store_factory, settings.ttl)
        self.vector_repo = DbStoreVectorRepository(
            conn, self.vector_factory, settings.index
        )
        self.is_setup = False
        self.settings = settings
        self.embeddings = None
        if embed is not None and settings.index.enabled:
            self.embeddings = ensure_embeddings(embed)

        self.ttl_config = None
        if settings.ttl.enabled:
            self.ttl_config = settings.ttl.into_ttl_config()

        self.lock = threading.Lock()
        self._ttl_sweeper_thread: threading.Thread | None = None
        self._ttl_stop_event = threading.Event()

    @classmethod
    @contextmanager
    def from_env(
        cls,
        *,
        embed: Embeddings | EmbeddingsFunc | str | None = None,
    ) -> Iterator[SurrealStore]:
        settings = SurrealStoreSettings.from_env()
        with cls.from_settings(
            settings,
            embed=embed,
        ) as store:
            yield store

    @classmethod
    @contextmanager
    def from_settings(
        cls,
        settings: SurrealStoreSettings,
        *,
        embed: Embeddings | EmbeddingsFunc | str | None = None,
    ) -> Iterator[SurrealStore]:
        with surreal_client(settings.db) as conn:
            store = cls(
                conn,
                settings=settings,
                embed=embed,
            )
            yield store

    def setup(self) -> None:
        with self.lock:
            if self.is_setup:
                return
            self.store_repo.setup()
            self.vector_repo.setup(store_table=self.store_factory.table)
            self.is_setup = True

    def probe(self) -> None:
        with self.lock:
            if self.is_setup:
                return
            self.store_repo.probe()
            self.vector_repo.probe()
            self.is_setup = True

    def _ensure_ready(self) -> None:
        if self.is_setup:
            return
        try:
            self.probe()
        except Exception as exc:
            raise RuntimeError(
                "SurrealDB store schema is not initialized. "
                "Call setup() on this store instance before use."
            ) from exc

    async def abatch(self, ops: Iterable[Op]) -> list[Result]:
        raise NotImplementedError("Use AsyncSurrealStore")

    def batch(self, ops: Iterable[Op]) -> list[Result]:
        self._ensure_ready()
        grouped, count = group_ops(ops)
        results: list[Result] = [None] * count
        with self.lock:
            if GetOp in grouped:
                self._batch_get(
                    cast(Sequence[tuple[int, GetOp]], grouped[GetOp]), results
                )
            if SearchOp in grouped:
                self._batch_search(
                    cast(Sequence[tuple[int, SearchOp]], grouped[SearchOp]), results
                )
            if ListNamespacesOp in grouped:
                self._batch_list_namespaces(
                    cast(
                        Sequence[tuple[int, ListNamespacesOp]],
                        grouped[ListNamespacesOp],
                    ),
                    results,
                )
            if PutOp in grouped:
                self._batch_put(cast(Sequence[tuple[int, PutOp]], grouped[PutOp]))
        return results

    def _batch_get(
        self,
        ops: Sequence[tuple[int, GetOp]],
        results: list[Result],
    ) -> None:
        for result_index, op in ops:
            item_id = self.store_factory.create_id(namespace=op.namespace, key=op.key)
            item = self.store_repo.get_by_id(item_id)
            if item is None:
                continue
            if (
                op.refresh_ttl
                and self.settings.ttl.enabled
                and item.ttl_minutes is not None
            ):
                expires_at = datetime.now(UTC) + timedelta(minutes=item.ttl_minutes)
                self.store_repo.refresh_expiry(item.id, expires_at)
            results[result_index] = item.to_item()

    def _batch_put(
        self,
        ops: Sequence[tuple[int, PutOp]],
    ) -> None:
        deduplicated: dict[tuple[tuple[str, ...], str], PutOp] = {}
        for _, op in ops:
            deduplicated[(op.namespace, op.key)] = op

        prepared: list[DbStoreItemWithVectorRequests] = []
        for op in deduplicated.values():
            item_id = self.store_factory.create_id(namespace=op.namespace, key=op.key)
            item = self.store_repo.get_by_id(item_id)
            if op.value is None:
                if item is not None:
                    if self._is_index_enabled():
                        self.vector_repo.delete_by_item(item)
                    self.store_repo.delete(item_id)
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

        items_with_vectors = self._generate_vectors(prepared)

        for item_with_vectors in items_with_vectors:
            self.store_repo.upsert(item_with_vectors.item)
            if self._is_index_enabled():
                self.vector_repo.delete_by_item(item_with_vectors.item)
                for vector in item_with_vectors.vectors:
                    self.vector_repo.upsert(vector)

    def _batch_search(
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
            query_vectors = self.embeddings.embed_documents(list(query_texts))

        for query_text, query_vector in zip(query_texts, query_vectors, strict=True):
            queries[hash(query_text)] = SearchQueryIndex(
                text=query_text, embedding=query_vector
            )

        for result_index, op in ops:
            query = queries.get(hash(op.query))
            if op.query and query is None:
                raise ValueError("Query not found in cache: " + op.query)
            result = self.store_repo.search(
                SearchQuery(
                    namespace=list(op.namespace_prefix),
                    query=query,
                    filters=op.filter,
                    limit=op.limit,
                    offset=op.offset,
                )
            )

            if op.refresh_ttl and self.settings.ttl.enabled:
                self._refresh_items(result)
            results[result_index] = [item.to_item() for item in result]

    def _refresh_items(self, items: Sequence[DbStoreItem | DbStoreItemScored]) -> None:
        now = datetime.now(UTC)
        for result in items:
            item = result.item if isinstance(result, DbStoreItemScored) else result
            if item.ttl_minutes is not None:
                self.store_repo.refresh_expiry(
                    item.id, now + timedelta(minutes=item.ttl_minutes)
                )

    def _batch_list_namespaces(
        self,
        ops: Sequence[tuple[int, ListNamespacesOp]],
        results: list[Result],
    ) -> None:
        raw_namespaces = self.store_repo.list_namespaces()
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

    def sweep_ttl(self) -> int:
        self._ensure_ready()
        with self.lock:
            deleted = self.store_repo.sweep_ttl()
            if self._is_index_enabled():
                for item in deleted:
                    self.vector_repo.delete_by_item(item)
            return len(deleted)

    def start_ttl_sweeper(
        self, sweep_interval_minutes: float | None = None
    ) -> concurrent.futures.Future[None]:
        """Periodically delete expired store items based on TTL.

        Returns:
            Future that can be waited on or cancelled.
        """
        if not self.ttl_config:
            future: concurrent.futures.Future[None] = concurrent.futures.Future()
            future.set_result(None)
            return future

        if self._ttl_sweeper_thread and self._ttl_sweeper_thread.is_alive():
            logger.info("TTL sweeper thread is already running")
            # Return a future that can be used to cancel the existing thread
            future = concurrent.futures.Future()
            future.add_done_callback(
                lambda f: self._ttl_stop_event.set() if f.cancelled() else None
            )
            return future

        self._ttl_stop_event.clear()

        interval = float(
            sweep_interval_minutes or self.ttl_config.get("sweep_interval_minutes") or 5
        )
        logger.info(f"Starting store TTL sweeper with interval {interval} minutes")

        future = concurrent.futures.Future()

        def _sweep_loop() -> None:
            try:
                while not self._ttl_stop_event.is_set():
                    if self._ttl_stop_event.wait(interval * 60):
                        break

                    try:
                        expired_items = self.sweep_ttl()
                        if expired_items > 0:
                            logger.info(f"Store swept {expired_items} expired items")
                    except Exception as exc:
                        logger.exception(
                            "Store TTL sweep iteration failed", exc_info=exc
                        )
                future.set_result(None)
            except Exception as exc:
                future.set_exception(exc)

        thread = threading.Thread(target=_sweep_loop, daemon=True, name="ttl-sweeper")
        self._ttl_sweeper_thread = thread
        thread.start()

        future.add_done_callback(
            lambda f: self._ttl_stop_event.set() if f.cancelled() else None
        )
        return future

    def stop_ttl_sweeper(self, timeout: float | None = None) -> bool:
        """Stop the TTL sweeper thread if it's running.

        Args:
            timeout: Maximum time to wait for the thread to stop, in seconds.
                If `None`, wait indefinitely.

        Returns:
            bool: True if the thread was successfully stopped or wasn't running,
                False if the timeout was reached before the thread stopped.
        """
        if not self._ttl_sweeper_thread or not self._ttl_sweeper_thread.is_alive():
            return True

        logger.info("Stopping TTL sweeper thread")
        self._ttl_stop_event.set()

        self._ttl_sweeper_thread.join(timeout)
        success = not self._ttl_sweeper_thread.is_alive()

        if success:
            self._ttl_sweeper_thread = None
            logger.info("TTL sweeper thread stopped")
        else:
            logger.warning("Timed out waiting for TTL sweeper thread to stop")

        return success

    def __del__(self) -> None:
        """Ensure the TTL sweeper thread is stopped when the object is garbage collected."""
        if hasattr(self, "_ttl_stop_event") and hasattr(self, "_ttl_sweeper_thread"):
            self.stop_ttl_sweeper(timeout=0.1)

    def _generate_vectors(
        self, items: list[DbStoreItemWithVectorRequests]
    ) -> list[DbStoreItemWithVectors]:
        if not items or not self.embeddings:
            return [
                DbStoreItemWithVectors(item=item.item, vectors=[]) for item in items
            ]

        texts = [request.text for item in items for request in item.vector_requests]
        embeddings = self.embeddings.embed_documents(texts)
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


def matches_namespace(
    namespace: tuple[str, ...], pattern: tuple[str, ...], match_type: str
) -> bool:
    if len(pattern) > len(namespace):
        return False
    if match_type == "prefix":
        candidate = namespace[: len(pattern)]
    elif match_type == "suffix":
        candidate = namespace[len(namespace) - len(pattern) :] if pattern else ()
    else:
        raise ValueError(f"Unsupported namespace match type: {match_type}")
    return all(
        expected == "*" or expected == actual
        for expected, actual in zip(pattern, candidate, strict=True)
    )


def group_ops(ops: Iterable[Op]) -> tuple[dict[type, list[tuple[int, Op]]], int]:
    grouped: dict[type, list[tuple[int, Op]]] = defaultdict(list)
    count = 0
    for index, op in enumerate(ops):
        grouped[type(op)].append((index, op))
        count += 1
    return grouped, count
