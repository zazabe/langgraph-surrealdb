import time

from langgraph.store.base import GetOp, PutOp

from langgraph_surrealdb import SurrealStoreIndexSettings, SurrealStoreTTLSettings
from tests.fixtures.store import CharacterEmbeddings, StoreFactory


async def test_crud_search_and_batch_order(store_factory: StoreFactory) -> None:
    async with store_factory() as store:
        store.put(("users", "one"), "a", {"name": "Alice", "score": 2})
        store.put(("users", "two"), "b", {"name": "Bob", "score": 4})

        item = store.get(("users", "one"), "a")
        assert item is not None
        assert item.value == {"name": "Alice", "score": 2}
        store.put(("users", "one"), "a", {"name": "Alice", "score": 3})
        updated = store.get(("users", "one"), "a")
        assert updated is not None
        assert updated.created_at == item.created_at
        assert updated.updated_at >= item.updated_at

        results = store.search(("users",), filter={"score": {"$gte": 3}})
        assert {result.key for result in results} == {"a", "b"}

        batch_results = store.batch(
            [
                PutOp(("batch",), "new", {"value": 1}),
                GetOp(("batch",), "new"),
            ]
        )
        assert batch_results == [None, None]
        assert store.get(("batch",), "new") is not None

        store.delete(("users", "one"), "a")
        assert store.get(("users", "one"), "a") is None


async def test_namespace_matching_and_truncation(
    store_factory: StoreFactory,
) -> None:
    async with store_factory() as store:
        namespaces = [
            ("tenant", "docs", "public"),
            ("tenant", "docs", "private"),
            ("tenant", "images", "public"),
            ("other", "docs", "public"),
        ]
        for index, namespace in enumerate(namespaces):
            store.put(namespace, str(index), {"index": index})

        assert store.list_namespaces(prefix=("tenant", "docs")) == [
            ("tenant", "docs", "private"),
            ("tenant", "docs", "public"),
        ]
        assert store.list_namespaces(prefix=("tenant", "*"), suffix=("public",)) == [
            ("tenant", "docs", "public"),
            ("tenant", "images", "public"),
        ]
        assert store.list_namespaces(prefix=("tenant",), max_depth=2) == [
            ("tenant", "docs"),
            ("tenant", "images"),
        ]


async def test_ttl_expiry_and_refresh(store_factory: StoreFactory) -> None:
    ttl_settings = SurrealStoreTTLSettings(enabled=True, refresh_on_read=True)
    async with store_factory(ttl=ttl_settings) as store:
        ttl = 0.01
        store.put(("ttl",), "item", {"value": 1}, ttl=ttl)
        time.sleep(0.35)
        assert store.get(("ttl",), "item", refresh_ttl=True) is not None
        time.sleep(0.35)
        assert store.get(("ttl",), "item", refresh_ttl=False) is not None
        time.sleep(0.35)
        assert store.get(("ttl",), "item", refresh_ttl=False) is not None
        assert store.sweep_ttl() == 1
        assert store.get(("ttl",), "item", refresh_ttl=False) is None


async def test_ttl_sweeper_deletes_expired_items(
    store_factory: StoreFactory,
) -> None:
    ttl_settings = SurrealStoreTTLSettings(enabled=True, refresh_on_read=False)
    async with store_factory(ttl=ttl_settings) as store:
        store.put(("ttl-sweeper",), "item", {"value": 1}, ttl=1 / 60)
        assert store.get(("ttl-sweeper",), "item", refresh_ttl=False) is not None

        sweeper = store.start_ttl_sweeper(sweep_interval_minutes=0.001)
        try:
            deadline = time.monotonic() + 2
            while time.monotonic() < deadline:
                if store.get(
                    ("ttl-sweeper",), "item", refresh_ttl=False
                ) is None:
                    break
                time.sleep(0.02)

            assert store.get(("ttl-sweeper",), "item", refresh_ttl=False) is None
        finally:
            assert store.stop_ttl_sweeper(timeout=1)
            sweeper.result(timeout=1)


async def test_vector_search(
    store_factory: StoreFactory,
) -> None:
    index = SurrealStoreIndexSettings(
        enabled=True,
        dimensions=32,
        fields=["text"],
        distance_type="cosine",
    )
    embed = CharacterEmbeddings(dims=32)

    async with store_factory(index=index, embed=embed) as store:
        store.put(("docs",), "apple", {"text": "red apple"})
        store.put(("docs",), "car", {"text": "blue car"})
        results = store.search(("docs",), query="apple")
        assert [result.key for result in results] == ["apple", "car"]
        assert results[0].score is not None
        assert results[1].score is not None
        assert results[0].score > results[1].score

        store.put(("docs",), "apple", {"text": "train engine"})
        updated = store.search(("docs",), query="apple")
        assert updated[0].key == "car"

        store.put(("docs",), "hidden", {"text": "apple apple apple"}, index=False)
        vector_results = store.search(("docs",), query="apple")
        assert all(result.key != "hidden" for result in vector_results)
        ordinary_results = store.search(("docs",))
        assert any(result.key == "hidden" for result in ordinary_results)
