"""Test SurrealSaver.

_ Note: Test copied from langgraph-checkpoint-sqlite and adjusted for SurrealDB._
"""

from typing import Any, cast

import pytest
from conftest import get_surreal_settings
from langchain_core.runnables import RunnableConfig
from langgraph.checkpoint.base import (
    Checkpoint,
    CheckpointMetadata,
    create_checkpoint,
    empty_checkpoint,
)

from langgraph_surrealdb.checkpoint import SurrealSaver
from langgraph_surrealdb.database.repository.checkpoints import _search_where


def test_dump():

    from langgraph.checkpoint.base import CheckpointMetadata

    from langgraph_surrealdb.database.models.checkpoint import (
        DbCheckpoint,
        DbCheckpointId,
    )

    metadata = CheckpointMetadata(
        source="input",
        step=0,
        parents={},
        run_id="",
        counters_since_delta_snapshot={},
        score=None,
    )
    a = DbCheckpoint(
        id=DbCheckpointId("checkpoints:abc"),
        thread_id="",
        checkpoint=b"",
        checkpoint_id="",
        checkpoint_ns="",
        metadata=metadata,
        parent_checkpoint_id="",
        type="",
    )

    print(a.model_dump())


class TestSurrealSaver:
    @pytest.fixture(autouse=True)
    def setup(self) -> None:
        self.settings = get_surreal_settings()
        # objects for test setup
        self.config_1: RunnableConfig = {
            "configurable": {
                "thread_id": "thread-1",
                # for backwards compatibility testing
                "checkpoint_id": "1",
                "checkpoint_ns": "",
            }
        }
        self.config_2: RunnableConfig = {
            "configurable": {
                "thread_id": "thread-2",
                "checkpoint_id": "2",
                "checkpoint_ns": "",
            }
        }
        self.config_3: RunnableConfig = {
            "configurable": {
                "thread_id": "thread-2",
                "checkpoint_id": "2-inner",
                "checkpoint_ns": "inner",
            }
        }

        self.chkpnt_1: Checkpoint = empty_checkpoint()
        self.chkpnt_2: Checkpoint = create_checkpoint(self.chkpnt_1, {}, 1)
        self.chkpnt_3: Checkpoint = empty_checkpoint()

        self.metadata_1: CheckpointMetadata = {
            "source": "input",
            "step": 2,
            "writes": {},
            "score": 1,
        }
        self.metadata_2: CheckpointMetadata = {
            "source": "loop",
            "step": 1,
            "writes": {"foo": "bar"},
        }
        self.metadata_3: CheckpointMetadata = {}

    def test_combined_metadata(self) -> None:
        with SurrealSaver.from_settings(self.settings) as saver:
            config: RunnableConfig = {
                "configurable": {
                    "thread_id": "thread-2",
                    "checkpoint_ns": "",
                    "__super_private_key": "super_private_value",
                },
                "metadata": {"run_id": "my_run_id"},
            }
            saver.put(config, self.chkpnt_2, self.metadata_2, {})
            checkpoint = saver.get_tuple(config)
            assert checkpoint is not None and checkpoint.metadata == {
                **self.metadata_2,
                "run_id": "my_run_id",
            }

    def test_search(self) -> None:
        with SurrealSaver.from_settings(self.settings) as saver:
            # set up test
            # save checkpoints
            saver.put(self.config_1, self.chkpnt_1, self.metadata_1, {})
            saver.put(self.config_2, self.chkpnt_2, self.metadata_2, {})
            saver.put(self.config_3, self.chkpnt_3, self.metadata_3, {})

            # call method / assertions
            query_1 = {"source": "input"}  # search by 1 key
            query_2 = {
                "step": 1,
                "writes": {"foo": "bar"},
            }  # search by multiple keys
            # search by no keys, return all checkpoints
            query_3: dict[str, Any] = {}
            query_4 = {"source": "update", "step": 1}  # no match

            search_results_1 = list(saver.list(None, filter=query_1))
            assert len(search_results_1) == 1
            assert search_results_1[0].metadata == self.metadata_1

            search_results_2 = list(saver.list(None, filter=query_2))
            assert len(search_results_2) == 1
            assert search_results_2[0].metadata == self.metadata_2

            search_results_3 = list(saver.list(None, filter=query_3))
            assert len(search_results_3) == 3

            search_results_4 = list(saver.list(None, filter=query_4))
            assert len(search_results_4) == 0

            # search by config (defaults to checkpoints across all namespaces)
            search_results_5 = list(
                saver.list({"configurable": {"thread_id": "thread-2"}})
            )
            assert len(search_results_5) == 2
            assert {
                search_results_5[0].config["configurable"]["checkpoint_ns"],
                search_results_5[1].config["configurable"]["checkpoint_ns"],
            } == {"", "inner"}

            # search with before param
            search_results_6 = list(saver.list(None, before=search_results_5[1].config))
            assert len(search_results_6) == 1
            assert search_results_6[0].config["configurable"]["thread_id"] == "thread-1"

            # search with limit param
            search_results_7 = list(
                saver.list({"configurable": {"thread_id": "thread-2"}}, limit=1)
            )
            assert len(search_results_7) == 1
            assert search_results_7[0].config["configurable"]["thread_id"] == "thread-2"

    def test_search_where(self) -> None:
        # call method / assertions
        expected_predicate_1 = "`checkpoint_id` < $before_checkpoint_id AND `metadata`.`source` = $m0 AND `metadata`.`step` = $m1 AND `metadata`.`writes` = $m2 AND `metadata`.`score` = $m3"
        expected_param_values_1 = {
            "before_checkpoint_id": "1",
            "m0": "input",
            "m1": 2,
            "m2": {},
            "m3": 1,
        }
        where, params = _search_where(
            None, None, None, cast(dict[str, Any], self.metadata_1), "1"
        )
        assert where == expected_predicate_1 and params == expected_param_values_1

    async def test_informative_async_errors(self) -> None:
        with SurrealSaver.from_settings(self.settings) as saver:
            # call method / assertions
            with pytest.raises(NotImplementedError, match="AsyncSurrealSaver"):
                await saver.aget(self.config_1)
            with pytest.raises(NotImplementedError, match="AsyncSurrealSaver"):
                await saver.aget_tuple(self.config_1)
            with pytest.raises(NotImplementedError, match="AsyncSurrealSaver"):
                async for _ in saver.alist(self.config_1):
                    pass

    def test_checkpoint_search_sql_injection_prevention(self) -> None:
        """Test that SQL injection via malicious filter keys is prevented in checkpoint search."""
        with SurrealSaver.from_settings(self.settings) as saver:
            # Setup: Create checkpoints with different metadata
            config_public: RunnableConfig = {
                "configurable": {
                    "thread_id": "thread-public",
                    "checkpoint_ns": "",
                }
            }
            config_private: RunnableConfig = {
                "configurable": {
                    "thread_id": "thread-private",
                    "checkpoint_ns": "",
                }
            }

            checkpoint_public = empty_checkpoint()
            checkpoint_private = empty_checkpoint()

            metadata_public: CheckpointMetadata = {
                "access": "public",
                "data": "public information",
            }
            metadata_private: CheckpointMetadata = {
                "access": "private",
                "data": "secret information",
                "password": "secret123",
            }

            saver.put(config_public, checkpoint_public, metadata_public, {})
            saver.put(config_private, checkpoint_private, metadata_private, {})

            # Normal query - should return only public checkpoint
            normal_results = list(saver.list(None, filter={"access": "public"}))
            assert len(normal_results) == 1
            assert normal_results[0].metadata["access"] == "public"

            # SQL injection attempt should raise ValueError
            malicious_key = (
                "access') = 'public' OR '1'='1' OR json_extract(metadata, '$."
            )

            with pytest.raises(ValueError, match="Invalid filter key"):
                list(saver.list(None, filter={malicious_key: "dummy"}))

    def test_limit_parameter_sql_injection_prevention(self) -> None:
        """Test that the limit parameter properly uses parameterized queries to prevent SQL injection."""
        with SurrealSaver.from_settings(self.settings) as saver:
            # Setup: Create multiple checkpoints
            for i in range(5):
                config: RunnableConfig = {
                    "configurable": {
                        "thread_id": f"thread-{i}",
                        "checkpoint_ns": "",
                    }
                }
                checkpoint = empty_checkpoint()
                metadata: CheckpointMetadata = {"index": i}
                saver.put(config, checkpoint, metadata, {})

            # Test that limit works correctly with valid integer
            results = list(saver.list(None, limit=2))
            assert len(results) == 2

            # Test that limit=0 returns no results
            results = list(saver.list(None, limit=0))
            assert len(results) == 0

            # Test that limit=None returns all results
            results = list(saver.list(None, limit=None))
            assert len(results) == 5

    def test_metadata_filter_keys_with_hyphens_and_digits(self) -> None:
        """Metadata keys with hyphens and digit-start should be filterable.

        This exposes incorrect JSON path handling (unquoted segments) by asserting
        that such filters successfully match saved checkpoints.
        """
        with SurrealSaver.from_settings(self.settings) as saver:
            config: RunnableConfig = {
                "configurable": {
                    "thread_id": "thread-hyphen-digit",
                    "checkpoint_ns": "",
                }
            }
            checkpoint = empty_checkpoint()
            metadata: CheckpointMetadata = {
                "access-level": "public",
                "user": {"access-level": "nested", "123abc": "ok2"},
                "123abc": "ok",
            }
            saver.put(config, checkpoint, metadata, {})

            # Top-level hyphenated key
            results = list(saver.list(None, filter={"access-level": "public"}))
            assert len(results) == 1

            # Nested hyphenated key via dotted path
            results = list(saver.list(None, filter={"user.access-level": "nested"}))
            assert len(results) == 1

            # Top-level digit-starting key
            results = list(saver.list(None, filter={"123abc": "ok"}))
            assert len(results) == 1

            # Nested digit-starting key via dotted path
            results = list(saver.list(None, filter={"user.123abc": "ok2"}))
            assert len(results) == 1
