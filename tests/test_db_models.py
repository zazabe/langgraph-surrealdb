from __future__ import annotations

from collections.abc import Generator

import pytest
from langchain_core.runnables import RunnableConfig
from langgraph.checkpoint.base import empty_checkpoint
from langgraph.checkpoint.serde.jsonplus import JsonPlusSerializer

from langgraph_surrealdb.checkpoint.config import CheckpointLookupConfig
from langgraph_surrealdb.database.models.checkpoint import DbCheckpoint
from langgraph_surrealdb.database.models.write import DbWrite


@pytest.fixture(autouse=True)
def cleanup_checkpoint_tables() -> Generator[None, None, None]:
    yield


def test_db_checkpoint_create_without_parent_checkpoint_id() -> None:
    serde = JsonPlusSerializer()
    config: RunnableConfig = {
        "configurable": {
            "thread_id": "thread-1",
            "checkpoint_ns": "ns-1",
        }
    }

    db_checkpoint = DbCheckpoint.create(
        serde=serde,
        config=config,
        checkpoint=empty_checkpoint(),
        metadata={},
    )

    assert db_checkpoint.parent_checkpoint_id == ""
    assert db_checkpoint.to_parent_config() is None


def test_db_checkpoint_create_with_parent_checkpoint_id() -> None:
    serde = JsonPlusSerializer()
    config: RunnableConfig = {
        "configurable": {
            "thread_id": "thread-1",
            "checkpoint_ns": "ns-1",
            "checkpoint_id": "parent-1",
        }
    }

    db_checkpoint = DbCheckpoint.create(
        serde=serde,
        config=config,
        checkpoint=empty_checkpoint(),
        metadata={},
    )

    assert db_checkpoint.parent_checkpoint_id == "parent-1"
    assert db_checkpoint.to_parent_config() == {
        "configurable": {
            "thread_id": "thread-1",
            "checkpoint_ns": "ns-1",
            "checkpoint_id": "parent-1",
        }
    }


def test_db_write_create_and_pending_write_round_trip() -> None:
    serde = JsonPlusSerializer()
    write = DbWrite.create(
        serde=serde,
        thread_id="thread-1",
        checkpoint_ns="ns-1",
        checkpoint_id="cp-1",
        task_id="task-1",
        idx=3,
        channel="messages",
        value={"role": "user", "text": "hello"},
    )

    assert write.thread_id == "thread-1"
    assert write.checkpoint_id == "cp-1"
    assert write.idx == 3
    assert write.to_pending_write(serde) == (
        "task-1",
        "messages",
        {"role": "user", "text": "hello"},
    )


def test_checkpoint_lookup_config_allows_extra_arbitrary_types() -> None:
    class ArbitraryType:
        pass

    arbitrary_type = ArbitraryType()
    config: RunnableConfig = {
        "configurable": {
            "thread_id": "thread-1",
            "checkpoint_ns": "ns-1",
            "arbitrary_type": arbitrary_type,
            "step_count": 3,
        }
    }

    parsed = CheckpointLookupConfig.from_runnable_config(config)

    assert parsed.thread_id == "thread-1"
    assert parsed.checkpoint_ns == "ns-1"
    assert parsed.model_extra is not None
    assert parsed.model_extra["arbitrary_type"] is arbitrary_type
    assert parsed.model_extra["step_count"] == 3
