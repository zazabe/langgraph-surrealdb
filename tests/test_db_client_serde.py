from __future__ import annotations

import pytest
from pydantic import ValidationError

from langgraph_surrealdb.database.client.error import (
    SurrealQueryRawResultError,
    SurrealQueryRawResultItemError,
)
from langgraph_surrealdb.database.client.serde import QueryRawResult
from langgraph_surrealdb.database.models.write import DbWrite


def test_check_returns_successful_results() -> None:
    response = QueryRawResult[int].model_validate(
        {
            "id": "request-1",
            "result": [
                {"status": "OK", "result": 1},
                {"status": "OK", "result": 2},
            ],
        }
    )

    checked = response.check()
    first = checked.first()
    last = checked.last()

    assert checked.id == "request-1"
    assert [item.result for item in checked.items()] == [1, 2]
    assert first is not None
    assert first.result == 1
    assert last is not None
    assert last.result == 2
    assert checked.len() == 2


def test_check_raises_for_rpc_error() -> None:
    response = QueryRawResult[object].model_validate(
        {
            "id": "request-1",
            "error": {
                "code": -32000,
                "kind": "RpcError",
                "message": "query failed",
                "cause": "database unavailable",
            },
        }
    )

    with pytest.raises(SurrealQueryRawResultError) as exc_info:
        response.check()

    assert exc_info.value.code == -32000
    assert exc_info.value.kind == "RpcError"
    assert exc_info.value.cause == "database unavailable"


def test_check_raises_for_statement_error() -> None:
    response = QueryRawResult[object].model_validate(
        {
            "id": "request-1",
            "result": [
                {
                    "status": "ERR",
                    "result": "The table 'missing' does not exist",
                    "kind": "QueryError",
                }
            ],
        }
    )

    with pytest.raises(SurrealQueryRawResultItemError) as exc_info:
        response.check()

    assert exc_info.value.result == "Item 0 is invalid: The table 'missing' does not exist"
    assert exc_info.value.kind == "QueryError"


def test_check_as_validates_result_type() -> None:
    response = QueryRawResult[object].model_validate(
        {
            "id": "request-1",
            "result": [{"status": "OK", "result": ["1", 2]}],
        }
    )

    checked = response.check_as(result_type=list[int])
    first = checked.first()
    assert first is not None
    assert first.result == [1, 2]


def test_check_as_passes_validation_context_to_nested_models() -> None:
    response = QueryRawResult[object].model_validate(
        {
            "id": "request-1",
            "result": [
                {
                    "status": "OK",
                    "result": [
                        {
                            "id": "writes:write-1",
                            "thread_id": "thread-1",
                            "checkpoint_ns": "",
                            "checkpoint_id": "checkpoint-1",
                            "task_id": "task-1",
                            "idx": 0,
                            "channel": "messages",
                            "type": "json",
                            "value": b"{}",
                        }
                    ],
                }
            ],
        }
    )

    checked = response.check_as(
        result_type=list[DbWrite],
        validation_context={"expected_table": "writes"},
    )
    first = checked.first()
    assert first is not None
    assert first.result[0].id.table == "writes"

    with pytest.raises(ValidationError, match="must use table 'other_writes'"):
        response.check_as(
            result_type=list[DbWrite],
            validation_context={"expected_table": "other_writes"},
        )
