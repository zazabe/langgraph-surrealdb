"""Run delta-channel conformance capabilities against AsyncSurrealSaver."""

from __future__ import annotations

import pytest

pytest.importorskip(
    "langgraph.checkpoint.conformance",
    reason="langgraph-checkpoint-conformance not installed",
)


@pytest.mark.asyncio
async def test_delta_channel_conformance(settings):
    from langgraph.checkpoint.conformance import validate
    from langgraph.checkpoint.conformance.initializer import checkpointer_test

    from langgraph_surrealdb.checkpoint import AsyncSurrealSaver

    @checkpointer_test(name="AsyncSurrealSaver")
    async def sqlite_saver():
        async with AsyncSurrealSaver.from_settings(settings) as saver:
            yield saver

    report = await validate(sqlite_saver)
    for cap, result in report.results.items():
        if result.passed is False:
            details = "\n".join(result.failures or [])
            pytest.fail(f"Capability {cap} failed:\n{details}")
