[![ci](https://github.com/zazabe/langgraph-surrealdb/actions/workflows/ci.yml/badge.svg)](https://github.com/zazabe/langgraph-surrealdb/actions/workflows/ci.yml)
[![PyPI version](https://img.shields.io/pypi/v/langgraph-surrealdb)](https://pypi.org/project/langgraph-surrealdb/)
![License](https://img.shields.io/pypi/l/langgraph-surrealdb)


# LangGraph Checkpoint for SurrealDB

SurrealDB-backed checkpointers for LangGraph.

## Install

```bash
pip install langgraph-surrealdb
```

## Configure SurrealDB

Set these environment variables (prefix: `LANGGRAPH_SURREALDB_`):

```bash
# required for all modes
export LANGGRAPH_SURREALDB_URL="ws://localhost:8000/rpc"
export LANGGRAPH_SURREALDB_NS="langgraph"
export LANGGRAPH_SURREALDB_DB="checkpoint"
export LANGGRAPH_SURREALDB_TABLE_PREFIX="prefix" # optionally add a prefix to checkpoint tables
# root mode
export LANGGRAPH_SURREALDB_AUTH_MODE="root"
export LANGGRAPH_SURREALDB_USER="root"
export LANGGRAPH_SURREALDB_PASSWORD="pass"

# record mode
# export LANGGRAPH_SURREALDB_AUTH_MODE="record"
# export LANGGRAPH_SURREALDB_USER="user"
# export LANGGRAPH_SURREALDB_PASSWORD="pass"
# export LANGGRAPH_SURREALDB_ACCESS="method"

# token mode
# export LANGGRAPH_SURREALDB_AUTH_MODE="token"
# export LANGGRAPH_SURREALDB_TOKEN="xyz"
```

Or create the saver directly from settings:

```python
from langgraph_surrealdb import (
    AsyncSurrealSaver,
    RootAuth,
    SurrealSaver,
    SurrealSaverSettings,
    SurrealSaverDatabaseSettings,
)

settings = SurrealSaverSettings(
    writes_table="writes", # optional, defaults to 'writes'
    checkpoints_table="checkpoints", # optional, defaults to 'checkpoints'
    db=SurrealSaverDatabaseSettings(
        url="ws://localhost:8000/rpc",
        namespace="langgraph",
        database="checkpoint",
        # supports RootAuth, TokenAuth and RecordAuth
        auth=RootAuth(
            username="root",
            password="root"
        )
    )
)

with SurrealSaver.from_settings(settings) as checkpointer:
    ...

async with AsyncSurrealSaver.from_settings(settings) as checkpointer:
    ...
```

## Initialize schema

> [!IMPORTANT]
> When using SurrealDB checkpointers for the first time, call a setup method
> to create required tables and indexes before using saver operations.

```python
# one-time setup
with SurrealSaver.from_env() as checkpointer:
    checkpointer.setup()

# async equivalent
async with AsyncSurrealSaver.from_env() as checkpointer:
    await checkpointer.setup()
```

## Use with LangGraph (sync)

```python
from langgraph.graph import StateGraph
from langgraph_surrealdb import SurrealSaver

# build your graph
builder = StateGraph(dict)
# ... add nodes and edges ...

with SurrealSaver.from_env() as checkpointer:
    checkpointer.setup()
    graph = builder.compile(checkpointer=checkpointer)
    result = graph.invoke(
        {"input": "hello"},
        config={"configurable": {"thread_id": "thread-1"}},
    )
```

## Use with LangGraph (async)

```python
from langgraph.graph import StateGraph
from langgraph_surrealdb import AsyncSurrealSaver

builder = StateGraph(dict)
# ... add nodes and edges ...

async with AsyncSurrealSaver.from_env() as checkpointer:
    await checkpointer.setup()
    graph = builder.compile(checkpointer=checkpointer)
    result = await graph.ainvoke(
        {"input": "hello"},
        config={"configurable": {"thread_id": "thread-1"}},
    )
```

Then reuse the same `thread_id` to resume conversation state across calls.

## Use the asynchronous store

`AsyncSurrealStore` implements LangGraph's cross-thread store API, including
structured filters, namespace listing, TTL, and optional semantic search.

```python
from langgraph_surrealdb import AsyncSurrealStore

async with AsyncSurrealStore.from_env(
    ttl={"default_ttl": 60, "refresh_on_read": True},
) as store:
    await store.setup()
    await store.aput(
        ("users", "user-1"),
        "preferences",
        {"theme": "dark"},
    )
    item = await store.aget(("users", "user-1"), "preferences")
```

Pass an embedding configuration to enable semantic search:

```python
async with AsyncSurrealStore.from_env(
    index={
        "dims": 1536,
        "embed": embeddings,
        "fields": ["text"],
    },
) as store:
    await store.setup()
    results = await store.asearch(("documents",), query="deployment guide")
```
