[![ci](https://github.com/zazabe/langgraph-surrealdb/actions/workflows/ci.yml/badge.svg)](https://github.com/zazabe/langgraph-surrealdb/actions/workflows/ci.yml)
[![PyPI version](https://img.shields.io/pypi/v/langgraph-surrealdb)](https://pypi.org/project/langgraph-surrealdb/)
![License](https://img.shields.io/pypi/l/langgraph-surrealdb)


# LangGraph SurrealDB

SurrealDB-backed checkpoint persistence and cross-thread stores for LangGraph,
with synchronous and asynchronous APIs.

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
export LANGGRAPH_SURREALDB_TABLE_PREFIX="prefix" # optionally add a prefix to checkpoint/store tables
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

The database configuration is shared by checkpoint and store integrations.

## Checkpoint

Persist LangGraph thread state with `SurrealSaver` or `AsyncSurrealSaver`.

### Configure from settings

```python
from langgraph_surrealdb import (
    AsyncSurrealSaver,
    RootAuth,
    SurrealCheckpointSettings,
    SurrealDatabaseSettings,
    SurrealSaver,
)

settings = SurrealCheckpointSettings(
    writes_table="writes",  # optional
    checkpoints_table="checkpoints",  # optional
    db=SurrealDatabaseSettings(
        url="ws://localhost:8000/rpc",
        namespace="langgraph",
        database="checkpoint",
        auth=RootAuth(username="root", password="root"),
    ),
)

with SurrealSaver.from_settings(settings) as checkpointer:
    ...

async with AsyncSurrealSaver.from_settings(settings) as checkpointer:
    ...
```

### Initialize schema

> [!IMPORTANT]
> Call `setup()` once before using a new checkpoint schema.

```python
# one-time setup
with SurrealSaver.from_env() as checkpointer:
    checkpointer.setup()

# async equivalent
async with AsyncSurrealSaver.from_env() as checkpointer:
    await checkpointer.setup()
```

### Use with LangGraph (sync)

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

### Use with LangGraph (async)

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

## Store

Persist cross-thread memories with `SurrealStore` or `AsyncSurrealStore`.
Both support structured filters, namespace listing, optional TTL, and optional
semantic search.

Store-specific environment variables use the
`LANGGRAPH_SURREALDB_STORE_` prefix:

```bash
export LANGGRAPH_SURREALDB_STORE_INDEX_ENABLED="true"
export LANGGRAPH_SURREALDB_STORE_INDEX_DIMENSIONS="1536"
export LANGGRAPH_SURREALDB_STORE_INDEX_FIELDS='["text"]'
export LANGGRAPH_SURREALDB_STORE_INDEX_DISTANCE_TYPE="cosine"

export LANGGRAPH_SURREALDB_STORE_TTL_ENABLED="true"
export LANGGRAPH_SURREALDB_STORE_TTL_DEFAULT_TTL="60"
export LANGGRAPH_SURREALDB_STORE_TTL_REFRESH_ON_READ="true"
export LANGGRAPH_SURREALDB_STORE_TTL_SWEEP_INTERVAL_MINUTES="5"
```

Indexing and TTL are disabled by default.

### Configure from settings

```python
from langgraph_surrealdb import (
    RootAuth,
    SurrealDatabaseSettings,
    SurrealStore,
    SurrealStoreIndexSettings,
    SurrealStoreSettings,
    SurrealStoreTTLSettings,
)

settings = SurrealStoreSettings(
    store_table="store",  # optional
    index=SurrealStoreIndexSettings(
        enabled=True,
        dimensions=1536,
        fields=["text"],
    ),
    ttl=SurrealStoreTTLSettings(
        enabled=True,
        default_ttl=60,
        refresh_on_read=True,
        sweep_interval_minutes=5,
    ),
    db=SurrealDatabaseSettings(
        url="ws://localhost:8000/rpc",
        namespace="langgraph",
        database="checkpoint",
        auth=RootAuth(username="root", password="root"),
    ),
)

with SurrealStore.from_settings(settings, embed=embeddings) as store:
    store.setup()
```

The vector table name is derived from `store_table` as
`<store_table>_vector`. Pass a LangChain `Embeddings` implementation, an
embedding function, or a provider string through `embed` when semantic search
is enabled.

### Initialize schema

> [!IMPORTANT]
> Call `setup()` once before using a new store schema. Vector tables and indexes
> are created only when semantic search is enabled.

```python
with SurrealStore.from_env(embed=embeddings) as store:
    store.setup()

async with AsyncSurrealStore.from_env(embed=embeddings) as store:
    await store.setup()
```

### Use the store (sync)

```python
from langgraph_surrealdb import SurrealStore

with SurrealStore.from_env() as store:
    store.setup()
    store.put(
        ("users", "user-1"),
        "preferences",
        {"theme": "dark"},
    )
    item = store.get(("users", "user-1"), "preferences")
    results = store.search(("users",), filter={"theme": "dark"})
```

### Use the store (async)

```python
from langgraph_surrealdb import AsyncSurrealStore

async with AsyncSurrealStore.from_env() as store:
    await store.setup()
    await store.aput(
        ("users", "user-1"),
        "preferences",
        {"theme": "dark"},
    )
    item = await store.aget(("users", "user-1"), "preferences")
```

### Semantic search

Semantic search uses hybrid retrieval, combining vector similarity with
full-text search and reciprocal rank fusion to rank the results.

```python
async with AsyncSurrealStore.from_env(embed=embeddings) as store:
    await store.setup()
    await store.aput(
        ("documents",),
        "deployment",
        {"text": "How to deploy the service"},
    )
    results = await store.asearch(("documents",), query="deployment guide")
```

Use `index=False` on `put`/`aput` to keep an item out of semantic search, or
pass a list of field paths to override the configured fields for that item.

### Use with LangGraph

```python
from langgraph.graph import StateGraph
from langgraph_surrealdb import SurrealStore

builder = StateGraph(dict)
# ... add nodes and edges ...

with SurrealStore.from_env() as store:
    store.setup()
    graph = builder.compile(store=store)
    result = graph.invoke(
        {"input": "hello"},
        config={"configurable": {"thread_id": "thread-1"}},
    )
```
