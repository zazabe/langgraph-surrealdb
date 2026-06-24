# LangGraph Checkpoint for SurrealDB

SurrealDB-backed checkpointers for LangGraph.

## Install

```bash
pip install langgraph-surrealdb
```

## Configure SurrealDB

Set these environment variables:

```bash
export SURREAL_URL="ws://localhost:8000/rpc"
export SURREAL_NS="langgraph"
export SURREAL_DB="checkpoint"
export SURREAL_USER="root"
export SURREAL_PASS="root"
export SURREAL_TOKEN=""
```

Or create the saver directly from settings:

```python
from langgraph_surrealdb import AsyncSurrealSaver, SurrealSaver, SurrealConnSettings

settings = SurrealConnSettings(
    url="ws://localhost:8000/rpc",
    namespace="langgraph",
    database="checkpoint",
    username="root",
    password="root",
    token=None,
)

with SurrealSaver.from_settings(settings) as checkpointer:
    ...

async with AsyncSurrealSaver.from_settings(settings) as checkpointer:
    ...
```

## Use with LangGraph (sync)

```python
from langgraph.graph import StateGraph
from langgraph_surrealdb import SurrealSaver

# build your graph
builder = StateGraph(dict)
# ... add nodes and edges ...

with SurrealSaver.from_env() as checkpointer:
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
    graph = builder.compile(checkpointer=checkpointer)
    result = await graph.ainvoke(
        {"input": "hello"},
        config={"configurable": {"thread_id": "thread-1"}},
    )
```

Then reuse the same `thread_id` to resume conversation state across calls.
