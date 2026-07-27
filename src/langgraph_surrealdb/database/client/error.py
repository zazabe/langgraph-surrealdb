import json
from textwrap import dedent
from typing import Any


class SurrealQueryError(Exception):
    """Exception raised when a SurrealDB query raw fails."""

    def __init__(
        self,
        message: str,
        *,
        query: str,
        vars: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.message = message
        self.query = query
        self.vars = vars

    def __str__(self) -> str:
        return dedent(f"""\
            {self.message}
            - query: {self.query}
            - vars: {self.vars}
            """)


class SurrealQueryRawResultError(Exception):
    """Exception raised when a SurrealDB query raw fails."""

    def __init__(
        self,
        message: str,
        *,
        kind: str,
        code: int,
        cause: Any | None = None,
    ) -> None:
        super().__init__(message)
        self.message = message
        self.kind = kind
        self.code = code
        self.cause = cause

    def __str__(self) -> str:
        return f"SurrealDB query raw failed (cause: {self.cause}, code: {self.code}, kind: {self.kind}):\n {self.message}"


class SurrealQueryRawResultItemError(Exception):
    """Exception raised when a SurrealDB query raw item fails."""

    def __init__(
        self,
        result: Any,
        *,
        kind: str | None = None,
    ) -> None:
        if isinstance(result, str):
            message = result
        elif isinstance(result, dict):
            message = json.dumps(result)
        else:
            message = str(result)
        super().__init__(message)
        self.message = message
        self.result = result
        self.kind = kind

    def __str__(self) -> str:
        return f"SurrealDB query raw item failed (kind: {self.kind}):\n {self.message}"
