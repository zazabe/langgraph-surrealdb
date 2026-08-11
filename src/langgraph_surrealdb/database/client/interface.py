from typing import Any, Protocol, overload

from pydantic import ValidationError
from surrealdb import SurrealError
from surrealdb.data.types.record_id import RecordIdType
from surrealdb.types import Value

from langgraph_surrealdb.database.client.error import SurrealQueryError
from langgraph_surrealdb.database.client.serde import QueryRawResult
from langgraph_surrealdb.database.models.common import (
    DbRecordId,
    SurrealModel,
    SurrealRecordId,
    TSurrealModel,
)


def _normalize_vars(
    vars: dict[str, Any] | None,
) -> dict[str, Value] | None:
    if vars is None:
        return None
    return {str(k): _normalize_var(v) for k, v in vars.items()}


def _normalize_var(vars: Any) -> Value:
    if isinstance(vars, DbRecordId):
        return vars.record_id
    if isinstance(vars, SurrealModel):
        return vars.record_id
    if isinstance(vars, list):
        return [_normalize_var(item) for item in vars]
    if isinstance(vars, dict):
        return {str(k): _normalize_var(v) for k, v in vars.items()}
    return vars


def _build_fn_args(vars: dict[str, Any] | None) -> tuple[str, dict[str, Value]]:
    if vars is None:
        return "", {}
    args = []
    params = {}
    for k, v in vars.items():
        if v is not None:
            args.append(f"${k}")
            params[k] = _normalize_var(v)
        else:
            args.append("NONE")
    return ", ".join(args), params


class SurrealConnectionProtocol(Protocol):
    def query(
        self,
        query: str,
        vars: dict[str, Value] | None = None,
    ) -> Value: ...

    def query_raw(
        self,
        query: str,
        params: dict[str, Value] | None = None,
    ) -> dict[str, Any]: ...

    def select(
        self,
        record: RecordIdType,
    ) -> Any: ...

    def upsert(
        self,
        record: RecordIdType,
        data: Value | None = None,
    ) -> Value: ...

    def delete(self, record: RecordIdType) -> Value: ...

    def close(self) -> None: ...


class SurrealConnection:
    _conn: SurrealConnectionProtocol

    def __init__(
        self,
        conn: SurrealConnectionProtocol,
        validation_context: dict[str, str] | None = None,
    ):
        """
        Create a new connection.

        Args:
            conn: The connection.
            validation_context: Pydantic validation context.

        Returns:
            The new connection.
        """
        self._conn = conn
        self._validation_context = validation_context

    def query_raw[T: Any](
        self,
        query: str,
        vars: dict[str, Any] | None = None,
    ) -> QueryRawResult[T]:
        params = _normalize_vars(vars)
        try:
            raw = self._conn.query_raw(query, params=params)
            return QueryRawResult[Any].model_validate(raw)
        except SurrealError as e:
            raise SurrealQueryError(
                message="SurrealDB query raw failed",
                query=query,
                vars=vars,
            ) from e

    @overload
    def query(
        self,
        query: str,
        vars: dict[str, Any] | None = None,
    ) -> Any: ...

    @overload
    def query[T: Any](
        self,
        query: str,
        vars: dict[str, Any] | None = None,
        *,
        result_type: type[T],
    ) -> T: ...

    @overload
    def query[T: Any](
        self,
        query: str,
        vars: dict[str, Any] | None = None,
        *,
        result_type: object,
    ) -> Any: ...

    def query[T: Any](
        self,
        query: str,
        vars: dict[str, Any] | None = None,
        *,
        result_type: object | None = None,
    ) -> Any:
        """
        Execute a single query and return the result.

        Args:
            query: The query to execute.
            vars: The variables to substitute into the query.
            result_type: The type of the result.

        Returns:
            The result of the query.
        """
        try:
            vars = _normalize_vars(vars)
            response = self.query_raw(
                query,
                vars=vars,
            )
            if result_type is None:
                checked = response.check()
            else:
                checked = response.check_as(
                    result_type=result_type, validation_context=self._validation_context
                )
            first = checked.first()
            if first is None:
                raise SurrealQueryError(
                    message="Query returned no results",
                    query=query,
                    vars=vars,
                )
            return first.result
        except ValidationError as e:
            raise SurrealQueryError(
                response=response.model_dump(),
                message="Invalid query result type",
                query=query,
                vars=vars,
            ) from e
        except Exception as e:
            raise SurrealQueryError(
                message=f"SurrealDB query failed: {str(e)}",
                query=query,
                vars=vars,
            ) from e

    def select(
        self,
        record: SurrealRecordId[TSurrealModel],
    ) -> TSurrealModel | None:
        """
        Select a record and return the result.

        Args:
            record: The record to select.

        Returns:
            The result of the query.
        """
        return self.query(
            "SELECT * FROM ONLY $record_id",
            vars={"record_id": record.record_id},
            result_type=record.record_type | None,
        )

    def upsert(
        self,
        record: TSurrealModel,
    ) -> TSurrealModel | None:
        """
        Upsert a record and return the before state.

        Args:
            record: The record to upsert.

        Returns:
            The before state of the record.
        """
        result = self.query(
            "UPSERT ONLY $record_id CONTENT $_content RETURN BEFORE",
            vars={"record_id": record.record_id, "_content": record.model_dump()},
            result_type=record.record_type | None,
        )
        return result

    def delete(
        self,
        record: SurrealRecordId[TSurrealModel],
    ) -> TSurrealModel | None:
        """
        Delete a record and return the before state.

        Args:
            record: The record to delete.

        Returns:
            The before state of the record.
        """
        return self.query(
            "DELETE ONLY $record_id RETURN BEFORE",
            vars={"record_id": record.record_id},
            result_type=record.record_type | None,
        )

    @overload
    def call(
        self,
        function: str,
        vars: dict[str, Any] | None = None,
    ) -> Any: ...

    @overload
    def call[T: Any](
        self,
        function: str,
        vars: dict[str, Any] | None = None,
        *,
        result_type: type[T],
    ) -> T: ...

    @overload
    def call[T: Any](
        self,
        function: str,
        vars: dict[str, Any] | None = None,
        *,
        result_type: object,
    ) -> Any: ...

    def call[T: Any](
        self,
        function: str,
        vars: dict[str, Any] | None = None,
        *,
        result_type: object | None = None,
    ) -> Any:
        args, vars = _build_fn_args(vars)
        return self.query(
            f"RETURN {function}({args})",
            vars=vars,
            result_type=result_type,
        )

    def close(self) -> None:
        return self._conn.close()


class SurrealAsyncConnectionProtocol(Protocol):
    async def query(
        self,
        query: str,
        vars: dict[str, Value] | None = None,
    ) -> Value: ...

    async def query_raw(
        self,
        query: str,
        params: dict[str, Value] | None = None,
    ) -> dict[str, Any]: ...

    async def select(
        self,
        record: RecordIdType,
    ) -> Any: ...

    async def upsert(
        self,
        record: RecordIdType,
        data: Value | None = None,
    ) -> Value: ...

    async def delete(
        self,
        record: RecordIdType,
    ) -> Value: ...

    async def close(self) -> None: ...


class SurrealAsyncConnection:
    _conn: SurrealAsyncConnectionProtocol

    def __init__(
        self,
        conn: SurrealAsyncConnectionProtocol,
        validation_context: dict[str, str] | None = None,
    ):
        """
        Create a new connection.

        Args:
            conn: The connection.
            validation_context: Pydantic validation context.

        Returns:
            The new connection.
        """
        self._validation_context = validation_context
        self._conn = conn

    async def query_raw[T: Any](
        self,
        query: str,
        vars: dict[str, Any] | None = None,
    ) -> QueryRawResult[T]:
        params = _normalize_vars(vars)
        try:
            raw = await self._conn.query_raw(query, params=params)
            return QueryRawResult[Any].model_validate(raw)
        except SurrealError as e:
            raise SurrealQueryError(
                message="SurrealDB query raw failed",
                query=query,
                vars=vars,
            ) from e

    @overload
    async def query(
        self,
        query: str,
        vars: dict[str, Any] | None = None,
    ) -> Any: ...

    @overload
    async def query[T: Any](
        self,
        query: str,
        vars: dict[str, Any] | None = None,
        *,
        result_type: type[T],
    ) -> T: ...

    @overload
    async def query[T: Any](
        self,
        query: str,
        vars: dict[str, Any] | None = None,
        *,
        result_type: object,
    ) -> Any: ...

    async def query[T: Any](
        self,
        query: str,
        vars: dict[str, Any] | None = None,
        *,
        result_type: object | None = None,
    ) -> Any:
        """
        Execute a single query and return the result.

        Args:
            query: The query to execute.
            vars: The variables to substitute into the query.

        Returns:
            The result of the query.
        """
        try:
            vars = _normalize_vars(vars)
            response = await self.query_raw(
                query,
                vars=vars,
            )
            if result_type is None:
                checked = response.check()
            else:
                checked = response.check_as(
                    result_type=result_type, validation_context=self._validation_context
                )
            first = checked.first()
            if first is None:
                raise SurrealQueryError(
                    message="Query returned no results",
                    query=query,
                    vars=vars,
                )
            return first.result
        except ValidationError as e:
            raise SurrealQueryError(
                response=response.model_dump(),
                message="Invalid query result type",
                query=query,
                vars=vars,
            ) from e

    async def select(
        self,
        record: SurrealRecordId[TSurrealModel],
    ) -> TSurrealModel | None:
        """
        Select a record and return the result.

        Args:
            record: The record to select.

        Returns:
            The result of the query.
        """
        return await self.query(
            "SELECT * FROM ONLY $record_id",
            vars={"record_id": record.record_id},
            result_type=record.record_type | None,
        )

    async def upsert(
        self,
        record: TSurrealModel,
    ) -> TSurrealModel | None:
        """
        Upsert a record and return the before state.

        Args:
            record: The record to upsert.

        Returns:
            The before state of the record.
        """
        result = await self.query(
            "UPSERT ONLY $record_id CONTENT $_content RETURN BEFORE",
            vars={
                "record_id": record.record_id,
                "_content": record.model_dump(),
            },
            result_type=record.record_type | None,
        )
        return result

    async def delete(
        self,
        record: SurrealRecordId[TSurrealModel],
    ) -> TSurrealModel | None:
        """
        Delete a record and return the before state.

        Args:
            record: The record to delete.

        Returns:
            The before state of the record.
        """
        return await self.query(
            "DELETE ONLY $record_id RETURN BEFORE",
            vars={"record_id": record.record_id},
            result_type=record.record_type | None,
        )

    @overload
    async def call(
        self,
        function: str,
        vars: dict[str, Any] | None = None,
    ) -> Any: ...

    @overload
    async def call[T: Any](
        self,
        function: str,
        vars: dict[str, Any] | None = None,
        *,
        result_type: type[T],
    ) -> T: ...

    @overload
    async def call[T: Any](
        self,
        function: str,
        vars: dict[str, Any] | None = None,
        *,
        result_type: object,
    ) -> Any: ...

    async def call[T: Any](
        self,
        function: str,
        vars: dict[str, Any] | None = None,
        *,
        result_type: object | None = None,
    ) -> Any:
        args, vars = _build_fn_args(vars)
        return await self.query(
            f"RETURN {function}({args})",
            vars=vars,
            result_type=result_type,
        )

    async def close(self) -> None:
        return await self._conn.close()
