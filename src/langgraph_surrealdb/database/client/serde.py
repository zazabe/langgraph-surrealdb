from typing import Any, Literal, cast, overload

from pydantic import BaseModel, TypeAdapter, model_validator

from langgraph_surrealdb.database.client.error import (
    SurrealQueryRawResultError,
    SurrealQueryRawResultItemError,
)


class QueryRawRpcError(BaseModel):
    cause: Any | None = None
    code: int
    kind: str
    message: str


class QueryRawItemOkResult[T: Any](BaseModel):
    status: Literal["OK"]
    result: T
    kind: str | None = None


class QueryRawItemErrResult(BaseModel):
    status: Literal["ERR"]
    result: Any
    kind: str | None = None


type QueryRawItemResult[T: Any] = QueryRawItemOkResult[T] | QueryRawItemErrResult


class QueryRawSuccessResult[T: Any](BaseModel):
    id: str
    result: list[QueryRawItemResult[T]]


class QueryRawErrorResult(BaseModel):
    id: str
    error: QueryRawRpcError


class QueryRawCheckedResult[T: Any](BaseModel):
    id: str
    result: list[QueryRawItemOkResult[T]]

    def items(self) -> list[QueryRawItemOkResult[T]]:
        return self.result

    def first(self) -> QueryRawItemOkResult[T] | None:
        return self.result[0] if len(self.result) > 0 else None

    def last(self) -> QueryRawItemOkResult[T] | None:
        return self.result[-1] if len(self.result) > 0 else None

    def len(self) -> int:
        return len(self.result)


class QueryRawResult[T: Any](BaseModel):
    id: str
    result: list[QueryRawItemResult[T]] | None = None
    error: QueryRawRpcError | None = None

    @model_validator(mode="after")
    def _validate_shape(self) -> "QueryRawResult[T]":
        has_result = self.result is not None
        has_error = self.error is not None
        if has_result == has_error:
            raise ValueError("Expected exactly one of 'result' or 'error'")
        return self

    def check(self) -> QueryRawCheckedResult[T]:
        if isinstance(self.error, QueryRawRpcError):
            raise SurrealQueryRawResultError(
                self.error.message,
                kind=self.error.kind,
                code=self.error.code,
                cause=self.error.cause,
            )
        for item in self.items():
            if isinstance(item, QueryRawItemErrResult):
                raise SurrealQueryRawResultItemError(
                    item.result,
                    kind=item.kind,
                )
        return QueryRawCheckedResult[T].model_construct(
            id=self.id,
            result=cast(list[QueryRawItemOkResult[T]], self.result),
        )

    @overload
    def check_as[R: Any](
        self,
        *,
        result_type: type[R],
        validation_context: Any | None = None,
    ) -> QueryRawCheckedResult[R]: ...

    @overload
    def check_as(
        self,
        *,
        result_type: object,
        validation_context: Any | None = None,
    ) -> QueryRawCheckedResult[Any]: ...

    def check_as(
        self,
        *,
        result_type: object,
        validation_context: Any | None = None,
    ) -> QueryRawCheckedResult[Any]:
        checked = self.check()
        result_adapter = TypeAdapter(result_type)
        typed_result = [
            QueryRawItemOkResult[Any](
                status=item.status,
                result=result_adapter.validate_python(
                    item.result, context=validation_context
                ),
                kind=item.kind,
            )
            for item in checked.result
        ]
        return QueryRawCheckedResult[Any](id=checked.id, result=typed_result)

    def is_error(self) -> bool:
        return self.error is not None

    def items(self) -> list[QueryRawItemResult[T]]:
        return self.result or []

    def first(self) -> QueryRawItemResult[T] | None:
        items = self.items()
        return items[0] if items else None

    def last(self) -> QueryRawItemResult[T] | None:
        items = self.items()
        return items[-1] if items else None

    def len(self) -> int:
        return len(self.items())
