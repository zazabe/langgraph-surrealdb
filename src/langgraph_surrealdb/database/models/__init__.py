import hashlib
from typing import Self

from pydantic import GetCoreSchemaHandler
from pydantic_core import CoreSchema, core_schema
from surrealdb import RecordID


class DbRecordId(str):
    _table: str
    _id: str

    def __new__(cls, value: str) -> Self:
        table, ident = value.split(":", 1)
        if not table or not ident:
            raise ValueError(f"Invalid record id '{value}', expected 'table:id'")
        instance = str.__new__(cls, f"{table}:{ident}")
        instance._table = table
        instance._id = ident
        return instance

    @property
    def table(self) -> str:
        return self._table

    @property
    def id(self) -> str:
        return self._id

    @property
    def record_id(self) -> RecordID:
        return RecordID(self._table, self._id)

    def assert_table(self, expected_table: str) -> None:
        if self._table != expected_table:
            raise ValueError(
                f"Expected table '{expected_table}', got '{self._table}' in '{self}'"
            )

    @classmethod
    def from_raw(cls, table: str, *parts: object) -> Self:
        raw = "|".join(str(p) for p in parts)
        digest = hashlib.sha1(raw.encode("utf-8")).hexdigest()[:16]
        return cls(f"{table}:{digest}")

    @classmethod
    def _coerce_prefixed_input(cls, value: object) -> str:
        if isinstance(value, cls):
            return str(value)
        if isinstance(value, RecordID):
            table = getattr(value, "table_name", None)
            ident = getattr(value, "id", None)
            if table is None or ident is None:
                return str(value)
            return f"{table}:{ident}"
        if isinstance(value, str):
            return value
        raise TypeError(f"{cls.__name__} must be str or RecordID")

    @classmethod
    def __get_pydantic_core_schema__(
        cls, _source_type: type, _handler: GetCoreSchemaHandler
    ) -> CoreSchema:
        return core_schema.no_info_before_validator_function(
            cls._coerce_prefixed_input,
            core_schema.no_info_after_validator_function(cls, core_schema.str_schema()),
        )
