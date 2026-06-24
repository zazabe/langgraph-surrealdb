import hashlib
from typing import ClassVar

from pydantic import GetCoreSchemaHandler
from pydantic_core import CoreSchema, core_schema
from surrealdb import RecordID
from typing_extensions import Self


class DbRecordId(str):
    prefix: ClassVar[str]
    _table: str
    _id: str

    def __new__(cls, value: str) -> Self:
        table, id = value.split(":", 1)
        if not cls.prefix:
            raise ValueError(f"{cls.__name__}.prefix must be set")
        if not table == cls.prefix:
            raise ValueError(
                f"{cls.__name__} must start with '{cls.prefix}:', got '{table}:'"
            )
        instance = str.__new__(cls, f"{table}:{id}")
        instance._table = table
        instance._id = id
        return instance

    @classmethod
    def from_raw(cls, *parts: object) -> Self:
        raw = "|".join(str(p) for p in parts)
        digest = hashlib.sha1(raw.encode("utf-8")).hexdigest()[:16]
        return cls(f"{cls.prefix}:{digest}")

    def to_record_id(self) -> RecordID:
        return RecordID(self._table, self._id)

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
