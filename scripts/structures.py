from enum import IntEnum
from construct import Array, Byte, Const, Enum, Int64ub, PascalString, Pointer, PrefixedArray, Probe, Struct, len_, this

class ColumnType(IntEnum):
    Integer = 0
    Float = 1
    VolatileString = 2
    HashtableString = 3

ColumnHeader = Struct(
    "type" / Enum(Byte, ColumnType),
    "name" / PascalString(Byte, "utf8"),
)

TableIndexList = Struct(
    "magic" / Const(b"INDEX"),
    "indices" / PrefixedArray(Int64ub, Struct(
        "position" / Int64ub,
        "offset" / Int64ub,
    )),
)

DatasetHeader = Struct(
    "name" / PascalString(Byte, "utf8"),
    "column_count" / Byte,
    "columns" / Array(this.column_count, ColumnHeader),
    "table_count" / Byte,
    "table_indices" / Array(this.table_count, Struct(
        "chromosome" / Byte,
        "offset" / Int64ub,
        "indices" / Pointer(this.offset, TableIndexList),
    )),
)

Database = Struct(
    "magic" / Const(b"ZygosDB"),
    "version" / Const(1, Byte),
    "dataset_count" / Byte,
    "dataset_headers" / Array(this.dataset_count, DatasetHeader),
)
