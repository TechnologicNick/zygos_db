from enum import IntEnum
from construct import Byte, Const, Enum, Int64ub, PascalString, Pointer, PrefixedArray, Struct, this

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
    "max_position" / Int64ub,
    "end_offset" / Int64ub,
    "indices" / PrefixedArray(Int64ub, Struct(
        "position" / Int64ub,
        "offset" / Int64ub,
    )),
)

DatasetHeader = Struct(
    "name" / PascalString(Byte, "utf8"),
    "columns" / PrefixedArray(Byte, ColumnHeader),
    "tables" / PrefixedArray(Byte, Struct(
        "chromosome" / Byte,
        "offset" / Int64ub,
        # "indices" / Pointer(this.offset, TableIndexList),
    )),
)

Database = Struct(
    "magic" / Const(b"ZygosDB"),
    "version" / Const(1, Byte),
    "datasets" / PrefixedArray(Byte, DatasetHeader),
)
