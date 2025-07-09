from dataclasses import dataclass

from pandas import DataFrame
from pycelonis.service.integration.service import PoolColumn, PoolColumnType

ColumnName = str


@dataclass(frozen=True)
class ColumnMeta:

    sql_ref: ColumnName
    column_type: PoolColumnType

    @classmethod
    def create_from_pycelonis_column(cls, column: PoolColumn) -> "ColumnMeta":
        return cls(sql_ref=column.name, column_type=column.type_)

    @classmethod
    def create_from_column_name(
        cls, name: ColumnName, table: DataFrame
    ) -> "ColumnMeta":
        match str(table.dtypes[name]):
            case "int":
                return cls(name, PoolColumnType.INTEGER)
            case "float":
                return cls(name, PoolColumnType.FLOAT)
            case "object":
                return cls(name, PoolColumnType.STRING)
            case "datetime":
                return cls(name, PoolColumnType.DATE)
        return cls(name, "")
