import re
from dataclasses import dataclass
from enum import Enum

from pycelonis.ems.data_integration.data_model_table import DataModelTable

from common.data_loader.meta_information.column_meta import ColumnMeta


class TableType(Enum):
    OBJECT = "OBJECT"
    EVENT = "EVENT"
    MAPPING = "MAPPING"


TableName = str
TableDisplayName = str


@dataclass(frozen=True)
class TableMeta:

    sql_ref: TableName
    display_name: TableDisplayName
    table_type: TableType
    columns: list[ColumnMeta]

    @staticmethod
    def classify_type_based_on_name(name: str) -> TableType:
        if name.startswith("t_"):
            name = name[2:]
        if name.startswith("r_"):
            return TableType.MAPPING
        if name.startswith("o_"):
            return TableType.OBJECT
        if name.startswith("e_"):
            return TableType.EVENT
        raise ValueError(f"Table type {name} not recognized")

    @staticmethod
    def construct_display_name(name: str) -> str:
        return re.split(r"(?<=[a-zA-Z0-9])_(?=[a-zA-Z0-9])", name)[-1]

    @classmethod
    def create_from_pycelonis_data_model_table(
        cls, table: DataModelTable
    ) -> "TableMeta":
        columns = [
            ColumnMeta.create_from_pycelonis_column(column)
            for column in table.get_columns()
        ]
        return cls(
            sql_ref=table.alias_or_name,
            display_name=cls.construct_display_name(table.alias_or_name),
            table_type=cls.classify_type_based_on_name(table.alias_or_name),
            columns=columns,
        )
