from dataclasses import dataclass

from pandas import DataFrame
from pycelonis.ems import DataModel

from common.data_loader.meta_information.column_meta import ColumnMeta
from common.data_loader.meta_information.foreign_key_meta import ForeignKeyMeta
from common.data_loader.meta_information.table_meta import (
    TableMeta,
    TableType,
    TableName,
)


@dataclass
class SQLView:

    tables: dict[TableName, TableMeta]
    foreign_keys: list[ForeignKeyMeta]

    def look_up_table_by_display_name(self, display_name: str) -> TableName:
        for table_name, table in self.tables.items():
            if display_name == table.display_name:
                return table_name
        raise ValueError(f"No table with {display_name=} found.")

    def look_up_foreign_key(
        self, table1: TableName, table2: TableName
    ) -> ForeignKeyMeta:
        tables = [table1, table2]
        foreign_keys = [
            foreign_key
            for foreign_key in self.foreign_keys
            if foreign_key.table_name_1 in tables and foreign_key.table_name_n in tables
        ]
        if len(foreign_keys) == 0:
            raise ValueError(f"No connection between tables {table1} -> {table2}")
        return foreign_keys[0]

    @classmethod
    def initial_view_from_pycelonis_data_model(cls, data_model: DataModel) -> "SQLView":
        return cls(
            tables={
                table.alias_or_name: TableMeta.create_from_pycelonis_data_model_table(
                    table
                )
                for table in data_model.get_tables()
                if len(table.get_columns()) > 0
            },
            foreign_keys=[
                ForeignKeyMeta.create_from_pycelonis_foreign_key(
                    data_model, foreign_key
                )
                for foreign_key in data_model.get_foreign_keys()
            ],
        )

    @classmethod
    def initial_view_from_ocel2(
        cls,
        tables: dict[str, DataFrame],
        foreign_key_relations: set[tuple[str, str, tuple[str, str]]],
        table_types: dict[str, TableType],
    ) -> "SQLView":
        return cls(
            tables={
                name: TableMeta(
                    sql_ref=name,
                    display_name=TableMeta.construct_display_name(name),
                    table_type=table_types[name],
                    columns=[
                        ColumnMeta.create_from_column_name(column, table)
                        for column in table.columns
                    ],
                )
                for name, table in tables.items()
            },
            foreign_keys=[
                ForeignKeyMeta(
                    foreign_key[0], foreign_key[2][0], foreign_key[1], foreign_key[2][1]
                )
                for foreign_key in foreign_key_relations
            ],
        )

    def cleanup_foreign_keys(self) -> None:
        foreign_keys_to_delete = []
        for foreign_key in self.foreign_keys:
            if (
                foreign_key.table_name_1 not in self.tables
                or foreign_key.table_name_n not in self.tables
            ):
                foreign_keys_to_delete.append(foreign_key)
        for foreign_key in foreign_keys_to_delete:
            self.foreign_keys.remove(foreign_key)
