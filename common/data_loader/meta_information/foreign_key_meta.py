from dataclasses import dataclass

from pycelonis.ems import DataModel
from pycelonis.ems.data_integration.foreign_key import ForeignKey

from common.data_loader.meta_information.column_meta import ColumnName
from common.data_loader.meta_information.table_meta import TableName


@dataclass(frozen=True)
class ForeignKeyMeta:

    table_name_1: TableName
    column_name_1: ColumnName
    table_name_n: TableName
    column_name_n: ColumnName

    @classmethod
    def create_from_pycelonis_foreign_key(
        cls, data_model: DataModel, foreign_key: ForeignKey
    ) -> "ForeignKeyMeta":
        return cls(
            table_name_1=data_model.get_table(
                foreign_key.source_table_id
            ).alias_or_name,
            table_name_n=data_model.get_table(
                foreign_key.target_table_id
            ).alias_or_name,
            column_name_1=foreign_key.columns[0].source_column_name,
            column_name_n=foreign_key.columns[0].target_column_name,
        )
