import logging
import os
import shutil
from typing import Callable, Dict

import duckdb
import pandas as pd

from common.data_loader.meta_information.sql_view import SQLView
from common.data_loader.sql_accessor.base import SQLAccessor
from common.data_loader.sql_accessor.helper import all_tables_to_parquet

logger = logging.getLogger(__name__)

# pylint: disable=unused-variable


class LocalDuckDBAccessor(SQLAccessor):
    def __init__(self) -> None:
        self._duckdb_connection = duckdb.connect(database=":memory:")

    def execute_query(self, query: str) -> pd.DataFrame:
        logger.info("Executing query: %s", query)
        return self._duckdb_connection.execute(query).fetch_df()

    def remove_tables(self, sql_view: SQLView) -> None:
        for table in sql_view.tables:
            self._duckdb_connection.execute(f"DROP TABLE IF EXISTS {table}")

    @classmethod
    def create_local_copy_of_data_model(
        cls,
        data_pool_id: str,
        data_model_id: str,
        view: SQLView,
        progress_call_back: Callable[[float], None],
    ) -> "LocalDuckDBAccessor":
        def _progress_call_back(progress: float) -> None:
            progress_call_back((progress * 4) / 5)

        shutil.rmtree("tmp", ignore_errors=True)

        all_tables_to_parquet(
            data_pool_id,
            data_model_id,
            output_dir="tmp",
            progress_call_back=_progress_call_back,
            export_foreign_keys=False,
        )

        parquet_files = [
            os.path.join("tmp", file)
            for file in os.listdir("tmp")
            if file.endswith(".parquet")
        ]
        created: list[str] = []
        duckdb_accessor = cls()

        for index, file_path in enumerate(parquet_files):
            table_name = "_".join(os.path.basename(file_path).split("_")[:-1])

            if table_name not in view.tables:
                continue

            df = pd.read_parquet(file_path)

            duckdb_accessor._duckdb_connection.register("tmp_df", df)

            if table_name not in created:
                duckdb_accessor.execute_query(
                    f"CREATE TABLE {table_name} AS SELECT * FROM tmp_df"
                )
                created.append(table_name)
            else:
                duckdb_accessor.execute_query(
                    f"INSERT INTO {table_name} SELECT * FROM tmp_df"
                )

            duckdb_accessor._duckdb_connection.unregister("tmp_df")
            progress_call_back(0.8 + ((index / len(parquet_files)) / 5))

        shutil.rmtree("tmp")
        return duckdb_accessor

    @classmethod
    def create_local_copy_from_ocel2(
        cls, tables: Dict[str, pd.DataFrame], view: SQLView
    ) -> "LocalDuckDBAccessor":

        created: list[str] = []
        duckdb_accessor = cls()

        for (
            table_name,
            pandas_table,
        ) in tables.items():
            if table_name not in view.tables:
                continue

            if table_name not in created:
                duckdb_accessor.execute_query(
                    f"CREATE TABLE {table_name} AS SELECT * FROM pandas_table"
                )
                created.append(table_name)
            else:
                duckdb_accessor.execute_query(
                    f"INSERT INTO {table_name} SELECT * FROM pandas_table"
                )

        return duckdb_accessor
