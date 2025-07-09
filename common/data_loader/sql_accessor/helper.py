import logging
import os
import time
from typing import Callable, Optional

import streamlit as st
import pycelonis
from pycelonis.config import Config
from pycelonis.ems.data_integration.data_export import DataExport
from pycelonis.ems.data_integration.data_model import DataModel
from pycelonis.ems.data_integration.data_model_table import DataModelTable
from pycelonis.errors import PyCelonisDataExportFailedError
from pycelonis.pql import PQL, PQLColumn, PQLFilter
from tqdm import tqdm

logger = logging.getLogger("cloud-process-mining-prototyping")


def build_full_table_query(table: DataModelTable, data_model: DataModel) -> PQL:
    # Right now there seems to be an issue with table.get_columns() being empty for OCDMs,
    # therefore we use the system catalog tables for now

    # for column in table.get_columns():
    #     query += PQLColumn(name=f"{column.name}", query=f""" "{table.alias_or_name}"."{column.name}" """)

    column_names_query = PQL(limit=1e6, distinct=True)
    column_names_query += PQLColumn(
        name="name", query=""" "System":"Columns"."COLUMN_NAME" """
    )
    column_names_query += PQLFilter(
        query=f""" FILTER "System":"Columns"."TABLE_NAME" = '{table.alias_or_name}'; """
    )
    columns_df = data_model.export_data_frame(column_names_query)

    query = PQL()
    for column_name in columns_df["name"]:
        query += PQLColumn(
            name=f"{column_name}",
            query=f""" "{table.alias_or_name}"."{column_name}" """,
        )
    return query


def sizeof_fmt(num: float, suffix: str = "B") -> str:
    for unit in ["", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi"]:
        if abs(num) < 1024.0:
            return f"{num:3.1f}{unit}{suffix}"
        num /= 1024.0
    return f"{num:.1f}Yi{suffix}"


def write_tables(
    output_dir: str,
    data_model: DataModel,
    exports: list[DataExport],
    progress_call_back: Optional[Callable[[float], None]] = None,
) -> dict[str, int]:
    counters = {
        "exported_tables": 0,
        "exported_files": 0,
        "total_size": 0,
    }
    for i, table in enumerate(tqdm(data_model.get_tables())):
        if progress_call_back is not None:
            progress_call_back(((i / len(data_model.get_tables())) / 2) + 0.5)
        try:
            data_export: DataExport = exports[i]
            data_export.wait_for_execution()

            for i, chunk in enumerate(data_export.get_chunks()):
                file = f"{output_dir}/{table.alias_or_name}_{i}.parquet"
                with open(file, "wb") as f_:
                    f_.write(chunk.read())
                counters["exported_files"] += 1
                counters["total_size"] += os.path.getsize(file)

            file_metadata = f"{output_dir}/{table.alias_or_name}.json"
            with open(file_metadata, "w", encoding="utf8") as f_:
                f_.write(table.json())
            counters["exported_tables"] += 1
        except PyCelonisDataExportFailedError as error:
            print(
                f"Could not export table '{table.alias_or_name}' to parquet.{os.linesep}The error is: {error}"
            )
    return counters


def write_foreign_keys(output_dir: str, data_model: DataModel) -> None:
    file_foreign_keys = f"{output_dir}/foreign_keys.json"
    with open(file_foreign_keys, "w", encoding="utf8") as f_:
        f_.write("[")
        f_.write(
            ",".join(
                [foreign_key.json() for foreign_key in data_model.get_foreign_keys()]
            )
        )
        f_.write("]")


def all_tables_to_parquet(
    data_pool_id: str,
    data_model_id: str,
    output_dir: str,
    progress_call_back: Optional[Callable[[float], None]] = None,
    export_foreign_keys: bool = True,
) -> None:
    """Exports all tables of the given datamodel to individual parquet files in the specified directory

    Args:
        data_pool_id (str): The data pool ID
        data_model_id (str): The data model ID
        output_dir (str): The output directory
    """
    Config.DISABLE_TQDM = True
    start_time = time.perf_counter()

    os.makedirs(output_dir, exist_ok=True)
    celonis = st.session_state.celonis_instance

    data_model = (
        celonis.data_integration.get_data_pools()
        .find_by_id(data_pool_id)
        .get_data_models()
        .find_by_id(data_model_id)
    )

    exports = list(range(len(data_model.get_tables())))
    for i, table in enumerate(data_model.get_tables()):
        try:
            # Export data to PARQUET files
            if progress_call_back is not None:
                progress_call_back((i / len(data_model.get_tables())) / 2)
            exports[i] = data_model.create_data_export(
                query=build_full_table_query(table, data_model),
                export_type=pycelonis.service.integration.service.ExportType.PARQUET,
            )
        except PyCelonisDataExportFailedError as error:
            logger.warning(
                "Could not export table '%s' to parquet.%sThe error is: %s",
                table.name,
                os.linesep,
                error,
            )

    counters = write_tables(
        output_dir, data_model, exports, progress_call_back=progress_call_back
    )
    if export_foreign_keys:
        write_foreign_keys(output_dir, data_model)
    end_time = time.perf_counter()
    logger.info(
        f"Exported {counters['exported_tables']} tables "
        + f"to {counters['exported_files']} files "
        + f"with a combined size of {sizeof_fmt(counters['total_size'])} in {end_time - start_time:.0f} seconds."
    )
    if counters["exported_tables"] != len(data_model.get_tables()):
        logger.warning(
            "%d tables could not be exported.",
            len(data_model.get_tables()) - counters["exported_tables"],
        )
