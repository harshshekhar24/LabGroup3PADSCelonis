import uuid
import networkx as nx
from collections import defaultdict

import streamlit as st
import pandas as pd

from common.data_loader.meta_information.column_meta import ColumnMeta
from common.data_loader.meta_information.table_meta import TableMeta, TableType


def change_page(page_name: str):
    st.session_state.page = page_name
    st.rerun()


# # Creates ab Event Log from the different Activity Tables of a Perspective
# def create_combined_eventlog(accessor, meta_infos):
#     # Get all event tables
#     event_tables = sorted([t for t in meta_infos.tables if t.startswith("e_")])
#     eventlog_dfs = []
#     object_columns = []
#
#     # Combine the event tables
#     for table in event_tables:
#         try:
#             df = accessor.execute_query(f"SELECT * FROM {table}")
#
#             if df is None or df.empty:
#                 continue  # Skip if table is empty
#             # Add the name of the Event
#             df["EventName"] = table.replace("e_celonis_", "")
#             df["EventID"] = df["ID"]
#             df["Timestamp"] = df["Time"]
#
#             object_columns = [col for col in df.columns if col.endswith("_ID")]
#             df_filtered = df[["EventID", "Timestamp", "EventName"] + object_columns]
#
#             eventlog_dfs.append(df_filtered)
#         except Exception as e:
#             st.warning(f"Table `{table}` could not be loaded: {e}")
#             continue
#
#     if not eventlog_dfs:
#         st.error("No valid Event Tables found.")
#         return pd.DataFrame()
#
#     # Add table to DuckDB
#     combined_eventlog = pd.concat(eventlog_dfs, ignore_index=True).sort_values(
#         "Timestamp"
#     )
#     combined_eventlog = combined_eventlog.fillna("")
#
#     accessor._duckdb_connection.register("combined_eventlog", combined_eventlog)
#     accessor._duckdb_connection.execute(
#         "CREATE OR REPLACE TABLE el_combined_eventlog AS SELECT * FROM combined_eventlog"
#     )
#
#     # Create meta information for the table
#     meta_infos.tables["el_combined_eventlog"] = TableMeta(
#         sql_ref="el_combined_eventlog",
#         display_name=TableMeta.construct_display_name("Combined eventlog"),
#         table_type=TableType.EVENT,
#         columns=[
#             ColumnMeta.create_from_column_name(column, combined_eventlog)
#             for column in combined_eventlog.columns
#         ],
#     )
#     return combined_eventlog


# Creates ab Event Log from the different Activity Tables of a Perspective
def create_combined_eventlog(accessor, meta_infos):
    # Get all event tables
    event_tables = sorted([t for t in meta_infos.tables if t.startswith("e_")])
    eventlog_dfs = []

    # Temporary list of Object Columns
    all_object_columns = []

    # Combine the event tables
    for table in event_tables:
        try:
            df = accessor.execute_query(f"SELECT * FROM {table}")

            if df is None or df.empty:
                continue  # Skip if table is empty

            df["EventName"] = table.replace("e_celonis_", "")
            df["EventID"] = df["ID"]
            df["Timestamp"] = df["Time"]

            # Finde Objektspalten in diesem spezifischen DataFrame
            object_columns_in_df = [col for col in df.columns if col.endswith("_ID")]
            all_object_columns.extend(
                object_columns_in_df
            )  # Füge sie zur Gesamtliste hinzu

            df_filtered = df[
                ["EventID", "Timestamp", "EventName"] + object_columns_in_df
            ]
            eventlog_dfs.append(df_filtered)
        except Exception as e:
            st.warning(f"Table `{table}` could not be loaded: {e}")
            continue

    if not eventlog_dfs:
        st.error("No valid Event Tables found.")
        return pd.DataFrame()

    # Add table to DuckDB
    combined_eventlog = pd.concat(eventlog_dfs, ignore_index=True).sort_values(
        "Timestamp"
    )

    combined_eventlog = combined_eventlog.fillna("")

    # Collect all object Columns
    object_columns = sorted(
        [col for col in combined_eventlog.columns if col.endswith("_ID")]
    )

    # Combine into long format to improve performance
    long_format = pd.melt(
        combined_eventlog,
        id_vars=["EventID"],
        value_vars=object_columns,
        value_name="ObjectID",
    )

    # Drop rows where no IDs are available
    long_format = long_format.dropna(subset=["ObjectID"])
    long_format = long_format[long_format["ObjectID"] != ""]

    # Find edges by self-join
    edges = pd.merge(long_format, long_format, on="EventID")

    # Filter reflexive edges
    edges = edges[edges["ObjectID_x"] != edges["ObjectID_y"]]

    # Create graph from edge list
    G = nx.from_pandas_edgelist(edges, "ObjectID_x", "ObjectID_y")

    # Add nodes that dont have any edges
    all_objects = long_format["ObjectID"].unique()
    G.add_nodes_from(all_objects)

    # Find connected components
    object_to_process_id_map = {}
    for i, component in enumerate(nx.connected_components(G)):
        process_execution_id = i
        for obj_id in component:
            object_to_process_id_map[obj_id] = str(process_execution_id)

    # Add execution Id to dataframe
    id_map_series = combined_eventlog[object_columns].apply(
        lambda row: next((val for val in row if val in object_to_process_id_map), None),
        axis=1,
    )
    combined_eventlog["Process_Execution_ID"] = id_map_series.map(
        object_to_process_id_map
    )

    null_pids = combined_eventlog["Process_Execution_ID"].isnull()
    combined_eventlog.loc[null_pids, "Process_Execution_ID"] = [
        f"p_iso_{uuid.uuid4()}" for _ in range(null_pids.sum())
    ]

    cols = combined_eventlog.columns.tolist()
    cols.insert(1, cols.pop(cols.index("Process_Execution_ID")))
    combined_eventlog = combined_eventlog[cols]

    accessor._duckdb_connection.register("combined_eventlog", combined_eventlog)
    accessor._duckdb_connection.execute(
        "CREATE OR REPLACE TABLE el_combined_eventlog AS SELECT * FROM combined_eventlog"
    )

    # Create meta information for the table
    meta_infos.tables["el_combined_eventlog"] = TableMeta(
        sql_ref="el_combined_eventlog",
        display_name=TableMeta.construct_display_name("Combined eventlog"),
        table_type=TableType.EVENT,
        columns=[
            ColumnMeta.create_from_column_name(column, combined_eventlog)
            for column in combined_eventlog.columns
        ],
    )
    return combined_eventlog


# Calculates positions for the different nodes of a Frequent Episode to avoid overlap
def assign_node_positions(
    selected_pattern, obj_event_map, layer_spacing=200, offset=200
):
    positions = {}
    event_layers = {}  # Y-value for every event
    obj_at_layer = defaultdict(list)

    # Assing Y value to activities, x Position is centered at X=0
    for i, step in enumerate(selected_pattern):
        act_id = f"a{i}"
        x = 0
        y = (i + 1) * layer_spacing
        positions[act_id] = (x, y)
        event_layers[act_id] = y

    # Find out on which y-Layer the objects start and end
    for obj, events in obj_event_map.items():
        start_y = event_layers[events[0]]
        end_y = event_layers[events[-1]]

        obj_at_layer[start_y - layer_spacing].append(("start", obj))
        obj_at_layer[end_y + layer_spacing].append(("end", obj))

    # Place object nodes with horizontal offset
    for y, obj_list in obj_at_layer.items():
        has_activity = any(pos_y == y for pos_y in event_layers.values())
        n = len(obj_list)
        for i, (kind, obj) in enumerate(obj_list):
            if not has_activity and n == 1:
                x = 0  # Only object on that layer
            else:
                factor = i // 2 + 1
                # One or more objects, and activity on that layer
                if i % 2 == 0:
                    x = 0 - factor * offset
                else:
                    x = 0 + factor * offset
            node_id = f"obj_{kind}_{obj}"
            positions[node_id] = (x, y)

    return positions


def flatten_event_log(df, time_col="Time", event_col="Events", delimiter=","):
    flat_data = []

    for _, row in df.iterrows():
        time = row[time_col]
        events_str = row.get(event_col, "")
        if pd.isna(events_str) or not str(events_str).strip():
            continue
        events = [e.strip() for e in str(events_str).split(delimiter) if e.strip()]
        for event in events:
            flat_data.append((int(time), event))

    return flat_data


def flatten_event_log_2(df, time_col="Timestamp", event_col="EventName"):
    flat_data = []
    df[time_col] = pd.to_datetime(df[time_col])

    for _, row in df.iterrows():
        time = int(row[time_col].timestamp())
        event = row.get(event_col, "")
        flat_data.append((time, event))

    return flat_data


def flatten_event_log_with_pid(
    df,
    time_col="Timestamp",
    event_col="EventName",
    pid_col="Process_Execution_ID",
    object_cols=None,
):
    """
    Flattens the log to: (timestamp, event, pid, [list of objects])
    """
    if object_cols is None:
        object_cols = [
            col for col in df.columns if col.endswith("_ID") and col != pid_col
        ]

    df[time_col] = pd.to_datetime(df[time_col])
    flat_data = []

    for _, row in df.iterrows():
        time = int(row[time_col].timestamp())
        event = row[event_col]
        pid = row[pid_col]

        # Instead of full object IDs, extract types from column names
        objects = [col.replace("_ID", "") for col in object_cols if pd.notna(row[col])]

        flat_data.append((time, event, pid, objects))

    return flat_data


def normalize_timestamps(flat_data):
    unique_times = sorted(set(t for t, *_ in flat_data))
    timestamp_to_index = {t: i + 1 for i, t in enumerate(unique_times)}  # Start from 1
    flatdata_indexed = [(timestamp_to_index[t], *rest) for t, *rest in flat_data]
    return flatdata_indexed
