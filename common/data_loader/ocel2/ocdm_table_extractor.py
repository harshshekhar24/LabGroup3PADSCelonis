import itertools
import re
import pandas as pd

from common.data_loader.meta_information.table_meta import TableType
from common.data_loader.ocel2.model import (
    OCEL2JSON,
    EventType,
    OCEL2Event,
    OCEL2Object,
    ObjectType,
)

NON_ALPHA_REGEX_PATTERN = re.compile("[^a-zA-Z]")


def create_ocdm_tables_from_data(
    ocel2_data: str,
) -> tuple[
    dict[str, pd.DataFrame], set[tuple[str, str, tuple[str, str]]], dict[str, TableType]
]:
    """Creates object-, event-, and relation-tables,
    as well as foreign key relations necessary to create OCDM."""
    ocel2_log = OCEL2JSON.parse_raw(ocel2_data)

    object_id_to_type_mapping = _object_id_to_type_mapping(ocel2_log)
    event_tables, event_object_relation_tables, event_relations = _extract_event_tables(
        ocel2_log, object_id_to_type_mapping
    )
    (
        object_tables,
        object_object_relation_tables,
        object_relations,
    ) = _extract_object_tables(ocel2_log, object_id_to_type_mapping)

    table_types = {}
    table_types.update({name: TableType.EVENT for name in event_tables})
    table_types.update({name: TableType.OBJECT for name in object_tables})
    table_types.update(
        {name: TableType.MAPPING for name in event_object_relation_tables}
    )
    table_types.update(
        {name: TableType.MAPPING for name in object_object_relation_tables}
    )

    tables = {}
    tables.update(event_tables)
    tables.update(object_tables)
    tables.update(event_object_relation_tables)
    tables.update(object_object_relation_tables)
    foreign_key_relations = event_relations.union(object_relations)

    return (
        tables,
        foreign_key_relations,
        table_types,
    )


def _get_object_foreign_key_column_name(object_type_name: str) -> str:
    name_stripped = NON_ALPHA_REGEX_PATTERN.sub("", object_type_name)
    return f"{name_stripped}_ID"


def _get_event_table_name(event_type_name: str) -> str:
    name_stripped = NON_ALPHA_REGEX_PATTERN.sub("", event_type_name)
    return f"e_celonis_{name_stripped}"


def _get_object_table_name(object_type_name: str) -> str:
    name_stripped = NON_ALPHA_REGEX_PATTERN.sub("", object_type_name)
    return f"o_celonis_{name_stripped}"


def _get_event_object_relation_table_name(
    event_type_name: str, object_type_name: str
) -> str:
    event_type_name_stripped = NON_ALPHA_REGEX_PATTERN.sub("", event_type_name)
    object_type_name_stripped = NON_ALPHA_REGEX_PATTERN.sub("", object_type_name)
    return f"r_e_celonis_{event_type_name_stripped}__{object_type_name_stripped}"


def _get_object_object_relation_table_name(
    object_type_name: str, related_object_type_name: str
) -> str:
    object_type_name_stripped = NON_ALPHA_REGEX_PATTERN.sub("", object_type_name)
    related_object_type_name_stripped = NON_ALPHA_REGEX_PATTERN.sub(
        "", related_object_type_name
    )
    return (
        f"r_o_celonis_{object_type_name_stripped}__{related_object_type_name_stripped}"
    )


def _object_id_to_type_mapping(ocel2_log: OCEL2JSON) -> dict[str, str]:
    return {obj.id: obj.type for obj in ocel2_log.objects}


def _initialize_empty_event_tables(
    ocel2_log: OCEL2JSON,
) -> tuple[dict[str, dict[str, list]], dict[tuple[str, str], list]]:
    event_tables: dict[str, dict[str, list]] = {
        event_type.name: {
            key: []
            for key in ["ID", "Time"]
            + [attribute.name for attribute in event_type.attributes]
        }
        for event_type in ocel2_log.eventTypes
    }
    event_object_relation_tables: dict[tuple[str, str], list] = {
        (event_type.name, object_type.name): []
        for event_type in ocel2_log.eventTypes
        for object_type in ocel2_log.objectTypes
    }
    return event_tables, event_object_relation_tables


def _add_event_to_event_tables(
    event: OCEL2Event,
    event_type_str_to_type_mapping: dict[str, EventType],
    object_id_to_type_mapping: dict[str, str],
    event_tables: dict[str, dict[str, list]],
    event_object_relation_tables: dict[tuple[str, str], list],
) -> tuple[dict[str, dict[str, list]], dict[tuple[str, str], list]]:
    event_tables[event.type]["ID"].append(event.id)
    event_tables[event.type]["Time"].append(event.time)

    if event.attributes:
        is_attribute_set = {
            attribute.name: False
            for attribute in event_type_str_to_type_mapping[event.type].attributes
        }
        for attribute in event.attributes:
            # OCEL2 supports more than one attribute value per attribute type.
            # This cannot be mapped to the OCDM. Thus, only taking the first attribute value per attribute type
            if not is_attribute_set[attribute.name]:
                event_tables[event.type][attribute.name].append(attribute.value)
                is_attribute_set[attribute.name] = True

        for attribute_name, is_set in is_attribute_set.items():
            if not is_set:
                event_tables[event.type][attribute_name].append(None)

    if event.relationships:
        for relationship in event.relationships:
            if relationship.objectId in object_id_to_type_mapping:
                obj_type = object_id_to_type_mapping[relationship.objectId]
                event_object_relation_tables[(event.type, obj_type)].append(
                    (event.id, relationship.objectId)
                )

    return event_tables, event_object_relation_tables


def _convert_event_table_dicts_to_pandas(
    event_tables_as_dict: dict[str, dict[str, list]],
) -> dict[str, pd.DataFrame]:
    event_tables_as_df = {}
    for event_table_name, event_table_dict in event_tables_as_dict.items():
        event_tables_as_df[event_table_name] = pd.DataFrame.from_dict(
            event_table_dict, orient="columns"
        )
    return event_tables_as_df


def _convert_event_object_relation_table_dicts_to_pandas(
    event_object_relation_tables_as_dict: dict[tuple[str, str], list],
) -> dict[tuple[str, str], pd.DataFrame]:
    event_object_relation_tables_as_df = {}
    for event_type_object_type, table in event_object_relation_tables_as_dict.items():
        if table:
            event_object_relation_tables_as_df[event_type_object_type] = pd.DataFrame(
                table,
                columns=[
                    "ID",
                    _get_object_foreign_key_column_name(event_type_object_type[1]),
                ],
            )
    return event_object_relation_tables_as_df


def _join_object_relations_to_event_tables(
    event_tables_as_df: dict[str, pd.DataFrame],
    event_object_relation_tables_as_df: dict[tuple[str, str], pd.DataFrame],
) -> tuple[
    dict[str, pd.DataFrame],
    dict[tuple[str, str], pd.DataFrame],
    set[tuple[str, str, tuple[str, str]]],
]:
    keys_to_del = set()
    foreign_key_relations = set()
    for event_type_object_type, df in event_object_relation_tables_as_df.items():
        if len(df) == df["ID"].nunique():
            event_tables_as_df[event_type_object_type[0]] = event_tables_as_df[
                event_type_object_type[0]
            ].merge(
                event_object_relation_tables_as_df[event_type_object_type],
                on="ID",
                how="left",
                validate="1:m",
            )
            keys_to_del.add(event_type_object_type)
            foreign_key_relations.add(
                (
                    _get_object_table_name(event_type_object_type[1]),
                    _get_event_table_name(event_type_object_type[0]),
                    (
                        "ID",
                        _get_object_foreign_key_column_name(event_type_object_type[1]),
                    ),
                )
            )
        else:
            foreign_key_relations.add(
                (
                    _get_object_table_name(event_type_object_type[1]),
                    _get_event_object_relation_table_name(*event_type_object_type),
                    (
                        "ID",
                        _get_object_foreign_key_column_name(event_type_object_type[1]),
                    ),
                )
            )
            foreign_key_relations.add(
                (
                    _get_event_table_name(event_type_object_type[0]),
                    _get_event_object_relation_table_name(*event_type_object_type),
                    ("ID", "ID"),
                )
            )

    for key in keys_to_del:
        del event_object_relation_tables_as_df[key]

    return event_tables_as_df, event_object_relation_tables_as_df, foreign_key_relations


def _extract_event_tables(
    ocel2_log: OCEL2JSON, object_id_to_type_mapping: dict[str, str]
) -> tuple[
    dict[str, pd.DataFrame],
    dict[str, pd.DataFrame],
    set[tuple[str, str, tuple[str, str]]],
]:
    event_type_str_to_type_mapping = {
        event_type.name: event_type for event_type in ocel2_log.eventTypes
    }
    (
        event_tables_as_dict,
        event_object_relation_tables_as_dict,
    ) = _initialize_empty_event_tables(ocel2_log)

    for event in ocel2_log.events:
        (
            event_tables_as_dict,
            event_object_relation_tables_as_dict,
        ) = _add_event_to_event_tables(
            event=event,
            event_type_str_to_type_mapping=event_type_str_to_type_mapping,
            object_id_to_type_mapping=object_id_to_type_mapping,
            event_tables=event_tables_as_dict,
            event_object_relation_tables=event_object_relation_tables_as_dict,
        )

    event_tables_as_df = _convert_event_table_dicts_to_pandas(event_tables_as_dict)
    event_object_relation_tables_as_df = (
        _convert_event_object_relation_table_dicts_to_pandas(
            event_object_relation_tables_as_dict
        )
    )
    (
        event_tables_as_df,
        event_object_relation_tables_as_df,
        foreign_key_relations,
    ) = _join_object_relations_to_event_tables(
        event_tables_as_df, event_object_relation_tables_as_df
    )
    event_tables_as_df = {
        _get_event_table_name(event_type_name): df
        for event_type_name, df in event_tables_as_df.items()
    }
    event_object_relation_tables_as_df_renamed = {
        _get_event_object_relation_table_name(event_type_name, object_type_name): df
        for (
            event_type_name,
            object_type_name,
        ), df in event_object_relation_tables_as_df.items()
    }

    return (
        event_tables_as_df,
        event_object_relation_tables_as_df_renamed,
        foreign_key_relations,
    )


def _initialize_empty_object_tables(
    ocel2_log: OCEL2JSON,
) -> tuple[dict[str, dict[str, list]], dict[tuple, set]]:
    object_tables: dict[str, dict[str, list]] = {
        object_type.name: {
            key: []
            for key in ["ID"] + [attribute.name for attribute in object_type.attributes]
        }
        for object_type in ocel2_log.objectTypes
    }
    object_object_relation_tables: dict[tuple, set] = {
        tuple(sorted((object_type.name, another_object_type.name))): set()
        for object_type, another_object_type in itertools.combinations(
            ocel2_log.objectTypes, 2
        )
    }
    return object_tables, object_object_relation_tables


def _add_object_to_object_tables(
    obj: OCEL2Object,
    object_tables: dict[str, dict[str, list]],
    object_object_relation_tables: dict[tuple, set],
    object_type_str_to_type_mapping: dict[str, ObjectType],
    object_id_to_type_mapping: dict[str, str],
) -> tuple[dict[str, dict[str, list]], dict[tuple, set]]:
    object_tables[obj.type]["ID"].append(obj.id)

    if obj.attributes:
        is_attribute_set = {
            attribute.name: False
            for attribute in object_type_str_to_type_mapping[obj.type].attributes
        }
        for attribute in obj.attributes:
            # OCEL2 supports more than one attribute value per attribute type.
            # This cannot be mapped to the OCDM. Thus, only taking the first attribute value per attribute type
            if not is_attribute_set[attribute.name]:
                object_tables[obj.type][attribute.name].append(attribute.value)
                is_attribute_set[attribute.name] = True

        for attribute_name, is_set in is_attribute_set.items():
            if not is_set:
                object_tables[obj.type][attribute_name].append(None)

    if obj.relationships:
        for relationship in obj.relationships:
            if relationship.objectId in object_id_to_type_mapping:
                obj_type = object_id_to_type_mapping[relationship.objectId]
                if obj.type < obj_type:
                    object_object_relation_tables[(obj.type, obj_type)].add(
                        (obj.id, relationship.objectId)
                    )
                else:
                    object_object_relation_tables[(obj_type, obj.type)].add(
                        (relationship.objectId, obj.id)
                    )

    return object_tables, object_object_relation_tables


def _convert_object_tables_dict_to_pandas(
    object_tables_as_dict: dict[str, dict[str, list]],
) -> dict[str, pd.DataFrame]:
    object_tables_as_df = {}
    for object_table_name, object_table_dict in object_tables_as_dict.items():
        object_tables_as_df[object_table_name] = pd.DataFrame.from_dict(
            object_table_dict, orient="columns"
        )
    return object_tables_as_df


def _convert_object_object_relation_table_dicts_to_pandas(
    object_object_relation_tables_as_dict: dict[tuple, set],
) -> dict[tuple[str, str], pd.DataFrame]:
    object_object_relation_tables_as_df = {}
    for object_type_object_type, table in object_object_relation_tables_as_dict.items():
        if table:
            df = pd.DataFrame(
                table,
                columns=[
                    _get_object_foreign_key_column_name(object_type_object_type[0]),
                    _get_object_foreign_key_column_name(object_type_object_type[1]),
                ],
            )
            df = df.sort_values(list(df.columns))
            df = df.reset_index(drop=True)
            object_object_relation_tables_as_df[object_type_object_type] = df
    return object_object_relation_tables_as_df  # type: ignore


def _join_object_relations_to_object_tables(
    object_tables_as_df: dict[str, pd.DataFrame],
    object_object_relation_tables_as_df: dict[tuple[str, str], pd.DataFrame],
) -> tuple[
    dict[str, pd.DataFrame],
    dict[tuple[str, str], pd.DataFrame],
    set[tuple[str, str, tuple[str, str]]],
]:
    keys_to_del = set()
    foreign_key_relations = set()
    for object_type_object_type, df in object_object_relation_tables_as_df.items():
        if (
            len(df)
            == df[
                _get_object_foreign_key_column_name(object_type_object_type[0])
            ].nunique()
        ):
            object_tables_as_df[object_type_object_type[0]] = object_tables_as_df[
                object_type_object_type[0]
            ].merge(
                df,
                how="left",
                left_on="ID",
                right_on=_get_object_foreign_key_column_name(
                    object_type_object_type[0]
                ),
            )
            object_tables_as_df[object_type_object_type[0]].drop(
                columns=_get_object_foreign_key_column_name(object_type_object_type[0]),
                inplace=True,
            )
            foreign_key_relations.add(
                (
                    _get_object_table_name(object_type_object_type[1]),
                    _get_object_table_name(object_type_object_type[0]),
                    (
                        "ID",
                        _get_object_foreign_key_column_name(object_type_object_type[1]),
                    ),
                )
            )
            keys_to_del.add(object_type_object_type)
        elif (
            len(df)
            == df[
                _get_object_foreign_key_column_name(object_type_object_type[1])
            ].nunique()
        ):
            object_tables_as_df[object_type_object_type[1]] = object_tables_as_df[
                object_type_object_type[1]
            ].merge(
                df,
                how="left",
                left_on="ID",
                right_on=_get_object_foreign_key_column_name(
                    object_type_object_type[1]
                ),
            )
            object_tables_as_df[object_type_object_type[1]].drop(
                columns=_get_object_foreign_key_column_name(object_type_object_type[1]),
                inplace=True,
            )
            foreign_key_relations.add(
                (
                    _get_object_table_name(object_type_object_type[0]),
                    _get_object_table_name(object_type_object_type[1]),
                    (
                        "ID",
                        _get_object_foreign_key_column_name(object_type_object_type[0]),
                    ),
                )
            )
            keys_to_del.add(object_type_object_type)
        else:
            foreign_key_relations.add(
                (
                    _get_object_table_name(object_type_object_type[0]),
                    _get_object_object_relation_table_name(*object_type_object_type),
                    (
                        "ID",
                        _get_object_foreign_key_column_name(object_type_object_type[0]),
                    ),
                )
            )
            foreign_key_relations.add(
                (
                    _get_object_table_name(object_type_object_type[1]),
                    _get_object_object_relation_table_name(*object_type_object_type),
                    (
                        "ID",
                        _get_object_foreign_key_column_name(object_type_object_type[1]),
                    ),
                )
            )

    for key in keys_to_del:
        del object_object_relation_tables_as_df[key]

    return (
        object_tables_as_df,
        object_object_relation_tables_as_df,
        foreign_key_relations,
    )


def _extract_object_tables(
    ocel2_log: OCEL2JSON, object_id_to_type_mapping: dict[str, str]
) -> tuple[
    dict[str, pd.DataFrame],
    dict[str, pd.DataFrame],
    set[tuple[str, str, tuple[str, str]]],
]:
    object_type_str_to_type_mapping = {
        obj_type.name: obj_type for obj_type in ocel2_log.objectTypes
    }
    (
        object_tables_as_dict,
        object_object_relation_tables_as_dict,
    ) = _initialize_empty_object_tables(ocel2_log)

    for obj in ocel2_log.objects:
        (
            object_tables_as_dict,
            object_object_relation_tables_as_dict,
        ) = _add_object_to_object_tables(
            obj=obj,
            object_tables=object_tables_as_dict,
            object_object_relation_tables=object_object_relation_tables_as_dict,
            object_type_str_to_type_mapping=object_type_str_to_type_mapping,
            object_id_to_type_mapping=object_id_to_type_mapping,
        )

    object_tables_as_df = _convert_object_tables_dict_to_pandas(
        object_tables_as_dict=object_tables_as_dict
    )
    object_object_relation_tables_as_df = (
        _convert_object_object_relation_table_dicts_to_pandas(
            object_object_relation_tables_as_dict=object_object_relation_tables_as_dict
        )
    )
    (
        object_tables_as_df,
        object_object_relation_tables_as_df,
        foreign_key_relations,
    ) = _join_object_relations_to_object_tables(
        object_tables_as_df, object_object_relation_tables_as_df
    )

    object_tables_as_df = {
        _get_object_table_name(object_table_name): df
        for object_table_name, df in object_tables_as_df.items()
    }
    object_object_relation_tables_as_df_renamed = {
        _get_object_object_relation_table_name(
            object_type_name, related_object_type_name
        ): df
        for (
            object_type_name,
            related_object_type_name,
        ), df in object_object_relation_tables_as_df.items()
    }

    return (
        object_tables_as_df,
        object_object_relation_tables_as_df_renamed,
        foreign_key_relations,
    )
