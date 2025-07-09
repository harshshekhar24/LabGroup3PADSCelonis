import types
import pytest
import pandas as pd
import streamlit as st
from unittest.mock import MagicMock, patch

from prototypes.draft import functions
from prototypes.draft.functions import create_combined_eventlog


def test_change_page(monkeypatch):
    sesssion_state = types.SimpleNamespace()
    monkeypatch.setattr(st, "session_state", sesssion_state)

    rerun_called = {"count": 0}

    monkeypatch.setattr(
        st, "rerun", lambda: rerun_called.update({"count": rerun_called["count"] + 1})
    )
    functions.change_page("pattern_mining")

    assert sesssion_state.page == "pattern_mining"
    assert rerun_called["count"] == 1


@pytest.fixture
def mock_accessor():
    accessor = MagicMock()
    accessor.execute_query = MagicMock()
    accessor._duckdb_connection.register = MagicMock()
    accessor._duckdb_connection.execute = MagicMock()
    return accessor


@pytest.fixture
def mock_meta_infos():
    class DummyMeta:
        def __init__(self):
            self.tables = {
                "e_celonis_login": None,
                "e_celonis_click": None,
                "not_event_table": None,
            }

    return DummyMeta()


def test_create_combined_eventlog(mock_accessor, mock_meta_infos):
    # Simulate return value of execute_query
    login_df = pd.DataFrame(
        {"ID": [1], "Time": ["2023-01-01 12:00:00"], "Event1_ID": [42]}
    )
    click_df = pd.DataFrame(
        {"ID": [2], "Time": ["2023-01-01 12:05:00"], "Event2_ID": [84]}
    )

    def query_side_effect(query):
        if "e_celonis_login" in query:
            return login_df
        elif "e_celonis_click" in query:
            return click_df
        else:
            return pd.DataFrame()

    mock_accessor.execute_query.side_effect = query_side_effect

    # Patch st.warning and st.error to avoid Streamlit dependency in test
    with patch("prototypes.draft.functions.st.warning"), patch(
        "prototypes.draft.functions.st.error"
    ), patch(
        "common.data_loader.meta_information.table_meta.TableMeta"
    ) as MockTableMeta, patch(
        "common.data_loader.meta_information.column_meta.ColumnMeta"
    ) as MockColumnMeta, patch(
        "common.data_loader.meta_information.table_meta.TableType"
    ) as MockTableType:

        MockTableMeta.construct_display_name.return_value = "Combined eventlog"
        MockTableMeta.return_value = "dummy_meta"
        MockColumnMeta.create_from_column_name.side_effect = (
            lambda col, df: f"meta_{col}"
        )
        MockTableType.EVENT = "EVENT"

        combined_df = create_combined_eventlog(mock_accessor, mock_meta_infos)

        assert not combined_df.empty
        assert set(combined_df.columns) == {
            "EventID",
            "Timestamp",
            "EventName",
            "Event1_ID",
            "Event2_ID",
        }
        assert len(combined_df) == 2
        assert mock_accessor._duckdb_connection.register.called
        assert mock_accessor._duckdb_connection.execute.called
        assert "el_combined_eventlog" in mock_meta_infos.tables
