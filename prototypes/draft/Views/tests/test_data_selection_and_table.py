from streamlit.testing.v1 import AppTest
import streamlit as st
import pytest


@pytest.fixture
def fake_picker(monkeypatch):
    class FakePicker:
        def _build_picker(self):
            pass

        async def _validate_preconditions(self):
            pass

        async def _fetch_data(self):
            pass

        async def _load_data_model(self):
            st.session_state.data_model = "MockModel"

    monkeypatch.setattr(
        "prototypes.draft.Views.data_selection.PyCelonisModelPickerComponent",
        FakePicker,
    )


def test_data_selection(fake_picker):
    at = AppTest.from_file("../../main.py")
    # Navigate to data_selection
    at.session_state["page"] = "start"
    at.run()
    # click "Use Internal Sample Data"
    for b in at.button:
        if b.label == "Use Internal Sample Data":
            b.click()

    at.run()
    assert at.session_state["page"] == "data_selection"
    assert "Select Datasource" == at.title[0].value
    assert "Load Datamodel" == at.button[0].label
    assert "Back" == at.button[1].label
    assert "Next" == at.button[2].label

    # Test next button disabled
    load_btn = next(b for b in at.button if b.label == "Load Datamodel")
    next_btn = next(b for b in at.button if b.label == "Next")
    assert load_btn
    assert next_btn.disabled

    # Click load to sets data_model

    # Click Next to data_selection_table
    at.session_state["sql_accessor"] = type(
        "Accessor",
        (),
        {"execute_query": lambda self, query: [{"id": 1, "value": "abc"}]},
    )()

    at.session_state["sql_view"] = type(
        "Meta",
        (),
        {"tables": {"table1": type("TableMeta", (), {"display_name": "My Table"})()}},
    )()
    at.session_state["data_model"] = "MockModel"
    load_btn.click()
    at.run()
    next_btn = next(b for b in at.button if b.label == "Next")
    next_btn.click()
    at.run()
    assert at.session_state["page"] == "data_selection_table"

    # On table view: selectbox populated
    assert "table1 / My Table" in at.selectbox[0].options
    df = at.dataframe[0].value
    assert "value" in df.columns
    assert df.iloc[0]["value"] == "abc"
    rendered = at.dataframe[0].value
    assert isinstance(rendered, list) or hasattr(rendered, "__getitem__")

    # Next from table to pattern mining
    next_btn = next(b for b in at.button if b.label == "Next")
    next_btn.click()
    at.run()
    assert at.session_state["page"] == "pattern_mining"
