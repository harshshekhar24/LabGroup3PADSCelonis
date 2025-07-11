import streamlit as st
import asyncio

from prototypes.draft.functions import change_page
from common.data_loader.picker_components.pycelonis import PyCelonisModelPickerComponent
from prototypes.draft.functions import create_combined_eventlog


async def run_picker(picker: PyCelonisModelPickerComponent):
    await picker._validate_preconditions()
    await picker._fetch_data()
    picker._build_picker()  # bulid the Dropdown


# Tests the data selection view
def data_selection_view():
    st.title("Select Datasource")
    picker = PyCelonisModelPickerComponent()

    # async workaround
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(run_picker(picker))

    st.markdown("---")

    async def load_and_go():
        await picker._load_data_model()

    # Button to trigger the loading of the Datamodel
    if st.button("Load Datamodel"):

        # execute_async_call(load_and_go)

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(picker._load_data_model())

        accessor = st.session_state.sql_accessor
        meta_infos = st.session_state.sql_view

        create_combined_eventlog(accessor, meta_infos)

        st.session_state.data_model = f"Datenmodell: {st.session_state.selected_model}"
        st.success("Datamodel has been loaded.")

    col1, col2, col3 = st.columns([1, 8, 1])

    with col1:
        if st.button("Back"):
            change_page("start")

    with col3:
        # Button to get to the next page, based on the presence of a data model
        if st.button("Next", disabled=(st.session_state.data_model is None)):
            change_page("pattern_mining")
