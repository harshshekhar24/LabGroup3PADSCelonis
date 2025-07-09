import streamlit as st

from prototypes.draft.functions import change_page
from common.data_loader.sql_accessor.base import SQLAccessor
from common.data_loader.meta_information.sql_view import SQLView


# This view shows the results and parameters of the mining algorithms
def data_selection_table():
    st.title("Select Table")
    accessor: SQLAccessor = st.session_state.sql_accessor
    meta_infos: SQLView = st.session_state.sql_view

    selected_table = st.selectbox(
        "Table to display",
        sorted(meta_infos.tables.keys()),
        format_func=(lambda x: f"{x} / {meta_infos.tables[x].display_name}"),
    )
    df = st.dataframe(
        accessor.execute_query(f"SELECT * FROM {selected_table} ORDER BY process_execution_id DESC LIMIT 5000")
    )
    st.session_state.df = df

    col1, col2, col3 = st.columns([1, 8, 1])

    with col1:
        if st.button("Back"):
            change_page("data_selection")
    with col3:
        if st.button("Next", disabled=(st.session_state.df is None)):
            change_page("pattern_mining")
