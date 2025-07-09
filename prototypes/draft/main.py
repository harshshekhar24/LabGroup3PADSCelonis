import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))
import streamlit as st

from Views.data_selection import data_selection_view
from prototypes.draft.Views.data_selection_table import data_selection_table
from prototypes.draft.Views.pattern_view import pattern_view
from prototypes.draft.Views.pattern_viz_view import pattern_viz_view
from prototypes.draft.Views.data_csv import upload_view
from prototypes.draft.functions import change_page

# Initialize session_state
if "page" not in st.session_state:
    st.session_state.page = "start"
    # st.session_state.page = "pattern_viz"

if "df" not in st.session_state:
    st.session_state.df = None

if "selected_model" not in st.session_state:
    st.session_state.selected_model = None

if "operation_mode" not in st.session_state:
    st.session_state.operation_mode = None


if "data_model" not in st.session_state:
    st.session_state.data_model = None
if "sql_accessor" not in st.session_state:
    st.session_state.sql_accessor = None
if "sql_view" not in st.session_state:
    st.session_state.sql_view = None


def main():
    # Set initial page settings
    st.set_page_config(layout="wide", page_title="Frequent Pattern Mining")

    # Routing for different pages
    if st.session_state.page == "start":
        st.title("Frequent Pattern Mining")
        st.write("This app helps you mine frequent patterns from sequential data.")
        st.write("Please select a data source to start.")
        col1, col2, col3 = st.columns([1, 8, 1])
        with col1:
            if st.button("Import Data from Celonis"):
                st.session_state.operation_mode = "celonisData"
                change_page("data_selection")
        with col3:
            if st.button("Upload CSV"):
                st.session_state.operation_mode = "uploadCSV"
                change_page("upload")

    elif st.session_state.page == "data_selection":
        data_selection_view()
    elif st.session_state.page == "upload":
        upload_view()
    elif st.session_state.page == "pattern_mining":
        pattern_view()
    elif st.session_state.page == "pattern_viz":
        pattern_viz_view()
    elif st.session_state.page == "data_selection_table":
        data_selection_table()


if __name__ == "__main__":
    main()
