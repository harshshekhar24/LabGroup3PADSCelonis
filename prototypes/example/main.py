import streamlit as st

from common.data_loader.loader import DataLoader
from common.data_loader.meta_information.sql_view import SQLView
from common.data_loader.picker_components.ocel2 import OCEL2ModelPickerComponent
from common.data_loader.picker_components.pycelonis import PyCelonisModelPickerComponent
from common.data_loader.sql_accessor.base import SQLAccessor
from common.utils.streamlit_utils import default_streamlit_setup


async def prototype() -> None:
    accessor: SQLAccessor = st.session_state.sql_accessor
    meta_infos: SQLView = st.session_state.sql_view

    selected_table = st.selectbox(
        "Table to display",
        meta_infos.tables.keys(),
        format_func=(lambda x: f"{x} / {meta_infos.tables[x].display_name}"),
    )
    st.dataframe(accessor.execute_query(f"SELECT * FROM {selected_table}"))


def main() -> None:
    default_streamlit_setup()

    DataLoader(
        prototype, [PyCelonisModelPickerComponent(), OCEL2ModelPickerComponent()]
    ).run()


if __name__ == "__main__":
    main()
