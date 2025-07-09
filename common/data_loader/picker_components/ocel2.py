import streamlit as st

from typing import Any

from common.data_loader.meta_information.sql_view import SQLView
from common.data_loader.ocel2.ocdm_table_extractor import create_ocdm_tables_from_data
from common.data_loader.sql_accessor.duckdb import LocalDuckDBAccessor
from common.data_loader.picker_components.base import PickerComponent


class OCEL2ModelPickerComponent(PickerComponent):
    def __init__(self, tab_title: str = "OCEL 2.0", **kwargs: Any):
        super().__init__(tab_title=tab_title, **kwargs)

        self.file_streamlit_key = "file_" + self.picker_key

    @staticmethod
    async def _validate_preconditions() -> None:
        pass

    async def _fetch_data(self) -> None:
        pass

    async def _load_data_model(self) -> None:
        if (
            self.file_streamlit_key in st.session_state
            and st.session_state[self.file_streamlit_key] is not None
        ):
            ocel2_data = st.session_state[self.file_streamlit_key].read()
            tables, foreign_key_relations, table_types = create_ocdm_tables_from_data(
                ocel2_data
            )

            st.session_state.sql_view = SQLView.initial_view_from_ocel2(
                tables, foreign_key_relations, table_types
            )
            st.session_state.sql_view.cleanup_foreign_keys()

            st.session_state.sql_accessor = (
                LocalDuckDBAccessor.create_local_copy_from_ocel2(
                    tables, st.session_state.sql_view
                )
            )

    def is_prototype_button_disabled(self) -> bool:
        return st.session_state[self.file_streamlit_key] is None

    def _build_picker(self) -> None:
        st.file_uploader(
            "Choose an OCEL2 json file",
            key=self.file_streamlit_key,
            type=["json", "jsonocel"],
        )
