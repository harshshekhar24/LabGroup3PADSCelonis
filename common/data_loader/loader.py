from typing import Callable, Awaitable

import streamlit as st

from common.data_loader.picker_components.base import PickerComponent
from common.utils.streamlit_utils import execute_async_call


class DataLoader:
    def __init__(
        self,
        prototype_callback: Callable[[], Awaitable[None]],
        picker_components: list[PickerComponent],
        key: str = "0",
    ):
        self.prototype_callback = prototype_callback
        self.picker_components = picker_components
        self.key = key
        self.data_model_is_loaded_key = "data_model_is_loaded" + key

    def _post_load_callback(self) -> None:
        for key in st.session_state.keys():
            if key not in ("sql_view", "sql_accessor"):
                del st.session_state[key]
        st.session_state[self.data_model_is_loaded_key] = True
        st.rerun()

    def run(self) -> None:
        if self.data_model_is_loaded_key not in st.session_state:
            st.session_state[self.data_model_is_loaded_key] = False

        if not st.session_state[self.data_model_is_loaded_key]:
            tabs = st.tabs(
                [picker.get_tab_title() for picker in self.picker_components]
            )
            for tab, picker in zip(tabs, self.picker_components):
                with tab:
                    picker.run(self._post_load_callback)
        else:
            execute_async_call(self.prototype_callback)
