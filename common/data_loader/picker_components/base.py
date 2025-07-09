from abc import ABC, abstractmethod
from typing import Callable

import streamlit as st

from common.utils.streamlit_utils import execute_async_call


class PickerComponent(ABC):
    def __init__(self, picker_key: str = "0", tab_title: str = ""):
        self.tab_title = tab_title
        self.load_callback: Callable[[], None] = lambda: None
        self.picker_key = picker_key + "_" + self.get_tab_title()
        self.execute_prototype_clicked_streamlit_key = (
            "execute_prototype_clicked_" + self.picker_key
        )

    def get_tab_title(self) -> str:
        return self.tab_title

    @staticmethod
    @abstractmethod
    async def _validate_preconditions() -> None:
        pass

    @abstractmethod
    async def _fetch_data(self) -> None:
        pass

    def _click_execute_prototype_button(self) -> None:
        st.session_state[self.execute_prototype_clicked_streamlit_key] = True

    @abstractmethod
    def is_prototype_button_disabled(self) -> bool:
        pass

    def _build_execute_prototype_button(self) -> None:
        st.button(
            "Execute Prototype",
            key="execute_prorotype_" + str(self.picker_key),
            disabled=self.is_prototype_button_disabled(),
            on_click=self._click_execute_prototype_button,
        )

    @abstractmethod
    async def _load_data_model(self) -> None:
        pass

    @abstractmethod
    def _build_picker(self) -> None:
        pass

    async def _build(self) -> None:
        if self.execute_prototype_clicked_streamlit_key not in st.session_state:
            st.session_state[self.execute_prototype_clicked_streamlit_key] = False

        with st.expander("Prototyping settings"):
            self._build_picker()

        self._build_execute_prototype_button()

        if st.session_state[self.execute_prototype_clicked_streamlit_key]:
            await self._load_data_model()
            self.load_callback()

    async def _fetch_and_build(
        self,
    ) -> None:

        await self._validate_preconditions()
        await self._fetch_data()
        await self._build()

    def run(self, load_callback: Callable[[], None]) -> None:
        self.load_callback = load_callback
        execute_async_call(self._fetch_and_build)
