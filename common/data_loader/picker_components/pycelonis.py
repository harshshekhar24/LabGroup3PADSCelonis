import os
from typing import Any, Optional

import streamlit as st
from pycelonis import get_celonis
from pycelonis.ems import DataModel, DataPool

from common.data_loader.meta_information.sql_view import SQLView
from common.data_loader.picker_components.base import PickerComponent
from common.data_loader.picker_components.helper import (
    get_selected_from_session_state_list,
)
from common.data_loader.sql_accessor.duckdb import LocalDuckDBAccessor


class PyCelonisModelPickerComponent(PickerComponent):
    def __init__(self, tab_title: str = "PyCelonis", **kwargs: Any):
        super().__init__(tab_title=tab_title, **kwargs)

    @staticmethod
    async def _validate_preconditions() -> None:
        if "celonis_instance" not in st.session_state:
            try:
                celonis = get_celonis(
                    base_url=os.environ["EMS_BASE_URL"],
                    api_token=os.environ["EMS_APP_KEY"],
                    permissions=False,
                )
            except Exception as exception:
                raise RuntimeError(
                    "[ERROR] Could not connect to PyCelonis! "
                    + "Make sure EMS_BASE_URL and EMS_APP_KEY are env variables are set correctly, "
                    + str(exception)
                ) from exception
            st.session_state.celonis_instance = celonis

    @staticmethod
    async def _fetch_data_pools() -> None:
        if "data_pools" not in st.session_state:
            st.session_state.data_pools = (
                st.session_state.celonis_instance.data_integration.get_data_pools()
            )

    @staticmethod
    async def _fetch_data_models(data_pool: DataPool) -> None:
        if (
            "prev_selected_data_pool_name" not in st.session_state
            or st.session_state.prev_selected_data_pool_name != data_pool.name
        ):
            st.session_state.prev_selected_data_pool_name = data_pool.name
            st.session_state.data_models = data_pool.get_data_models()

    async def _fetch_data(self) -> None:
        await self._fetch_data_pools()

        selected_data_pool: Optional[DataPool] = (
            self._get_selected_data_pool_from_state()
        )
        if selected_data_pool is not None:
            await self._fetch_data_models(selected_data_pool)

    @staticmethod
    def _build_data_pool_selector() -> None:
        st.selectbox(
            "Data Pool",
            [""]
            + sorted([data_pool.name for data_pool in st.session_state.data_pools]),
            key="selected_data_pool_name",
        )

    @staticmethod
    def _build_data_model_selector() -> None:
        if "data_models" not in st.session_state:
            st.session_state.data_models = []
        st.selectbox(
            "Data Model",
            [""]
            + sorted([data_model.name for data_model in st.session_state.data_models]),
            key="selected_data_model_name",
            disabled=not bool(st.session_state.selected_data_pool_name),
        )

    def is_prototype_button_disabled(self) -> bool:
        return not (
            st.session_state.selected_data_pool_name
            and st.session_state.selected_data_model_name
        )

    async def _load_data_model(self) -> None:
        progress_bar = st.progress(0, text="Loading Data Model")

        data_model = self._get_selected_data_model_from_state()
        data_pool = self._get_selected_data_pool_from_state()

        def progress_call_back(progress: float) -> None:
            progress_bar.progress(0.05 + progress * 0.95, text="Loading Data Model")

        progress_bar.progress(0, text="Preparing App")
        progress_bar.progress(0.05, text="Loading initial View")
        st.session_state.sql_view = SQLView.initial_view_from_pycelonis_data_model(
            data_model
        )
        st.session_state.sql_view.cleanup_foreign_keys()

        st.session_state.sql_accessor = (
            LocalDuckDBAccessor.create_local_copy_of_data_model(
                data_pool.id if data_pool else None,
                data_model.id if data_model else None,
                st.session_state.sql_view,
                progress_call_back,
            )
        )

        progress_bar.progress(1.0, text="Finished")

    def _build_picker(self) -> None:
        col1, col2 = st.columns(2)

        with col1:
            self._build_data_pool_selector()
        with col2:
            self._build_data_model_selector()

    @staticmethod
    def _get_selected_data_pool_from_state() -> Optional[DataPool]:
        def data_pool_to_name(data_pool: DataPool) -> Optional[str]:
            return data_pool.name

        return get_selected_from_session_state_list(
            element_key="selected_data_pool_name",
            list_key="data_pools",
            get_element_identifier=data_pool_to_name,
        )

    @staticmethod
    def _get_selected_data_model_from_state() -> Optional[DataModel]:
        def data_model_to_name(data_model: DataModel) -> Optional[str]:
            return data_model.name

        return get_selected_from_session_state_list(
            element_key="selected_data_model_name",
            list_key="data_models",
            get_element_identifier=data_model_to_name,
        )
