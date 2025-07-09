from pathlib import Path
from typing import Callable, Any

import trio
import streamlit as st
from streamlit.web import bootstrap


def default_streamlit_setup() -> None:
    st.set_page_config(layout="wide")


def execute_streamlit_run_via_bootstrap(
    calling_file_absolute_path: str, main_file_name: str = "main.py"
) -> None:
    main_script_path = str(
        Path(calling_file_absolute_path).parent.joinpath(main_file_name)
    )
    bootstrap.run(
        main_script_path=main_script_path, is_hello=False, args=[], flag_options={}
    )


def execute_async_call(function: Callable, *args: Any, **kwargs: Any) -> None:
    trio.run(function, *args, **kwargs)
