import logging
from typing import Callable, Optional, TypeVar

import streamlit as st

T = TypeVar("T")


def get_selected_from_session_state_list(
    element_key: str,
    list_key: str,
    get_element_identifier: Callable[[T], Optional[str]],
) -> Optional[T]:
    if element_key not in st.session_state:
        logging.info(
            "Getting %s inside %s from the streamlit state failed,"
            + "since %s is not available in the state.",
            element_key,
            list_key,
            element_key,
        )
        return None
    if not st.session_state[element_key]:
        logging.info(
            "Getting %s inside %s from the streamlit state failed, "
            + "since %s is empty.",
            element_key,
            list_key,
            element_key,
        )
        return None
    if list_key not in st.session_state:
        logging.info(
            "Getting %s inside %s from the streamlit state failed, "
            + "since %s is not available in the state.",
            element_key,
            list_key,
            list_key,
        )
        return None
    if not st.session_state[list_key]:
        logging.info(
            "Getting %s inside %s from the streamlit state failed, "
            + "since %s is empty.",
            element_key,
            list_key,
            list_key,
        )
        return None

    for element in st.session_state[list_key]:
        element_identifier = get_element_identifier(element)
        if (
            element_identifier is not None
            and element_identifier == st.session_state[element_key]
        ):
            return element

    logging.info(
        "Getting %s inside %s from the streamlit state failed, "
        + "since %s did not match any of %s in the state.",
        element_key,
        list_key,
        element_key,
        list_key,
    )
    return None
