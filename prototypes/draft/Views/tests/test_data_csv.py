import pandas as pd
from streamlit.testing.v1 import AppTest

dummy_df = pd.DataFrame({"Time": [1, 2, 3], "Event": ["a", "b", "c"]})


def test_upload(monkeypatch):
    at = AppTest.from_file("../../main.py")
    # Navigate to upload page from start
    at.session_state["page"] = "start"
    # find and click the "Upload CSV" button (second button)
    at.run(timeout=10)
    for b in at.button:
        if b.label == "Upload CSV":
            b.click()
    at.run(timeout=10)
    assert at.session_state["page"] == "upload"

    # Check initial state
    assert at.title[0].value == "Frequent Pattern Mining"
    assert len(at.dataframe) == 0
    assert len(at.get("file_uploader")) == 1

    # Before csv is uploaded, next is disabled
    next_btn = next(b for b in at.button if b.label == "Next")
    assert next_btn.disabled

    # Set the dataframe to simulate file upload
    at.session_state["df"] = dummy_df
    at.run()

    # Check if the rendered dataframe matches the dummy dataframe
    rendered_df = at.dataframe[0].value
    pd.testing.assert_frame_equal(rendered_df, dummy_df)

    # Test next button and enabled
    next_btn = next(b for b in at.button if b.label == "Next")
    assert not next_btn.disabled
    next_btn.click()  # "Next" button
    at.run()

    assert at.session_state.page == "pattern_mining"

    # Test back button
    at.session_state["page"] = "upload"
    at.run()
    at.button[0].click()  # "Back" button
    at.run()
    assert at.session_state.page == "start"
