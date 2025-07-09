from streamlit.testing.v1 import AppTest


def test_start_page():
    at = AppTest.from_file("../main.py")
    at.run()

    # Check if the session state is initialized correctly
    assert "page" in at.session_state
    assert "df" in at.session_state
    assert "selected_model" in at.session_state
    assert "data_model" in at.session_state
    assert "sql_accessor" in at.session_state
    assert "sql_view" in at.session_state
    assert at.session_state.page == "start"

    # Check the title
    assert at.title[0].value == "Frequent Pattern Mining"
    assert "please select a data source" in at.markdown[1].value.lower()

    # Two buttons: Internal sample & Upload CSV
    labels = [b.label for b in at.button]
    assert "Use Internal Sample Data" in labels
    assert "Upload CSV" in labels
