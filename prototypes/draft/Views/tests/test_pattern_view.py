import pandas as pd
from streamlit.testing.v1 import AppTest

dummy_df = pd.DataFrame({"Time": [1, 2, 3], "Event": ["A", "B", "C"]})


def test_pattern_view(monkeypatch):
    at = AppTest.from_file("../../main.py")

    # Set Session-State
    at.session_state["page"] = "pattern_mining"
    at.session_state["df"] = dummy_df

    # Monkey‐patch EMMA steps to no‑ops that record calls
    monkeypatch.setattr(
        "algorithms.emma.phase1_itemset_mining.mine_fima",
        lambda flat, m: {1: {"items": ["A"], "times": [1, 2]}},
    )
    monkeypatch.setattr(
        "algorithms.emma.phase2_encoding.encode_itemsets_from_table",
        lambda table: [[1], [1, 2]],
    )
    monkeypatch.setattr(
        "algorithms.emma.phase3_episode_mining.mine_serial_episodes",
        lambda f1, edb, ms, mw: {(1,): [(1, 2)]},
    )
    at.run()

    # Page title and inputs present
    assert at.title[0].value == "Pattern Mining"
    assert at.number_input[0].value == 2  # default minsup
    assert at.number_input[1].value == 3  # default maxwin

    # Click "Start Mining"
    start_btn = next(b for b in at.button if b.label == "Start Mining")
    start_btn.click()
    at.run()

    # After mining, final table of episodes appears
    assert at.table  # at least one table widget rendered
