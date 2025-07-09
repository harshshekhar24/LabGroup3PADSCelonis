import streamlit as st
import pandas as pd

from algorithms.emma.phase3_episode_mining import run_emma
from prototypes.draft.functions import change_page, flatten_event_log, flatten_event_log_2


def pattern_view():
    if "episodes" not in st.session_state:
        st.session_state.episodes = None
    if "mining_done" not in st.session_state:
        st.session_state.mining_done = False
        
    st.title("Pattern Mining")

    # Get or set defaults for minsup and maxwin
    minsup = st.number_input(
        "Minimum Support",
        min_value=1,
        value=2,
        help="Minimum number of occurrences for an episode to be considered frequent",
    )
    maxwin = st.number_input(
        "Maximum Window Size",
        min_value=1,
        value=3,
        help="Sliding window size in time units for extending episodes",
    )
    col1, col2, col3 = st.columns([2, 6, 2])

    with col1:
        start_mining = st.button("Start Mining")

    with col3:
        if st.button("Go to Visualization", disabled= not st.session_state.mining_done):
            change_page("pattern_viz")

    if start_mining:
        if st.session_state.operation_mode == "uploadCSV":
            df = st.session_state.df
        else:
            accessor = st.session_state.sql_accessor
            df = accessor.execute_query("SELECT * FROM 'el_combined_eventlog' LIMIT 50000")

        if df is None or df.empty:
            st.error("No input data available. Please upload or select a table first.")
            return

        flat_data = flatten_event_log_2(df)

        episodes = run_emma(flat_data, minsup, maxwin)
        st.session_state.episodes = episodes
        st.session_state.mining_done = True
        st.rerun()

    if st.session_state.episodes is not None:
        # Display final episodes
        st.subheader("Frequent Episodes")
        rows = []
        for ep in st.session_state.episodes:
            try:
                episode_steps = ["{" + ",".join(step["activity"]) + "}" if isinstance(step, dict) and "activity" in step else str(step) for step in ep["Episode"]]
                pattern_str = "<" + ", ".join(episode_steps) + ">"
            except Exception as e:
                pattern_str = f"[Error formatting episode: {e}]"

            rows.append({
                "Pattern ID": ep["PatternID"],
                "Episode": pattern_str,
                "Support": ep["Support"]
            })

        df_episodes = pd.DataFrame(rows)
        st.dataframe(df_episodes, use_container_width=True)

    if st.button("Back"):
        if st.session_state.operation_mode == "uploadCSV":
            change_page("upload")
        else:
            change_page("data_selection_table")





