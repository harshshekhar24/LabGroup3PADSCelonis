import streamlit as st
import pandas as pd
from prototypes.draft.functions import change_page


# This view allows the user to upload a CSV that should be used for mining
def upload_view():
    st.title("Frequent Pattern Mining")
    st.write("This app helps you mine frequent patterns from sequential data.")

    uploaded_file = st.file_uploader("Upload your CSV file", type=["csv"])

    if st.session_state.df is not None:
        # File uploaded, show preview
        st.write("Preview of uploaded file:")
        st.dataframe(st.session_state.df)
    elif uploaded_file is not None:
        # No file uploaded, show Upload-Field
        df = pd.read_csv(uploaded_file)
        st.write("Preview of uploaded file:")
        st.dataframe(df)
        st.session_state.df = df

    col1, col2, col3 = st.columns([1, 8, 1])

    with col1:
        if st.button("Back"):
            change_page("start")

    with col3:
        if st.button("Next", disabled=(st.session_state.df is None)):
            change_page("pattern_mining")
