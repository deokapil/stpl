import streamlit as st
import duckdb
import pandas as pd

@st.cache_resource
def setup_duckdb():
    # Create a DuckDB connection
    conn = duckdb.connect(database=':memory:')

    df = pd.read_csv("Datasets/ipl_obo_summary.csv", na_values=['-', 'NA'], keep_default_na=True)
    conn.register("ipl_data", df)
    return conn