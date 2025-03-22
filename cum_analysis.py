import streamlit as st
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from utils import setup_duckdb

# Set page configuration
# App title and description
def app():
    conn = setup_duckdb()

    innings = conn.execute("SELECT DISTINCT inning FROM ipl_data ORDER BY inning").fetchall()
    innings = [row[0] for row in innings]

    teams = conn.execute("SELECT DISTINCT batting_team FROM ipl_data ORDER BY batting_team").fetchall()
    teams = [row[0] for row in teams]

    overs_range = conn.execute("SELECT MIN(over), MAX(over) FROM ipl_data").fetchone()

    bowlers = conn.execute("SELECT DISTINCT bowler FROM ipl_data ORDER BY bowler").fetchall()
    bowlers = [row[0] for row in bowlers]

    st.sidebar.header("Filters")

    # Add filters to sidebar
    selected_innings = st.sidebar.multiselect("Select Innings", innings, default=innings)
    selected_batting_teams = st.sidebar.multiselect("Select Batting Teams", teams, default=None)
    selected_bowling_teams = st.sidebar.multiselect("Select Bowling Teams", teams, default=None)

    # Add over range slider
    selected_overs = st.sidebar.slider("Over Range", int(overs_range[0]), int(overs_range[1]),
                                       (int(overs_range[0]), int(overs_range[1])))

    # Bowler selection
    selected_bowlers = st.sidebar.multiselect("Select Bowlers", bowlers, default=None)

    has_target = st.sidebar.checkbox("Filter by Target Runs")
    if has_target:
        target_range = conn.execute("SELECT MIN(target_runs), MAX(target_runs) FROM ipl_data").fetchone()
        min_target = int(target_range[0]) if target_range[0] > 0 else 0
        max_target = int(target_range[1]) if target_range[1] > 0 else 200
        selected_target = st.sidebar.slider("Target Runs Range", min_target, max_target, (min_target, max_target))

    where_clauses = []
    params = {}

    if selected_innings:
        placeholders = ', '.join([f'{i}' for i in selected_innings])
        where_clauses.append(f"inning IN ({placeholders})")
        for i, inning in enumerate(selected_innings):
            params[i] = inning

    if selected_batting_teams:
        start_idx = len(params)
        placeholders = ', '.join([f'${i}' for i in range(start_idx, start_idx + len(selected_batting_teams))])
        where_clauses.append(f"batting_team IN ({placeholders})")
        for i, team in enumerate(selected_batting_teams, start=start_idx):
            params[i] = team

    if selected_bowling_teams:
        start_idx = len(params)
        placeholders = ', '.join([f'${i}' for i in range(start_idx, start_idx + len(selected_bowling_teams))])
        where_clauses.append(f"bowling_team IN ({placeholders})")
        for i, team in enumerate(selected_bowling_teams, start=start_idx):
            params[i] = team

    if selected_bowlers:
        start_idx = len(params)
        placeholders = ', '.join([f'${i}' for i in range(start_idx, start_idx + len(selected_bowlers))])
        where_clauses.append(f"bowler IN ({placeholders})")
        for i, bowler in enumerate(selected_bowlers, start=start_idx):
            params[i] = bowler

    where_clauses.append(f"over >= {selected_overs[0]} AND over <= {selected_overs[1]}")

    if has_target:
        where_clauses.append(
            f"(target_runs >= {selected_target[0]} AND target_runs <= {selected_target[1]}) OR target_runs = 0")

        # Construct the full WHERE clause
    where_clause = " AND ".join(where_clauses) if where_clauses else "1=1"

    tab1, tab2, tab3, tab4 = st.tabs(["Runs Analysis", "Wickets Analysis", "Bowler Performance", "Match Progression"])

    with tab1:
        st.header("Runs Analysis")

        # SQL queries for runs analysis
        runs_stats_sql = f"""
                SELECT 
                    AVG(runs_in_over) as avg_runs_per_over,
                    SUM(runs_in_over) as total_runs,
                    MAX(runs_in_over) as max_runs_in_over,
                    MEDIAN(runs_in_over) as median_runs_per_over,
                    STDDEV(runs_in_over) as std_dev_runs
                FROM ipl_data
                WHERE {where_clause}
                """

        runs_by_over_sql = f"""
                SELECT 
                    over,
                    AVG(runs_in_over) as avg_runs
                FROM ipl_data
                WHERE {where_clause}
                GROUP BY over
                ORDER BY over
                """

        # Execute the SQL queries
        st.write(runs_stats_sql)
        st.write(params)
        runs_stats = conn.execute(runs_stats_sql).fetchone()
        runs_by_over = conn.execute(runs_by_over_sql).fetchdf()

        # Get runs distribution data
        runs_distribution_sql = f"""
                SELECT runs_in_over, COUNT(*) as frequency
                FROM ipl_data
                WHERE {where_clause}
                GROUP BY runs_in_over
                ORDER BY runs_in_over
                """
        runs_distribution = conn.execute(runs_distribution_sql).fetchdf()

        # Display statistics
        st.subheader("Runs Statistics")

        col1, col2 = st.columns(2)
        with col1:
            st.metric("Average Runs per Over", f"{runs_stats[0]:.2f}")
            st.metric("Total Runs", f"{runs_stats[1]}")
            st.metric("Maximum Runs in an Over", f"{runs_stats[2]}")

        with col2:
            st.metric("Median Runs per Over", f"{runs_stats[3]:.2f}")
            st.metric("Standard Deviation", f"{runs_stats[4]:.2f}")
            st.metric("Economy Rate", f"{runs_stats[0]:.2f}")

            # Display runs distribution
        if not runs_distribution.empty:
            st.subheader("Runs Distribution")
            fig, ax = plt.subplots(figsize=(10, 6))
            sns.barplot(x='runs_in_over', y='frequency', data=runs_distribution, ax=ax)
            ax.set_xlabel('Runs in Over')
            ax.set_ylabel('Frequency')
            ax.set_title('Distribution of Runs per Over')
            st.pyplot(fig)

            # Display runs by over
        if not runs_by_over.empty:
            st.subheader("Average Runs by Over Number")
            fig, ax = plt.subplots(figsize=(12, 6))
            sns.barplot(x='over', y='avg_runs', data=runs_by_over, ax=ax)
            ax.set_xlabel('Over Number')
            ax.set_ylabel('Average Runs')
            ax.set_title('Average Runs by Over Number')
            st.pyplot(fig)

    with tab2:
        st.write("TODO")

    with tab3:
        st.write("TODO")

    with tab4:
        st.write("TODO")