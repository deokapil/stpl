import pandas as pd
import duckdb


def analyze_ipl_data(csv_path, output_path=None):
    """
    Transform IPL ball-by-ball data into over-by-over summaries with cumulative runs.

    Parameters:
    -----------
    csv_path : str
        Path to the CSV file containing ball-by-ball IPL data
    output_path : str, optional
        Path where the output CSV will be saved. If None, doesn't save to a file.

    Returns:
    --------
    pandas.DataFrame
        DataFrame containing the over-by-over summary with cumulative runs
    """
    df = pd.read_csv(csv_path, na_values=['-'], keep_default_na=True)
    # Connect to DuckDB
    conn = duckdb.connect(database=':memory:')

    # Read the CSV file
    print(f"Reading data from {csv_path}...")
    conn.execute(f"CREATE TABLE ipl_data AS SELECT * FROM df")

    # Create the over-by-over summary with cumulative runs
    print("Transforming ball-by-ball data to over-by-over summary...")
    query = """
    WITH over_summary AS (
        SELECT 
            match_id,
            inning,
            batting_team,
            bowling_team,
            over,
            bowler,
            season,
            city,
            venue,
            target_runs,
            target_overs,
            SUM(total_runs) AS runs_in_over,
            SUM(is_wicket) AS wickets_in_over
        FROM 
            ipl_data
        GROUP BY 
            match_id, inning, batting_team, bowling_team, over, bowler, 
            season, city, venue, target_runs, target_overs
    )

    SELECT 
        os.*,
        SUM(runs_in_over) OVER (
            PARTITION BY match_id, inning 
            ORDER BY over
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS cumulative_runs,
        SUM(wickets_in_over) OVER (
            PARTITION BY match_id, inning 
            ORDER BY over
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS cumulative_wickets
    FROM 
        over_summary os
    ORDER BY 
        match_id, inning, over
    """

    # Execute the query and get the results as a DataFrame
    result_df = conn.execute(query).fetchdf()

    # Save to CSV if output path is provided
    if output_path:
        print(f"Saving results to {output_path}...")
        result_df.to_csv(output_path, index=False)

    # Close the connection
    conn.close()

    print("Analysis completed successfully!")
    return result_df


if __name__ == "__main__":
    # Example usage
    input_file = "../out.csv"
    output_file = "../ipl_obo_summary.csv"

    # Run the analysis
    over_summary_df = analyze_ipl_data(input_file, output_file)

    # Display the first few rows of the result
    print("\nSample of the over-by-over summary:")
    print(over_summary_df.head())

    # Show basic statistics
    print("\nBasic statistics:")
    print(f"Total number of overs analyzed: {len(over_summary_df)}")
    print(f"Average runs per over: {over_summary_df['runs_in_over'].mean():.2f}")
    print(f"Maximum runs in a single over: {over_summary_df['runs_in_over'].max()}")
    print(f"Total wickets: {over_summary_df['wickets_in_over'].sum()}")