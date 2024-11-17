import pandas as pd

# Load the parsed logs from the CSV file
bgl_csv_path = 'csv/BGL.csv'
df = pd.read_csv(bgl_csv_path)

def formatSortAndIndexByDate(df: pd.DataFrame):
    # 
    df['Datetime'] = pd.to_datetime(df['Time'], format='%Y-%m-%d-%H.%M.%S.%f')

    df = df.sort_values(by='Datetime')

    # Set 'Datetime' as index for time-based resampling
    df.set_index('Datetime', inplace=True)

    return df

def sliding_window_aggregate(df, window_size='1h', step_size='5min'):
    """
    Aggregates log data using a sliding window approach.

    :param df: The DataFrame containing log data.
    :param window_size: The size of the sliding window (e.g., '30T' for 30 minutes).
    :param step_size: The step size for the sliding window (e.g., '15T' for 15 minutes).
    :return: A DataFrame with aggregated data for each window, including an 'Anomaly' column.
    """
    df = formatSortAndIndexByDate(df)

    # Determine anomalous entries
    # anomalies: rows where the 'Label' column is not "-"
    df['Anomaly'] = df[df['Label'] != "-"].shape[0] > 0
    df['Anomaly'] = df['Anomaly'].astype(int)

    # Define the step and window sizes as timedeltas
    window_delta = pd.to_timedelta(window_size)
    step_delta = pd.to_timedelta(step_size)

    # Initialize an empty list to store the results
    results = []

    # Determine the start and end time of the sliding windows
    start_time = df.index.min()
    end_time = df.index.max()

    current_start = start_time
    current_end = current_start + window_delta
    while current_end <= end_time:
        # Define the end of the current window
        current_end = current_start + window_delta
        # Filter rows within the current window
        window_df = df.loc[current_start:current_end]
        # print('window_df', window_df)
        cluster_counts = window_df.groupby('Event ID').size()
        # print("cluster_counts, first window\n", cluster_counts)
        window_anomalies = window_df['Anomaly'].sum()
        # print('window_anomalies', window_anomalies)
        window_data = {
        'Window Start' : current_start,
        'Window End':  current_end,
        'Anomaly' : 1 if window_anomalies > 0 else 0,
        **cluster_counts,
        }
        results.append(window_data)
        current_start += step_delta

    result_df = pd.DataFrame(results).fillna(0)
    return result_df

dataframe = sliding_window_aggregate(df)
print(dataframe.head())
print(dataframe.tail())
dataframe.to_csv('csv/BGL-aggregated2.csv')