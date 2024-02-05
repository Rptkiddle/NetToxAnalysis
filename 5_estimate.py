#!/usr/bin/env python3
import argparse
import pickle
import numpy as np
import pandas as pd
from idtxl.data import Data
from idtxl.multivariate_te import MultivariateTE

def main(input_file, output_file, retox_column, source_column, datetime_column, window_size, iterations):
    '''takes a .csv file with toxicity score, sender, and datetime in separate columns, returns .pkl file with toxicity network data, and .csv files with attribution data'''

    # 0. CONFIGURATION:
    network_analysis = MultivariateTE()

    # Settings for IDTxl estimator (see docs):
    settings = {
        'cmi_estimator': 'JidtGaussianCMI',
        'max_lag_sources': 6,
        'min_lag_sources': 1,
        'alpha_max_stat': 0.01,
        'alpha_min_stat': 0.01,
        'permute_in_time': True,
        'verbose': True
    }

    # 1. LOAD AND REFORMAT DATA:

    # Load resampled data
    resampled = pd.read_csv(input_file)

    # Reformat date column (str -> datetime):
    resampled[datetime_column] = pd.to_datetime(resampled[datetime_column])

    # Set datetime index (time series data):
    resampled = resampled.set_index(datetime_column).sort_index()

    # Pivot long into wide (sparse) format:
    pivoted = pd.pivot(resampled, columns=source_column, values=retox_column)

    # 2. GENERATE GLOBAL ID LIST FROM RESAMPLED DATA:
    global_IDs = dict(zip(pivoted.columns.to_list(), range(pivoted.shape[1])))

    # Dump global IDs to pkl file:
    with open(f'{output_file}_globalID_iterations_{iterations}.pkl', 'wb') as f:
        pickle.dump(global_IDs, f)

    # 3. PERFORM ESTIMATIONS

    # Outputs:
    results_ts = {}
    convert_ID = {}

    # Set initial window bounds:
    begin, end = 0, window_size

    # Begin run:
    for iteration in range(iterations):

        print(f'Starting estimation: {iteration}, going to {iterations}.')

        # Take window; drop any chats/channels for which there is missing data (nan):
        embedding = pivoted.iloc[begin:end].dropna(axis=1, how='any')

        # Retain local ids for building conversion dictionary:
        local_IDs = dict(zip(embedding.columns.to_list(), range(embedding.shape[1])))

        print(f'This embedding starts at: {embedding.first_valid_index()} and ends at {embedding.last_valid_index()}.')

        # Instantiate data object with window data:
        data = Data(embedding, dim_order='sp')

        # Pass window to estimator:
        results = network_analysis.analyse_network(settings=settings, data=data)

        # Update results dict (key = datetime start of target variable):
        results_ts.update({str(embedding.first_valid_index()): results})

        # Update conversion dict (key = datetime start of target variable):
        convert_ID.update({str(embedding.first_valid_index()): {local_IDs[channel]: global_IDs[channel] for channel in local_IDs.keys()}})

        # Increment window for the next iteration:
        begin += window_size
        end += window_size

    # Dump results to pkl file:
    with open(f'{output_file}_results_iterations_{iterations}.pkl', 'wb') as f:
        pickle.dump(results_ts, f)

    # Dump conversion IDs to pkl file:
    with open(f'{output_file}_localID_iterations_{iterations}.pkl', 'wb') as f:
        pickle.dump(convert_ID, f)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run network toxicity analysis.")
    parser.add_argument("input_file", help="Name of the input CSV file")
    parser.add_argument("output_file", help="Base name for the output files")
    parser.add_argument("--retox", default="retox", help="Name of the toxicity score column")
    parser.add_argument("--source", default="source", help="Name of the source column")
    parser.add_argument("--datetime", default="date", help="Name of the datetime column")
    parser.add_argument("--window_size", type=int, help="Number of samples in window")
    parser.add_argument("--iterations", type=int, help="Number of windows to process")
    args = parser.parse_args()

    main(args.input_file, args.output_file, args.retox, args.source, args.datetime, args.window_size, args.iterations)

