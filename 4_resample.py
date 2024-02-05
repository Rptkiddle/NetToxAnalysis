#!/usr/bin/env python3

import numpy as np
import pandas as pd
import argparse

def main(input_file, output_file, toxicity_column, source_column, datetime_column):
    '''takes a .csv file with toxicity scores, sender, and datetime in separate columns, returns .csv file with resampled toxicity scores'''
    # Load data for resampling
    data = pd.read_csv(input_file)

    # Ensure datetime
    data[datetime_column] = pd.to_datetimetime(data[datetime_column])

    # Set datetimetimeindex (time series data)
    data = data.set_index(datetime_column).sort_index()

    # Generate new sources dataframe 'presampled' for 5 minute intervals
    presampled = data.filter([source_column, toxicity_column])\
                    .groupby(source_column)\
                    .resample('5T').count()\
                    .rename(columns={source_column: 'test'}).drop(columns=['test'])

    # Add placeholder columns for resampled estimates and window checks
    presampled['retox'] = np.nan

    # Group by channels and resample
    resampled = presampled.groupby(level=0).apply(lambda df: re_tox(df, data, source_column, toxicity_column))

    # Save resampled data
    resampled.to_csv(output_file)

def re_tox(df, data, source_column, toxicity_column):
    '''if >= 5 toxicity scores in 5-minute window, average over 5-minute window; else, average over 24-hour window'''
    channel = df.index.get_level_values(0)[0]
    timestamps = df.index.get_level_values(1)
    print(f"Beginning resampling of channel:'{channel}'; for {len(timestamps)} 5-minute windows...")
    df['retox'] = np.where(
        df[toxicity_column] >= 5,  # condition to check
        mav_true(channel, timestamps, data, source_column, toxicity_column),  # return if true
        mav_false(channel, timestamps, data, source_column, toxicity_column),  # return if false
    )
    print(f"Completed resampling for channel:'{channel}'")
    return df

def mav_true(channel, timestamps, data, source_column, toxicity_column):
    '''if >= 5 toxicity scores in 5-minute window, average over 5-minute window; else return NaN'''
    print('mav_true starting..')
    scores = []
    source = data[data[source_column] == channel]
    for time in timestamps:
        start, stop = str(time), str(time + pd.Timedelta(5, unit='m'))
        values = source.loc[start:stop][toxicity_column].values
        if len(values) >= 5:
            scores.append(np.average(values))
        else:
            scores.append(float('NaN'))
    print('mav_true complete..')
    return np.array(scores)

def mav_false(channel, timestamps, data, source_column, toxicity_column):
    '''if >= 1 toxicity score(s) in 24-hour window, average over 24-hour window; else return NaN'''
    print('mav_false starting..')
    scores = []
    source = data[data[source_column] == channel]
    for time in timestamps:
        start = str(time + pd.Timedelta(5, unit='m') - pd.Timedelta(1, unit='d'))
        stop = str(time + pd.Timedelta(5, unit='m'))
        values = source.loc[start:stop][toxicity_column].tail(5).values
        if len(values) >= 1:
            scores.append(np.average(values))
        else:
            scores.append(float('NaN'))
    print('mav_false complete..')
    return np.array(scores)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Process and resample data.')
    parser.add_argument('input_file', help='Path to the input CSV file')
    parser.add_argument('output_file', help='Path to the output CSV file')
    parser.add_argument('--toxicity', default='toxicity', help='Name of the toxicity score column')
    parser.add_argument('--source', default='source', help='Name of the source column')
    parser.add_argument('--datetime', default='date', help='Name of the datetime column')

    args = parser.parse_args()

    main(args.input_file, args.output_file, args.toxicity, args.source, args.datetime)
