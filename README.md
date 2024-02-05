# Network Toxicity Analysis

This repo is associated with the work: 

http://dx.doi.org/10.1007/s42001-023-00239-2

*'Network toxicity analysis: an information-theoretic approach to studying the social dynamics of online toxicity'*

Authors: Rupert Kiddle, Petter Tornberg, Damian Trilling.

This repository contains example code for the analysis of network toxicity in social media data. It requires the use of the Perspective API (https://www.perspectiveapi.com/) for toxicity classification. It requires the use of the IDTxl package for multivariate transfer entropy estimation (https://github.com/pwollstadt/IDTxl).



**WORKFLOW:**

- `1_prep.ipynb`: takes messaging data (.csv) and removes duplicates, returning unique messages in a .csv file for classification. 

- `2_classify.py`: takes messages (.csv) and queries the Perspective API (key required) in order to classify the toxicity of each; returns a JSON file of results. 

- `3_merge.ipynb`: takes the JSON file returned above; returns a .csv file with timeseries data and toxicity scores combined; provides additional filtering options. 

- `4_resample.py`: takes the combined data from above and applies a source-level resampling strategy (suitable for the analysis of Chats/Channels on Telegram, see associated paper for full details) to achieve an equal sampling rate; returns resampled data.

- `5_estimate.py`: takes the resampled toxicity data from the previous step and iteratively calculates multivariate transfer entropy using a sliding window technique; returns .pkl files containing all results as well as identification features to reconcile edges with sources in the next step; provides configuration options for IDTxl package. 

- `6_extract.ipynb`: takes the files produced in the previous two steps and restructures the data into an adjacency matrix format suitable for network analysis; returns a numpy array.
 
 
