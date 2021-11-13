#!/bin/sh

# BELOW IS THE LIST OF ALL THE KWARGS NEEDED FOR ESTIMATION SCRIPT TO RUN

# estimation: Estimation to be performed. Options are 'report', 'longitude', 'latitude', 'tilt_azimuth'
ESTIMATION_TO_RUN=report

# DATA SOURCES
# data_source: String. Input signal data source. Options are 's3' and 'cassandra'.
DATA_SOURCE=s3

# s3_location: Absolute path to s3 location of files
S3_LOCATION=s3://pv.insight.test/test-5007/

# power_column_label: Repeating portion of the power column label
POWER_COLUMN_LABEL=ac_power_inv_

########

# SYSTEM SOURCES
# Instance name (all instances must have the same name)
EC2_NAME=pv-system-profiler-prod

# conda env name
CONDA_ENV=pvi-user

# system_summary_file: Full path to csv file containing longitude and manual time shift flag for each system, None if no file provided.
SYSTEM_SUMMARY_FILE=s3://pv.insight.test/constellation_site_list_5007.csv


########

# ADVANCED SETTINGS
# fix_time_shits: String, 'True' or 'False'. Specifies if time shifts are to be fixed when running the pipeline.
FIX_TIME_SHIFTS=True

# gmt_offset: String. Single value of gmt offset to be used for all estimations. If None a list with individual gmt offsets needs to be provided.
GMT_OFFSET=None

# output_file: Absolute path to csv file containing report results
OUTPUT_FILE=./results.csv

# time_zone_correction: String, 'True' or 'False'. Specifies if the time zone correction is performed when running the pipeline.
TIME_ZONE_CORRECTION=False

# convert_to_ts: String, 'True' or 'False'. Specifies if conversion to time series is performed when running the pipeline.
CONVERT_TO_TS=False

# input_site_file:  csv file containing list of sites to be evaluated. 'None' if no input file is provided
INPUT_SITE_FILE=None


########
# EXTRA SETTINGS
# check_json: String, 'True' or 'False'. Check json file for location information.
CHECK_JSON=False

# number of files to read. If 'all' all files in folder are read
N_FILES=all

# file_label:  Repeating portion of data files label. If 'None', no file label is used
FILE_LABEL=None



# CODE RUN
ESTIMATION_SCRIPT=github/pv-system-profiler/pvsystemprofiler/scripts/parameter_estimation_script.py;
# Script to Run
SCRIPT=github/pv-system-profiler/pvsystemprofiler/scripts/run_partition_script.py;

python $SCRIPT $ESTIMATION_TO_RUN $INPUT_SITE_FILE $N_FILES $S3_LOCATION $FILE_LABEL $POWER_COLUMN_LABEL $OUTPUT_FILE $FIX_TIME_SHIFTS $TIME_ZONE_CORRECTION $CHECK_JSON $CONVERT_TO_TS $SYSTEM_SUMMARY_FILE $GMT_OFFSET $DATA_SOURCE $ESTIMATION_SCRIPT $CONDA_ENV $EC2_NAME