#!/bin/sh

# BELOW IS THE LIST OF ALL THE KWARGS NEEDED FOR ESTIMATION SCRIPT TO RUN

# Desired state: options are 'single', 'partition'
STATE=partition

# estimation: Estimation to be performed. Options are 'report', 'longitude', 'latitude', 'tilt_azimuth'
ESTIMATION=tilt_azimuth

# input_site_file:  csv file containing list of sites to be evaluated. 'None' if no input file is provided
INPUT_SITE_FILE=None

# number of files to read. If 'all' all files in folder are read
N_FILES=all

# s3_location: Absolute path to s3 location of files
S3_LOCATION=s3://pv.insight.misc/pv_fleets/

# file_label:  Repeating portion of data files label. If 'None', no file label is used
FILE_LABEL=_20201006_composite

# power_column_label: Repeating portion of the power column label
POWER_COLUMN_LABEL=ac_power_inv_

# output_file: Absolute path to csv file containing report results
OUTPUT_FILE=./results.csv

# fix_time_shits: String, 'True' or 'False'. Specifies if time shifts are to be
FIX_TIME_SHIFTS=True

# time_zone_correction: String, 'True' or 'False'. Specifies if the time zone correction is performed when running the pipeline.
TIME_ZONE_CORRECTION=False

# check_json: String, 'True' or 'False'. Check json file for location information.
CHECK_JSON=False

# convert_to_ts: String, 'True' or 'False'. Specifies if conversion to time series is performed when running the pipeline.
CONVERT_TO_TS=False

# system_summary_file: Full path to csv file containing longitude and manual time shift flag for each system, None if no file provided.
SYSTEM_SUMMARY_FILE=s3://pv.insight.misc/report_files/lon_lat_precalculates.csv

# gmt_offset: String. Single value of gmt offset to be used for all estimations. If None a list with individual gmt offsets needs to be provided.
GMT_OFFSET=None

# data_source: String. Input signal data source. Options are 's3' and 'cassandra'.
DATA_SOURCE=s3

# CODE RUN
if [ $STATE == partition ]; then SCRIPT=pvsystemprofiler/scripts/run_partition_script.py;
elif [ $STATE == single ]; then SCRIPT=pvsystemprofiler/scripts/parameter_estimation_script.py;
fi
python $SCRIPT $ESTIMATION $INPUT_SITE_FILE $N_FILES $S3_LOCATION $FILE_LABEL $POWER_COLUMN_LABEL $OUTPUT_FILE $FIX_TIME_SHIFTS $TIME_ZONE_CORRECTION $CHECK_JSON $CONVERT_TO_TS $SYSTEM_SUMMARY_FILE $GMT_OFFSET $DATA_SOURCE
