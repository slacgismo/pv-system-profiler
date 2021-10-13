#!/bin/sh

ESTIMATION_SCRIPT=parameter_estimation_script.py
PARTITION_SCRIPT=run_partition_script.py

# Local run
# LOCATION=/users/mac/Documents/github/pv-system-profiler/pvsystemprofiler/scripts/
# aws run
LOCATION=/Users/derins/Documents/gismo/pv-insight/pv-system-profiler/pvsystemprofiler/scripts/

# longitude and latitude
# ESTIMATION_TO_RUN=report
#ESTIMATION_TO_RUN=longitude
#ESTIMATION_TO_RUN=latitude
# INPUT_LIST=constellation_site_list.csv

# tilt and azimuth
ESTIMATION_TO_RUN=tilt_azimuth
INPUT_LIST=lon_lat_precalculates.csv

# Serial
# python $LOCATION$ESTIMATION_SCRIPT $ESTIMATION_TO_RUN None all s3://pv.insight.misc/pv_fleets/ '_20201006_composite' ac_power_inv_ ./results.csv True False False False s3://pv.insight.misc/report_files/$INPUT_LIST None s3
#python $LOCATION$ESTIMATION_SCRIPT $ESTIMATION_TO_RUN None all s3://pv.insight.misc/pv_fleets/ '_20201006_composite' ac_power_inv_ /home/ubuntu/results.csv True False False False s3://pv.insight.misc/report_files/$INPUT_LIST None s3

# Partitioned
python $LOCATION$PARTITION_SCRIPT $ESTIMATION_TO_RUN None all s3://pv.insight.misc/pv_fleets/ '_20201006_composite' ac_power_inv_ ./results.csv True False False False s3://pv.insight.misc/report_files/$INPUT_LIST None s3 /home/ubuntu/github/pv-system-profiler/pvsystemprofiler/scripts/$ESTIMATION_SCRIPT pvi-user londonoh_div
