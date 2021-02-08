import pandas as pd
import numpy as np
import os
import sys
from time import time

sys.path.append('/home/ubuntu/github/pv-system-profiler/')
sys.path.append('/home/ubuntu/github/solar-data-tools/')

from solardatatools import DataHandler
from solardatatools.utilities import progress
from solardatatools.dataio import load_constellation_data
from pvsystemprofiler.latitude_study import LatitudeStudy
import json

try:
    with open('./param_lat.txt') as f:
        file_locations = f.read()
        file_dict = json.loads(file_locations)
except FileNotFoundError:
    print('Error reading input file')

results_file = file_dict['results_file']
site_list_file = file_dict['site_list_file']
report_file = file_dict['report_file']
system_descriptory_string = 'ac_power_inv_'

if os.path.isfile(results_file):
    full_df = pd.read_csv(results_file, index_col=0)
    start_at = len(full_df['site'].unique()) - 1
    partial_df_len = int(len(full_df) / len(full_df['system'].unique()))
    full_df['site'] = full_df['site'].apply(str)
    full_df['system'] = full_df['system'].apply(str)
    checked_systems = full_df['system'].unique().tolist()
else:
    start_at = 0
    full_df = pd.DataFrame()
    partial_df_len = None
    checked_systems = []

df_report = pd.read_csv(report_file, index_col=0)
df_report['site'] = df_report['site'].apply(str)
df_report['system'] = df_report['system'].apply(str)

df_input = pd.read_csv(site_list_file, index_col=0)
df_input['site'] = df_input['site'].apply(str)
df_input['system'] = df_input['system'].apply(str)
passing_systems = df_report.loc[df_report['passes pipeline'] == True, 'system'].unique().tolist()
mask = df_input['system'].isin(passing_systems)

df_site = df_input[mask]
sites = df_site['site'].unique().tolist()
site_system_dict = {}

for site_ix, site_id in enumerate(sites):
    systems_in_site = df_site[df_site['site'] == site_id]['system'].values.tolist()
    site_system_dict[site_id] = systems_in_site

site_run_time = 0
total_time = 0

for site_ix, site_id in enumerate(sites[start_at:]):
    t0 = time()
    msg = 'Site/Accum. run time: {0:2.2f} s/{1:2.2f} m'.format(site_run_time, total_time / 60.0)

    progress(site_ix, len(sites), msg, bar_length=20)
    df = load_constellation_data(file_id=site_id)
    dh = DataHandler(df)

    for sys_ix, sys_id in enumerate(site_system_dict[site_id]):
        if sys_id not in checked_systems:
            print(site_id, sys_id)
            sys_tag = system_descriptory_string + str(sys_id)
            mask1 = df_site['site'] == site_id
            mask2 = df_site['system'] == sys_id
            mask3 = mask1 & mask2

            real_latitude = float(df_site.loc[mask3, 'latitude'])
            gmt_offset = float(df_site.loc[mask3, 'gmt_offset'])
            manual_time_shift = int(df_site.loc[df_site['system'] == sys_id, 'time_shift_manual'].values[0])

            if manual_time_shift == 1:
                dh.fix_dst()

            try:
                dh.run_pipeline(power_col=sys_tag, fix_shifts=False, correct_tz=False, verbose=False)
            except ValueError:
                max_val = np.nanquantile(df[sys_tag], 0.95)
                dh.run_pipeline(power_col=sys_tag, fix_shifts=False, correct_tz=True, verbose=False,
                                max_val=max_val * 3)

            try:
                passes_estimation = True
                lat_study = LatitudeStudy(data_handler=dh, lat_true_value=real_latitude)
                lat_study.run()
                partial_df = lat_study.results.sort_index().copy()
            except ValueError:
                passes_estimation = False
                partial_df = pd.DataFrame(columns=['declination_method', 'daylight_calculation', \
                                                   'data_matrix', 'threshold', 'day_selection_method', \
                                                   'latitude', 'residual'])

            partial_df['site'] = site_id
            partial_df['system'] = sys_id
            partial_df['length'] = dh.num_days
            partial_df['data sampling'] = dh.data_sampling
            partial_df['data quality score'] = dh.data_quality_score
            partial_df['data clearness score'] = dh.data_clearness_score
            partial_df['inverter clipping'] = dh.inverter_clipping
            partial_df['time shift manual'] = manual_time_shift
            partial_df['runs estimation'] = passes_estimation
            full_df = full_df.append(partial_df)
            full_df.index = np.arange(len(full_df))
            full_df.to_csv(results_file)

    t1 = time()
    site_run_time = t1 - t0
    total_time += site_run_time

msg = 'Site/Accum. run time: {0:2.2f} s/{1:2.2f} m'. \
    format(site_run_time, total_time / 60.0)
progress(len(sites), len(sites), msg, bar_length=20)
