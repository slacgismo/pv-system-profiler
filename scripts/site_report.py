import pandas as pd
import numpy as np
import os
import sys
import json
from time import time

sys.path.append('/home/ubuntu/github/pv-system-profiler/')
sys.path.append('/home/ubuntu/github/solar-data-tools/')
from solardatatools import DataHandler
from solardatatools.utilities import progress
from solardatatools.dataio import load_constellation_data

try:
    with open('./param.txt') as f:
        file_locations = f.read()
        file_dict = json.loads(file_locations)
except FileNotFoundError:
    print('Error reading input file')

system_descriptory_string = 'ac_power_inv_'
results_file = file_dict['results_file']
site_list_file = file_dict['site_list_file']
system_descriptory_string = 'ac_power_inv_'

if os.path.isfile(results_file):
    full_df = pd.read_csv(results_file, index_col=0)
    full_df['site'] = full_df['site'].apply(str)
    full_df['system'] = full_df['system'].apply(str)
    start_at = len(full_df['site'].unique()) - 1
    partial_df_len = int(len(full_df) / len(full_df['system'].unique()))
    checked_systems = full_df['system'].unique().tolist()
else:
    start_at = 0
    full_df = pd.DataFrame()
    partial_df_len = None
    checked_systems = []

input_df = pd.read_csv(site_list_file, index_col=0)
input_df['site'] = input_df['site'].apply(str)
input_df['system'] = input_df['system'].apply(str)

mask1 = input_df['data_available_csv'] == True
mask2 = input_df['data_available_json'] == True
mask3 = mask1 & mask2

df_site = input_df[mask3]
sites = df_site['site'].unique().tolist()
site_system_dict = {}

for site_ix, site_id in enumerate(sites):
    systems_in_site = df_site[df_site['site'] == site_id]['system'].values.tolist()
    site_system_dict[site_id] = systems_in_site

site_run_time = 0
total_time = 0

partial_df = pd.DataFrame(columns=['site', 'system', 'longitude', 'latitude', 'tilt', 'azimuth', 'gmt_offset',
                                   'length', 'capacity_estimate', 'data_sampling',
                                   'data quality_score', 'data clearness_score', 'inverter_clipping',
                                   'time_shifts_corrected', 'time_zone_correction', 'capacity_changes',
                                   'normal_quality_scores', 'manual_time_shift', 'passes pipeline'])

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

            manual_time_shift = df_site.loc[df_site['system'] == sys_id, 'time_shift_manual'].values[0]
            lon = float(df_site.loc[df_site['system'] == sys_id, 'longitude'])
            lat = float(df_site.loc[df_site['system'] == sys_id, 'latitude'])
            tilt = float(df_site.loc[df_site['system'] == sys_id, 'tilt'])
            azim = float(df_site.loc[df_site['system'] == sys_id, 'azimuth'])
            gmt_offset = float(df_site.loc[df_site['system'] == sys_id, 'gmt_offset'])
            passes_pipeline = True

            if manual_time_shift == 1:
                dh.fix_dst()
            try:
                try:
                    dh.run_pipeline(power_col=sys_tag, fix_shifts=False, correct_tz=False, verbose=False)
                except ValueError:
                    max_val = np.nanquantile(df[sys_tag], 0.95)
                    dh.run_pipeline(power_col=sys_tag, fix_shifts=False, correct_tz=True, verbose=False,
                                    max_val=max_val * 3)
            except ValueError:
                passes_pipeline = False

            v1 = site_id
            v2 = sys_id
            v3 = lon
            v4 = lat
            v5 = tilt
            v6 = azim
            v7 = gmt_offset
            v8 = dh.num_days
            v9 = dh.capacity_estimate
            v10 = dh.data_sampling
            v11 = dh.data_quality_score
            v12 = dh.data_clearness_score
            v13 = dh.inverter_clipping
            v14 = dh.time_shifts
            v15 = dh.tz_correction
            v16 = dh.capacity_changes
            v17 = dh.normal_quality_scores
            v18 = manual_time_shift
            v19 = passes_pipeline
            partial_df.loc[0] = v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12, v13, v14, v15, v16, v17, \
                                v18, v19
            full_df = full_df.append(partial_df)
            full_df.index = np.arange(len(full_df))
            full_df.to_csv(results_file)

    t1 = time()
    site_run_time = t1 - t0
    total_time += site_run_time

msg = 'Site/Accum. run time: {0:2.2f} s/{1:2.2f} m'. \
    format(site_run_time, total_time / 60.0)
progress(len(sites), len(sites), msg, bar_length=20)
