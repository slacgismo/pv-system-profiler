import pandas as pd
import numpy as np
import os
import sys
from time import time

# power data location
data_location = './'
#output results file location
results_file = './data/results_longitude.csv'
#site list file location
site_list_file = './site_list.csv'
# solar data tools path
s1 = '/home/pv-system-profiler/'
# pv system profiler path
s2 = '/home/solar-data-tools/'
sys.path.append(s1)
sys.path.append(s2)

from solardatatools import DataHandler
from solardatatools.utilities import progress
from pvsystemprofiler.longitude_study import LongitudeStudy

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

df_site = pd.read_csv(site_list_file, index_col=0)
df_site['site'] = df_site['site'].apply(str)
df_site['system'] = df_site['system'].apply(str)

sites = df_site['site'].unique().tolist()
site_system_dict = {}

for site_ix, site_id in enumerate(sites):
    systems_in_site = df_site[df_site['site'] == site_id]['system'].values.tolist()
    site_system_dict[site_id] = systems_in_site

site_run_time = 0
total_time = 0
counter = 0

for site_ix, site_id in enumerate(sites[start_at:]):
    t0 = time()
    msg = 'Site/Accum. run time: {0:2.2f} s/{1:2.2f} m'. \
        format(site_run_time, total_time / 60.0)

    progress(site_ix, len(sites), msg, bar_length=20)
    df = load_constellation_data(file_id=site_id)
    dh = DataHandler(df)
    cols = df.columns

    for sys_ix, sys_id in enumerate(site_system_dict[site_id]):
        if sys_id not in checked_systems:
            print(site_id, sys_id)
            counter += 1
            sys_tag = 'ac_power_inv_' + str(sys_id)

            if sys_tag in cols:
                data_available = True
            else:
                data_available = False

            if data_available: 

                mask1 = df_site['site'] == site_id
                mask2 = df_site['system'] == sys_id
                mask3 = mask1 & mask2

                real_longitude = float(df_site.loc[mask3, 'longitude'])
                gmt_offset = float(df_site.loc[mask3, 'gmt_offset'])
                counter += 1
                sys_tag = 'ac_power_inv_' + str(sys_id)
                manual_time_shift = int(df_site.loc[df_site['system'] == sys_id, 'time_shift_manual'].values[0])


                dh.run_pipeline(power_col=sys_tag, fix_shifts=False, correct_tz=False, verbose=False)

                lon_study = LongitudeStudy(data_handler=dh, gmt_offset=gmt_offset, true_value=real_longitude)

                lon_study.run(verbose=False)

                partial_df = lon_study.results.sort_index().copy()

                partial_df['site'] = site_id
                partial_df['system'] = sys_id
                partial_df['length'] = dh.num_days
                partial_df['data sampling'] = dh.data_sampling
                partial_df['data quality score'] = dh.data_quality_score
                partial_df['data clearness score'] = dh.data_clearness_score
                partial_df['inverter clipping'] = dh.inverter_clipping
                partial_df['time_shift_manual'] = manual_time_shift
                full_df = full_df.append(partial_df)
                full_df.index = np.arange(len(full_df))
                full_df.to_csv(results_file)

    t1 = time()
    site_run_time = t1 - t0
    total_time += site_run_time

msg = 'Site/Accum. run time: {0:2.2f} s/{1:2.2f} m'. \
    format(site_run_time, total_time / 60.0)
progress(len(sites), len(sites), msg, bar_length=20)
