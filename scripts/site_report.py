import pandas as pd
import numpy as np
import os
import sys
import json
from time import time

from solardatatools import DataHandler
from solardatatools.utilities import progress
from solardatatools.dataio import load_constellation_data
from solardatatools.dataio import load_cassandra_data


def get_io_file_locations(text_file):
    try:
        with open(text_file) as f:
            file_locations = f.read()
            file_dict = json.loads(file_locations)
    except FileNotFoundError:
        ('Error reading input file')
    return file_dict['results_file'], file_dict['site_list_file']


def get_tag(dh_tag, ds, pc_id, i):
    if ds == 'constellation':
        pc = pc_id + str(i)
    if ds == 'source_2':
        pc = dh_tag.data_frame.columns[i]
    return pc


def resume_run(output_file):
    if os.path.isfile(results_file):
        df = pd.read_csv(output_file, index_col=0)
        df['site'] = df['site'].apply(str)
        df['system'] = df['system'].apply(str)
        start_index = len(df['site'].unique()) - 1
        checked_list = df['system'].unique().tolist()
    else:
        start_index = 0
        df = pd.DataFrame()
        checked_list = []
    return df, checked_list, start_index


def load_input_dataframe(list_file):
    df = pd.read_csv(list_file, index_col=0)
    df['site'] = df['site'].apply(str)
    df['system'] = df['system'].apply(str)
    return df


def filter_sites(unfiltered_df):
    mask1 = unfiltered_df['data_available_csv'] == True
    mask2 = unfiltered_df['data_available_json'] == True
    mask3 = mask1 & mask2
    return unfiltered_df[mask3]


def create_site_system_dict(index_df):
    site_list = index_df['site'].unique().tolist()
    ss_dict = {}
    for site in site_list:
        systems_in_site = index_df[index_df['site'] == site]['system'].values.tolist()
        ss_dict[site] = systems_in_site
    return site_list, ss_dict


def initialize_results_df():
    p_df = pd.DataFrame(columns=['site', 'system', 'longitude', 'latitude', 'tilt', 'azimuth', 'gmt_offset', 'length',
                                 'capacity_estimate', 'data_sampling', 'data quality_score', 'data clearness_score',
                                 'inverter_clipping', 'time_shifts_corrected', 'time_zone_correction',
                                 'capacity_changes', 'normal_quality_scores', 'manual_time_shift', 'passes pipeline'])
    return p_df


def load_data(source):
    if source == 'constellation':
        output_df = load_constellation_data(file_id=site_id)
    if source == 'source_2':
        output_df = load_cassandra_data(file_id=site_id)
    return output_df


if __name__ == '__main__':
    input_text_file = sys.argv[1]
    power_column_id = str(sys.argv[2])
    data_source = str(sys.argv[3])
    results_file = str(sys.argv[4])
    site_list_file = str(sys.argv[5])

    full_df, checked_systems, start_at = resume_run(results_file)

    input_df = load_input_dataframe(site_list_file)

    df_site = filter_sites(input_df)

    sites, site_system_dict = create_site_system_dict(df_site)

    site_run_time = 0
    total_time = 0
    partial_df = initialize_results_df()

    for site_ix, site_id in enumerate(sites[start_at:]):
        t0 = time()
        msg = 'Site/Accum. run time: {0:2.2f} s/{1:2.2f} m'.format(site_run_time, total_time / 60.0)

        progress(site_ix, len(sites), msg, bar_length=20)

        df = load_data(data_source)

        dh = DataHandler(df)

        for sys_ix, sys_id in enumerate(site_system_dict[site_id]):
            if sys_id not in checked_systems:
                #print(site_id, sys_id)
                sys_tag = get_tag(dh, data_source, power_column_id, sys_id)

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
                partial_df.loc[0] = v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12, v13, v14, v15, v16, v17, v18, v19
                full_df = full_df.append(partial_df)
                full_df.index = np.arange(len(full_df))
                full_df.to_csv(results_file)

        t1 = time()
        site_run_time = t1 - t0
        total_time += site_run_time

    msg = 'Site/Accum. run time: {0:2.2f} s/{1:2.2f} m'.format(site_run_time, total_time / 60.0)
    progress(len(sites), len(sites), msg, bar_length=20)
