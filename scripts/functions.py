import pandas as pd
import os
import json
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

def get_sys_location(df, i):
    longitude = float(df.loc[df_site['system'] == i, 'longitude'])
    latitude = float(df.loc[df_site['system'] == i, 'latitude'])
    return longitude, latitude


def get_sys_orientation(df, i):
    tilt = float(df_site.loc[df_site['system'] == i, 'tilt'])
    azimuth = float(df_site.loc[df_site['system'] == i, 'azimuth'])
    return tilt, azimuth

def get_sys_gmt_offset(df, i):
    return float(df_site.loc[df_site['system'] == sys_id, 'gmt_offset'])

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