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

def get_sys_location(df, sys_id):
    longitude = float(df.loc[df['system'] == sys_id, 'longitude'])
    latitude = float(df.loc[df['system'] == sys_id, 'latitude'])
    return longitude, latitude

def get_sys_orientation(df, sys_id):
    tilt = float(df.loc[df['system'] == sys_id, 'tilt'])
    azimuth = float(df.loc[df['system'] == sys_id, 'azimuth'])
    return tilt, azimuth

def get_sys_gmt_offset(df, sys_id):
    return float(df.loc[df['system'] == sys_id, 'gmt_offset'])

def get_tag(dh, ds, pc_id, sys_id):
    if ds == 'constellation':
        pc = pc_id + str(sys_id)
    if ds == 'source_2':
        pc = dh.data_frame.columns[sys_id]
    return pc


def resume_run(output_file):
    if os.path.isfile(output_file):
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


def filter_sites(df):
    mask1 = df['data_available_csv'] == True
    mask2 = df['data_available_json'] == True
    mask3 = mask1 & mask2
    return df[mask3]


def create_site_system_dict(df):
    site_list = df['site'].unique().tolist()
    ss_dict = {}
    for site in site_list:
        systems_in_site = df[df['site'] == site]['system'].values.tolist()
        ss_dict[site] = systems_in_site
    return site_list, ss_dict


def initialize_results_df():
    p_df = pd.DataFrame(columns=['site', 'system', 'longitude', 'latitude', 'tilt', 'azimuth', 'gmt_offset', 'length',
                                 'capacity_estimate', 'data_sampling', 'data quality_score', 'data clearness_score',
                                 'inverter_clipping', 'time_shifts_corrected', 'time_zone_correction',
                                 'capacity_changes', 'normal_quality_scores', 'manual_time_shift', 'passes pipeline'])
    return p_df


def load_data(data_source, site_id):
    if data_source == 'constellation':
        df = load_constellation_data(file_id=site_id)
    if data_source == 'source_2':
        df = load_cassandra_data(file_id=site_id)
    return df