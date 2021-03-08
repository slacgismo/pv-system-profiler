import os
import json
import boto3
import numpy as np
import pandas as pd
from solardatatools.dataio import load_constellation_data
from solardatatools.dataio import load_cassandra_data
from solardatatools.utilities import progress



def get_s3_bucket_and_prefix(s3_location):
    if s3_location[-1] != '/':
        s3_location += '/'
    i = s3_location.find('//') + 2
    j = s3_location.find('/', i)
    bucket = s3_location[i:j]
    prefix = s3_location[j+1:-1]
    return bucket, prefix


def get_checked_sites(df, file_label, ext):
    if len(df) != 0:
        checked_sites = df['site'].unique().tolist()
        checked_sites.sort()
        checked_sites = checked_sites[:-1]
        checked_sites_list = siteid_to_filename(checked_sites, file_label, ext)
    else:
        checked_sites_list = []
    return checked_sites_list


def siteid_to_filename(sites, file_label, ext):
    checked_sites = []
    for site_id in sites:
        file_name = site_id + file_label + ext
        checked_sites.append(file_name)
    return checked_sites


def load_generic_data(location, file_label, file_id, extension='.csv'):
    to_read = location + file_id + file_label + extension
    df = pd.read_csv(to_read, index_col=0)
    return df


def create_site_label(file_id):
    file_name = file_id.split('/')[1]
    i = file_name.find(file_label)
    file_id = file_name[:i]
    return file_id


def create_system_list(file_label, power_label, location, s3_bucket, prefix):
    file_list = enumerate_files(s3_bucket, prefix)
    ll = len(power_label)
    system_list = pd.DataFrame(columns=['site', 'system'])

    for file_ix, file_id in enumerate(file_list):
        progress(file_ix, len(file_list), 'Generating system list', bar_length=20)
        file_name = file_id.split('/')[1]
        i = file_name.find(file_label)
        file_id = file_name[:i]
        df = load_generic_data(location, file_label, file_id)
        cols = df.columns
        for col_label in cols:
            if col_label.find(power_label) != -1:
                system_id = col_label[ll:]
                system_list.loc[len(system_list)] = file_id, system_id
    progress(len(file_list), len(file_list), 'Generating system list', bar_length=20)
    return system_list


def enumerate_files(s3_bucket, prefix, extension='.csv'):
    s3 = boto3.client('s3')
    output_list = []
    for obj in s3.list_objects_v2(Bucket=s3_bucket, Prefix=prefix)['Contents']:
        if (obj['Key']).find(extension) != -1:
            file_name = obj['Key']
            i = file_name.rfind('/')
            file_name = file_name[i+1:]
            output_list.append(file_name)
    return output_list


def get_io_file_locations(text_file):
    try:
        with open(text_file) as f:
            file_locations = f.read()
            file_dict = json.loads(file_locations)
    except FileNotFoundError:
        print('Error reading input file')
    return file_dict['results_file'], file_dict['site_list_file']


def get_lon_from_list(df, sys_id):
    longitude = float(df.loc[df['system'] == sys_id, 'longitude'])
    return longitude


def get_lon_from_report(df, site_id, sys_id):
    mask1 = df['site'] == site_id
    mask2 = df['system'] == sys_id
    mask3 = mask1 & mask2
    return float(df.loc[mask3, 'longitude'])


def get_lat_from_list(df, sys_id):
    latitude = float(df.loc[df['system'] == sys_id, 'latitude'])
    return latitude


def get_lat_from_report(df, site_id, sys_id):
    mask1 = df['site'] == site_id
    mask2 = df['system'] == sys_id
    mask3 = mask1 & mask2
    return float(df.loc[mask3, 'latitude'])


def get_orientation_from_list(df, sys_id):
    tilt = float(df.loc[df['system'] == sys_id, 'tilt'])
    azimuth = float(df.loc[df['system'] == sys_id, 'azimuth'])
    return tilt, azimuth


def get_gmt_offset_from_list(df, sys_id):
    return float(df.loc[df['system'] == sys_id, 'gmt_offset'])


def get_gmt_offset_from_report(df, site_id, sys_id):
    mask1 = df['site'] == site_id
    mask2 = df['system'] == sys_id
    mask3 = mask1 & mask2
    return float(df.loc[mask3, 'gmt_offset'])


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


def resume_run_from_file_list(output_file):
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
    cols = df.columns
    if 'data_available_csv' in cols:
        mask1 = df['data_available_csv'] == True
    else:
        mask1 = True
    if 'data_available_json' in cols:
        mask2 = df['data_available_json'] == True
    else:
        mask2 = True
    mask3 = mask1 & mask2
    return df[mask3]


def create_system_dict(df):
    site_list = df['site'].unique().tolist()
    ss_dict = {}
    for site in site_list:
        systems_in_site = df[df['site'] == site]['system'].values.tolist()
        ss_dict[site] = systems_in_site
    return site_list, ss_dict


def initialize_results_df():
    p_df = pd.DataFrame(columns=['site', 'system', 'length', 'capacity_estimate', 'data_sampling', 'data quality_score',
                                 'data clearness_score', 'inverter_clipping', 'time_shifts_corrected',
                                 'time_zone_correction', 'capacity_changes', 'normal_quality_scores',
                                 'passes pipeline'])
    return p_df


def load_data(data_source, site_id):
    for file_id in file_list:
        file_name = file_id.split('/')[1]
        loc = location + file_name
        df = pd.read_csv(str(loc), index_col=0)

    if data_source == 'constellation':
        df = load_constellation_data(file_id=site_id)
    if data_source == 'source_2':
        df = load_cassandra_data(file_id=site_id)
    return df


def get_inspected_time_shift(df, sys_id):
    return int(df.loc[df['system'] == sys_id, 'manual_time_shift'].values[0])


def run_failsafe_pipeline(dh_in, df_in, sys_tag, time_zone_correction):
    if time_zone_correction == True:
        tzc = True
    else:
        tzc = False
    try:
        dh_in.run_pipeline(power_col=sys_tag, fix_shifts=False, correct_tz=tzc, verbose=False)
    except:
        max_val = np.nanquantile(df_in[sys_tag], 0.95)
        dh_in.run_pipeline(power_col=sys_tag, fix_shifts=False, correct_tz=tzc, verbose=False,
                           max_val=max_val * 3)
    return
