import os
import json
import boto3
import subprocess
import paramiko
from smart_open import smart_open
import numpy as np
import pandas as pd
from solardatatools.dataio import load_constellation_data
from solardatatools.dataio import load_cassandra_data
from solardatatools.utilities import progress


def remote_execute(user, instance_id, key, shell_commands, verbose=True):
    k = paramiko.RSAKey.from_private_key_file(key)
    c = paramiko.SSHClient()
    c.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    c.connect(hostname=instance_id, username=user, pkey=k, allow_agent=False, look_for_keys=False)
    command_dict = {}
    for command in shell_commands:
        if verbose:
            print("running command: {}".format(command))
        stdin, stdout, stderr = c.exec_command(command)
        command_dict[command] = [stdout.read(), stderr.read()]
    c.close()
    return command_dict


def copy_to_s3(input_file_name, bucket, destination_file_name):
    content = open(input_file_name, 'rb')
    s3 = boto3.client('s3')
    s3.put_object(Bucket=bucket, Key=destination_file_name, Body=content)


def log_file_versions(utility, output_folder_location='./', conda_location='/home/ubuntu/miniconda3/',
                      repository_location=None):
    conda = conda_location + 'bin/conda' + ' '

    conda_list = subprocess.check_output(conda + "env list", shell=True, encoding='utf-8')
    for line in conda_list.splitlines():
        if '*' in line:
            i = line.find('/')
            active_conda_environment = line[i:].split('/')[-1]
            output_string = 'active conda environment:' + ' ' + active_conda_environment + '\n'

    pip = conda_location + 'envs/' + active_conda_environment + '/bin/pip'
    pip_list = subprocess.check_output(pip + ' ' + 'list', shell=True, encoding='utf-8')

    for line in pip_list.split():
        if utility in line:
            if line.find('/') != -1:
                i = line.find('/')
                location = line[i:]
                output_string += 'repository location:' + ' ' + line[i:] + '\n'

    try:
        if repository_location is not None:
            location = repository_location

        repository = subprocess.check_output('/usr/bin/git -C' + ' ' + location + ' ' + 'log -n 1', shell=True,
                                             encoding='utf-8')
        for line in repository.splitlines():
            if line.find('commit') != - 1:
                i = line.find(' ')
                commit_id = line[i:]
                output_string += 'commit id:' + ' ' + commit_id + '\n'
            if line.find('Author') != - 1:
                i = line.find(': ')
                author = line[i + 1:]
                output_string += 'author:' + ' ' + author + '\n'
            if line.find('Date') != - 1:
                i = line.find(' ')
                date = line[i:]
                output_string += 'date:' + ' ' + date + '\n'
    except:
        pass

    output_file = open(output_folder_location + utility + '_' + 'versions.log', 'w')
    output_file.write(output_string)
    output_file.close()
    return


def string_to_boolean(value):
    if value == 'True':
        return True
    elif value == 'False':
        return False


def create_json_dict(json_list, location):
    system_dict = {}
    for file in json_list:
        for line in smart_open(location + file, 'rb'):
            file_json = json.loads(line)
            if len(file_json['Inverters']) != 0:
                for inv_id in file_json['Inverters']:
                    system = file_json['Inverters'][inv_id]['inverter_id']
                    system_dict[system] = file
    return system_dict


def extract_sys_parameters(file_name, system, location):
    for line in smart_open(location + file_name, 'rb'):
        file_json = json.loads(line)
        parameters = []
        if len(file_json['Inverters']) == 0:
            parameters = [np.nan] * 5
        else:
            try:
                zc = file_json['Site']['location'][-5:]
                zc = '00000' if not zc.isnumeric() else zc
            except KeyError:
                zc = '00000'
            parameters.append(zc)
            for inv_id in file_json['Inverters']:
                mount_id = 'Mount ' + inv_id.split(' ')[1]
                sys_id = file_json['Inverters'][inv_id]['inverter_id']
                if sys_id == system:
                    for coord_ix, coord_id in enumerate(['longitude', 'latitude', 'tilt', 'azimuth']):
                        try:
                            if coord_id in ['longitude', 'latitude']:
                                val = float(file_json['Site'][coord_id])
                            if coord_id in ['tilt', 'azimuth']:
                                val = file_json['Mount'][mount_id][coord_id]
                        except KeyError:
                            val = np.nan
                        val = np.nan if val == '' else val
                        parameters.append(val)
        file_id = file_json['System']['system_id']
        parameters.append(file_id)
        return parameters


def get_s3_bucket_and_prefix(s3_location):
    if s3_location[-1] != '/':
        s3_location += '/'
    i = s3_location.find('//') + 2
    j = s3_location.find('/', i)
    bucket = s3_location[i:j]
    prefix = s3_location[j + 1:-1]
    return bucket, prefix


def get_checked_sites(df):
    if not df.empty:
        checked_sites = df['site'].unique().tolist()
        checked_sites.sort()
    else:
        checked_sites = []
    return checked_sites


def siteid_to_filename(sites, file_label, ext):
    checked_sites = []
    for site_id in sites:
        file_name = site_id + file_label + ext
        checked_sites.append(file_name)
    return checked_sites

def filename_to_siteid(sites):
    site_list = []
    for site_id in sites:
        site_id = site_id.split('.')[0]
        site_list.append(site_id)
    return site_list


def load_generic_data(location, file_label, file_id, extension='.csv', parse_dates=[0]):
    to_read = location + file_id + file_label + extension
    df = pd.read_csv(to_read, index_col=0, parse_dates=parse_dates)
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
            file_name = file_name[i + 1:]
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


def run_failsafe_pipeline(dh_in, df_in, sys_tag, fts, tzc):
    try:
        dh_in.run_pipeline(power_col=sys_tag, fix_shifts=fts, correct_tz=tzc, verbose=False)
    except:
        max_val = np.nanquantile(df_in[sys_tag], 0.95)
        dh_in.run_pipeline(power_col=sys_tag, fix_shifts=False, correct_tz=tzc, verbose=False,
                           max_val=max_val * 3)
    return
