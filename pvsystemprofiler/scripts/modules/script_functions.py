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
    """
    Executes a list of bash commands remotely on Amazon Web Services (AWS) instances from another computer.
    :param user: AWS instance user name, i.e. `ubuntu`.
    :param instance_id: AWS public ip address of instance.
    :param key: AWS key. Usually a *.pem file located in the .aws folder.
    :param shell_commands: list of shell commands to be executed remotely.
    :param verbose: provides the output to each remote command execution.
    """
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


def copy_to_s3(input_file_name, input_file_location):
    """
    Copies a local file to a Amazon Web Services (AWS) s3 bucket.
    :param input_file_name: name of the file to be copied to the AWS s3 bucket.
    :param input_file_location: full path to the destination AWS s3 bucket.
    """
    bucket, prefix = get_s3_bucket_and_prefix(input_file_location)
    destination_file_name = prefix + '/generated_site_list.csv'

    content = open(input_file_name, 'rb')
    s3 = boto3.client('s3')
    s3.put_object(Bucket=bucket, Key=destination_file_name, Body=content)


def log_file_versions(utility, active_conda_env=None, output_folder_location='./',
                      conda_location='/home/ubuntu/miniconda3/', repository_location='/home/ubuntu/github/'):
    """
    Writes a text file with the utility version as well as other available package information. This utility may have
    been installed in conda using pip or may be in a local GitHub repository.
    :param utility: name of the utility to be logged. Examples are `solar-data-tools' and 'pv-system-profiler'
    :param active_conda_env: optional. Name of the conda environment being used. Examples are pvi-user and `pvi-dev`
    :param output_folder_location: location of the output log files.
    :param conda_location: optional. Full path to the local conda folder location.
    :param repository_location: optional. Location of the GitHub repository.
    :return:
    """
    if active_conda_env is None:
        conda = conda_location + 'bin/conda' + ' '
        conda_env = subprocess.check_output(conda + "env list", shell=True, encoding='utf-8')
        for line in conda_env.splitlines():
            if '*' in line:
                i = line.find('/')
                active_conda_environment = line[i:].split('/')[-1]
    else:
        active_conda_environment = active_conda_env

    output_string = 'active conda environment:' + ' ' + active_conda_environment + '\n'
    pip = conda_location + 'envs/' + active_conda_environment + '/bin/pip'
    try:
        pip_list = subprocess.check_output(pip + ' ' + 'show' + ' ' + utility, shell=True, encoding='utf-8')

        for line in pip_list.splitlines():
            if 'Location' in line:
                i = line.find(':')
                location = line[i + 1:]
            if 'Version' in line:
                i = line.find(':')
                location = line[i + 1:]
                output_string += 'utility version:' + ' ' + location + '\n'
    except:
        pip_list = ''
        output_string += 'utility version:' + ' ' + 'n/a' + '\n'
    try:
        if pip_list.find('site-packages') != -1:
            repository = ''
        else:
            if repository_location is not None:
                location = repository_location + utility
            repository = subprocess.check_output('/usr/bin/git -C' + ' ' + location + ' ' + 'log -n 1', shell=True,
                                                 encoding='utf-8')
        output_string += 'repository location:' + ' ' + location + '\n'

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
        output_string += 'commit id:' + ' ' + 'n/a' + '\n'
        output_string += 'author:' + ' ' + 'n/a' + '\n'
        output_string += 'date:' + ' ' + 'n/a' + '\n'

    output_file = open(output_folder_location + utility + '_' + 'versions.log', 'w')
    output_file.write(output_string)
    output_file.close()
    return


def string_to_boolean(value):
    """
    Intended to be used when getting values from a terminal using `sys.argv`. Transforms `True` and `False` strings into
    boolean attributes
    :param value: string, `True` or `False`.
    :return: boolean equivalent of string
    """
    if value in ['True', 'true']:
        return True
    elif value in ['False', 'false']:
        return False


def create_json_dict(json_list, location):
    """
    returns a dictionary containing the system ids given a location containing json files with site information.
    :param json_list: list of json files containing site information.
    :param location: absolute path to folder containing json files with site information.
    :return: dictionary containing system ids as keys and site ids as values.
    """
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
    """
    Retrieves a list with zip code, inverter id, longitude, latitude, tilt and azimuth for a given system from
    a json file containing site information.
    site.
    :param file_name: String. Name of the json file containing site information.
    :param system: String. system id for which information is to be retrieved from json file
    :param location: String. Full path to folder containing site json file.
    :return: list with system zip code, inverter id, longitude, latitude, tilt and azimuth
    """
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
    """
    Splits absolute s3 location into parameters used to copy and execute commands remotely.
    :param s3_location: full path to s3 bucket location.
    :return: s3 bucket and prefix.
    """
    if s3_location[-1] != '/':
        s3_location += '/'
    i = s3_location.find('//') + 2
    j = s3_location.find('/', i)
    bucket = s3_location[i:j]
    prefix = s3_location[j + 1:-1]
    return bucket, prefix


def get_checked_sites(df):
    """
    Returns list of sites that have already been analyzed
    :param df: pandas dataframe containing results from reports or parameter study.
    :return:  List with sites that have already been analyzed.
    """
    if df is not None:
        checked_sites = df['site'].unique().tolist()
        checked_sites.sort()
    else:
        checked_sites = []
    return checked_sites


def siteid_to_filename(sites, file_label, ext='csv'):
    """
    Given a list of site ids returns a list of strings corresponding to the file name obtained by concatenating it to
    the 'file_label` string.  For `sites` in ['1', '2'] with `file_label`='_signal'. This method will return the list
    ['1_signal.csv', '2_signal.csv'].
    :param sites: List of strings containing the file id, i.e. non-repeating part of the input signal csv file names.
    :param file_label: String. Repeating part of the input signal csv file name.
    :param ext: String, optional. Input signal file extension
    :return: full file name corresponding to site ids in `sites` list
    """
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


def load_generic_data(location, file_label, file_id, extension='.csv', parse_dates=[0], nrows=None):
    to_read = location + file_id + file_label + extension
    if nrows is None:
        df = pd.read_csv(to_read, index_col=0, parse_dates=parse_dates)
    else:
        df = pd.read_csv(to_read, index_col=0, parse_dates=parse_dates, nrows=nrows)
    return df


def create_system_list(file_label, power_label, location, s3_bucket, prefix):
    file_list = enumerate_files(s3_bucket, prefix)
    ll = len(power_label)
    system_list = pd.DataFrame(columns=['site', 'system'])

    for file_ix, file_id in enumerate(file_list):
        progress(file_ix, len(file_list), 'Generating system list', bar_length=20)
        file_name = file_id.split('/')[1]
        i = file_name.find(file_label)
        file_id = file_name[:i]
        df = load_generic_data(location, file_label, file_id, nrows=2)
        cols = df.columns
        for col_label in cols:
            if col_label.find(power_label) != -1:
                system_id = col_label[ll:]
                system_list.loc[len(system_list)] = file_id, system_id
    progress(len(file_list), len(file_list), 'Generating system list', bar_length=20)
    return system_list


def enumerate_files(s3_bucket, prefix, extension='.csv', file_size_list=False):
    s3 = boto3.client('s3')
    output_list = []
    size_list = []
    for obj in s3.list_objects_v2(Bucket=s3_bucket, Prefix=prefix)['Contents']:
        if (obj['Key']).find(extension) != -1:
            file_name = obj['Key']
            file_size = obj['Size']
            i = file_name.rfind('/')
            file_name = file_name[i + 1:]
            output_list.append(file_name)
            size_list.append(file_size)
    if file_size_list:
        return output_list, size_list
    else:
        return output_list


def resume_run(output_file):
    if os.path.isfile(output_file):
        df = pd.read_csv(output_file, index_col=0)
        df['site'] = df['site'].apply(str)
        df['system'] = df['system'].apply(str)
    else:
        df = None
    return df


def create_system_dict(df):
    site_list = df['site'].unique().tolist()
    ss_dict = {}
    for site in site_list:
        systems_in_site = df[df['site'] == site]['system'].values.tolist()
        ss_dict[site] = systems_in_site
    return site_list, ss_dict


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


def run_failsafe_pipeline(dh_in, df_in, sys_tag, fts, tzc):
    try:
        try:
            dh_in.run_pipeline(power_col=sys_tag, fix_shifts=fts, correct_tz=tzc, verbose=False)
        except ValueError:
            max_val = np.nanquantile(df_in[sys_tag], 0.95)
            dh_in.run_pipeline(power_col=sys_tag, fix_shifts=False, correct_tz=tzc, verbose=False, max_val=max_val * 3)
    except:
        return False
    return True
