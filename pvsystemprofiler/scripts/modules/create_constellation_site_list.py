import sys
from smart_open import smart_open
import numpy as np
import pandas as pd
import re
import json
from solardatatools.dataio import load_constellation_data
from solardatatools.utilities import progress

sys.path.append('/Users/londonoh/Documents/github/pv-system-profiler/')
from pvsystemprofiler.scripts.modules.script_functions import enumerate_files


def extract_parameters_from_json(item, parameter_id):
    param = item[parameter_id]
    if param == '':
        param = np.nan
    return param


def check_csv_for_signal(df_in, power_identifier):
    df_out = df_in.copy()
    df_out['data_available_csv'] = True
    sites = df_out['site'].unique().tolist()
    site_system_dict = {}
    for site_ix, site_id in enumerate(sites):
        systems_in_site = df_out[df_in['site'] == site_id]['system'].values.tolist()
        site_system_dict[site_id] = systems_in_site

    for site_ix, site_id in enumerate(sites):
        progress(site_ix, len(sites), 'Checking csv files', bar_length=20)
        df = load_constellation_data(file_id=site_id)
        cols = df.columns
        for sys_id in site_system_dict[site_id]:
            if sys_id != 'not available':
                sys_tag = power_identifier + sys_id
                if sys_tag not in cols:
                    df_in.loc[df_out['system'] == sys_id, 'data_available_csv'] = False
        progress(len(sites), len(sites), 'Checking csv files', bar_length=20)
    return df_in


def check_csv_for_signal_single(site_id, sys_id, power_identifier):
    df = load_constellation_data(file_id=site_id)
    cols = df.columns
    sys_tag = power_identifier + sys_id
    if sys_tag in cols:
        return True
    else:
        return False


def create_constellation_site_list(location, s3_bucket, prefix):
    sites = enumerate_files(s3_bucket, prefix)
    site_list = pd.DataFrame(columns=['site', 'system', 'longitude', 'latitude', 'tilt', 'azimuth', 'zip_code',
                                      'data_available_json'])
    i = 0
    for site_ix, site_id in enumerate(sites):
        progress(site_ix, len(sites), 'Reading json files', bar_length=20)
        cropped_site = re.split("[/._]", site_id)[2]
        for line in smart_open(location + cropped_site + '_system_details.json', 'rb'):
            file_json = json.loads(line)
            if len(file_json['Inverters']) == 0:
                site = cropped_site
                system = 'not available'
                lon = np.nan
                lat = np.nan
                tilt = np.nan
                azim = np.nan
                zc = '00000'
                jsonc = False
                site_list.loc[len(site_list)] = site, system, lon, lat, tilt, azim, zc, jsonc
            else:
                jsonc = True
                for inv_id in file_json['Inverters']:
                    site = cropped_site
                    system = file_json['Inverters'][inv_id]['inverter_id']
                    loc_param = ['longitude', 'latitude']

                    for param_ix, param_id in enumerate(loc_param):
                        try:
                            loc_param[param_ix] = extract_parameters_from_json(file_json['Site'], param_id)
                        except KeyError:
                            loc_param[param_ix] = np.nan
                    if np.nan not in loc_param:
                        try:
                            zc = file_json['Site']['location'][-5:]
                            if zc.isdigit():
                                zip_code = zc
                            else:
                                zip_code = '0000'
                        except KeyError:
                            zip_code = '00000'

                    mount_id = 'Mount ' + inv_id.split(' ')[1]
                    or_param = ['tilt', 'azimuth']
                    for param_ix, param_id in enumerate(or_param):
                        try:
                            or_param[param_ix] = extract_parameters_from_json(file_json['Mount'][mount_id], param_id)
                        except KeyError:
                            or_param[param_ix] = np.nan

                    site_list.loc[len(site_list)] = site, system, loc_param[0], loc_param[1], or_param[0], or_param[1], \
                                                    zc, jsonc
    progress(len(sites), len(sites), 'Reading json files', bar_length=20)
    return site_list
