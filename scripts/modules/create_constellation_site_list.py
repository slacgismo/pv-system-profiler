from smart_open import smart_open
from functions import enumerate_files
import numpy as np
import re
import json
sys.path.append('/home/ubuntu/github/pv-system-profiler/')
sys.path.append('/home/ubuntu/github/solar-data-tools/')
from solardatatools.dataio import load_constellation_data
from solardatatools import DataHandler

def extract_parameters_from_json(item, parameter_id):
    param = item[parameter_id]
    if param == '':
        param = np.nan
    return param


def check_signal_availability(df_in, power_identifier):
    df_in['csv_complete'] = False
    sites = df_in['site'].unique().tolist()
    site_system_dict = {}
    for site_ix, site_id in enumerate(sites):
        systems_in_site = df_in[df_in['site'] == site_id]['system'].values.tolist()
        site_system_dict[site_id] = systems_in_site

    for site_ix, site_id in enumerate(sites):
        df = load_constellation_data(file_id=site_id)
        dh = DataHandler(df)
        cols = df.columns
        for sys_id in site_system_dict[site_id]:
            if sys_id != 'not available':
                sys_tag = power_identifier + sys_id
                if sys_tag in cols:
                    print(sys_tag)
                    df_in.loc[df_in['system'] == sys_id, 'csv_complete'] = True
    return df_in


def create_constellation_site_list(s3_bucket, prefix):
    sites = enumerate_files(s3_bucket, prefix)
    site_list = pd.DataFrame(columns=['site', 'system', 'longitude', 'latitude', 'tilt', 'azimuth', 'zip_code',
                                      'json_complete'])
    for site_ix, site_id in enumerate(sites[:]):
        cropped_site = re.split("[/._]", site_id)[2]
        print(cropped_site)
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
                jc = False
            else:
                jc = True
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

            site_list.loc[len(site_list)] = site, system, loc_param[0], loc_param[1], or_param[0], or_param[1], zc, jc
    return site_list
