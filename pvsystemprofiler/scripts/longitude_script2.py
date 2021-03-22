import sys
import os
import pandas as pd
import numpy as np
from time import time
#sys.path.append('/home/ubuntu/github/pv-system-profiler/')
#sys.path.append('/home/ubuntu/github/solar-data-tools/')
sys.path.append('/Users/londonoh/Documents/github/pv-system-profiler/')
sys.path.append('/Users/londonoh/Documents/github/solar-data-tools/')
from solardatatools import DataHandler
from solardatatools.utilities import progress
from modules.script_functions import run_failsafe_pipeline
from modules.script_functions import resume_run
from modules.script_functions import load_generic_data
from modules.script_functions import enumerate_files
from modules.script_functions import get_checked_sites
from modules.script_functions import get_s3_bucket_and_prefix
from modules.script_functions import siteid_to_filename
from modules.script_functions import create_json_dict
from modules.script_functions import string_to_boolean
from modules.script_functions import log_file_versions
from pvsystemprofiler.longitude_study import LongitudeStudy


def run_failsafe_lon_estimation(dh_in, real_longitude, gmt_offset):
    try:
        runs_pipeline = True
        lon_study = LongitudeStudy(data_handler=dh_in, gmt_offset=gmt_offset, true_value=real_longitude)
        #lon_study.run(verbose=False)
        lon_study.run(verbose=True)
        p_df = lon_study.results.sort_index().copy()
    except ValueError:
        runs_pipeline = False
        p_df = pd.DataFrame(columns=['longitude', 'estimator', 'eot_calculation', 'solar_noon_method',
                                     'day_selection_method', 'data_matrix', 'residual', 'site', 'system',
                                     'length', 'data sampling', 'data quality score', 'data clearness score',
                                     'inverter clipping', 'time shift manual'])
        partial_df.loc[0, :] = np.nan
    return p_df, runs_pipeline


def evaluate_systems(df, df_ground_data, power_column_label, site_id, checked_systems, time_shift_inspection,
                     fix_time_shifts, time_zone_correction, json_file_dict=None):
    ll = len(power_column_label)
    cols = df.columns
    i = 0
    for col_label in cols:
        if col_label.find(power_column_label) != -1:
            system_id = col_label[ll:]
            if system_id not in checked_systems:
                print(site_id, system_id)
                i += 1
                sys_tag = power_column_label + system_id
                real_longitude = int(df_ground_data.loc[df_ground_data['system'] == system_id, 'longitude'])
                gmt_offset = int(df_ground_data.loc[df_ground_data['system'] == system_id, 'gmt_offset'])

                dh = DataHandler(df)
                if time_shift_inspection:
                    manual_time_shift = int(df_ground_data.loc[df_ground_data['system'] == system_id,
                                                                'time_shift_manual'].values[0])
                    if manual_time_shift == 1:
                        dh.fix_dst()

                try:
                    run_failsafe_pipeline(dh, df, sys_tag, fix_time_shifts, time_zone_correction)
                    passes_pipeline = True
                except:
                    passes_pipeline = False

                partial_df, passes_estimation = run_failsafe_lon_estimation(dh, real_longitude, gmt_offset)
                partial_df['site'] = site_id
                partial_df['system'] = system_id
                if time_shift_inspection:
                    partial_df['manual_time shift'] = manual_time_shift
                if passes_pipeline is True:
                    partial_df['length'] = dh.num_days
                    partial_df['data sampling'] = dh.data_sampling
                    partial_df['data quality score'] = dh.data_quality_score
                    partial_df['data clearness score'] = dh.data_clearness_score
                    partial_df['inverter clipping'] = dh.inverter_clipping
                    partial_df['runs estimation'] = passes_estimation
                else:
                    partial_df['length'] = np.nan
                    partial_df['data sampling'] = np.nan
                    partial_df['data quality score'] = np.nan
                    partial_df['data clearness score'] = np.nan
                    partial_df['inverter clipping'] = np.nan
                    partial_df['runs estimation'] = np.nan
    return partial_df


def main(input_file, df_ground_data, n_files, s3_location, file_label, power_column_label, full_df, checked_systems,
         output_file, time_shift_inspection, fix_time_shifts, time_zone_correction, check_json, ext='.csv'):
    site_run_time = 0
    total_time = 0
    s3_bucket, prefix = get_s3_bucket_and_prefix(s3_location)
    full_site_list = enumerate_files(s3_bucket, prefix)
    previously_checked_site_list = get_checked_sites(full_df, file_label, ext)

    file_list = list(set(full_site_list) - set(previously_checked_site_list))

    if check_json:
        json_files = enumerate_files(s3_bucket, prefix, extension='.json')
        print('Generating system list from json files')
        json_file_dict = create_json_dict(json_files, s3_location)
        print('List generation completed')
    else:
        json_file_dict = None

    if input_file != 'None':
        input_file_df = pd.read_csv(input_file, index_col=0)
        site_list = input_file_df['site'].apply(str)
        site_list = site_list.tolist()
        input_file_list = siteid_to_filename(site_list, file_label, ext)
        #file_list = list(set(input_file_list) & set(file_list))
        manually_checked_sites = df_ground_data['site'].apply(str)
        file_list = list(set(input_file_list) & set(file_list) & set(manually_checked_sites))
    file_list.sort()
    file_list = file_list[:1]

    if n_files != 'all':
        file_list = file_list[:int(n_files)]
    for file_ix, file_id in enumerate(file_list):
        t0 = time()
        msg = 'Site/Accum. run time: {0:2.2f} s/{1:2.2f} m'.format(site_run_time, total_time / 60.0)
        progress(file_ix, len(file_list), msg, bar_length=20)
        if file_label != '':
            i = file_id.find(file_label)
            site_id = file_id[:i]
        else:
            site_id = file_id.split('.')[0]

        df = load_generic_data(s3_location, file_label, site_id)
        partial_df = evaluate_systems(df, df_ground_data, power_column_label, site_id, checked_systems,
                                      time_shift_inspection, fix_time_shifts, time_zone_correction, json_file_dict)

        full_df = full_df.append(partial_df)
        full_df.index = np.arange(len(full_df))
        full_df.to_csv(output_file)
        t1 = time()
        site_run_time = t1 - t0
        total_time += site_run_time

    msg = 'Site/Accum. run time: {0:2.2f} s/{1:2.2f} m'.format(site_run_time, total_time / 60.0)
    progress(len(file_list), len(file_list), msg, bar_length=20)
    print('finished')
    return


if __name__ == '__main__':
    '''
        :input_file:  csv file containing list of sites to be evaluated. 'None' if no input file is provided.
        :df_ground_data: location of pandas dataframe containing longitude and gmt offset for each system.
        :n_files: number of files to read. If 'all' all files in folder are read.
        :s3_location: Absolute path to s3 location of files.
        :file_label:  Repeating portion of data files label. If 'None', no file label is used. 
        :power_column_label: Repeating portion of the power column label. 
        :output_file: Absolute path to csv file containing report results.
        :time_shift_inspection: String, 'True' or 'False'. Determines indicates if manual time shift inspection should 
        be taken into account for pipeline run.
        :fix_time_shifts: String, 'True' or 'False', determines if time shifts are fixed when running the pipeline
        :time_zone_correction: String, 'True' or 'False', determines if time zone correction is performed when running 
        the pipeline
        :check_json: String, 'True' or 'False'. Check json file for location information. 
        '''

    input_file = str(sys.argv[1])
    df_ground_data = str(sys.argv[2])
    n_files = str(sys.argv[3])
    s3_location = str(sys.argv[4])
    file_label = str(sys.argv[5])
    power_column_label = str(sys.argv[6])
    output_file = str(sys.argv[7])
    time_shift_inspection = str(sys.argv[8])
    fix_time_shifts = string_to_boolean(str(sys.argv[9]))
    time_zone_correction = string_to_boolean(str(sys.argv[10]))
    check_json = string_to_boolean(str(sys.argv[11]))

    local_output_folder = output_file.split('data')[0]
    #log_file_versions('solar_data_tools', local_output_folder)

    if file_label == 'None':
        file_label = ''

    full_df, checked_systems, start_at = resume_run(output_file)
    df_ground_data = pd.read_csv('s3://pv.insight.misc/report_files/constellation_site_list.csv', index_col=0)
    df_ground_data = df_ground_data[df_ground_data['time_shift_manual'].isin([1, -1])]

    main(input_file, df_ground_data, n_files, s3_location, file_label, power_column_label, full_df, checked_systems,
         output_file,
         time_shift_inspection, fix_time_shifts, time_zone_correction, check_json)
