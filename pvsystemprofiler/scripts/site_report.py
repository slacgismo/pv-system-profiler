import sys
import os
import pandas as pd
import numpy as np
from time import time
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
from modules.script_functions import extract_sys_parameters
from modules.script_functions import string_to_boolean
from modules.script_functions import write_git_version_logfile

def evaluate_systems(df, power_column_label, site_id, checked_systems, fix_time_shifts, time_zone_correction,
                     json_file_dict=None):
    cols = ['site', 'system', 'passes pipeline', 'length', 'capacity_estimate', 'data_sampling', 'data quality_score',
            'data clearness_score', 'inverter_clipping', 'time_shifts_corrected', 'time_zone_correction',
            'capacity_changes', 'normal_quality_scores', 'zip_code', 'longitude', 'latitude', 'tilt', 'azimuth',
            'sys_id']

    if json_file_dict is None:
        partial_df = pd.DataFrame(columns=cols[:-6])
    else:
        partial_df = pd.DataFrame(columns=cols)

    ll = len(power_column_label)
    cols = df.columns
    i = 0
    for col_label in cols:
        if col_label.find(power_column_label) != -1:
            system_id = col_label[ll:]
            if system_id not in checked_systems:
                #print(site_id, system_id)
                i += 1
                sys_tag = power_column_label + system_id
                dh = DataHandler(df)
                try:
                    run_failsafe_pipeline(dh, df, sys_tag, fix_time_shifts, time_zone_correction)
                    passes_pipeline = True
                except:
                    passes_pipeline = False

                results_list = [site_id, system_id, passes_pipeline, dh.num_days, dh.capacity_estimate,
                                dh.data_sampling,
                                dh.data_quality_score, dh.data_clearness_score, dh.inverter_clipping,
                                dh.time_shifts, dh.tz_correction, dh.capacity_changes, dh.normal_quality_scores]

                if json_file_dict is not None:
                    if system_id in json_file_dict.keys():
                        source_file = json_file_dict[system_id]
                        location_results = extract_sys_parameters(source_file, system_id, s3_location)
                    else:
                        location_results = [np.nan] * 5
                    results_list += location_results

                partial_df.loc[i] = results_list
    return partial_df


def main(input_file, n_files, s3_location, file_label, power_column_label, full_df, checked_systems, output_file,
         fix_time_shifts, time_zone_correction, check_json, ext='.csv' ):
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
        file_list = list(set(input_file_list) & set(file_list))

    file_list.sort()

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

        partial_df = evaluate_systems(df, power_column_label, site_id, checked_systems, fix_time_shifts,
                                      time_zone_correction, json_file_dict)

        full_df = full_df.append(partial_df)
        full_df.index = np.arange(len(full_df))
        full_df.to_csv(output_file)
        t1 = time()
        site_run_time = t1 - t0
        total_time += site_run_time

    msg = 'Site/Accum. run time: {0:2.2f} s/{1:2.2f} m'.format(site_run_time, total_time / 60.0)
    progress(len(file_list), len(file_list), msg, bar_length=20)
    return


if __name__ == '__main__':
    '''
        :input_file:  csv file containing list of sites to be evaluated. 'None' if no input file is provided.
        :param n_files: number of files to read. If 'all' all files in folder are read.
        :s3_location: Absolute path to s3 location of files.
        :param file_label:  Repeating portion of data files label. If 'None', no file label is used. 
        :param power_column_label: Repeating portion of the power column label. 
        :param output_file: Absolute path to csv file containing report results.
        :git_repository_location: absolute path to github repository location.
        :fix_time_shifts: String, 'True' or 'False', determines if time shifts are fixed when running the pipeline
        :time_zone_correction: String, 'True' or 'False', determines if time zone correction is performed when running 
        the pipeline
        :check_json: String, 'True' or 'False'. Check json file for location information. 
        '''

    input_file = str(sys.argv[1])
    n_files = str(sys.argv[2])
    s3_location = str(sys.argv[3])
    file_label = str(sys.argv[4])
    power_column_label = str(sys.argv[5])
    output_file = str(sys.argv[6])
    git_repository_location = str(sys.argv[7])
    fix_time_shifts = string_to_boolean(str(sys.argv[8]))
    time_zone_correction = string_to_boolean(str(sys.argv[9]))
    check_json = string_to_boolean(str(sys.argv[10]))

    if file_label == 'None':
        file_label = ''

    full_df, checked_systems, start_at = resume_run(output_file)

    write_git_version_logfile(git_repository_location)

    main(input_file, n_files, s3_location, file_label, power_column_label, full_df, checked_systems, output_file,
         fix_time_shifts, time_zone_correction, check_json)
