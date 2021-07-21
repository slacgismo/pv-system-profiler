""" Longitude run script
This run script allows to run the longitude_study for multiple sites. The site ids to be evaluated can be provided in
 a csv file. Alternatively, the path to a folder containing the input signals of the sites in separate csv files can be
 provided.  The script provides the option to provided the full path to csv file containing latitude and gmt offset for
 each system for comparison.
"""
import sys
from pathlib import Path
import pandas as pd
import numpy as np
from time import time
# TODO: remove pth.append after package is deployed
filepath = Path(__file__).resolve().parents[2]
sys.path.append(str(filepath))
from solardatatools import DataHandler
from solardatatools.utilities import progress
from pvsystemprofiler.scripts.modules.script_functions import run_failsafe_pipeline
from pvsystemprofiler.scripts.modules.script_functions import resume_run
from pvsystemprofiler.scripts.modules.script_functions import load_generic_data
from pvsystemprofiler.scripts.modules.script_functions import enumerate_files
from pvsystemprofiler.scripts.modules.script_functions import get_checked_sites
from pvsystemprofiler.scripts.modules.script_functions import create_json_dict
from pvsystemprofiler.scripts.modules.script_functions import string_to_boolean
from pvsystemprofiler.scripts.modules.script_functions import log_file_versions
from pvsystemprofiler.longitude_study import LongitudeStudy
from pvsystemprofiler.scripts.modules.script_functions import filename_to_siteid


def run_failsafe_lon_estimation(dh_in, real_longitude, gmt_offset):
    try:
        runs_lon_estimation = True
        lon_study = LongitudeStudy(data_handler=dh_in, gmt_offset=gmt_offset, true_value=real_longitude)
        lon_study.run(verbose=False)
        p_df = lon_study.results.sort_index().copy()
    except:
        runs_lon_estimation = False
        p_df = pd.DataFrame(columns=['longitude', 'estimator', 'eot_calculation', 'solar_noon_method',
                                     'day_selection_method', 'data_matrix', 'residual', 'site', 'system',
                                     'length', 'data sampling', 'data quality score', 'data clearness score',
                                     'inverter clipping', 'time shift manual'])
        p_df.loc[0, :] = np.nan
    return p_df, runs_lon_estimation


def evaluate_systems(df, df_ground_data, power_column_label, site_id, time_shift_inspection, fix_time_shifts,
                     time_zone_correction):
    ll = len(power_column_label)
    cols = df.columns
    i = 0
    partial_df = pd.DataFrame()
    for col_label in cols:
        if col_label.find(power_column_label) != -1:
            system_id = col_label[ll:]
            if system_id in df_ground_data['system'].tolist():
                i += 1
                sys_tag = power_column_label + system_id

                real_longitude = float(df_ground_data.loc[df_ground_data['system'] == system_id, 'longitude'])
                gmt_offset = float(df_ground_data.loc[df_ground_data['system'] == system_id, 'gmt_offset'])

                dh = DataHandler(df)
                if time_shift_inspection:
                    manual_time_shift = int(df_ground_data.loc[df_ground_data['system'] == system_id,
                                                               'time_shift_manual'].values[0])
                    if manual_time_shift == 1:
                        dh.fix_dst()

                passes_pipeline = run_failsafe_pipeline(df, dh, sys_tag, fix_time_shifts, time_zone_correction)

                if passes_pipeline:
                    results_df, passes_estimation = run_failsafe_lon_estimation(dh, real_longitude, gmt_offset)
                    results_df['length'] = dh.num_days
                    results_df['data sampling'] = dh.data_sampling
                    results_df['data quality score'] = dh.data_quality_score
                    results_df['data clearness score'] = dh.data_clearness_score
                    results_df['inverter clipping'] = dh.inverter_clipping
                    results_df['runs estimation'] = passes_estimation
                else:
                    results_df = pd.DataFrame()
                    results_df['length'] = np.nan
                    results_df['data sampling'] = np.nan
                    results_df['data quality score'] = np.nan
                    results_df['data clearness score'] = np.nan
                    results_df['inverter clipping'] = np.nan
                    results_df['runs estimation'] = np.nan

                results_df['site'] = site_id
                results_df['system'] = system_id

                if time_shift_inspection:
                    results_df['manual_time shift'] = manual_time_shift

                partial_df = partial_df.append(results_df)
    return partial_df


def main(input_site_file, df_ground_data, n_files, s3_location, file_label, power_column_label, full_df, output_file,
         time_shift_inspection, fix_time_shifts, time_zone_correction, check_json):
    site_run_time = 0
    total_time = 0

    full_site_list = enumerate_files(s3_location)
    full_site_list = filename_to_siteid(full_site_list)

    previously_checked_site_list = get_checked_sites(full_df)
    file_list = list(set(full_site_list) - set(previously_checked_site_list))

    if check_json:
        json_files = enumerate_files(s3_bucket, prefix, extension='.json')
        print('Generating system list from json files')
        json_file_dict = create_json_dict(json_files, s3_location)
        print('List generation completed')
    else:
        json_file_dict = None

    if input_site_file != 'None':
        input_file_df = pd.read_csv(input_site_file, index_col=0)
        site_list = input_file_df['site'].apply(str)
        site_list = site_list.tolist()
        manually_checked_sites = df_ground_data['site_file'].apply(str).tolist()
        file_list = list(set(site_list) & set(file_list) & set(manually_checked_sites))

    file_list.sort()

    if n_files != 'all':
        file_list = file_list[:int(n_files)]
    if full_df is None:
        full_df = pd.DataFrame()
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
        partial_df = evaluate_systems(df, df_ground_data, power_column_label, site_id, time_shift_inspection,
                                      fix_time_shifts, time_zone_correction)
        if not partial_df.empty:
            full_df = full_df.append(partial_df)
            full_df.index = np.arange(len(full_df))
            full_df.to_csv(output_file)
            t1 = time()
            site_run_time = t1 - t0
            total_time += site_run_time

    msg = 'Site/Accum. run time: {0:2.2f} s/{1:2.2f} m'.format(site_run_time, total_time / 60.0)
    if len(file_list) != 0:
        progress(len(file_list), len(file_list), msg, bar_length=20)
    print('finished')
    return


if __name__ == '__main__':
    input_site_file = str(sys.argv[1])
    n_files = str(sys.argv[2])
    s3_location = str(sys.argv[3])
    file_label = str(sys.argv[4])
    power_column_label = str(sys.argv[5])
    output_file = str(sys.argv[6])
    time_shift_inspection = str(sys.argv[7])
    fix_time_shifts = string_to_boolean(str(sys.argv[8]))
    time_zone_correction = string_to_boolean(str(sys.argv[9]))
    check_json = string_to_boolean(str(sys.argv[10]))
    system_summary_file = str(sys.argv[11])

    '''
    :param input_site_file:  csv file containing list of sites to be evaluated. 'None' if no input file is provided.
    :param n_files: number of files to read. If 'all' all files in folder are read.
    :param s3_location: Absolute path to s3 location of files.
    :param file_label:  Repeating portion of data files label. If 'None', no file label is used. 
    :param power_column_label: Repeating portion of the power column label. 
    :param output_file: Absolute path to csv file containing report results.
    :param time_shift_inspection: String, 'True' or 'False'. Determines if manual time shift inspection is performed 
    when running the pipeline.
    :param fix_time_shifts: String, 'True' or 'False'. Determines if time shifts are fixed when running the pipeline.
    :param time_zone_correction: String, 'True' or 'False'. Determines if time zone correction is performed when 
    running the pipeline.
    :param check_json: String, 'True' or 'False'. Check json file for location information.
    :param system_summary_file: Full path to csv file containing longitude and gmt offset for each system. 
    '''

    log_file_versions('solar-data-tools', active_conda_env='pvi-user')
    log_file_versions('pv-system-profiler')
    if file_label == 'None':
        file_label = ''

    full_df = resume_run(output_file)
    df_ground_data = pd.read_csv(system_summary_file, index_col=0)
    df_ground_data = df_ground_data[~df_ground_data['time_shift_manual'].isnull()]
    df_ground_data['time_shift_manual'] = df_ground_data['time_shift_manual'].apply(int)
    df_ground_data = df_ground_data[df_ground_data['time_shift_manual'].isin([0, 1])]
    df_ground_data['site'] = df_ground_data['site'].apply(str)
    df_ground_data['system'] = df_ground_data['system'].apply(str)
    df_ground_data['site_file'] = df_ground_data['site'].apply(lambda x: str(x) + '_20201006_composite')

    main(input_site_file, df_ground_data, n_files, s3_location, file_label, power_column_label, full_df, output_file,
         time_shift_inspection, fix_time_shifts, time_zone_correction, check_json)
