""" Site report script
This run script is used to generate a report of sites based on csv files containing input power or current signals of
the systems. The script looks for csv files containing input signals located in a 's3_location'. If json files with
additional data ae provided, the script is able to read this information and include it in the report. The file
'system_summary_file' containing site id, system id and 'time_shift_manual' may also be provided. The parameter
'input_site_file' allows to provide a csv file with the ids of the files to be evaluated by the report script.
"""
import sys
from pathlib import Path
import pandas as pd
import numpy as np
from time import time

# TODO: remove pth.append after package is deployed
filepath = Path(__file__).resolve().parents[2]
sys.path.append(str(filepath))
from solardatatools.utilities import progress
from pvsystemprofiler.scripts.modules.script_functions import run_failsafe_pipeline
from pvsystemprofiler.scripts.modules.script_functions import resume_run
from pvsystemprofiler.scripts.modules.script_functions import load_generic_data
from pvsystemprofiler.scripts.modules.script_functions import enumerate_files
from pvsystemprofiler.scripts.modules.script_functions import get_checked_sites
from pvsystemprofiler.scripts.modules.script_functions import create_json_dict
from pvsystemprofiler.scripts.modules.script_functions import log_file_versions
from pvsystemprofiler.scripts.modules.script_functions import filename_to_siteid
from pvsystemprofiler.scripts.modules.script_functions import extract_sys_parameters
from solardatatools import DataHandler
from solardatatools.dataio import load_cassandra_data


def load_ground_data(df_loc):
    df = pd.read_csv(df_loc, index_col=0)
    df = df[~df['time_shift_manual'].isnull()]
    df['time_shift_manual'] = df['time_shift_manual'].apply(int)
    df = df[df['time_shift_manual'].isin([0, 1])]
    df['site'] = df['site'].apply(str)
    df['system'] = df['system'].apply(str)
    df['site_file'] = df['site'].apply(lambda x: str(x) + '_20201006_composite')
    return df


def evaluate_systems(df, df_ground_data, power_column_label, site_id, time_shift_inspection, fix_time_shifts,
                     time_zone_correction, json_file_dict=None, convert_to_ts=False, data_type='a'):
    partial_df_cols = ['site', 'system', 'passes pipeline', 'length', 'capacity_estimate', 'data_sampling',
                       'data quality_score', 'data clearness_score', 'inverter_clipping', 'time_shifts_corrected',
                       'time_zone_correction', 'capacity_changes', 'normal_quality_scores', 'zip_code', 'longitude',
                       'latitude', 'tilt', 'azimuth', 'sys_id']

    if json_file_dict is None:
        partial_df = pd.DataFrame(columns=partial_df_cols[:13])
    else:
        partial_df = pd.DataFrame(columns=partial_df_cols)
    if time_shift_inspection:
        partial_df['manual_time_shift'] = np.nan

    ll = len(power_column_label)
    if data_type == 'a':
        cols = df.columns
    elif data_type == 'b':
        cols = []
        dh = DataHandler(df, convert_to_ts=convert_to_ts)
        for el in dh.keys:
            cols.append(el[-1])

    for col_label in cols:
        if col_label.find(power_column_label) != -1:
            system_id = col_label[ll:]
            if df_ground_data is None or system_id in df_ground_data['system'].tolist():
                sys_tag = power_column_label + system_id

                if time_shift_inspection:
                    manual_time_shift = int(df_ground_data.loc[df_ground_data['system'] == system_id,
                                                               'time_shift_manual'].values[0])
                else:
                    manual_time_shift = 0

                dh, passes_pipeline = run_failsafe_pipeline(df, manual_time_shift, sys_tag, fix_time_shifts,
                                                            time_zone_correction, convert_to_ts)

                if passes_pipeline:
                    results_list = [site_id, system_id, passes_pipeline, dh.num_days, dh.capacity_estimate,
                                    dh.data_sampling,
                                    dh.data_quality_score, dh.data_clearness_score, dh.inverter_clipping,
                                    dh.time_shifts, dh.tz_correction, dh.capacity_changes, dh.normal_quality_scores]

                else:
                    results_list = [site_id, system_id, passes_pipeline] + [np.nan] * 10

                if json_file_dict is not None:
                    if system_id in json_file_dict.keys():
                        source_file = json_file_dict[system_id]
                        location_results = extract_sys_parameters(source_file, system_id, s3_location)
                    else:
                        location_results = [np.nan] * 5
                    results_list += location_results

                if time_shift_inspection:
                    results_list += str(manual_time_shift)

                partial_df.loc[len(partial_df)] = results_list
    return partial_df


def main(input_site_list, df_ground_data, n_files, s3_location, file_label, power_column_label, full_df, output_file,
         time_shift_inspection, fix_time_shifts, time_zone_correction, check_json, ext='.csv'):
    site_run_time = 0
    total_time = 0
    if s3_location is not None:
        full_site_list = enumerate_files(s3_location)
        full_site_list = filename_to_siteid(full_site_list)
    else:
        full_site_list = []

    previously_checked_site_list = get_checked_sites(full_df)
    file_list = list(set(full_site_list) - set(previously_checked_site_list))

    if check_json:
        json_files = enumerate_files(s3_location, extension='.json')
        print('Generating system list from json files')
        json_file_dict = create_json_dict(json_files, s3_location)
        print('List generation completed')
    else:
        json_file_dict = None

    if input_site_list != 'None':
        input_site_list_df = pd.read_csv(input_site_list, index_col=0)
        site_list = input_site_list_df['site'].apply(str)
        site_list = site_list.tolist()
        if file_list:
            file_list = list(set(site_list) & set(file_list))
        else:
            file_list = list(set(site_list))
        if time_shift_inspection:
            manually_checked_sites = df_ground_data['site_file'].apply(str).tolist()
            file_list = list(set(file_list) & set(manually_checked_sites))
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
        # else:
        site_id = file_id.split('.')[0]
        try:
            df = load_generic_data(s3_location, file_label, site_id)
            data_type = 'a'
            convert_to_ts = False
        except TypeError:
            df = load_cassandra_data(site_id)
            data_type = 'b'
            convert_to_ts = True
        partial_df = evaluate_systems(df, df_ground_data, power_column_label, site_id, time_shift_inspection,
                                      fix_time_shifts, time_zone_correction, json_file_dict, convert_to_ts, data_type)
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
    return


if __name__ == '__main__':
    input_site_file = str(sys.argv[1]) if str(sys.argv[1]) != 'None' else None
    n_files = str(sys.argv[2])
    s3_location = str(sys.argv[3]) if str(sys.argv[3]) != 'None' else None
    file_label = str(sys.argv[4]) if str(sys.argv[4]) != 'None' else ''
    power_column_label = str(sys.argv[5])
    output_file = str(sys.argv[6])
    time_shift_inspection = True if str(sys.argv[7]) == 'True' else False
    fix_time_shifts = True if str(sys.argv[8]) == 'True' else False
    time_zone_correction = True if str(sys.argv[9]) == 'True' else False
    check_json = True if str(sys.argv[10]) == 'True' else False
    system_summary_file = str(sys.argv[11]) if str(sys.argv[11]) != 'None' else None
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
    :param system_summary_file: Full path to csv file containing manual time shift flag for each system, None if no file
    provided. 
    '''
    log_file_versions('solar-data-tools', active_conda_env='pvi-user')
    log_file_versions('pv-system-profiler')

    full_df = resume_run(output_file)

    if system_summary_file is not None:
        df_ground_data = load_ground_data(system_summary_file)
    else:
        df_ground_data = None
    main(input_site_file, df_ground_data, n_files, s3_location, file_label, power_column_label, full_df, output_file,
         time_shift_inspection, fix_time_shifts, time_zone_correction, check_json)
