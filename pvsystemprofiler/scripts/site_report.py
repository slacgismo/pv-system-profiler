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
from pvsystemprofiler.scripts.modules.script_functions import get_commandline_inputs
from pvsystemprofiler.scripts.modules.script_functions import load_system_metadata
from pvsystemprofiler.scripts.modules.script_functions import generate_list
from solardatatools import DataHandler
from solardatatools.dataio import load_cassandra_data


def evaluate_systems(site_id, inputs_dict, df, site_metadata, json_file_dict=None):
    partial_df_cols = ['site', 'system', 'passes pipeline', 'length', 'capacity_estimate', 'data_sampling',
                       'data quality_score', 'data clearness_score', 'inverter_clipping', 'time_shifts_corrected',
                       'time_zone_correction', 'capacity_changes', 'normal_quality_scores', 'zip_code', 'longitude',
                       'latitude', 'tilt', 'azimuth', 'sys_id']
    ll = len(inputs_dict['power_column_label'])

    if json_file_dict is None:
        partial_df = pd.DataFrame(columns=partial_df_cols[:13])
    else:
        partial_df = pd.DataFrame(columns=partial_df_cols)

    if inputs_dict['time_shift_manual']:
        partial_df['manual_time_shift'] = np.nan

    dh = DataHandler(df, convert_to_ts=inputs_dict['convert_to_ts'])
    if inputs_dict['time_shift_manual'] == 1:
        dh.fix_dst()

    if inputs_dict['convert_to_ts']:
        if inputs_dict['convert_to_ts']:
            cols = [el[-1] for el in dh.keys]
    else:
        cols = dh.keys

    for col_label in cols:
        if col_label.find(inputs_dict['power_column_label']) != -1:
            system_id = col_label[ll:]
            if df_system_metadata is None or system_id in df_system_metadata['system'].tolist():
                sys_tag = inputs_dict['power_column_label'] + system_id

                if inputs_dict['time_shift_manual']:
                    manual_time_shift = int(site_metadata.loc[site_metadata['system'] == system_id,
                                                              'time_shift_manual'].values[0])
                else:
                    manual_time_shift = 0

                dh, passes_pipeline = run_failsafe_pipeline(dh, sys_tag, inputs_dict['fix_time_shifts'],
                                                            inputs_dict['time_zone_correction'])

                if passes_pipeline:
                    results_list = [site_id, system_id, passes_pipeline, dh.num_days, dh.capacity_estimate,
                                    dh.data_sampling,
                                    dh.data_quality_score, dh.data_clearness_score, dh.inverter_clipping,
                                    dh.time_shifts, dh.tz_correction, dh.capacity_changes, dh.normal_quality_scores]

                else:
                    results_list = [site_id, system_id, passes_pipeline] + [np.nan] * 10

                if json_file_dict:
                    if system_id in json_file_dict.keys():
                        source_file = json_file_dict[system_id]
                        location_results = extract_sys_parameters(source_file, system_id, inputs_dict['s3_location'])
                    else:
                        location_results = [np.nan] * 5
                    results_list += location_results

                if inputs_dict['time_shift_manual']:
                    results_list += str(manual_time_shift)

                partial_df.loc[len(partial_df)] = results_list
    return partial_df


def main(full_df, inputs_dict, df_system_metadata, ext='.csv'):
    site_run_time = 0
    total_time = 0
    file_list, json_file_dict = generate_list(inputs_dict, full_df)

    if inputs_dict['n_files'] != 'all':
        file_list = file_list[:int(inputs_dict['n_files'])]
    if full_df is None:
        full_df = pd.DataFrame()

    for file_ix, file_id in enumerate(file_list):
        t0 = time()
        msg = 'Site/Accum. run time: {0:2.2f} s/{1:2.2f} m'.format(site_run_time, total_time / 60.0)
        progress(file_ix, len(file_list), msg, bar_length=20)

        if inputs_dict['file_label']:
            i = file_id.find(inputs_dict['file_label'])
            site_id = file_id[:i]
            mask = df_system_metadata['site'] == site_id.split(inputs_dict['file_label'])[0]
        else:
            site_id = file_id.split('.')[0]
            mask = df_system_metadata['site'] == site_id
        site_metadata = df_system_metadata[mask]

        if inputs_dict['data_source'] == 'aws':
            df = load_generic_data(inputs_dict['s3_location'], inputs_dict['file_label'], site_id)
        if inputs_dict['data_source'] == 'cassandra':
            df = load_cassandra_data(site_id)

        partial_df = evaluate_systems(site_id, inputs_dict, df, site_metadata, json_file_dict)
        if not partial_df.empty:
            full_df = full_df.append(partial_df)
            full_df.index = np.arange(len(full_df))
            full_df.to_csv(inputs_dict['output_file'])
            t1 = time()
            site_run_time = t1 - t0
            total_time += site_run_time

    msg = 'Site/Accum. run time: {0:2.2f} s/{1:2.2f} m'.format(site_run_time, total_time / 60.0)
    if len(file_list) != 0:
        progress(len(file_list), len(file_list), msg, bar_length=20)
    return


if __name__ == '__main__':
    '''
    :param input_site_file:  csv file containing list of sites to be evaluated. 'None' if no input file is provided.
    :param n_files: number of files to read. If 'all' all files in folder are read.
    :param s3_location: Absolute path to s3 location of files.
    :param file_label:  Repeating portion of data files label. If 'None', no file label is used. 
    :param power_column_label: Repeating portion of the power column label. 
    :param output_file: Absolute path to csv file containing report results.
    :param fix_time_shits: String, 'True' or 'False'. Determines if time shifts are fixed when running the pipeline.
    :param time_zone_correction: String, 'True' or 'False'. Determines if time zone correction is performed when 
    running the pipeline.
    :param check_json: String, 'True' or 'False'. Check json file for location information.
    :param convert_to_ts: String, 'True' or 'False'.  Determines if conversion to time series is performed when 
    running the pipeline.
    :param system_summary_file: Full path to csv file containing manual time shift flag for each system, None if no file
    provided. 
    :param gmt_offset: String. Single value of gmt offset to be used for all estimations. If None a list with individual
    gmt offsets needs to be provided.
    :param data_source: String. Input signal data source. Options are 'aws' and 'cassandra'.
    '''

    inputs_dict = get_commandline_inputs()

    log_file_versions('solar-data-tools', active_conda_env='pvi-user')
    log_file_versions('pv-system-profiler')

    full_df = resume_run(inputs_dict['output_file'])

    ssf = inputs_dict['system_summary_file']
    if ssf:
        df_system_metadata = load_system_metadata(df_in=ssf, file_label=inputs_dict['file_label'])
        cols = df_system_metadata.columns
        for param in ['longitude', 'latitude', 'tilt', 'azimuth',
                      'estimated_longitude', 'estimated_latitude',
                      'time_shift_manual']:
            if param in cols:
                inputs_dict[param] = True
            else:
                inputs_dict[param] = False
    else:
        df_system_metadata = None

main(full_df, inputs_dict, df_system_metadata)
