""" Tilt and azimuth run script
This run script allows to run the tilt_azimuth_study for multiple sites. The site ids to be evaluated can be provided in
 a csv file. Alternatively, the path to a folder containing the input signals of the sites in separate csv files can be
 provided. The most common use of this script is to estimate tilt and azimuth provided input values of longitude and
 latitude. In this case, a csv system_summary_file containing values of longitude and latitude need to be provided.
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
from pvsystemprofiler.scripts.modules.script_functions import load_system_metadata
from pvsystemprofiler.scripts.modules.script_functions import generate_list
from pvsystemprofiler.tilt_azimuth_study import TiltAzimuthStudy
from pvsystemprofiler.scripts.modules.script_functions import filename_to_siteid
from solardatatools.dataio import load_cassandra_data
from pvsystemprofiler.scripts.modules.script_functions import get_commandline_inputs
from solardatatools import DataHandler

def run_failsafe_ta_estimation(dh, nrandom, threshold, lon, lat, tilt, azim, real_lat, real_tilt, real_azim, gmt_offset,
                               cp, tq):
    try:
        runs_ta_estimation = True
        ta_study = TiltAzimuthStudy(data_handler=dh, nrandom_init_values=nrandom, daytime_threshold=threshold,
                                    lon_input=lon, lat_input=lat, tilt_input=tilt, azimuth_input=azim,
                                    lat_true_value=real_lat, tilt_true_value=real_tilt, azimuth_true_value=real_azim,
                                    gmt_offset=gmt_offset, cvx_parameter=cp, threshold_quantile=tq)
        ta_study.run()
        p_df = ta_study.results.sort_index().copy()
    except:
        runs_ta_estimation = False
        cols = ['day range', 'declination method', 'latitude initial value', 'tilt initial value',
                'azimuth initial value']
        if lat:
            cols.append('latitude')
        if tilt:
            cols.append('tilt')
        if azim:
            cols.append('azimuth')
        if lat:
            cols.append('latitude_residual')
        if tilt:
            cols.append('tilt_residual')
        if azim:
            cols.append('azimuth_residual')
        p_df = pd.DataFrame(columns=cols)
        p_df.loc[0, :] = np.nan
    return p_df, runs_ta_estimation


def evaluate_systems(site_id, inputs_dict, df, site_metadata, json_file_dict=None):
    ll = len(inputs_dict['power_column_label'])

    dh = DataHandler(df, convert_to_ts=inputs_dict['convert_to_ts'])
    if inputs_dict['time_shift_manual'] == 1:
        dh.fix_dst()

    if inputs_dict['convert_to_ts']:
        if inputs_dict['convert_to_ts']:
            cols = [el[-1] for el in dh.keys]
    else:
        cols = dh.keys

    i = 0
    partial_df = pd.DataFrame()
    for col_label in cols:
        if col_label.find(inputs_dict['power_column_label']) != -1:
            system_id = col_label[ll:]
            if system_id in site_metadata['system'].tolist():
                i += 1
                sys_tag = inputs_dict['power_column_label'] + system_id
                sys_mask = site_metadata['system'] == system_id
                if inputs_dict['estimated_longitude']:
                    longitude_input = float(site_metadata.loc[sys_mask, 'estimated_longitude'])
                if inputs_dict['estimated_latitude']:
                    real_latitude = float(site_metadata.loc[sys_mask, 'latitude'])
                if inputs_dict['tilt']:
                    real_tilt = float(site_metadata.loc[sys_mask, 'tilt'])
                if inputs_dict['azimuth']:
                    real_azimuth = float(site_metadata.loc[sys_mask, 'azimuth'])
                    latitude_input = float(site_metadata.loc[sys_mask, 'estimated_latitude'])
                if inputs_dict['gmt_offset']:
                    gmt_offset = inputs_dict['gmt_offset']
                else:
                    gmt_offset = float(site_metadata.loc[sys_mask, 'gmt_offset'])

                if inputs_dict['time_shift_manual']:
                    manual_time_shift = int(df_system_metadata.loc[df_system_metadata['system'] == system_id,
                                                                  'time_shift_manual'].values[0])
                else:
                    manual_time_shift = 0

                dh, passes_pipeline = run_failsafe_pipeline(dh, sys_tag, inputs_dict['fix_time_shifts'],
                                                            inputs_dict['time_zone_correction'])

                if passes_pipeline:
                    results_df, passes_estimation = run_failsafe_ta_estimation(dh, 1, None, longitude_input,
                                                                               latitude_input, None, None,
                                                                               real_latitude, real_tilt, real_azimuth,
                                                                               gmt_offset, inputs_dict['cp'],
                                                                               inputs_dict['tq'])
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

                if inputs_dict['time_shift_manual']:
                    results_df['manual_time shift'] = manual_time_shift

                partial_df = partial_df.append(results_df)
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

        # TODO: integrate option for other data inputs
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
    print('finished')
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
      :param system_summary_file: Full path to csv file containing longitude, latitude and gmt offset for each system. 
      :param gmt_offset: String. Single value of gmt offset to be used for all estimations. If None a list with 
      individual gmt offsets needs to be provided.
      :param data_source: String. Input signal data source. Options are 'aws' and 'cassandra'.
      '''
    # log_file_versions('solar-data-tools', active_conda_env='pvi-user')
    # log_file_versions('pv-system-profiler')

    inputs_dict = get_commandline_inputs()

    # threshold values
    inputs_dict['cp'] = [0.5, 0.55, 0.6, 0.65, 0.7, 0.75, 0.8, 0.85, 0.9, 0.95]
    inputs_dict['tq'] = inputs_dict['cp']

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
