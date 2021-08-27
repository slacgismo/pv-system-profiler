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
from pvsystemprofiler.tilt_azimuth_study import TiltAzimuthStudy
from pvsystemprofiler.scripts.modules.script_functions import filename_to_siteid
from solardatatools.dataio import load_cassandra_data

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
        if lat is not None:
            cols.append('latitude')
        if tilt is not None:
            cols.append('tilt')
        if azim is not None:
            cols.append('azimuth')
        if lat is not None:
            cols.append('latitude_residual')
        if tilt is not None:
            cols.append('tilt_residual')
        if azim is not None:
            cols.append('azimuth_residual')
        p_df = pd.DataFrame(columns=cols)
        p_df.loc[0, :] = np.nan
    return p_df, runs_ta_estimation


def evaluate_systems(df, df_system_metadata, power_column_label, site_id, time_shift_inspection, fix_time_shifts,
                     time_zone_correction, cp, tq, gmt, convert_to_ts, data_source):
    ll = len(power_column_label)

    if data_source == 'aws':
        cols = df.columns
    elif data_source == 'cassandra':
        cols = []
        dh = DataHandler(df, convert_to_ts=convert_to_ts)
        for el in dh.keys:
            cols.append(el[-1])

    i = 0
    partial_df = pd.DataFrame()
    for col_label in cols:
        if col_label.find(power_column_label) != -1:
            system_id = col_label[ll:]
            if system_id in df_system_metadata['system'].tolist():
                i += 1
                sys_tag = power_column_label + system_id
                longitude_input = float(df_system_metadata.loc[df_system_metadata['system'] == system_id,
                                                               'estimated_longitude'])
                real_latitude = float(df_system_metadata.loc[df_system_metadata['system'] == system_id, 'latitude'])
                real_tilt = float(df_system_metadata.loc[df_system_metadata['system'] == system_id, 'tilt'])
                real_azimuth = float(df_system_metadata.loc[df_system_metadata['system'] == system_id, 'azimuth'])
                latitude_input = float(df_system_metadata.loc[df_system_metadata['system'] == system_id,
                                                              'estimated_latitude'])

                if gmt is not None:
                    gmt_offset = gmt
                else:
                    gmt_offset = float(df_system_metadata.loc[df_system_metadata['system'] == system_id, 'gmt_offset'])

                if time_shift_inspection:
                    manual_time_shift = int(df_system_metadata.loc[df_system_metadata['system'] == system_id,
                                                                   'time_shift_manual'].values[0])
                else:
                    manual_time_shift = 0

                dh, passes_pipeline = run_failsafe_pipeline(df, manual_time_shift, sys_tag, fix_time_shifts,
                                                            time_zone_correction, convert_to_ts=convert_to_ts)

                if passes_pipeline:
                    results_df, passes_estimation = run_failsafe_ta_estimation(dh, 1, None, longitude_input,
                                                                               latitude_input, None, None,
                                                                               real_latitude, real_tilt, real_azimuth,
                                                                               gmt_offset, cp, tq)
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


def main(input_site_file, df_system_metadata, n_files, s3_location, file_label, power_column_label, full_df,
         output_file, time_shift_inspection, fix_time_shifts, time_zone_correction, check_json, cp, tq, gmt_offset,
         convert_to_ts, data_source):
    site_run_time = 0
    total_time = 0

    full_site_list = enumerate_files(s3_location)
    full_site_list = filename_to_siteid(full_site_list)

    previously_checked_site_list = get_checked_sites(full_df)
    file_list = list(set(full_site_list) - set(previously_checked_site_list))

    if check_json:
        json_files = enumerate_files(s3_location, extension='.json')
        print('Generating system list from json files')
        json_file_dict = create_json_dict(json_files, s3_location)
        print('List generation completed')
    else:
        json_file_dict = None

    if input_site_file is not None:
        input_file_df = pd.read_csv(input_site_file, index_col=0)
        site_list = input_file_df['site'].apply(str)
        site_list = site_list.tolist()
        manually_checked_sites = df_system_metadata['site_file'].apply(str).tolist()
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
        if file_label is not None:
            i = file_id.find(file_label)
            site_id = file_id[:i]
        else:
            site_id = file_id.split('.')[0]

        if data_source == 'aws':
            df = load_generic_data(s3_location, file_label, site_id)
        elif data_source == 'cassandra':
            df = load_cassandra_data(site_id)

        partial_df = evaluate_systems(df, df_system_metadata, power_column_label, site_id, time_shift_inspection,
                                      fix_time_shifts, time_zone_correction, cp, tq, gmt_offset, convert_to_ts,
                                      data_source)
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
    input_site_file = str(sys.argv[1]) if str(sys.argv[1]) != 'None' else None
    n_files = str(sys.argv[2])
    s3_location = str(sys.argv[3]) if str(sys.argv[3]) != 'None' else None
    file_label = str(sys.argv[4]) if str(sys.argv[4]) != 'None' else ''
    power_column_label = str(sys.argv[5])
    output_file = str(sys.argv[6])
    fix_time_shifts = True if str(sys.argv[7]) == 'True' else False
    time_zone_correction = True if str(sys.argv[8]) == 'True' else False
    check_json = True if str(sys.argv[9]) == 'True' else False
    convert_to_ts = True if str(sys.argv[10]) == 'True' else False
    system_summary_file = str(sys.argv[11]) if str(sys.argv[11]) != 'None' else None
    gmt_offset = str(sys.argv[12]) if str(sys.argv[12]) != 'None' else None
    data_source = str(sys.argv[13])
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
    log_file_versions('solar-data-tools', active_conda_env='pvi-user')
    log_file_versions('pv-system-profiler')
    # threshold values
    cp = [0.5, 0.55, 0.6, 0.65, 0.7, 0.75, 0.8, 0.85, 0.9, 0.95]
    tq = cp

    full_df = resume_run(output_file)
    if system_summary_file is not None:
        df_system_metadata = pd.read_csv(system_summary_file, index_col=0)
        df_system_metadata['site'] = df_system_metadata['site'].apply(str)
        df_system_metadata['system'] = df_system_metadata['system'].apply(str)
        df_system_metadata['site_file'] = df_system_metadata['site'].apply(lambda x: str(x) + '_20201006_composite')
        if 'time_shift_manual' in df_system_metadata.columns:
            time_shift_inspection = True
            df_system_metadata = df_system_metadata[~df_system_metadata['time_shift_manual'].isnull()]
            df_system_metadata['time_shift_manual'] = df_system_metadata['time_shift_manual'].apply(int)
            df_system_metadata = df_system_metadata[df_system_metadata['time_shift_manual'].isin([0, 1])]
        else:
            time_shift_inspection = False
    else:
        df_system_metadata = None



    main(input_site_file, df_system_metadata, n_files, s3_location, file_label, power_column_label, full_df,
         output_file, time_shift_inspection, fix_time_shifts, time_zone_correction, check_json, cp, tq, gmt_offset,
         convert_to_ts, data_source)
