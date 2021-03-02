import numpy as np
import sys
from time import time
from solardatatools import DataHandler
from solardatatools.utilities import progress
from modules.script_functions import run_failsafe_pipeline
from modules.script_functions import resume_run
from modules.script_functions import get_tag
from modules.script_functions import load_data
from modules.script_functions import get_lon_from_list
from modules.script_functions import get_lat_from_list
from modules.script_functions import get_orientation_from_list
from modules.script_functions import get_gmt_offset_from_list
from modules.script_functions import load_input_dataframe
from modules.script_functions import create_site_system_dict
from modules.script_functions import initialize_results_df
from modules.script_functions import create_site_list


def evaluate_systems(df_site, df, dh, partial_df, full_df, data_source, power_column_label, checked_systems,
                     site_system_dict, site_id):
    for sys_ix, sys_id in enumerate(site_system_dict[site_id]):
        if sys_id not in checked_systems:
            sys_tag = get_tag(dh, data_source, power_column_label, sys_id)
            cols = df_site.columns
            if sys_tag in cols:
                #print(site_id, sys_id)
                if 'time_shift_manual' in cols:
                    manual_time_shift = df_site.loc[df_site['system'] == sys_id, 'time_shift_manual'].values[0]
                else:
                    manual_time_shift = None
                lon = get_lon_from_list(df_site, sys_id)
                lat = get_lat_from_list(df_site, sys_id)
                tilt, azim = get_orientation_from_list(df_site, sys_id)
                if 'gmt_offset' in cols:
                    gmt_offset = get_gmt_offset_from_list(df_site, sys_id)
                else:
                    gmt_offset = None
                passes_pipeline = True

                try:
                    run_failsafe_pipeline(dh, df, sys_tag)
                except ValueError:
                    passes_pipeline = False
                results_list = [site_id, sys_id, lon, lat, tilt, azim, gmt_offset, dh.num_days, dh.capacity_estimate,
                                dh.data_sampling, dh.data_quality_score, dh.data_clearness_score, dh.inverter_clipping,
                                dh.time_shifts, dh.tz_correction, dh.capacity_changes, dh.normal_quality_scores,
                                manual_time_shift, passes_pipeline]
                partial_df.loc[0] = results_list
                full_df = full_df.append(partial_df)
                full_df.index = np.arange(len(full_df))
    return full_df


def main(data_source, power_column_id, df_site, sites, site_system_dict, start_at, full_df, partial_df, checked_systems,
         output_file):
    site_run_time = 0
    total_time = 0
    for site_ix, site_id in enumerate(sites[start_at:]):
        t0 = time()
        msg = 'Site/Accum. run time: {0:2.2f} s/{1:2.2f} m'.format(site_run_time, total_time / 60.0)

        progress(site_ix, len(sites), msg, bar_length=20)
        df = load_data(data_source, site_id)
        dh = DataHandler(df)
        full_df = evaluate_systems(df_site, df, dh, partial_df, full_df, data_source, power_column_id, checked_systems,
                                   site_system_dict, site_id)
        full_df.to_csv(output_file)
        t1 = time()
        site_run_time = t1 - t0
        total_time += site_run_time

    msg = 'Site/Accum. run time: {0:2.2f} s/{1:2.2f} m'.format(site_run_time, total_time / 60.0)
    progress(len(full_df), len(full_df), msg, bar_length=20)
    return


if __name__ == '__main__':
    '''
        :param data_source: String. Source of power data. 'constellation'. 
        :param power_column_label: String. Label of power columns in csv file. 'ac_power_inv_').
        :param input file: String. Absolute path to csv file containing the site list. Option 'generate' generates the site 
        list. 
        :param output_file: String. Absolute path to csv containing report results.
        :s3_location: String. Read only when 'input_file='generate. Absolute path to s3 folder containing json and csv 
        files with system information. For 's3://my_bucket/a/b/c' bucket= 'my_bucket'.
        :prefix: String. Read only when 'input_file='generate. Prefix to s3 folder containing json and csv files with 
        system information. For my_bucket prefix= 'a/b/c/.
        :prefix: Path to directory. For my_bucket path to list c contents is 'a/b/c/
        '''
    data_source = str(sys.argv[1])
    power_column_label = str(sys.argv[2])
    input_file = str(sys.argv[3])
    output_file = str(sys.argv[4])
    if input_file == 'generate':
        s3_location = str(sys.argv[5])
        s3_bucket = str(sys.argv[6])
        prefix = str(sys.argv[7])
    full_df, checked_systems, start_at = resume_run(output_file)

    if input_file == 'generate':
        print('Generating site list')
        input_df = create_site_list(s3_location, s3_bucket, prefix)
        input_df.to_csv('./generated_site_list.csv')
        print('Site list generated and saved as ./generated_site_list')
    else:
        print('Using input file' + ' ' + input_file)
        input_df = load_input_dataframe(input_file)

    df_site = input_df

    sites, site_system_dict = create_site_system_dict(df_site)
    partial_df = initialize_results_df()

    main(data_source, power_column_label, df_site, sites, site_system_dict, start_at, full_df, partial_df,
         checked_systems, output_file)
