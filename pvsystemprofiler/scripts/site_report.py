import pandas as pd
import numpy as np
import sys
from time import time
from solardatatools import DataHandler
from solardatatools.utilities import progress
from modules.script_functions import run_failsafe_pipeline
from modules.script_functions import resume_run
from modules.script_functions import load_generic_data
from modules.script_functions import enumerate_files
from modules.script_functions import get_checked_sites


def evaluate_systems(df, power_column_label, site_id, checked_systems):
    partial_df = pd.DataFrame(columns=['site', 'system', 'passes pipeline', 'length', 'capacity_estimate',
                                       'data_sampling', 'data quality_score', 'data clearness_score',
                                       'inverter_clipping', 'time_shifts_corrected', 'time_zone_correction',
                                       'capacity_changes', 'normal_quality_scores'])

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
                dh = DataHandler(df)
                try:
                    run_failsafe_pipeline(dh, df, sys_tag)
                    passes_pipeline = True
                except KeyError:
                    passes_pipeline = False

                results_list = [site_id, system_id, passes_pipeline, dh.num_days, dh.capacity_estimate,
                                dh.data_sampling,
                                dh.data_quality_score, dh.data_clearness_score, dh.inverter_clipping,
                                dh.time_shifts, dh.tz_correction, dh.capacity_changes, dh.normal_quality_scores]

                partial_df.loc[i] = results_list
    return partial_df


def main(s3_location, s3_bucket, prefix, file_label, power_column_label, full_df,
         checked_systems, output_file, ext='.csv'):
    site_run_time = 0
    total_time = 0
    full_site_list = enumerate_files(s3_bucket, prefix)
    #full_site_list = full_site_list[:2]

    previously_checked_site_list = get_checked_sites(full_df, prefix, file_label, ext)

    file_list = list(set(full_site_list) - set(previously_checked_site_list))
    file_list.sort()

    for file_ix, file_id in enumerate(file_list):
        t0 = time()
        msg = 'Site/Accum. run time: {0:2.2f} s/{1:2.2f} m'.format(site_run_time, total_time / 60.0)
        progress(file_ix, len(file_list), msg, bar_length=20)
        file_name = file_id.split('/')[1]
        i = file_name.find(file_label)
        site_id = file_name[:i]
        df = load_generic_data(s3_location, file_label, site_id)

        partial_df = evaluate_systems(df, power_column_label, site_id, checked_systems)

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
        :param power_column_label: String. Label of power columns in csv file. 'ac_power_inv_').
        :param output_file: String. Absolute path to csv containing report results.
        :s3_location: String. Read only when 'input_file='generate. Absolute path to s3 folder containing json and csv 
        files with system information. For 's3://my_bucket/a/b/c' bucket= 'my_bucket'.
        :prefix: String. Read only when 'input_file='generate. Prefix to s3 folder containing json and csv files with 
        system information. For my_bucket prefix= 'a/b/c/.
        :prefix: Path to directory. For my_bucket path to list c contents is 'a/b/c/
        '''
    file_label = str(sys.argv[1])
    power_column_label = str(sys.argv[2])
    output_file = str(sys.argv[3])
    s3_location = str(sys.argv[4])
    s3_bucket = str(sys.argv[5])
    prefix = str(sys.argv[6])
    full_df, checked_systems, start_at = resume_run(output_file)

    main(s3_location, s3_bucket, prefix, file_label, power_column_label, full_df, checked_systems, output_file)
