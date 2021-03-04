import numpy as np
import sys
from time import time
from solardatatools import DataHandler
from solardatatools.utilities import progress
from modules.script_functions import run_failsafe_pipeline
from modules.script_functions import resume_run
from modules.script_functions import get_tag
from modules.script_functions import load_generic_data
from modules.script_functions import get_lon_from_list
from modules.script_functions import get_lat_from_list
from modules.script_functions import get_orientation_from_list
from modules.script_functions import get_gmt_offset_from_list
from modules.script_functions import load_input_dataframe
from modules.script_functions import create_system_dict
from modules.script_functions import initialize_results_df
from modules.script_functions import create_system_list
from modules.script_functions import create_site_label
from modules.script_functions import enumerate_files
import pandas as pd

def evaluate_systems(df, power_column_label, site_id):
    partial_df = pd.DataFrame(columns=['site', 'system', 'passes pipeline'])
    ll = len(power_column_label)
    cols = df.columns
    for col_label in cols:
        if col_label.find(power_column_label) != -1:
            system_id = col_label[ll:]
            sys_tag = power_column_label + system_id
            dh = DataHandler(df)
            passes_pipeline = True

            results_list = [site_id, system_id, passes_pipeline]
            partial_df.loc[0] = results_list
    return partial_df

def main(s3_location, s3_bucket, prefix, file_label, power_column_label, start_at, full_df,
         checked_systems, output_file, ext='.csv'):

    site_run_time = 0
    total_time = 0
    file_list = enumerate_files(s3_bucket, prefix)
    file_list = file_list[:5]
    df, checked_list, start_index = resume_run(output_file)

    print(start_index)

    if start_index != 0:
        last_read_file = df.loc[len(df) - 1, 'site'] + file_label + ext
        for site_ix, site_id in enumerate(file_list):
            file_id = site_id.split('/')[1]
            if file_id == last_read_file:
                start_at = site_ix
            else:
                start_at = 0
    file_list = file_list[start_index:]

    print(file_list)
    #print(df, start_at)
    #print(df.loc[start_at])


    for file_ix, file_id in enumerate(file_list):
        t0 = time()
        msg = 'Site/Accum. run time: {0:2.2f} s/{1:2.2f} m'.format(site_run_time, total_time / 60.0)
        progress(file_ix, len(file_list), msg, bar_length=20)

        file_name = file_id.split('/')[1]
        i = file_name.find(file_label)
        site_id = file_name[:i]
        df = load_generic_data(s3_location, file_label, site_id)
        partial_df = evaluate_systems(df, power_column_label, site_id)

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
        :param input file: String. Absolute path to csv file containing the site list. Option 'generate' generates the site 
        list. 
        :param output_file: String. Absolute path to csv containing report results.
        :s3_location: String. Read only when 'input_file='generate. Absolute path to s3 folder containing json and csv 
        files with system information. For 's3://my_bucket/a/b/c' bucket= 'my_bucket'.
        :prefix: String. Read only when 'input_file='generate. Prefix to s3 folder containing json and csv files with 
        system information. For my_bucket prefix= 'a/b/c/.
        :prefix: Path to directory. For my_bucket path to list c contents is 'a/b/c/
        '''
    file_label = str(sys.argv[1])
    power_column_label = str(sys.argv[2])
    input_file = str(sys.argv[3])
    output_file = str(sys.argv[4])
    s3_location = str(sys.argv[5])
    s3_bucket = str(sys.argv[6])
    prefix = str(sys.argv[7])
    full_df, checked_systems, start_at = resume_run(output_file)
    #if input_file == 'generate':
    #    input_df = create_system_list(file_label, power_column_label, s3_location, s3_bucket, prefix)
    #    input_df.to_csv('./generated_system_list.csv')
    #    print('System list generated and saved as ./generated_system_list')
    #else:
    #    print('Using input file' + ' ' + input_file)
    #    input_df = load_input_dataframe(input_file)

    # df_site = input_df
    #
    # sites, site_system_dict = create_system_dict(df_site)

    main(s3_location, s3_bucket, prefix, file_label, power_column_label, start_at, full_df,
          checked_systems, output_file)
