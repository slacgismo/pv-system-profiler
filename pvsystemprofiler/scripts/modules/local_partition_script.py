import sys
import os
import pandas as pd

start_index = int(sys.argv[1])
end_index = int(sys.argv[2])
script_name = str(sys.argv[3])
global_input_file = str(sys.argv[4])
local_working_folder = str(sys.argv[5])
local_input_file = str(sys.argv[6])
local_output_file = str(sys.argv[7])
power_column_id = str(sys.argv[8])
time_shift_inspection = str(sys.argv[9])
s3_location = str(sys.argv[10])
n_files = str(sys.argv[11])
file_label = str(sys.argv[12])
fix_time_shifts = str(sys.argv[13])
time_zone_correction = str(sys.argv[14])
check_json = str(sys.argv[15])
supplementary_file = str(sys.argv[16])
python_command = str(sys.argv[17])

df_full = pd.read_csv(global_input_file, index_col=0)
df_part = df_full.copy()
df_part = df_part[start_index:end_index]
df_part.to_csv(local_input_file)

command = 'setsid nohup' + ' ' + python_command + ' ' + script_name
arguments = local_input_file + ' '  \
            + n_files + ' ' \
            + s3_location + ' ' \
            + file_label + ' ' \
            + power_column_id + ' ' \
            + local_output_file + ' ' \
            + time_shift_inspection + ' '\
            + fix_time_shifts + ' '\
            + time_zone_correction + ' ' \
            + check_json + ' ' \
            + supplementary_file
full_command = command + ' ' + arguments + '>out &'
file1 = open(local_working_folder + 'run_local_partition.sh', "w")
print('local input file', local_input_file)
print('file1', file1)
file1.write('#!/bin/sh\n')
file1.write(full_command)
file1.close()
os.system(full_command)
