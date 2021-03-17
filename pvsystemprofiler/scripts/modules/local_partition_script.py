import sys
import os
import pandas as pd

python = '/home/ubuntu/miniconda3/envs/pvi-dev/bin/python'
start_index = int(sys.argv[1])
end_index = int(sys.argv[2])
script_name = str(sys.argv[3])
global_input_file = str(sys.argv[4])
local_input_file = str(sys.argv[5])
local_output_file = str(sys.argv[6])
data_source = str(sys.argv[7])
power_column_id = str(sys.argv[8])
time_shift_inspection = str(sys.argv[9])
s3_location = str(sys.argv[10])
n_files = str(sys.argv[11])
file_label = str(sys.argv[12])
fix_time_shifts = str(sys.argv[13])
time_zone_correction = str(sys.argv[14])
check_json = str(sys.argv[15])

df_full = pd.read_csv(global_input_file, index_col=0)
df_part = df_full.copy()
df_part = df_part[start_index:end_index]
df_part.to_csv(local_input_file)

command = 'setsid nohup' + ' ' + python + ' ' + script_name
arguments = local_input_file + ' '  \
            + n_files + ' ' \
            + s3_location + ' ' \
            + file_label + ' ' \
            + power_column_id + ' ' \
            + local_output_file + ' ' \
            + fix_time_shifts + ' '\
            + time_zone_correction + ' ' \
            + check_json
os.system(command + ' ' + arguments + '>out &')
