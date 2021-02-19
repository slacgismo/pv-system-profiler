import sys
import os
import pandas as pd

python = '/home/ubuntu/miniconda3/envs/pvi-dev/bin/python'
start_index = int(sys.argv[1])
end_index = int(sys.argv[2])
global_input_file = str(sys.argv[3])
local_input_file = str(sys.argv[4])
output_file = str(sys.argv[5])
#output_folder = str(sys.argv[6])

print(global_input_file)
print(local_input_file)
print(output_file)

df_full = pd.read_csv(global_input_file, index_col=0)
df_part = df_full.copy()
df_part = df_part[start_index:end_index]
df_part.to_csv(local_input_file)

# data_source = str(sys.argv[1])
# power_column_id = str(sys.argv[2])
# input_file = str(sys.argv[3])
# output_file = str(sys.argv[4])
# time_shift_inspection = str(sys.argv[5])
#
#print(input_file)
#command = 'setsid nohup' + ' ' + python + ' ' + '/home/ubuntu/github/pv-system-profiler/scripts/longitude_script.py'
#arguments = 'constellation' + ' ' + 'ac_power_inv_' + ' ' + input_file + ' ' + output_file + 'True'

#print(command)
#print(arguments)
#os.chdir(output_folder)
#os.system(command + ' ' + arguments +'>out &')
