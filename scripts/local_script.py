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

# print(global_input_file)
# print(local_input_file)
# print(local_output_file)

df_full = pd.read_csv(global_input_file, index_col=0)
df_part = df_full.copy()
df_part = df_part[start_index:end_index]
df_part.to_csv(local_input_file)


command = 'setsid nohup' + ' ' + python + ' ' + script_name
arguments = 'constellation' + ' ' + 'ac_power_inv_' + ' ' + local_input_file + ' ' + local_output_file + ' ' + 'True'
os.system(command + ' ' + arguments +'>out &')
