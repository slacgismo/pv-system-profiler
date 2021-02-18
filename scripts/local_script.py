import sys
import os
import json
import pandas as pd

python = '/home/ubuntu/miniconda3/envs/pvi-dev/bin/python'
start_index = int(sys.argv[1])
end_index = int(sys.argv[2])
input_file = int(sys.argv[3])
output_file = int(sys.argv[4])

df_full = pd.read_csv(input_file, index_col=0)
df_part = df_full.copy()
df_part = df_part[start_index:end_index]
df_part.to_csv(output_file)

# os.chdir(local_location)
# os.system('setsid nohup' + ' ' + python + ' ' + '/home/ubuntu/github/pv-system-profiler/scripts/longitude_script.py>out &')

