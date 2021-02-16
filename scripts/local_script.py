import sys
import os
import json
import pandas as pd

python = '/home/ubuntu/miniconda3/envs/pvi-dev/bin/python'
s3_location = sys.argv[1]
ii = int(sys.argv[2])
jj = int(sys.argv[3])  
i = int(sys.argv[4]) + 1
n_chunks = int(sys.argv[5])  

site_list_file = s3_location + 'constellation_site_list.csv' 
site_report_file = s3_location + 'constellation_site_report.csv'

local_location = '/home/ubuntu/results_lon_' + str(i) + '_of_' + str(n_chunks) + '/'
results_file = local_location + 'data/constellation_lon_{}_of_{}.csv'.format(str(i), str(n_chunks))
local_site_list_file = local_location + 'data/constellation_site_list_chunk_{}_of_{}.csv'.format(str(i), str(n_chunks))


os.system('rm results* -rf')
os.system('mkdir -p ' + local_location +'data/')

df = pd.read_csv(site_list_file, index_col=0)
df_chunk = df.copy()
df_chunk = df_chunk[ii:jj]
df_chunk.to_csv(local_site_list_file)

input_file = {"results_file": results_file, "site_list_file": local_site_list_file, "report_file": site_report_file}
with open(local_location +  'param_lon.txt', 'w') as fp:
    json.dump(input_file, fp)

#os.chdir(local_location)
#os.system('setsid nohup' + ' ' + python '/home/ubuntu/github/pv-system-profiler/scripts/longitude_script.py>out&')
#os.system('setsid nohup' + ' ' + python + ' ' + '/home/ubuntu/github/pv-system-profiler/scripts/longitude_script.py>out&')
#os.system('setsid nohup python /home/ubuntu/github/pv-system-profiler/scripts/longitude_script.py>out&')
#os.system('setsid nohup python' + ' ' + '/home/ubuntu/github/pv-system-profiler/scripts/longitude_script.py>out &')

