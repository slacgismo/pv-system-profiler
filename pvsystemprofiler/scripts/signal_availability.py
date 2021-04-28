import pandas as pd
from solardatatools import DataHandler
import sys
sys.path.append('..')
sys.path.append('/Users/londonoh/Documents/github/pv-system-profiler')
from solardatatools.dataio import load_constellation_data


df_site = pd.read_csv('s3://pv.insight.misc/report_files/constellation_site_list.csv', index_col=0)
df_out = df_site.copy()
for i in df_site.index:
    print(i)
    site_id = df_site.loc[i, 'site']
    system_id = df_site.loc[i, 'system']
    sys_tag_power = 'ac_power_inv_' + str(system_id)
    sys_tag_current = 'dc_current_inv_' + str(system_id)
    df, data = load_constellation_data(file_id=site_id, json_file=True)
    cols = df.columns
    for sys_tag in [sys_tag_power, sys_tag_current]:
        try:
            dh = DataHandler(df)
            if sys_tag in cols:
                if sys_tag == sys_tag_power:
                    df_out.loc[df_out['system'] == system_id, 'power_available'] = True
                else:
                    df_out.loc[df_out['system'] == system_id, 'power_available'] = False
                if sys_tag == sys_tag_current:
                    df_out.loc[df_out['system'] == system_id, 'current_available'] = True
                else:
                    df_out.loc[df_out['system'] == system_id, 'current_available'] = False

                #print(1)
        except KeyError:
            pass
            #print(2)
df_out.to_csv('/Users/londonoh/Desktop/signal_availability.csv')