import numpy as np
import sys
from time import time
from solardatatools import DataHandler
from solardatatools.utilities import progress
from modules.functions import run_failsafe_pipeline
from modules.functions import resume_run
from modules.functions import get_tag
from modules.functions import load_data
from modules.functions import get_lon_from_list
from modules.functions import get_lat_from_list
from modules.functions import get_orientation_from_list
from modules.functions import get_gmt_offset_from_list
from modules.functions import load_input_dataframe
from modules.functions import filter_sites
from modules.functions import create_site_system_dict
from modules.functions import initialize_results_df

if __name__ == '__main__':
    data_source = str(sys.argv[1])
    power_column_id = str(sys.argv[2])
    input_file = str(sys.argv[3])
    output_file = str(sys.argv[4])

    full_df, checked_systems, start_at = resume_run(output_file)
    input_df = load_input_dataframe(input_file)
    df_site = filter_sites(input_df)
    sites, site_system_dict = create_site_system_dict(df_site)

    site_run_time = 0
    total_time = 0
    partial_df = initialize_results_df()
    for site_ix, site_id in enumerate(sites[start_at:]):
        t0 = time()
        msg = 'Site/Accum. run time: {0:2.2f} s/{1:2.2f} m'.format(site_run_time, total_time / 60.0)

        progress(site_ix, len(sites), msg, bar_length=20)
        df = load_data(data_source, site_id)
        dh = DataHandler(df)

        for sys_ix, sys_id in enumerate(site_system_dict[site_id]):
            if sys_id not in checked_systems:
                print(site_id, sys_id)
                # print(site_id, sys_id)
                sys_tag = get_tag(dh, data_source, power_column_id, sys_id)

                manual_time_shift = df_site.loc[df_site['system'] == sys_id, 'time_shift_manual'].values[0]
                lon = get_lon_from_list(df_site, sys_id)
                lat = get_lat_from_list(df_site, sys_id)
                tilt, azim = get_orientation_from_list(df_site, sys_id)
                gmt_offset = get_gmt_offset_from_list(df_site, sys_id)
                passes_pipeline = True

                try:
                    run_failsafe_pipeline(dh, df, sys_tag)
                except ValueError:
                    passes_pipeline = False

                v1 = site_id
                v2 = sys_id
                v3 = lon
                v4 = lat
                v5 = tilt
                v6 = azim
                v7 = gmt_offset
                v8 = dh.num_days
                v9 = dh.capacity_estimate
                v10 = dh.data_sampling
                v11 = dh.data_quality_score
                v12 = dh.data_clearness_score
                v13 = dh.inverter_clipping
                v14 = dh.time_shifts
                v15 = dh.tz_correction
                v16 = dh.capacity_changes
                v17 = dh.normal_quality_scores
                v18 = manual_time_shift
                v19 = passes_pipeline
                partial_df.loc[0] = v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12, v13, v14, v15, v16, v17, v18, v19
                full_df = full_df.append(partial_df)
                full_df.index = np.arange(len(full_df))
                full_df.to_csv(output_file)

        t1 = time()
        site_run_time = t1 - t0
        total_time += site_run_time

    msg = 'Site/Accum. run time: {0:2.2f} s/{1:2.2f} m'.format(site_run_time, total_time / 60.0)
    progress(len(sites), len(sites), msg, bar_length=20)
