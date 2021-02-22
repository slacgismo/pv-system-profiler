import sys
import pandas as pd
import numpy as np
from time import time
from modules.functions import resume_run
from modules.functions import create_site_system_dict
from modules.functions import load_data
from modules.functions import get_tag
from modules.functions import get_lon_from_report
from modules.functions import get_gmt_offset_from_report
from modules.functions import get_inspected_time_shift
from modules.functions import run_failsafe_pipeline
sys.path.append('/home/ubuntu/github/pv-system-profiler/')
sys.path.append('/home/ubuntu/github/solar-data-tools/')

from solardatatools import DataHandler
from solardatatools.utilities import progress
from pvsystemprofiler.longitude_study import LongitudeStudy


def run_failsafe_lon_estimation(dh_in):
    try:
        runs_pipeline = True
        lon_study = LongitudeStudy(data_handler=dh_in, gmt_offset=gmt_offset, true_value=real_longitude)
        lon_study.run(verbose=False)
        p_df = lon_study.results.sort_index().copy()
    except ValueError:
        runs_pipeline = False
        p_df = pd.DataFrame(columns=['longitude', 'estimator', 'eot_calculation', 'solar_noon_method',
                                     'day_selection_method', 'data_matrix', 'residual', 'site', 'system',
                                     'length', 'data sampling', 'data quality score', 'data clearness score',
                                     'inverter clipping', 'time shift manual'])
        partial_df.loc[0, :] = np.nan
    return p_df, runs_pipeline


if __name__ == '__main__':
    data_source = str(sys.argv[1])
    power_column_id = str(sys.argv[2])
    input_file = str(sys.argv[3])
    output_file = str(sys.argv[4])
    time_shift_inspection = str(sys.argv[5])

    full_df, checked_systems, start_at = resume_run(output_file)

    df_site = pd.read_csv(input_file, index_col=0)
    df_site['site'] = df_site['site'].apply(str)
    df_site['system'] = df_site['system'].apply(str)

    sites, site_system_dict = create_site_system_dict(df_site)

    site_run_time = 0
    total_time = 0

    for site_ix, site_id in enumerate(sites[start_at:]):
        t0 = time()
        msg = 'Site/Accum. run time: {0:2.2f} s/{1:2.2f} m'.format(site_run_time, total_time / 60.0)

        progress(site_ix, len(sites), msg, bar_length=20)
        df = load_data(data_source, site_id)
        dh = DataHandler(df)

        for sys_ix, sys_id in enumerate(site_system_dict[site_id]):
            if sys_id not in checked_systems:
                print(site_id, sys_id)

                sys_tag = get_tag(dh, data_source, power_column_id, sys_id)
                real_longitude = get_lon_from_report(df_site, site_id, sys_id)
                gmt_offset = get_gmt_offset_from_report(df_site, site_id, sys_id)

                if time_shift_inspection:
                    manual_time_shift = get_inspected_time_shift(df_site, sys_id)

                    if manual_time_shift == 1:
                        dh.fix_dst()

                run_failsafe_pipeline(dh, df, sys_tag)
                partial_df, passes_estimation = run_failsafe_lon_estimation(dh)

                partial_df['site'] = site_id
                partial_df['system'] = sys_id
                partial_df['length'] = dh.num_days
                partial_df['data sampling'] = dh.data_sampling
                partial_df['data quality score'] = dh.data_quality_score
                partial_df['data clearness score'] = dh.data_clearness_score
                partial_df['inverter clipping'] = dh.inverter_clipping
                partial_df['runs estimation'] = passes_estimation
                if time_shift_inspection:
                    partial_df['time shift manual'] = manual_time_shift
                full_df = full_df.append(partial_df)
                full_df.index = np.arange(len(full_df))
                full_df.to_csv(output_file)

        t1 = time()
        site_run_time = t1 - t0
        total_time += site_run_time

msg = 'Site/Accum. run time: {0:2.2f} s/{1:2.2f} m'. \
    format(site_run_time, total_time / 60.0)
print('done')
progress(len(sites), len(sites), msg, bar_length=20)
