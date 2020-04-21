''' Utilities Module
This module contains data preprocessing functions used by other modules.
'''
from sys import path
#path.append('..')
path.append('/Users/elpiniki/Documents/StatisticalClearSky-master')
path.append('..')
from solardatatools import make_time_series, standardize_time_axis, make_2d, plot_2d
#from statistical_clear_sky.utilities.data_conversion import make_time_series
from cassandra.cluster import Cluster
import pandas as pd
import numpy as np
import s3fs
from os.path import expanduser

home = expanduser('~')
path.append(home + '/Documents/github/pv-system-profiler')


TZ_LOOKUP = {
    'America/Anchorage': 9,
    'America/Chicago': 6,
    'America/Denver': 7,
    'America/Los_Angeles': 8,
    'America/New_York': 5,
    'America/Phoenix': 7,
    'Pacific/Honolulu': 10
}

def get_credentials():
    """
    This function gets credentials for service client connection with AWS.
    param: not applicable
    return: access key and secret access key
    """
    with open(home + '/.aws/credentials') as f:
        lns = f.readlines()
    my_dict = {l.split(' = ')[0]: l.split(' = ')[1][:-1] for l in lns if len(l.split(' = ')) == 2 }
    return my_dict['aws_access_key_id'], my_dict['aws_secret_access_key']

#initial site TAEAC1031314 with shading
def sunpower_index_load(site_id):
    with open('/Users/elpiniki/.aws/cassandra_cluster') as f:
        cluster_ip = f.readline().strip('\n')
        cluster = Cluster([cluster_ip])
        session = cluster.connect('measurements')

        cql = """
            select site, meas_name, ts, sensor, meas_val_f
            from measurement_raw
            where site in ('{}')
                and meas_name = 'ac_power';
        """.format(site_id)
        rows = session.execute(cql)
        df = pd.DataFrame(list(rows), )
        df.replace(-999999.0, np.NaN, inplace=True)
        df_ts, info = make_time_series(df, return_keys=True, localize_time=-8, timestamp_key='ts',
                        value_key='meas_val_f', name_key='meas_name',
                        groupby_keys=['site', 'sensor'])
        df_standardized = standardize_time_axis(df_ts)

    return df_standardized.index


def sunpower_data_load(site_id):
    with open('/Users/elpiniki/.aws/cassandra_cluster') as f:
        cluster_ip = f.readline().strip('\n')
        cluster = Cluster([cluster_ip])
        session = cluster.connect('measurements')

        cql = """
            select site, meas_name, ts, sensor, meas_val_f
            from measurement_raw
            where site in ('{}')
                and meas_name = 'ac_power';
        """.format(site_id)
        rows = session.execute(cql)
        df = pd.DataFrame(list(rows), )
        df.replace(-999999.0, np.NaN, inplace=True)
        df_ts, info = make_time_series(df, return_keys=True, localize_time=-8, timestamp_key='ts',
                        value_key='meas_val_f', name_key='meas_name',
                        groupby_keys=['site', 'sensor'])
        df_standardized = standardize_time_axis(df_ts)
        key_string = info[0][1]
        power_matrix = make_2d(df_standardized,
                    key=key_string,
                    zero_nighttime=True,
                    interp_missing=True)

        #df = df.drop(['ac_power_01'], axis=1)
    print ("SunPower data loaded")
    return power_matrix, df_standardized.index

def nrel_data_load(n, local=True):
    if local:
        base = '/Users/elpiniki/Documents/StatisticalClearSky-master/data/PVO/'
    if not local:
        base = 's3://pvinsight.nrel/PVO/'
    #meta = pd.read_csv('/Users/elpiniki/Documents/github/pv-system-profiler/data/PVO/sys_meta.csv')
    meta = pd.read_csv('/Documents/github/pv-system-profiler/data/PVO/sys_meta.csv')
    id = meta['ID'][n]
    df = pd.read_csv(base+'PVOutput/{}.csv'.format(id), index_col=0,
                      parse_dates=[0], usecols=[1, 3])
    tz = meta['TimeZone'][n]
    df.index = df.index.tz_localize(tz).tz_convert('Etc/GMT+{}'.format(TZ_LOOKUP[tz]))   # fix daylight savings
    start = df.index[1]
    end = df.index[-2]
    time_index = pd.date_range(start=start, end=end, freq='5min')
    df = df.reindex(index=time_index, fill_value=0)
    print ("NREL data loaded")
    return df

#datanrel = nrel_data_load(101, local=False)

def data_preprocess(data):
    days = data.resample('D').max().index[1:-1]
    start = days[1].date()
    end = days[-2].date()
    delta = end - start
    #print(start.date(), end.date())
    #print(delta.days)
    power_signals_d = data.loc[start:end].iloc[:-1].values.reshape(288, -1, order='F') #for 5 min sample time
    return(power_signals_d, delta.days, start)
    #return()

def nrel_meta_data(n):
    meta = pd.read_csv('/Documents/github/pv-system-profiler/data/PVO/sys_meta.csv')
    id = meta['ID'][n]
    tz = meta['TimeZone'][n]
    real_longitude = meta['Longitude'][n]
    return(tz, real_longitude)

def sunpower_meta_data(site_id):
    meta = pd.read_csv(home + '/Documents/github/pv-system-profiler/data/SunPower/deviceMetaData_20171108_LatLon.csv')
    tz = 'America/Los_Angeles'
    meta_site = meta.loc[meta["serialNumber"]== site_id]
    real_longitude = meta_site['lon'].values[0]
    return(tz, real_longitude)
#data_preprocess(datanrel)
#data_preprocess(datasunpower)
