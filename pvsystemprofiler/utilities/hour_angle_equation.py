"""Omega, the hour angle is estimated as defined on p. 13 in:
       Duffie, John A., and William A. Beckman. Solar engineering of thermal
       processes. New York: Wiley, 1991."""

import numpy as np
from pvsystemprofiler.utilities.time_convert import clock_to_solar

def find_omega(data_sampling,num_days, lon, doy, gmt_offset):

        hours_day = np.arange(0, 1440, data_sampling)
        hours_doy = np.tile(hours_day.reshape(-1, 1), (1, num_days))
        hours_doy_solar = clock_to_solar(hours_doy, lon, doy, gmt_offset, eot='duffie')
        hours_doy_solar = hours_doy_solar / 60
        omega = np.deg2rad(15 * (hours_doy_solar - 12))
        return omega
