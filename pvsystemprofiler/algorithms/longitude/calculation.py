import numpy as np
from pvsystemprofiler.algorithms.longitude.direct_calculation import calc_lon


def calculate_longitude(eot, solarnoon, days, gmt_offset):
    sn = 60 * solarnoon[days]  # convert hours to minutes
    eot_days = eot[days]
    estimates = calc_lon(sn, eot_days, gmt_offset)
    return np.nanmedian(estimates)
