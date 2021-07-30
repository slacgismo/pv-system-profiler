import numpy as np
from pvsystemprofiler.algorithms.longitude.direct_calculation import calc_lon


def calculate_longitude(eot, solarnoon, days, gmt_offset):
    sn = 60 * solarnoon[days]  # convert hours to minutes
    estimates = calc_lon(sn, eot, gmt_offset)
    return np.nanmedian(estimates)
