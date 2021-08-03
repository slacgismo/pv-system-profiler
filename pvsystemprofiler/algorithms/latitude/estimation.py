import numpy as np
from pvsystemprofiler.algorithms.latitude.calculation import calc_lat


def estimate_latitude(hours_daylight, delta):
    latitude_estimate = calc_lat(hours_daylight, delta)
    return np.nanmedian(latitude_estimate)

