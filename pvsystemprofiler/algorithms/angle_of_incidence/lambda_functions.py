from pvsystemprofiler.utilities.angle_of_incidence_function import func_costheta
import numpy as np


def select_function(lat_precalc, tilt_precalc, azim_precalc):
    if lat_precalc is not None:
        if tilt_precalc is None:
            if azim_precalc is None:
                func = lambda x, beta, gamma: func_costheta(x, np.deg2rad(lat_precalc), beta, gamma)
    return func
