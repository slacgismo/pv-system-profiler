from pvsystemprofiler.utilities.angle_of_incidence_function import func_costheta
import numpy as np


def select_function(lat_precalc, tilt_precalc, azim_precalc):
    b_dict = {'phi': [-1.57, 1.57], 'beta': [0, 1.57], 'gamma': [-3.14, 3.14]}

    if lat_precalc is None and tilt_precalc is None and azim_precalc is None:
        func = lambda x, phi, beta, gamma: func_costheta(x, phi, beta, gamma)
        bounds = ([b_dict['phi'][0], b_dict['beta'][0], b_dict['gamma'][0]], [b_dict['phi'][1],
                                                                              b_dict['beta'][1], b_dict['gamma'][1]])
        init_values = [10, 10, 10]
        dict_keys = ['latitude_estimate', 'tilt_estimate', 'azimuth_estimate']

    if lat_precalc is not None and tilt_precalc is None and azim_precalc is None:
        func = lambda x, beta, gamma: func_costheta(x, np.deg2rad(lat_precalc), beta, gamma)
        bounds = ([b_dict['beta'][0], b_dict['gamma'][0]], [b_dict['beta'][1], b_dict['gamma'][1]])
        init_values = [10, 10]
        dict_keys = ['tilt_estimate', 'azimuth_estimate']

    return func, bounds, init_values, dict_keys

