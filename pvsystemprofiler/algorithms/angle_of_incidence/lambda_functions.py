from pvsystemprofiler.utilities.angle_of_incidence_function import func_costheta
import numpy as np


def select_function(lat_precalc, tilt_precalc, azim_precalc):
    b_dict = {'phi': [-1.57, 1.57], 'beta': [0, 1.57], 'gamma': [-3.14, 3.14]}

    if lat_precalc is None and tilt_precalc is None and azim_precalc is None:
        func = lambda x, phi, beta, gamma: func_costheta(x, phi, beta, gamma)
        bounds = ([b_dict['phi'][0], b_dict['beta'][0], b_dict['gamma'][0]],
                  [b_dict['phi'][1], b_dict['beta'][1], b_dict['gamma'][1]])
        dict_keys = ['latitude_estimate', 'tilt_estimate', 'azimuth_estimate']

    if lat_precalc is not None and tilt_precalc is None and azim_precalc is None:
        func = lambda x, beta, gamma: func_costheta(x, np.deg2rad(lat_precalc), beta, gamma)
        bounds = ([b_dict['beta'][0], b_dict['gamma'][0]],
                  [b_dict['beta'][1], b_dict['gamma'][1]])
        dict_keys = ['tilt_estimate', 'azimuth_estimate']

    if lat_precalc is None and tilt_precalc is not None and azim_precalc is None:
        func = lambda x, phi, gamma: func_costheta(x, phi, np.deg2rad(tilt_precalc), gamma)
        bounds = ([b_dict['phi'][0], b_dict['gamma'][0]],
                  [b_dict['phi'][1], b_dict['gamma'][1]])
        dict_keys = ['latitude_estimate', 'azimuth_estimate']

    if lat_precalc is None and tilt_precalc is None and azim_precalc is not None:
        func = lambda x, phi, beta: func_costheta(x, phi, beta, np.deg2rad(azim_precalc))
        bounds = ([b_dict['phi'][0], b_dict['beta'][0]],
                  [b_dict['phi'][1], b_dict['beta'][1]])
        dict_keys = ['latitude_estimate', 'tilt_estimate']

    if lat_precalc is None and tilt_precalc is not None and azim_precalc is not None:
        func = lambda x, phi: func_costheta(x, phi, np.deg2rad(tilt_precalc), np.deg2rad(azim_precalc))
        bounds = ([b_dict['phi'][0]], [b_dict['phi'][1]])
        dict_keys = ['latitude_estimate']

    if lat_precalc is not None and tilt_precalc is None and azim_precalc is not None:
        func = lambda x, beta: func_costheta(x, np.deg2rad(lat_precalc), beta, np.deg2rad(azim_precalc))
        bounds = ([b_dict['beta'][0]], [b_dict['beta'][1]])
        dict_keys = ['tilt_estimate']

    if lat_precalc is not None and tilt_precalc is not None and azim_precalc is None:
        func = lambda x, gamma: func_costheta(x, np.deg2rad(lat_precalc), np.deg2rad(tilt_precalc),  gamma)
        bounds = ([b_dict['gamma'][0]], [b_dict['gamma'][1]])
        dict_keys = ['azimuth_estimate']

    init_values = 10*np.ones(len(dict_keys))

    return func, bounds, init_values, dict_keys

