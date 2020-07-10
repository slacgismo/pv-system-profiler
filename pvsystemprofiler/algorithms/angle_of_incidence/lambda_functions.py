from pvsystemprofiler.utilities.angle_of_incidence_function import func_costheta
import numpy as np


def select_function(lat_precalc, tilt_precalc, azim_precalc):
    if lat_precalc is None and tilt_precalc is None and azim_precalc is None:
        func = lambda x, phi, beta, gamma: func_costheta(x, phi, beta, gamma)
        dict_keys = ['latitude_estimate', 'tilt_estimate', 'azimuth_estimate']

    if lat_precalc is not None and tilt_precalc is None and azim_precalc is None:
        func = lambda x, beta, gamma: func_costheta(x, np.deg2rad(lat_precalc), beta, gamma)
        dict_keys = ['tilt_estimate', 'azimuth_estimate']

    if lat_precalc is None and tilt_precalc is not None and azim_precalc is None:
        func = lambda x, phi, gamma: func_costheta(x, phi, np.deg2rad(tilt_precalc), gamma)
        dict_keys = ['latitude_estimate', 'azimuth_estimate']

    if lat_precalc is None and tilt_precalc is None and azim_precalc is not None:
        func = lambda x, phi, beta: func_costheta(x, phi, beta, np.deg2rad(azim_precalc))
        dict_keys = ['latitude_estimate', 'tilt_estimate']

    if lat_precalc is None and tilt_precalc is not None and azim_precalc is not None:
        func = lambda x, phi: func_costheta(x, phi, np.deg2rad(tilt_precalc), np.deg2rad(azim_precalc))
        dict_keys = ['latitude_estimate']

    if lat_precalc is not None and tilt_precalc is None and azim_precalc is not None:
        func = lambda x, beta: func_costheta(x, np.deg2rad(lat_precalc), beta, np.deg2rad(azim_precalc))
        dict_keys = ['tilt_estimate']

    if lat_precalc is not None and tilt_precalc is not None and azim_precalc is None:
        func = lambda x, gamma: func_costheta(x, np.deg2rad(lat_precalc), np.deg2rad(tilt_precalc), gamma)
        dict_keys = ['azimuth_estimate']

    #latitude -90 to 90
    #tilt 0 to 180
    #azimuth -180 to 180

    bounds_dict = {'latitude': [-1.57, 1.57], 'tilt': [0, 1.57], 'azimuth': [-3.14, 3.14]}
    bounds = []
    for el in dict_keys:
        g_param = el.split('_')[0]
        bounds.append(bounds_dict[g_param])
    bounds = tuple(np.transpose(bounds).tolist())

    init_values = 10 * np.ones(len(dict_keys))

    return func, bounds, init_values, dict_keys

