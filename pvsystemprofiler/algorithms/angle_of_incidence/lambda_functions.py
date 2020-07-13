"""
This module is used to set the hour_angle_equation in terms of the unknowns. The hour equation is a function of the
declination (delta), the hour angle (omega) , latitude (phi), tilt (beta) and azimuth (gamma). The declination and the
hour angle are treated as input parameters for all cases. Latitude, tilt and azimuth can be given as input parameters
(precalculates) or left as unknowns. In total, seven different combinations arise from having these three parameters
as an inputs or as a unknowns. The seven conditionals below correspond to those combinations. The output function "func"
is used as one of the inputs to run_curve_fit which in turn is used to fit the unknowns.
"""
from pvsystemprofiler.utilities.angle_of_incidence_function import func_costheta
import numpy as np

def select_function(lat_precalc=None, tilt_precalc=None, azim_precalc=None):
    '''
    :param lat_precalc: (optional) Latitude precalculate in degrees.
    :param tilt_precalc: (optional) Tilt precalculate in degrees.
    :param azim_precalc: (optional) Azimuth precalculate in degrees.
    :return: Customized function 'func', 'bounds' tuple and 'init_values' array used by run_curve_fit as
             input. Array dict_keys containing the parameters left as variables.
    '''

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

